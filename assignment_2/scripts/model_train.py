import argparse
import os
import glob
import sys
import time
import logging
import pandas as pd
import pickle
import pprint
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.metrics import make_scorer, roc_auc_score
import xgboost as xgb
from sklearn.metrics import log_loss, average_precision_score
from collections import Counter


# ----------------------------------------------------------------------
# Logging configuration
# ----------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Graceful exit helper for Docker + Airflow
# ----------------------------------------------------------------------
def graceful_exit(spark, reason):
    """
    Cleanly stop Spark in Dockerized Airflow and exit without triggering retries.
    """
    logger.warning(f"⚠️ Skipping model training: {reason}")

    try:
        if spark:
            sc = spark.sparkContext
            sc.stop()
            spark.stop()
            logger.info("✅ SparkSession stopped successfully.")
    except Exception as e:
        logger.error(f"Error stopping SparkSession: {e}")

    # Flush logs before exiting
    sys.stdout.flush()
    sys.stderr.flush()
    time.sleep(2)  # give Spark JVM time to shut down

    logger.info("🪶 Graceful exit complete — task will end successfully.")
    os._exit(0)


# ----------------------------------------------------------------------
# Main entry
# ----------------------------------------------------------------------
def main(snapshotdate):
    logger.info("\n\n--- starting model_train job ---\n")

    # Initialize Spark
    spark = (pyspark.sql.SparkSession.builder
             .appName("model_train_dev")
             .master("local[*]")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.allowMultipleContexts", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------
    train_test_period_months = 10
    oot_period_months = 2
    train_test_ratio = 0.8
    model_train_date = datetime.strptime(snapshotdate, "%Y-%m-%d")

    config = dict(
        model_train_date_str=snapshotdate,
        model_train_date=model_train_date,
        train_test_period_months=train_test_period_months,
        oot_period_months=oot_period_months,
        oot_end_date=model_train_date - timedelta(days=1),
        oot_start_date=model_train_date - relativedelta(months=oot_period_months),
        train_test_end_date=model_train_date - relativedelta(months=oot_period_months) - timedelta(days=1),
        train_test_start_date=model_train_date - relativedelta(months=train_test_period_months + oot_period_months),
        train_test_ratio=train_test_ratio
    )
    pprint.pprint(config)

    threshold = train_test_period_months + oot_period_months

    # ------------------------------------------------------------------
    # Load label store
    # ------------------------------------------------------------------
    folder_path = "datamart/gold/label_store/"
    files_list = glob.glob(os.path.join(folder_path, "*.parquet"))
    if len(files_list) < threshold:
        graceful_exit(spark, f"Label store files ({len(files_list)}) < required ({threshold}).")

    label_store_sdf = spark.read.parquet(*files_list)
    row_count = label_store_sdf.count()
    if row_count < threshold:
        graceful_exit(spark, f"Label store rows ({row_count}) < required ({threshold}).")

    labels_sdf = label_store_sdf.filter(
        (col("snapshot_date") >= config["train_test_start_date"]) &
        (col("snapshot_date") <= config["oot_end_date"])
    )
    if labels_sdf.count() == 0:
        graceful_exit(spark, "Label store returned 0 rows for configured window.")

    # ------------------------------------------------------------------
    # Load financial/risk features
    # ------------------------------------------------------------------
    feature_fin_risk_location = "datamart/gold/feature_store/cust_fin_risk/"
    files_list = glob.glob(os.path.join(feature_fin_risk_location, "*.parquet"))
    if len(files_list) < threshold:
        graceful_exit(spark, f"Financial/risk features files ({len(files_list)}) < required ({threshold}).")

    feature_fin_risk_df = spark.read.parquet(*files_list)
    row_count = feature_fin_risk_df.count()
    if row_count < threshold:
        graceful_exit(spark, f"Financial/risk feature rows ({row_count}) < required ({threshold}).")

    feature_fin_risk_df = feature_fin_risk_df.filter(
        (col("snapshot_date") >= config["train_test_start_date"]) &
        (col("snapshot_date") <= config["oot_end_date"])
    )
    if feature_fin_risk_df.count() == 0:
        graceful_exit(spark, "Financial/risk feature set returned 0 rows for configured window.")

    # ------------------------------------------------------------------
    # Load engineering features
    # ------------------------------------------------------------------
    feature_eng_location = "datamart/gold/feature_store/eng/"
    files_list = glob.glob(os.path.join(feature_eng_location, "*.parquet"))
    if len(files_list) < threshold:
        graceful_exit(spark, f"Engineering features files ({len(files_list)}) < required ({threshold}).")

    feature_eng_df = spark.read.parquet(*files_list)
    row_count = feature_eng_df.count()
    if row_count < threshold:
        graceful_exit(spark, f"Engineering feature rows ({row_count}) < required ({threshold}).")

    feature_eng_df = feature_eng_df.filter(
        (col("snapshot_date") >= config["train_test_start_date"]) &
        (col("snapshot_date") <= config["oot_end_date"])
    )
    if feature_eng_df.count() == 0:
        graceful_exit(spark, "Engineering feature set returned 0 rows for configured window.")

    # ------------------------------------------------------------------
    # Prepare data for modeling
    # ------------------------------------------------------------------
    data_pdf = (labels_sdf
                .join(feature_fin_risk_df, ["Customer_ID", "snapshot_date"], "left")
                .join(feature_eng_df, ["Customer_ID", "snapshot_date"], "left")
                ).toPandas()

    # 1) Ensure snapshot_date is a proper datetime (fixes TypeError)
    data_pdf["snapshot_date"] = pd.to_datetime(data_pdf["snapshot_date"], errors="coerce")
    # If any bad/empty dates slipped through, drop them
    data_pdf = data_pdf.dropna(subset=["snapshot_date"])

    # 2) (Optional but safe) Ensure label is numeric int
    if not pd.api.types.is_integer_dtype(data_pdf["label"]):
        data_pdf["label"] = pd.to_numeric(data_pdf["label"], errors="coerce").fillna(0).astype(int)

    # 3) Build windows using datetime comparisons (NO .date() calls)
    oot_mask = (
        (data_pdf["snapshot_date"] >= config["oot_start_date"]) &
        (data_pdf["snapshot_date"] <= config["oot_end_date"])
    )
    tt_mask = (
        (data_pdf["snapshot_date"] >= config["train_test_start_date"]) &
        (data_pdf["snapshot_date"] <= config["train_test_end_date"])
    )

    oot_pdf = data_pdf.loc[oot_mask].copy()
    train_test_pdf = data_pdf.loc[tt_mask].copy()

    train_counts = Counter(train_test_pdf["label"])
    oot_counts = Counter(oot_pdf["label"])

    logger.info(f"Label distribution in TRAIN/TEST window: {dict(train_counts)}")
    logger.info(f"Label distribution in OOT window: {dict(oot_counts)}")

    # If train/test labels have only one class, exit gracefully
    if train_test_pdf["label"].nunique() < 2:
        graceful_exit(spark, f"Single-class labels in TRAIN/TEST window: {dict(train_counts)}. "
                            f"Fix label store or widen training dates.")
    # If OOT has one class, skip AUC later
    single_class_oot = (oot_pdf["label"].nunique() < 2)

    if train_test_pdf.shape[0] == 0 or oot_pdf.shape[0] == 0:
        graceful_exit(
            spark,
            f"Insufficient samples after date filtering: train={train_test_pdf.shape[0]}, oot={oot_pdf.shape[0]}"
        )

    # ------------------------------------------------------------------
    # Sanity check: label balance
    # ------------------------------------------------------------------
    # Count label distribution
    train_counts = train_test_pdf["label"].value_counts().to_dict()
    oot_counts = oot_pdf["label"].value_counts().to_dict()

    logger.info(f"Label distribution in TRAIN/TEST window: {train_counts}")
    logger.info(f"Label distribution in OOT window: {oot_counts}")

    # Minimum thresholds — adjust to your business tolerance
    MIN_POSITIVE_SAMPLES = 10     # require at least 10 positive labels
    MIN_TOTAL_SAMPLES = 100       # optional, require enough data to train

    if train_counts.get(1, 0) < MIN_POSITIVE_SAMPLES:
        graceful_exit(spark, f"Too few positive labels in training data: {train_counts}")
    if train_test_pdf.shape[0] < MIN_TOTAL_SAMPLES:
        graceful_exit(spark, f"Too few total rows in training data: {train_test_pdf.shape[0]}")


    # 4) Define features
    drop_cols = {"loan_id", "Customer_ID", "label", "label_def", "snapshot_date"}
    feature_cols = [c for c in data_pdf.columns
                    if c not in drop_cols and pd.api.types.is_numeric_dtype(data_pdf[c])]

    X_oot, y_oot = oot_pdf[feature_cols], oot_pdf["label"]
    stratify_arg = train_test_pdf["label"] if train_test_pdf["label"].nunique() > 1 else None
    X_train, X_test, y_train, y_test = train_test_split(
        train_test_pdf[feature_cols],
        train_test_pdf["label"],
        test_size=1 - config["train_test_ratio"],
        random_state=88,
        shuffle=True,
        stratify=stratify_arg
    )

    if X_train.shape[0] == 0 or X_test.shape[0] == 0:
        graceful_exit(spark, "Train/test split produced zero samples.")

    # ------------------------------------------------------------------
    # Model training
    # ------------------------------------------------------------------
    pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler())
    ])
    X_train_processed = pipe.fit_transform(X_train)
    X_test_processed = pipe.transform(X_test)
    X_oot_processed = pipe.transform(X_oot)

    xgb_clf = xgb.XGBClassifier(eval_metric='logloss', random_state=88)
    param_dist = {
        'n_estimators': [25, 50],
        'max_depth': [2, 3],
        'learning_rate': [0.01, 0.1],
        'subsample': [0.6, 0.8],
        'colsample_bytree': [0.6, 0.8],
        'gamma': [0, 0.1],
        'min_child_weight': [1, 3, 5],
        'reg_alpha': [0, 0.1, 1],
        'reg_lambda': [1, 1.5, 2]
    }

    auc_scorer = make_scorer(roc_auc_score, needs_proba=True)
    random_search = RandomizedSearchCV(
        estimator=xgb_clf,
        param_distributions=param_dist,
        scoring=auc_scorer,
        n_iter=20,
        cv=3,
        verbose=1,
        random_state=42,
        n_jobs=-1
    )

    random_search.fit(X_train_processed, y_train)

    # ------------------------------------------------------------------
    # Evaluate + save (robust version)
    # ------------------------------------------------------------------
    

    best_model = random_search.best_estimator_

    train_auc = roc_auc_score(y_train, best_model.predict_proba(X_train_processed)[:, 1])
    test_auc  = roc_auc_score(y_test,  best_model.predict_proba(X_test_processed)[:, 1])

    # --- Safe OOT evaluation ---
    y_oot_proba = best_model.predict_proba(X_oot_processed)[:, 1]
    if len(np.unique(y_oot)) < 2:
        oot_auc = None
        oot_logloss = log_loss(y_oot, y_oot_proba, labels=[0, 1])
        try:
            oot_ap = average_precision_score(y_oot, y_oot_proba)
        except Exception:
            oot_ap = None
        logger.warning(
            f"⚠️ OOT has only one class (counts={dict(pd.Series(y_oot).value_counts())}). "
            f"Skipping OOT AUC. Using log loss instead: {oot_logloss:.4f}"
        )
    else:
        oot_auc = roc_auc_score(y_oot, y_oot_proba)
        oot_logloss = log_loss(y_oot, y_oot_proba, labels=[0, 1])
        oot_ap = average_precision_score(y_oot, y_oot_proba)

    # Log final metrics
    logger.info(
        f"Train AUC={train_auc:.4f}, Test AUC={test_auc:.4f}, "
        + (f"OOT AUC={oot_auc:.4f}, " if oot_auc is not None else "OOT AUC=NA, ")
        + f"OOT LogLoss={oot_logloss:.4f}"
        + (f", OOT AP={oot_ap:.4f}" if oot_ap is not None else "")
    )

    # Save model artefact
    model_artefact = {
        "model": best_model,
        "model_version": "credit_model_" + snapshotdate.replace("-", "_"),
        "preprocessing_transformers": {"stdscaler": pipe},
        "data_dates": config,
        "results": {
            "auc_train": train_auc,
            "auc_test": test_auc,
            "auc_oot": oot_auc,
            "logloss_oot": oot_logloss,
            "ap_oot": oot_ap
        }
    }

    os.makedirs("model_bank", exist_ok=True)
    file_path = os.path.join("model_bank", f"{model_artefact['model_version']}.pkl")
    with open(file_path, "wb") as f:
        pickle.dump(model_artefact, f)

    logger.info(f"✅ Model saved to {file_path}")

    spark.stop()
    logger.info("\n\n--- completed job successfully ---\n")



# ----------------------------------------------------------------------
# Entrypoint
# ----------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    main(args.snapshotdate)
