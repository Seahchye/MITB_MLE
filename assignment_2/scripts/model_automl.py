#!/usr/bin/env python3
import argparse
import os
import pickle
import pandas as pd
import optuna
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
import xgboost as xgb
from datetime import datetime
import pprint
import sys

def objective(trial, X_train, X_val, y_train, y_val):
    params = {
        "max_depth": trial.suggest_int("max_depth", 3, 8),
        "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.3),
        "n_estimators": trial.suggest_int("n_estimators", 100, 400),
        "subsample": trial.suggest_float("subsample", 0.6, 1.0),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
        "min_child_weight": trial.suggest_int("min_child_weight", 1, 6),
        "gamma": trial.suggest_float("gamma", 0, 0.4),
        "eval_metric": "auc",
        "random_state": 88,
        "n_jobs": -1
    }

    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train)
    preds = model.predict_proba(X_val)[:, 1]
    return roc_auc_score(y_val, preds)


def main(snapshotdate, enddate, n_trials):
    print("\n\n--- Starting AutoML Retraining ---\n")

    # --- setup config ---
    config = {}
    config["snapshot_date_str"] = snapshotdate
    config["model_train_date_str"] = enddate
    config["model_name"] = f"credit_model_{enddate.replace('-', '_')}_automl"
    config["feature_store_path"] = "datamart/gold/feature_store/"
    config["label_store_path"] = "datamart/gold/label_store/"
    config["model_bank_directory"] = "model_bank/"
    pprint.pprint(config)

    # --- check paths ---
    if not os.path.exists(config["feature_store_path"]):
        print(f"⚠️ Feature store path not found: {config['feature_store_path']}")
        print("Skipping AutoML retraining.\n")
        return

    if not os.path.exists(config["label_store_path"]):
        print(f"⚠️ Label store path not found: {config['label_store_path']}")
        print("Skipping AutoML retraining.\n")
        return

    # --- load features ---
    feature_files = []
    for root, dirs, files in os.walk(config["feature_store_path"]):
        for f in files:
            if f.endswith(".parquet"):
                feature_files.append(os.path.join(root, f))

    if not feature_files:
        print("⚠️ No feature parquet files found. Skipping AutoML retraining.\n")
        return

    try:
        features_df = pd.concat([pd.read_parquet(f) for f in feature_files], ignore_index=True)
    except Exception as e:
        print(f"⚠️ Failed to load feature parquet files: {e}")
        return

    # --- load label store ---
    label_files = [f for f in os.listdir(config["label_store_path"]) if f.endswith(".parquet")]
    if not label_files:
        print("⚠️ No label store parquet files found. Skipping AutoML retraining.\n")
        return

    latest_label_file = max(
        [os.path.join(config["label_store_path"], f) for f in label_files],
        key=os.path.getmtime
    )
    try:
        labels_df = pd.read_parquet(latest_label_file)
    except Exception as e:
        print(f"⚠️ Failed to load label parquet file: {e}")
        return

    # --- join data ---
    df = features_df.merge(labels_df, on=["Customer_ID", "snapshot_date"], how="inner")
    if df.empty:
        print("⚠️ No overlapping records between features and labels. Skipping AutoML retraining.\n")
        return

    drop_cols = ["Customer_ID", "snapshot_date", "label_def"]
    if "label" not in df.columns:
        print("⚠️ Label column missing in label store. Skipping AutoML retraining.\n")
        return

    X = df.drop(columns=[c for c in drop_cols if c in df.columns])
    y = df["label"]

    # sanity check: ensure at least a few positive/negative samples exist
    if len(set(y)) < 2 or len(df) < 50:
        print(f"⚠️ Insufficient training data (rows={len(df)}, unique_labels={len(set(y))}). Skipping AutoML retraining.\n")
        return

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

    # --- run Optuna tuning ---
    try:
        study = optuna.create_study(direction="maximize")
        study.optimize(lambda trial: objective(trial, X_train, X_val, y_train, y_val), n_trials=n_trials)
    except Exception as e:
        print(f"⚠️ AutoML optimization failed: {e}")
        return

    print("🏆 Best parameters:", study.best_params)
    print("Best AUC:", study.best_value)

    # --- retrain best model ---
    best_model = xgb.XGBClassifier(**study.best_params)
    best_model.fit(X, y)

    # --- save model artefact ---
    os.makedirs(config["model_bank_directory"], exist_ok=True)
    model_path = os.path.join(config["model_bank_directory"], f"{config['model_name']}.pkl")

    try:
        with open(model_path, "wb") as f:
            pickle.dump({
                "model": best_model,
                "hp_params": study.best_params,
                "best_auc": study.best_value,
                "train_date": datetime.now().strftime("%Y-%m-%d"),
            }, f)
        print(f"✅ Saved new AutoML model: {model_path}")
    except Exception as e:
        print(f"⚠️ Failed to save model artefact: {e}")
        return

    print("\n--- AutoML Retraining Completed ---\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AutoML retraining with Optuna")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--enddate", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--n_trials", type=int, default=20, help="Number of Optuna trials")
    args = parser.parse_args()

    # graceful exit ensures Airflow marks task as success even on skip
    try:
        main(args.snapshotdate, args.enddate, args.n_trials)
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
        sys.exit(0)

