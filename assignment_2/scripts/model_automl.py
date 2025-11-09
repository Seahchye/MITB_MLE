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
import glob


# ==========================================================
# 🔹 Feature and Label Loading Utilities
# ==========================================================
def load_feature_folder(folder_path: str, snapshot_cutoff: datetime) -> pd.DataFrame:
    """Load all parquet files in a folder up to the cutoff snapshot date."""
    files = sorted(glob.glob(os.path.join(folder_path, "*.parquet")))
    if not files:
        print(f"⚠️ No parquet files found in {folder_path}")
        return pd.DataFrame()

    valid_files = []
    for f in files:
        try:
            date_str = "_".join(os.path.basename(f).split("_")[-3:]).replace(".parquet", "")
            file_date = datetime.strptime(date_str, "%Y_%m_%d")
            if file_date <= snapshot_cutoff:
                valid_files.append(f)
        except Exception:
            valid_files.append(f)

    if not valid_files:
        print(f"⚠️ No feature files ≤ {snapshot_cutoff.date()} found in {folder_path}")
        return pd.DataFrame()

    try:
        df = pd.concat([pd.read_parquet(f) for f in valid_files], ignore_index=True)
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
        df["Customer_ID"] = df["Customer_ID"].astype(str).str.strip()
        df = df[df["snapshot_date"] <= snapshot_cutoff]
        print(f"✅ Loaded {len(df):,} rows from {folder_path}")
        return df
    except Exception as e:
        print(f"⚠️ Failed to load parquet files from {folder_path}: {e}")
        return pd.DataFrame()


def load_unified_features(snapshotdate: str) -> pd.DataFrame:
    """Merge both feature folders (cust_fin_risk + eng) up to snapshotdate."""
    snapshot_cutoff = datetime.strptime(snapshotdate, "%Y-%m-%d")
    base_path = "datamart/gold/feature_store"

    fin_df = load_feature_folder(os.path.join(base_path, "cust_fin_risk"), snapshot_cutoff)
    eng_df = load_feature_folder(os.path.join(base_path, "eng"), snapshot_cutoff)

    if fin_df.empty or eng_df.empty:
        print("⚠️ One or both feature groups are empty.")
        return pd.DataFrame()

    # find overlapping months
    fin_months = set(fin_df["snapshot_date"].dt.to_period("M").astype(str))
    eng_months = set(eng_df["snapshot_date"].dt.to_period("M").astype(str))
    common_months = sorted(fin_months & eng_months)

    if not common_months:
        print("⚠️ No overlapping months between feature folders.")
        print("cust_fin_risk months:", sorted(fin_months))
        print("eng months:", sorted(eng_months))
        return pd.DataFrame()

    fin_df = fin_df[fin_df["snapshot_date"].dt.to_period("M").astype(str).isin(common_months)]
    eng_df = eng_df[eng_df["snapshot_date"].dt.to_period("M").astype(str).isin(common_months)]

    unified_df = fin_df.merge(
        eng_df,
        on=["Customer_ID", "snapshot_date"],
        how="inner",
        suffixes=("_fin", "_eng")
    )

    print(f"✅ Unified features up to {snapshot_cutoff.date()}: {len(unified_df):,} rows, {len(unified_df.columns)} cols")
    return unified_df


def load_labels(snapshot_cutoff: datetime, label_store_path="datamart/gold/label_store") -> pd.DataFrame:
    """Load the most recent label parquet ≤ snapshot_cutoff."""
    label_files = sorted(glob.glob(os.path.join(label_store_path, "*.parquet")))
    if not label_files:
        print(f"⚠️ No label files found in {label_store_path}")
        return pd.DataFrame()

    valid_files = []
    for f in label_files:
        try:
            date_str = "_".join(os.path.basename(f).split("_")[-3:]).replace(".parquet", "")
            file_date = datetime.strptime(date_str, "%Y_%m_%d")
            if file_date <= snapshot_cutoff:
                valid_files.append(f)
        except Exception:
            valid_files.append(f)

    if not valid_files:
        print(f"⚠️ No label files ≤ {snapshot_cutoff.date()} found.")
        return pd.DataFrame()

    latest_file = sorted(valid_files)[-1]
    print(f"✅ Using label store: {os.path.basename(latest_file)}")

    try:
        df = pd.read_parquet(latest_file)
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
        df["Customer_ID"] = df["Customer_ID"].astype(str).str.strip()
        return df
    except Exception as e:
        print(f"⚠️ Failed to read label parquet {latest_file}: {e}")
        return pd.DataFrame()


def build_training_dataset(snapshotdate: str) -> pd.DataFrame:
    """Create a unified training dataset (features + labels) up to snapshotdate."""
    snapshot_cutoff = datetime.strptime(snapshotdate, "%Y-%m-%d")

    features_df = load_unified_features(snapshotdate)
    if features_df.empty:
        print("⚠️ Skipping: no features available.")
        return pd.DataFrame()

    labels_df = load_labels(snapshot_cutoff)
    if labels_df.empty:
        print("⚠️ Skipping: no labels available.")
        return pd.DataFrame()

    feat_months = set(features_df["snapshot_date"].dt.to_period("M").astype(str))
    label_months = set(labels_df["snapshot_date"].dt.to_period("M").astype(str))
    common_months = sorted(feat_months & label_months)

    if not common_months:
        print("⚠️ No overlapping months between features and labels.")
        print("Feature months:", sorted(feat_months))
        print("Label months:", sorted(label_months))
        return pd.DataFrame()

    features_df = features_df[features_df["snapshot_date"].dt.to_period("M").astype(str).isin(common_months)]
    labels_df = labels_df[labels_df["snapshot_date"].dt.to_period("M").astype(str).isin(common_months)]

    print("Feature snapshot_date unique:", features_df["snapshot_date"].sort_values().unique()[:10])
    print("Label snapshot_date unique:", labels_df["snapshot_date"].sort_values().unique()[:10])

    features_df["Customer_ID"] = features_df["Customer_ID"].astype(str).str.strip().str.upper()
    labels_df["Customer_ID"] = labels_df["Customer_ID"].astype(str).str.strip().str.upper()

    print("Feature Customer_ID sample:", features_df["Customer_ID"].head().tolist())
    print("Label Customer_ID sample:", labels_df["Customer_ID"].head().tolist())

    merged = pd.merge(
        features_df[["Customer_ID","snapshot_date"]],
        labels_df[["Customer_ID","snapshot_date"]],
        on=["Customer_ID","snapshot_date"],
        how="inner"
    )
    print("Overlapping keys:", len(merged))

    train_df = features_df.merge(labels_df, on=["Customer_ID", "snapshot_date"], how="inner")
    print(f"✅ Training dataset ready: {len(train_df):,} rows × {len(train_df.columns)} cols")
    return train_df


# ==========================================================
# 🔹 XGBoost AutoML Objective
# ==========================================================
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


# ==========================================================
# 🔹 Main AutoML Logic
# ==========================================================
def main(snapshotdate, n_trials):
    print("\n\n--- Starting AutoML Retraining ---\n")

    # --- setup config ---
    config = {}
    config["snapshot_date_str"] = snapshotdate
    config["model_train_date_str"] = "2024-06-01"
    config["model_name"] = f"credit_model_{config['model_train_date_str'].replace('-', '_')}_automl"
    config["model_bank_directory"] = "model_bank/"
    pprint.pprint(config)




    # --- build full dataset ---
    train_df = build_training_dataset(snapshotdate)
    if train_df.empty:
        print("⚠️ No valid training data found. Skipping AutoML retraining.")
        return

    # --- prepare X, y ---
    drop_cols = ["Customer_ID", "snapshot_date", "label_def"]
    if "label" not in train_df.columns:
        print("⚠️ Label column missing in dataset.")
        return

    X = train_df.drop(columns=[c for c in drop_cols if c in train_df.columns])
    y = train_df["label"]

    if len(set(y)) < 2 or len(train_df) < 50:
        print(f"⚠️ Insufficient training data (rows={len(train_df)}, unique_labels={len(set(y))}). Skipping retraining.")
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


# ==========================================================
# 🔹 Entry Point
# ==========================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AutoML retraining with Optuna")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--n_trials", type=int, default=20, help="Number of Optuna trials")
    args = parser.parse_args()

    try:
        main(args.snapshotdate, args.n_trials)
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
        sys.exit(0)
