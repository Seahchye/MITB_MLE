#!/usr/bin/env python3
import argparse
import os
import pandas as pd
import numpy as np
import pprint
from sklearn.metrics import roc_auc_score, f1_score

def main(snapshotdate, enddate):
    print("\n\n--- Starting Model Monitoring ---\n")

    # --- setup config ---
    config = {}
    config["snapshot_date_str"] = snapshotdate
    config["model_date_str"] = enddate
    config["model_name"] = f"credit_model_{enddate.replace('-', '_')}"
    config["pred_path"] = f"datamart/gold/model_predictions/{config['model_name']}/"
    config["label_path"] = "datamart/gold/label_store/"
    config["monitor_output_path"] = "datamart/gold/model_monitor/"
    pprint.pprint(config)

    # --- load prediction parquet ---
    pred_file_pattern = f"{config['model_name']}_predictions_{snapshotdate.replace('-', '_')}.parquet"
    pred_file = os.path.join(config["pred_path"], pred_file_pattern)

    if not os.path.exists(pred_file):
        print(f"⚠️  No prediction file found for snapshot={snapshotdate}. Skipping monitoring step.")
        print(f"   Expected file: {pred_file}")
        print("\n--- Model Monitoring Skipped ---\n")
        return

    preds = pd.read_parquet(pred_file)
    print(f"✅ Loaded predictions: {preds.shape[0]} rows")

    # --- load label parquet ---
    label_files = [f for f in os.listdir(config["label_path"]) if f.endswith(".parquet")]
    if not label_files:
        raise FileNotFoundError("❌ No label store parquet files found.")

    label_file = max(
        [os.path.join(config["label_path"], f) for f in label_files],
        key=os.path.getmtime
    )
    labels = pd.read_parquet(label_file)
    print(f"✅ Loaded label store: {labels.shape[0]} rows")

    # --- merge and compute metrics ---
    merged = preds.merge(labels, on=["Customer_ID", "snapshot_date"], how="inner")
    if merged.empty:
        raise ValueError("❌ No overlapping records found between predictions and labels.")

    auc = roc_auc_score(merged["label"], merged["model_predictions"])
    f1 = f1_score(merged["label"], (merged["model_predictions"] > 0.5).astype(int))
    print(f"📊 AUC={auc:.4f}, F1={f1:.4f}")

    # --- save monitoring result ---
    os.makedirs(config["monitor_output_path"], exist_ok=True)

    result_file = os.path.join(
        config["monitor_output_path"],
        f"monitor_results_{config['snapshot_date_str'].replace('-', '_')}.csv"
    )

    pd.DataFrame({
        "snapshot_date": [config["snapshot_date_str"]],
        "model_name": [config["model_name"]],
        "auc": [auc],
        "f1_score": [f1],
        "records": [len(merged)]
    }).to_csv(result_file, index=False)

    print(f"✅ Metrics saved to {result_file}")

    print("\n--- Model Monitoring Completed ---\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Model monitoring script")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD (inference date)")
    parser.add_argument("--enddate", type=str, required=True, help="YYYY-MM-DD (model date)")
    args = parser.parse_args()
    main(args.snapshotdate, args.enddate)
