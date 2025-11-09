import argparse
import os
import glob
import pprint
from datetime import datetime
import pyspark
import pyspark.sql.functions as F
import utils.data_processing_gold_table as gold_utils


def main(snapshotdate):
    print('\n\n--- Starting Gold Label Store Job ---\n\n')

    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("gold_label_store") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    date_str = snapshotdate
    print(f"📅 Running for snapshot date: {date_str}")

    # ---------------------------------------------------------------------
    # Directories
    # ---------------------------------------------------------------------
    silver_clks_directory = "datamart/silver/clks/"
    silver_fin_directory = "datamart/silver/fin/"
    silver_lms_directory = "datamart/silver/lms/"

    gold_clks_directory = "datamart/gold/feature_store/eng/"
    gold_fin_directory = "datamart/gold/feature_store/cust_fin_risk/"
    gold_label_store_directory = "datamart/gold/label_store/"

    os.makedirs(gold_clks_directory, exist_ok=True)
    os.makedirs(gold_fin_directory, exist_ok=True)
    os.makedirs(gold_label_store_directory, exist_ok=True)

    # ---------------------------------------------------------------------
    # Feature Store: Engagement
    # ---------------------------------------------------------------------
    print("\n🟩 Generating engagement feature store ...")
    eng_df = gold_utils.process_fts_gold_engag_table(
        date_str, silver_clks_directory, gold_clks_directory, spark
    )
    if eng_df is None:
        print("⚠️ Engagement feature store skipped (no upstream data)")

    # ---------------------------------------------------------------------
    # Feature Store: Financial Risk
    # ---------------------------------------------------------------------
    print("\n🟩 Generating financial feature store ...")
    fin_df = gold_utils.process_fts_gold_cust_risk_table(
        date_str, silver_fin_directory, gold_fin_directory, spark
    )
    if fin_df is None:
        print("⚠️ Financial feature store skipped (no upstream data)")

    # ---------------------------------------------------------------------
    # Label Store (Aligned with Feature Store)
    # ---------------------------------------------------------------------
    print("\n🟨 Generating label store ...")
    label_df = gold_utils.process_labels_gold_table(
        date_str,
        silver_lms_directory,
        gold_label_store_directory,
        spark,
        dpd=30,
        mob=6,
    )

    if label_df is None:
        print("⚠️ Label store generation skipped — missing inputs or empty filters.\n")
    else:
        print("✅ Label store generation completed successfully.\n")

    # ---------------------------------------------------------------------
    # Optional: Inspect outputs
    # ---------------------------------------------------------------------
    def show_if_exists(path: str, name: str):
        if not os.path.exists(path):
            print(f"⚠️ {name} folder not found: {path}")
            return
        files = glob.glob(os.path.join(path, "*.parquet"))
        if not files:
            print(f"⚠️ No parquet files in {name} folder: {path}")
            return
        df = spark.read.parquet(*files)
        print(f"📊 {name} ({len(files)} file(s)): {df.count()} rows")
        df.show(5, truncate=False)

    print("\n--- Output inspection summary ---")
    show_if_exists(gold_clks_directory, "Engagement Features")
    show_if_exists(gold_fin_directory, "Financial Features")
    show_if_exists(gold_label_store_directory, "Label Store")

    spark.stop()
    print("\n🎉 Completed Gold Label Store Job\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Gold Label Store generation pipeline")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    main(args.snapshotdate)
