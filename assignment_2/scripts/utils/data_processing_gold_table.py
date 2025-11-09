import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pprint
import pyspark
import pyspark.sql.functions as F
import argparse

from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType


# --------------------------------------------------------------------
# LABEL STORE (aligned with feature store)
# --------------------------------------------------------------------

def _read_parquet_safe(spark, path, select_cols=None, label:str=""):
    """
    Safely read a parquet dataset. Returns (df_or_None, reason_str_or_None).
    Handles:
      - missing path
      - empty/invalid parquet folder (UNABLE_TO_INFER_SCHEMA)
    """
    if not os.path.exists(path):
        return None, f"missing ({path})"
    try:
        df = spark.read.parquet(path)
        if select_cols:
            df = df.select(*select_cols)
        # trigger a lightweight action to make sure it’s not empty/invalid
        if df.take(1) == []:  # empty after read
            return None, f"empty ({path})"
        return df, None
    except Exception as e:
        return None, f"unreadable ({path}) – {type(e).__name__}: {e}"

def process_labels_gold_table(snapshot_date_str, silver_lms_directory, gold_label_store_directory, spark, dpd, mob):
    """
    Build label store for the given snapshot month and align to feature-store customers
    (financial + engagement) of the SAME snapshot month.
    """
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    # 1) Load monthly loan data (labels source)
    lms_path = os.path.join(
        silver_lms_directory,
        f"silver_loan_mthly_{snapshot_date_str.replace('-', '_')}.parquet"
    )
    loans_df, reason = _read_parquet_safe(spark, lms_path, label="lms")
    if loans_df is None:
        print(f"⚠️ Loan data not available: {reason}. Skipping label store generation.")
        return None

    print(f"✅ Loaded monthly loan data: {loans_df.count()} rows from {lms_path}")

    # 2) Apply business filters and create binary label
    loans_df = (loans_df
        .withColumn("mob", F.col("mob").cast("int"))
        .withColumn("dpd", F.col("dpd").cast("int"))
    )
    loans_df = loans_df.filter((F.col("mob") >= mob) & (F.col("dpd") >= dpd))

    # If EVERYTHING fails the filter, we still produce 0/1 labels for overlap customers
    # but keep the label construction explicit:
    if loans_df.take(1) == []:
        print(f"ℹ️ After filters (mob≥{mob}, dpd≥{dpd}) there are 0 rows; will still align to FS customers with label=0 if needed.")

    labels_df = (loans_df
        .withColumn("label", F.when(F.col("dpd") >= dpd, 1).otherwise(0).cast("int"))
        .select("Customer_ID", "loan_id", "label")
        .distinct()
        .withColumn("snapshot_date", F.lit(snapshot_date_str))
    )
    filtered_count = labels_df.count()
    print(f"✅ Filtered loans by mob≥{mob}, dpd≥{dpd}: {filtered_count} rows")

    # 3) Read SAME-SNAPSHOT feature-store customer universes (financial & engagement)
    fin_fs_path = f"datamart/gold/feature_store/cust_fin_risk/gold_ft_store_cust_fin_risk_{snapshot_date_str.replace('-', '_')}.parquet"
    eng_fs_path = f"datamart/gold/feature_store/eng/gold_ft_store_engagement_{snapshot_date_str.replace('-', '_')}.parquet"

    fin_df, fin_reason = _read_parquet_safe(spark, fin_fs_path, select_cols=["Customer_ID"], label="fin")
    if fin_df is None:
        print(f"⚠️ Financial FS not available: {fin_reason}")

    eng_df, eng_reason = _read_parquet_safe(spark, eng_fs_path, select_cols=["Customer_ID"], label="eng")
    if eng_df is None:
        print(f"⚠️ Engagement FS not available: {eng_reason}")

    if fin_df is None and eng_df is None:
        print("⚠️ No feature-store universe available; skipping label store generation.")
        return None

    fs_customers = fin_df.union(eng_df).distinct() if (fin_df is not None and eng_df is not None) else (fin_df or eng_df)
    fs_cnt = fs_customers.count()
    print(f"✅ Feature-store customer universe (this snapshot): {fs_cnt} customers")

    # 4) Align labels to FS customers. If filtered labels are empty, still create zero labels for FS universe.
    if filtered_count == 0:
        aligned_df = (fs_customers
            .withColumn("loan_id", F.lit(None).cast("string"))
            .withColumn("label", F.lit(0).cast("int"))
            .withColumn("snapshot_date", F.lit(snapshot_date_str))
        )
        print("ℹ️ No rows passed mob/dpd filter; generating label=0 for all FS customers.")
    else:
        before = labels_df.select("Customer_ID").distinct().count()
        aligned_df = labels_df.join(fs_customers, on="Customer_ID", how="inner")
        after = aligned_df.select("Customer_ID").distinct().count()
        print(f"✅ Aligned label customers to FS: {before:,} → {after:,}")

        if after == 0:
            # fallback: zero labels for FS customers
            aligned_df = (fs_customers
                .withColumn("loan_id", F.lit(None).cast("string"))
                .withColumn("label", F.lit(0).cast("int"))
                .withColumn("snapshot_date", F.lit(snapshot_date_str))
            )
            print("ℹ️ No overlapping customers; generating label=0 for FS customers.")

    # 5) Save label store
    os.makedirs(gold_label_store_directory, exist_ok=True)
    out_path = os.path.join(
        gold_label_store_directory,
        f"gold_label_store_{snapshot_date_str.replace('-', '_')}.parquet"
    )
    aligned_df.write.mode("overwrite").parquet(out_path)
    print(f"✅ Saved aligned label store: {out_path} (rows={aligned_df.count()})")

    return aligned_df


# --------------------------------------------------------------------
# FEATURE STORE - ENGAGEMENT
# --------------------------------------------------------------------
def process_fts_gold_engag_table(snapshot_date_str, silver_clks_directory, gold_clks_directory, spark):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    all_dfs = []

    for i in range(1, 7):  # previous 6 months
        month_date = snapshot_date - relativedelta(months=i)
        partition_name = f"silver_clks_mthly_{month_date.strftime('%Y_%m_%d')}.parquet"
        filepath = os.path.join(silver_clks_directory, partition_name)

        try:
            df = spark.read.parquet(filepath)
            print(f"Loaded from: {filepath}, row count: {df.count()}")
            df = df.select('Customer_ID', 'fe_1').withColumn('months_ago', F.lit(i))
            all_dfs.append(df)
        except Exception:
            print(f"⚠️ No data found for {i} months ago")

    if not all_dfs:
        print("⚠️ No engagement data loaded for any of the previous 6 months.")
        return None

    union_df = all_dfs[0]
    for df in all_dfs[1:]:
        union_df = union_df.unionByName(df)

    pivot_df = (
        union_df.groupBy('Customer_ID')
        .pivot('months_ago', [1, 2, 3, 4, 5, 6])
        .agg(F.first('fe_1'))
    )

    for i in range(1, 7):
        if str(i) in pivot_df.columns:
            pivot_df = pivot_df.withColumnRenamed(str(i), f'click_{i}m')
        else:
            pivot_df = pivot_df.withColumn(f'click_{i}m', lit(None).cast("int"))

    pivot_df = pivot_df.withColumn("snapshot_date", F.lit(snapshot_date_str))
    ordered_cols = ['Customer_ID', 'snapshot_date'] + [f'click_{i}m' for i in range(1, 7)]
    df_final = pivot_df.select(ordered_cols)

    os.makedirs(gold_clks_directory, exist_ok=True)
    partition_name = f"gold_ft_store_engagement_{snapshot_date_str.replace('-', '_')}.parquet"
    filepath = os.path.join(gold_clks_directory, partition_name)
    df_final.write.mode("overwrite").parquet(filepath)
    print(f"✅ Saved engagement gold features to: {filepath}")

    return df_final


# --------------------------------------------------------------------
# FEATURE STORE - FINANCIAL RISK
# --------------------------------------------------------------------
def process_fts_gold_cust_risk_table(snapshot_date_str, silver_fin_directory, gold_fin_directory, spark):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    partition_name = f"silver_fin_mthly_{snapshot_date_str.replace('-', '_')}.parquet"
    filepath = os.path.join(silver_fin_directory, partition_name)

    if not os.path.exists(filepath):
        print(f"⚠️ Missing silver finance file: {filepath}")
        return None

    df = spark.read.parquet(filepath)
    print(f"✅ Loaded financial data: {df.count()} rows")

    df = df.select(
        "Customer_ID", "snapshot_date", 'Credit_History_Age', 'Num_Fin_Pdts',
        'EMI_to_Salary', 'Debt_to_Salary', 'Repayment_Ability', 'Loans_per_Credit_Item',
        'Loan_Extent', 'Outstanding_Debt', 'Interest_Rate', 'Delay_from_due_date',
        'Changed_Credit_Limit'
    )

    os.makedirs(gold_fin_directory, exist_ok=True)
    partition_name = f"gold_ft_store_cust_fin_risk_{snapshot_date_str.replace('-', '_')}.parquet"
    filepath = os.path.join(gold_fin_directory, partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print(f"✅ Saved financial gold features to: {filepath}")

    return df


# --------------------------------------------------------------------
# MAIN EXECUTION
# --------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate gold tables (features + labels)")
    parser.add_argument("--snapshotdate", type=str, required=True, help="Snapshot date in YYYY-MM-DD format")
    args = parser.parse_args()

    print("\n\n--- Starting Gold Table Generation ---\n")

    spark = pyspark.sql.SparkSession.builder \
        .appName("gold_processing") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    snapshot_date_str = args.snapshotdate
    print(f"Running for snapshot_date: {snapshot_date_str}")

    # directories
    silver_fin_directory = "datamart/silver/fin/"
    silver_clks_directory = "datamart/silver/clks/"
    silver_lms_directory = "datamart/silver/lms/"
    gold_fin_directory = "datamart/gold/feature_store/cust_fin_risk/"
    gold_clks_directory = "datamart/gold/feature_store/eng/"
    gold_label_store_directory = "datamart/gold/label_store/"

    # process each table
    process_fts_gold_cust_risk_table(snapshot_date_str, silver_fin_directory, gold_fin_directory, spark)
    process_fts_gold_engag_table(snapshot_date_str, silver_clks_directory, gold_clks_directory, spark)
    process_labels_gold_table(snapshot_date_str, silver_lms_directory, gold_label_store_directory, spark, dpd=5, mob=3)

    spark.stop()
    print("\n--- Completed Gold Table Generation ---\n")
