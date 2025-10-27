import argparse
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

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

# import utils.data_processing_bronze_table
# import utils.data_processing_silver_table
import utils.data_processing_gold_table

def main(snapshotdate):
    print('\n\n---starting job---\n\n')
    
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("dev") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    # load arguments
    date_str = snapshotdate

    # Build Feature Store
    # engagement_tab
    silver_clks_directory = "datamart/silver/clks/"
    gold_clks_directory = "datamart/gold/feature_store/eng/"

    if not os.path.exists(gold_clks_directory):
        os.makedirs(gold_clks_directory)

    # process clicks data
    utils.data_processing_gold_table.process_fts_gold_engag_table(date_str, silver_clks_directory, gold_clks_directory, spark)

    # cust_fin_risk_tab
    silver_fin_directory = "datamart/silver/fin/"
    gold_fin_directory = "datamart/gold/feature_store/cust_fin_risk/"

    if not os.path.exists(gold_fin_directory):
        os.makedirs(gold_fin_directory)

    # process financial data
    utils.data_processing_gold_table.process_fts_gold_cust_risk_table(date_str, silver_fin_directory, gold_fin_directory, spark)

    # Inspect Feature Store Tables
    ## engagement_tab
    folder_path = gold_clks_directory
    files_list = [folder_path+os.path.basename(f) for f in glob.glob(os.path.join(folder_path, '*'))]
    df = spark.read.parquet(*files_list)
    print("row_count:",df.count())
    df.show()

    ## cust_fin_risk_tab
    folder_path = gold_fin_directory
    files_list = [folder_path+os.path.basename(f) for f in glob.glob(os.path.join(folder_path, '*'))]
    df = spark.read.parquet(*files_list)
    print("row_count:",df.count())
    df.show()



    # Build Label Store (based on Loan Mgmt System Data)
    silver_lms_directory = "datamart/silver/lms/"
    gold_label_store_directory = "datamart/gold/label_store/"

    if not os.path.exists(gold_label_store_directory):
        os.makedirs(gold_label_store_directory)

    # run gold backfill
    # process gold loan data
    utils.data_processing_gold_table.process_labels_gold_table(date_str, silver_lms_directory, gold_label_store_directory, spark, dpd = 30, mob = 6)

    # Inspect Label Store 
    folder_path = gold_label_store_directory
    files_list = [folder_path+os.path.basename(f) for f in glob.glob(os.path.join(folder_path, '*'))]
    df = spark.read.option("header", "true").parquet(*files_list)
    print("row_count:",df.count())
    df.show()

    # end spark session
    spark.stop()
    
    print('\n\n---completed job---\n\n')

if __name__ == "__main__":
    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    
    args = parser.parse_args()
    
    # Call main with arguments explicitly passed
    main(args.snapshotdate)
