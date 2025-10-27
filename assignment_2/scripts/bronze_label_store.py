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

import utils.data_processing_bronze_table
# import utils.data_processing_silver_table
# import utils.data_processing_gold_table

# to call this script: python bronze_label_store.py --snapshotdate "2023-01-01"

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
    
    # Load LMS data
    bronze_lms_directory = "datamart/bronze/lms/"
    
    if not os.path.exists(bronze_lms_directory):
        os.makedirs(bronze_lms_directory)

    # run loan data processing
    utils.data_processing_bronze_table.process_bronze_loan_table(date_str, bronze_lms_directory, spark)
    
    # Clickstream Data
    bronze_clks_directory = "datamart/bronze/clks/"

    if not os.path.exists(bronze_clks_directory):
        os.makedirs(bronze_clks_directory)

    # run clickstream data processing
    utils.data_processing_bronze_table.process_bronze_clickstream_table(date_str, bronze_clks_directory, spark)
    
    # Load Attributes Data
    bronze_attr_directory = "datamart/bronze/attr/"

    if not os.path.exists(bronze_attr_directory):
        os.makedirs(bronze_attr_directory)

    # run attributes data processing
    utils.data_processing_bronze_table.process_bronze_attributes_table(date_str, bronze_attr_directory, spark)

    # Financials Data
    bronze_fin_directory = "datamart/bronze/fin/"

    if not os.path.exists(bronze_fin_directory):
        os.makedirs(bronze_fin_directory)

    # run financials data processing
    utils.data_processing_bronze_table.process_bronze_financials_table(date_str, bronze_fin_directory, spark)

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
