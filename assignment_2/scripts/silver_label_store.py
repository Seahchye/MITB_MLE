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
import utils.data_processing_silver_table
# import utils.data_processing_gold_table

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

    # Load Loan Management System Data
    bronze_lms_directory = "datamart/bronze/lms/"
    silver_lms_directory = "datamart/silver/lms/"

    if not os.path.exists(silver_lms_directory):
        os.makedirs(silver_lms_directory)

    # process loan data
    utils.data_processing_silver_table.process_silver_loan_table(date_str, bronze_lms_directory, silver_lms_directory, spark)

    # Clickstream Data
    bronze_clks_directory = "datamart/bronze/clks/"
    silver_clks_directory = "datamart/silver/clks/"

    if not os.path.exists(silver_clks_directory):
        os.makedirs(silver_clks_directory)

    # process clickstream data
    utils.data_processing_silver_table.process_silver_clickstream_table(date_str, bronze_clks_directory, silver_clks_directory, spark)

    # Attributes Data
    bronze_attr_directory = "datamart/bronze/attr/"
    silver_attr_directory = "datamart/silver/attr/"

    if not os.path.exists(silver_attr_directory):
        os.makedirs(silver_attr_directory)

    # process attributes data
    utils.data_processing_silver_table.process_silver_attributes_table(date_str, bronze_attr_directory, silver_attr_directory, spark)

    # Financials Data
    bronze_fin_directory = "datamart/bronze/fin/"
    silver_fin_directory = "datamart/silver/fin/"

    if not os.path.exists(silver_fin_directory):
        os.makedirs(silver_fin_directory)

    # process financial data
    utils.data_processing_silver_table.process_silver_financials_table(date_str, bronze_fin_directory, silver_fin_directory, spark)

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

