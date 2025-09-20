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

# to call this script: python bronze_data_snapshotdate.py --snapshotdate "2023-01-01"

def main(snapshotdate):
    print('\n\n---starting job---\n\n')

    spark = pyspark.sql.SparkSession.builder \
            .appName("dev") \
            .master("local[*]") \
            .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    date_str = snapshotdate

    bronze_lms_directory = "datamart/bronze/lms"

    if not os.path.exists(bronze_lms_directory):
        os.makedirs(bronze_lms_directory)

    utils.data_processing_bronze_table.process_bronze_table(date_str, bronze_lms_directory, spark)
    
    spark.stop()

    print('\n\n---job completed---\n\n')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")

    args = parser.parse_args()

    main(args.snapshotdate)