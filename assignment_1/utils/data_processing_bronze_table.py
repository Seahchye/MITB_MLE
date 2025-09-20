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
from functools import reduce

from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

def process_bronze_table(snapshot_date_str, bronze_lms_directory, spark):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")

    csv_file_path = glob.glob("data/*.csv")

    dfs = [spark.read.csv(f, header=True, inferSchema= True) for f in csv_file_path]

    merged = reduce(lambda df1, df2: df1.join(df2, on="Customer_ID", how="outer"), dfs)

    merged_filter = merged.filter(col("snapshot_date") == snapshot_date)

    print(snapshot_date_str + 'row_count:', merged_filter.count())

    partition_name = "bronze_loan_daily_" + snapshot_date_str.replace('-','_') + '.csv'
    filepath = bronze_lms_directory + partition_name
    merged_filter.toPandas().to_csv(filepath, index=False)
    print('saved to:', filepath)

    return merged_filter