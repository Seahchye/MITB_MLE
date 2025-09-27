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

from pyspark.sql.functions import col, when, trim, regexp_replace
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

def process_silver_table(snapshot_date_str, bronze_lms_directory, silver_loan_daily_directory, spark):
    # prepare arguments
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    # connect to bronze table
    partition_name = "bronze_loan_daily_" + snapshot_date_str.replace('-','_') + '.csv'
    filepath = bronze_lms_directory + partition_name
    silver = spark.read.csv(filepath, header=True, inferSchema=True)
    print('loaded from:', filepath, 'row count:', silver.count())

    drop_cols = [
        "loan_id",
        "loan_start_date",
        "tenure",
        "Monthly_Inhand_Salary",
        "Credit_Mix",
        "Payment_of_Min_Amount",
        "Payment_Behaviour",
        "Name",
        "SSN",
        "Credit_History_Age"
    ]

    silver = silver.drop(*[c for c in drop_cols if c in silver.columns])

    # Clean up Age column
    silver = silver.withColumn("Age", regexp_replace(col("Age"), "[^0-9]", ""))
    silver = silver.withColumn("Age", col("Age").cast("int"))
    median_age = silver.filter((col("Age") >= 18) & (col("Age") <= 100)) \
                   .approxQuantile("Age", [0.5], 0.01)
    median_age = median_age[0] if median_age else 30
    silver = silver.withColumn("Age",
    when((col("Age") < 18) | (col("Age") > 100), median_age).otherwise(col("Age")))

    silver = silver.withColumn("Occupation", when(col("Occupation") == "_______", "Unknown").otherwise(col("Occupation")))

    silver = silver.withColumn("Annual_Income", regexp_replace(col("Annual_Income"), "[^0-9.]", ""))

    silver = silver.withColumn("Num_Bank_Accounts",
    when(col("Num_Bank_Accounts") > 10, 10).otherwise(col("Num_Bank_Accounts")))

    silver = silver.withColumn("Num_Credit_Card",
    when(col("Num_Credit_Card") > 10, 10).otherwise(col("Num_Credit_Card")))

    silver = silver.withColumn("Interest_Rate",
    when(col("Interest_Rate") > 34, 35).otherwise(col("Interest_Rate")))

    silver = silver.withColumn("Num_of_Loan", regexp_replace(col("Num_of_Loan"), "[^0-9]", ""))

    silver = silver.withColumn("Num_of_Loan",
    when(col("Num_of_Loan") > 9, 10).otherwise(col("Num_of_Loan")))

    silver = silver.withColumn("Num_of_Delayed_Payment", regexp_replace(col("Num_of_Delayed_Payment"), "[^0-9]", ""))

    silver = silver.withColumn("Num_of_Delayed_Payment",
    when(col("Num_of_Delayed_Payment") > 28, 29).otherwise(col("Num_of_Delayed_Payment")))

    silver = silver.withColumn("Changed_Credit_Limit", regexp_replace(col("Changed_Credit_Limit"), "[^0-9.]", ""))

    silver = silver.withColumn("Num_Credit_Inquiries",
    when(col("Num_Credit_Inquiries") > 17, 18).otherwise(col("Num_Credit_Inquiries")))

    silver = silver.withColumn("Outstanding_Debt", regexp_replace(col("Outstanding_Debt"), "[^0-9.]", ""))

    silver = silver.withColumn("Credit_Utilization_Ratio", regexp_replace(col("Credit_Utilization_Ratio"), "[^0-9.]", ""))


    # clean data: enforce schema / data type
    # Dictionary specifying columns and their desired datatypes
    column_type_map = {
        "Age": IntegerType(),
        "Customer_ID": StringType(),
        "loan_amt": IntegerType(),
        "due_amt": IntegerType(),
        "paid_amt": IntegerType(),
        "overdue_amt": IntegerType(),
        "balance": IntegerType(),
        "snapshot_date": DateType(),
        "Occupation": StringType(),
        "Annual_Income": FloatType(),
        "Num_Bank_Accounts": IntegerType(),
        "Num_Credit_Card": IntegerType(),
        "Interest_Rate": IntegerType(),
        "Num_of_Loan": IntegerType(),
        "Type_of_Loan": StringType(),
        "Delay_from_due_date": IntegerType(),
        "Num_of_Delayed_Payment": IntegerType(),
        "Changed_Credit_Limit": FloatType(),
        "Num_Credit_Inquiries": IntegerType(),
        "Outstanding_Debt": FloatType(),
        "Credit_Utilization_Ratio": FloatType(),
        "Total_EMI_per_month": FloatType(),
        "Amount_invested_monthly": FloatType(),
        "Monthly_Balance": FloatType(),
        "fe_1": IntegerType(),
        "fe_2": IntegerType(),
        "fe_3": IntegerType(),
        "fe_4": IntegerType(),
        "fe_5": IntegerType(),
        "fe_6": IntegerType(),
        "fe_7": IntegerType(),
        "fe_8": IntegerType(),
        "fe_9": IntegerType(),
        "fe_10": IntegerType(),
        "fe_11": IntegerType(),
        "fe_12": IntegerType(),
        "fe_13": IntegerType(),
        "fe_14": IntegerType(),
        "fe_15": IntegerType(),
        "fe_16": IntegerType(),
        "fe_17": IntegerType(),
        "fe_18": IntegerType(),
        "fe_19": IntegerType(),
        "fe_20": IntegerType(),
    }

    for column, new_type in column_type_map.items():
        silver = silver.withColumn(column, col(column).cast(new_type))

    # augment data: add month on book
    silver = silver.withColumn("mob", col("installment_num").cast(IntegerType()))

    # augment data: add days past due
    silver = silver.withColumn("installments_missed", F.ceil(col("overdue_amt") / col("due_amt")).cast(IntegerType()))
  
    silver = silver.withColumn("first_missed_date", F.when(col("installments_missed") > 0, F.add_months(col("snapshot_date"), -1 * col("installments_missed"))).cast(DateType()))
    silver = silver.withColumn("dpd", F.when(col("overdue_amt") > 0.0, F.datediff(col("snapshot_date"), col("first_missed_date"))).otherwise(0).cast(IntegerType()))

    silver = silver.fillna({
        "installments_missed": 0,
        "Annual_Income": 0,
        "Num_Bank_Accounts": 0,
        "Num_Credit_Card": 0,
        "Interest_Rate": 0,
        "Num_of_Loan": 0,
        "Type_of_Loan": 'Unknown',
        "Delay_from_due_date": 0,
        "Num_of_Delayed_Payment": 0,
        "Changed_Credit_Limit": 0.0,
        "Credit_Utilization_Ratio": 0.0,
        "Total_EMI_per_month": 0.0,
        "Amount_invested_monthly": 0.0,
        "Monthly_Balance": 0.0})

    # save silver table - IRL connect to database to write
    partition_name = "silver_loan_daily_" + snapshot_date_str.replace('-','_') + '.parquet'
    filepath = silver_loan_daily_directory + partition_name
    silver.write.mode("overwrite").parquet(filepath)
    # df.toPandas().to_parquet(filepath,
    #           compression='gzip')
    print('saved to:', filepath)
    
    return silver