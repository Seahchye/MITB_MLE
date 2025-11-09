import argparse
import os
import glob
import pandas as pd
import pickle
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

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

import xgboost as xgb
from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import make_scorer, f1_score, roc_auc_score
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split


# to call this script: python model_inference.py --snapshotdate "2024-09-01" --modelname "credit_model_2024_09_01.pkl"

def main(snapshotdate):
    print('\n\n---starting job---\n\n')
    
    # Initialize SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("dev") \
        .master("local[*]") \
        .getOrCreate()
    
    # Set log level to ERROR to hide warnings
    spark.sparkContext.setLogLevel("ERROR")

    
    # --- set up config ---
    config = {}
    config["snapshot_date_str"] = snapshotdate
    config["snapshot_date"] = datetime.strptime(config["snapshot_date_str"], "%Y-%m-%d")
    config["model_date"] = datetime(2024, 6, 1)
    config["model_name"] = "credit_model_" + config["model_date"].strftime("%Y_%m_%d")
    config["model_bank_directory"] = "model_bank/"
    config["model_artefact_filepath"] = os.path.join(config["model_bank_directory"], config["model_name"] + ".pkl")
    
    pprint.pprint(config)
    
    if config["snapshot_date"] < config["model_date"]:
        print(f"Snapshot date is before model date, so no results")
        spark.stop()
        return

    # Full path to the file
    file_path = config["model_artefact_filepath"]

    # --- load model artefact from model bank ---
    # Load the model from the pickle file
    with open(file_path, 'rb') as file:
        model_artefact = pickle.load(file)
    
    print("Model loaded successfully! ")

    # --- get features ---
    feature_fin_risk_location = "datamart/gold/feature_store/cust_fin_risk/"

    files_list = [feature_fin_risk_location+os.path.basename(f) for f in glob.glob(os.path.join(feature_fin_risk_location, '*'))]
    feature_fin_risk_df = spark.read.option("header", "true").parquet(*files_list)
    print("row_count:",feature_fin_risk_df.count())
    
    feature_fin_risk_df.show()
    
    available_dates_fin = [r["snapshot_date"] for r in feature_fin_risk_df.select("snapshot_date").distinct().collect()]

    if config["snapshot_date_str"] not in available_dates_fin:
        # fallback to latest available
        latest_date = max(set(available_dates_fin))
        print(f"⚠️ No data found for {config['snapshot_date_str']}. Using latest available snapshot: {latest_date}")
        config["snapshot_date_str"] = latest_date


    # extract label store
    feature_fin_risk_df = feature_fin_risk_df.filter(col("snapshot_date") == config["snapshot_date_str"])
    
    print("extracted features_sdf", feature_fin_risk_df.count())

    feature_eng_location = "datamart/gold/feature_store/eng/"

    files_list = [feature_eng_location+os.path.basename(f) for f in glob.glob(os.path.join(feature_eng_location, '*'))]
    feature_eng_df = spark.read.option("header", "true").parquet(*files_list)
    print("row_count:",feature_eng_df.count())
    
    feature_eng_df.show()
    
    available_dates_eng = [r["snapshot_date"] for r in feature_eng_df.select("snapshot_date").distinct().collect()]

    if config["snapshot_date_str"] not in available_dates_eng:
        # fallback to latest available
        latest_date = max(set(available_dates_eng))
        print(f"⚠️ No data found for {config['snapshot_date_str']}. Using latest available snapshot: {latest_date}")
        config["snapshot_date_str"] = latest_date

    # extract label store
    feature_eng_df = feature_eng_df.filter(col("snapshot_date") == config["snapshot_date_str"])
    
    print("extracted features_sdf", feature_eng_df.count())

    # stop job early if still empty
    if feature_fin_risk_df.count() == 0 or feature_eng_df.count() == 0:
        raise ValueError(f"No feature data found for snapshot_date={config['snapshot_date_str']}. Check feature store paths.")

    # --- prepare data for modeling ---
    # prepare data for modeling
    data_pdf = (feature_eng_df
                .join(feature_fin_risk_df, on=["Customer_ID", "snapshot_date"], how="left")).toPandas()
    
    drop_cols = {"loan_id", "Customer_ID", "label", "label_def", "snapshot_date"}
    
    feature_cols = [
    c for c in data_pdf.columns
    if c not in drop_cols and pd.api.types.is_numeric_dtype(data_pdf[c])
    ]

    # --- preprocess data for modeling ---
    # prepare X_inference
    X_inference = data_pdf[feature_cols]
    
    # apply transformer - standard scaler
    transformer_stdscaler = model_artefact["preprocessing_transformers"]["stdscaler"]
    X_inference = transformer_stdscaler.transform(X_inference)
    
    print('X_inference', X_inference.shape[0])


    # --- model prediction inference ---
    # load model
    model = model_artefact["model"]
    
    # predict model
    y_inference = model.predict_proba(X_inference)[:, 1]
    
    # prepare output
    y_inference_pdf = data_pdf[["Customer_ID","snapshot_date"]].copy()
    y_inference_pdf["model_name"] = config["model_name"]
    y_inference_pdf["model_predictions"] = y_inference
    

    # --- save model inference to datamart gold table ---
    # create bronze datalake
    gold_directory = f"datamart/gold/model_predictions/{config['model_name']}/"
    print(gold_directory)
    
    if not os.path.exists(gold_directory):
        os.makedirs(gold_directory)
    
    # save gold table - IRL connect to database to write
    partition_name = config["model_name"] + "_predictions_" + config["snapshot_date_str"].replace('-','_') + '.parquet'
    filepath = gold_directory + partition_name
    spark.createDataFrame(y_inference_pdf).write.mode("overwrite").parquet(filepath)
    # df.toPandas().to_parquet(filepath,
    #           compression='gzip')
    print('saved to:', filepath)

    
    # --- end spark session --- 
    spark.stop()
    
    print('\n\n---completed job---\n\n')


if __name__ == "__main__":
    # Setup argparse to parse command-line arguments
    parser = argparse.ArgumentParser(description="run job")
    parser.add_argument("--snapshotdate", type=str, required=True, help="YYYY-MM-DD")
    
    args = parser.parse_args()
    
    # Call main with arguments explicitly passed
    main(args.snapshotdate)
