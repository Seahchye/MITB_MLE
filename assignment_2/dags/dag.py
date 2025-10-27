from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

end_date=datetime(2024, 12, 1)

with DAG(
    'dag',
    default_args=default_args,
    description='data pipeline run once a month',
    schedule_interval='0 0 1 * *',  # At 00:00 on day-of-month 1
    start_date=datetime(2023, 1, 1),
    end_date=end_date,
    catchup=True,
) as dag:

    # data pipeline

    # --- label store ---

    dep_check_source_label_data = BashOperator(
    task_id="dep_check_source_label_data",
    bash_command=(
        "test -s /opt/airflow/scripts/data/feature_clickstream.csv && "
        "test -s /opt/airflow/scripts/data/features_attributes.csv && "
        "test -s /opt/airflow/scripts/data/features_financials.csv && "
        "test -s /opt/airflow/scripts/data/lms_loan_daily.csv"
    ),
)

    bronze_label_store = BashOperator(
        task_id='run_bronze_label_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 bronze_label_store.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    silver_label_store = BashOperator(
        task_id='run_silver_label_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 silver_label_store.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )

    gold_label_store = BashOperator(
        task_id='run_gold_label_store',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 gold_label_store.py '
            '--snapshotdate "{{ ds }}"'
        ),
    )
    label_store_completed = BashOperator(
        task_id="label_store_completed",
        bash_command=(
        'LABEL_PATH="/opt/airflow/scripts/datamart/gold/label_store"; '
        'echo "🔍 Checking parquet files in $LABEL_PATH"; '
        'if ls $LABEL_PATH/*.parquet 1> /dev/null 2>&1; then '
        '  echo "✅ Parquet files found in $LABEL_PATH"; '
        '  ls -lh $LABEL_PATH/*.parquet; '
        'else '
        '  echo "❌ No parquet files found in $LABEL_PATH" && exit 1; '
        'fi'
    ),
)

    # Define task dependencies to run scripts sequentially
    dep_check_source_label_data >> bronze_label_store >> silver_label_store >> gold_label_store >> label_store_completed
 
 
    # --- feature store ---
    dep_check_source_data_bronze_1 = DummyOperator(task_id="dep_check_source_data_bronze_1")

    dep_check_source_data_bronze_2 = DummyOperator(task_id="dep_check_source_data_bronze_2")

    dep_check_source_data_bronze_3 = DummyOperator(task_id="dep_check_source_data_bronze_3")

    bronze_table_1 = DummyOperator(task_id="bronze_table_1")
    
    bronze_table_2 = DummyOperator(task_id="bronze_table_2")

    bronze_table_3 = DummyOperator(task_id="bronze_table_3")

    silver_table_1 = DummyOperator(task_id="silver_table_1")

    silver_table_2 = DummyOperator(task_id="silver_table_2")

    gold_feature_store = DummyOperator(task_id="gold_feature_store")

    model_train = BashOperator(
        task_id='model_train',
        bash_command=(
            f'cd /opt/airflow/scripts && '
            f'python3 model_train.py '
            f'--snapshotdate "{end_date:%Y-%m-%d}"'
        ),
    )

    
    # Define task dependencies to run scripts sequentially
    dep_check_source_data_bronze_1 >> bronze_table_1 >> silver_table_1 >> gold_feature_store
    dep_check_source_data_bronze_2 >> bronze_table_2 >> silver_table_1 >> gold_feature_store
    dep_check_source_data_bronze_3 >> bronze_table_3 >> silver_table_2 >> gold_feature_store
    gold_feature_store >> model_train


    # --- model inference ---
    model_inference_start = BashOperator(
    task_id='model_inference',
    bash_command=(
        f'cd /opt/airflow/scripts && '
        f'python3 model_inference.py '
        f'--snapshotdate "{{{{ ds }}}}" '   
        f'--enddate "{end_date:%Y-%m-%d}"'  
    ),
)

    model_1_inference = DummyOperator(task_id="model_1_inference")

    model_2_inference = DummyOperator(task_id="model_2_inference")

    model_inference_completed = DummyOperator(task_id="model_inference_completed")
    
    # Define task dependencies to run scripts sequentially
    model_train >> model_inference_start
    model_inference_start >> model_1_inference >> model_inference_completed
    model_inference_start >> model_2_inference >> model_inference_completed


    # --- model monitoring ---
    model_monitor_start = BashOperator(
        task_id='model_monitor',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 model_monitor.py '
            '--snapshotdate "{{ ds }}" '
            f'--enddate "{end_date:%Y-%m-%d}"'
        ),
    )

    model_1_monitor = DummyOperator(task_id="model_1_monitor")

    model_2_monitor = DummyOperator(task_id="model_2_monitor")

    model_monitor_completed = DummyOperator(task_id="model_monitor_completed")
    
    # Define task dependencies to run scripts sequentially
    model_inference_completed >> model_monitor_start
    model_monitor_start >> model_1_monitor >> model_monitor_completed
    model_monitor_start >> model_2_monitor >> model_monitor_completed


    # --- model auto training ---

    model_automl_start = BashOperator(
        task_id='model_automl',
        bash_command=(
            'cd /opt/airflow/scripts && '
            'python3 model_automl.py '
            '--snapshotdate "{{ ds }}" '
            f'--enddate "{end_date:%Y-%m-%d}" '
            '--n_trials 25'
        ),
    )
    
    model_1_automl = DummyOperator(task_id="model_1_automl")

    model_2_automl = DummyOperator(task_id="model_2_automl")

    model_automl_completed = DummyOperator(task_id="model_automl_completed")
    
    # Define task dependencies to run scripts sequentially
    model_train >> model_automl_start
    label_store_completed >> model_automl_start
    model_automl_start >> model_1_automl >> model_automl_completed
    model_automl_start >> model_2_automl >> model_automl_completed