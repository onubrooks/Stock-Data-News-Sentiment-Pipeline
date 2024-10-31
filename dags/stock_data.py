import os

import polars as pl
from bronze.stock_data_elt import (
    clean_insider_transaction_data,
    clean_stock_data,
    clean_technical_indicators_data,
    fetch_insider_transaction_data,
    fetch_stock_data,
    fetch_technical_indicators_data,
)
from cuallee import Check, CheckLevel
from silver.feature_engineer_and_join_data import (
    engineer_features,
    join_and_merge_datasets,
)
from utils.data_quality_checks import check_data_quality_instance
from utils.s3_helper import (
    get_insider_transaction_data_from_s3,
    get_stock_data_from_s3,
    get_technical_indicators_data_from_s3,
    store_engineered_data_in_s3,
    store_insider_transaction_data_in_s3,
    store_stock_data_in_s3,
    store_technical_indicators_data_in_s3,
)

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.dates import days_ago

stock_data_file_path = (
    f'{os.getenv("AIRFLOW_HOME")}/data/bronze/stock_data.csv'
)
transaction_data_file_path = (
    f'{os.getenv("AIRFLOW_HOME")}/data/bronze/insider_transaction_data.csv'
)
technical_indicators_data_file_path = (
    f'{os.getenv("AIRFLOW_HOME")}/data/bronze/technical_indicators_data.csv'
)

with DAG(
    'Stock-Data-Pipeline',
    start_date=days_ago(2),
    schedule_interval=None,  # Adjust schedule as needed
    catchup=False,
) as dag:

    # Tasks to fetch and clean stock data
    fetch_stock_data = PythonOperator(
        task_id='fetch_stock_data', python_callable=fetch_stock_data
    )

    check_stock_data_quality = BranchPythonOperator(
        task_id='check_stock_data_quality',
        provide_context=True,
        python_callable=check_data_quality_instance,
        op_kwargs={
            'df': pl.read_csv(stock_data_file_path),
            'column_name': 'close',
            'data': 'stock',
        },
    )
    stop_stock_pipeline = BashOperator(
        task_id='stop_stock_pipeline', bash_command='exit 1'
    )

    clean_stock_data = PythonOperator(
        task_id='clean_stock_data', python_callable=clean_stock_data
    )

    store_stock_data_s3 = PythonOperator(
        task_id='store_stock_data_s3', python_callable=store_stock_data_in_s3
    )

    # Tasks to fetch and clean insider transaction data
    fetch_insider_transaction_data = PythonOperator(
        task_id='fetch_insider_transaction_data',
        python_callable=fetch_insider_transaction_data,
    )

    check_transaction_data_quality = BranchPythonOperator(
        task_id='check_transaction_data_quality',
        provide_context=True,
        python_callable=check_data_quality_instance,
        op_kwargs={
            'df': pl.read_csv(transaction_data_file_path),
            'column_name': 'ticker',
            'data': 'insider_transaction',
        },
    )
    stop_transaction_pipeline = BashOperator(
        task_id='stop_transaction_pipeline', bash_command='exit 1'
    )

    clean_insider_transaction_data = PythonOperator(
        task_id='clean_insider_transaction_data',
        python_callable=clean_insider_transaction_data,
    )

    store_insider_transaction_data_s3 = PythonOperator(
        task_id='store_insider_transaction_data_s3',
        python_callable=store_insider_transaction_data_in_s3,
    )

    # Tasks to fetch and clean technical indicators data
    fetch_technical_indicators_data = PythonOperator(
        task_id='fetch_technical_indicators_data',
        python_callable=fetch_technical_indicators_data,
    )

    check_indicators_data_quality = BranchPythonOperator(
        task_id='check_indicators_data_quality',
        provide_context=True,
        python_callable=check_data_quality_instance,
        op_kwargs={
            'df': pl.read_csv(technical_indicators_data_file_path),
            'column_name': 'symbol',
            'data': 'technical_indicators',
        },
    )
    stop_indicators_pipeline = BashOperator(
        task_id='stop_indicators_pipeline', bash_command='exit 1'
    )

    clean_technical_indicators_data = PythonOperator(
        task_id='clean_technical_indicators_data',
        python_callable=clean_technical_indicators_data,
    )

    store_technical_indicators_data_s3 = PythonOperator(
        task_id='store_technical_indicators_data_s3',
        python_callable=store_technical_indicators_data_in_s3,
    )

    # tasks to merge and feature engineer all data

    get_stock_data_s3 = PythonOperator(
        task_id='get_stock_data_s3', python_callable=get_stock_data_from_s3
    )

    get_insider_transactions_s3 = PythonOperator(
        task_id='get_insider_transactions_s3',
        python_callable=get_insider_transaction_data_from_s3,
    )

    get_technical_indicators_s3 = PythonOperator(
        task_id='get_technical_indicators_s3',
        python_callable=get_technical_indicators_data_from_s3,
    )

    join_and_merge = PythonOperator(
        task_id='join_and_merge', python_callable=join_and_merge_datasets
    )

    engineer_ml_features = PythonOperator(
        task_id='engineer_ml_features', python_callable=engineer_features
    )

    store_engineered_data_s3 = PythonOperator(
        task_id='store_engineered_data_s3',
        python_callable=store_engineered_data_in_s3,
    )

    (
        fetch_stock_data
        >> check_stock_data_quality
        >> clean_stock_data
        >> store_stock_data_s3
    )
    check_stock_data_quality >> stop_stock_pipeline

    (
        fetch_insider_transaction_data
        >> check_transaction_data_quality
        >> clean_insider_transaction_data
        >> store_insider_transaction_data_s3
    )
    check_transaction_data_quality >> stop_transaction_pipeline

    (
        fetch_technical_indicators_data
        >> check_indicators_data_quality
        >> clean_technical_indicators_data
        >> store_technical_indicators_data_s3
    )
    check_indicators_data_quality >> stop_indicators_pipeline

    store_stock_data_s3 >> get_stock_data_s3
    store_insider_transaction_data_s3 >> get_stock_data_s3
    store_technical_indicators_data_s3 >> get_stock_data_s3

    get_stock_data_s3 >> join_and_merge
    get_insider_transactions_s3 >> join_and_merge
    get_technical_indicators_s3 >> join_and_merge
    join_and_merge >> engineer_ml_features >> store_engineered_data_s3
