from gold.ml_pipeline import combine_data_for_ml, run_ml
from utils.s3_helper import (
    get_engineered_news_sentiment_data_from_s3,
    get_engineered_stock_data_from_s3,
    store_ml_data_s3,
    store_ml_model_s3,
)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
    'Predice-Stock-ML-Pipeline',
    start_date=days_ago(2),
    schedule_interval=None,  # Adjust schedule as needed
    catchup=False,
) as dag:

    get_engineered_news_sentiment_data_s3 = PythonOperator(
        task_id='get_engineered_news_sentiment_data_s3',
        python_callable=get_engineered_news_sentiment_data_from_s3,
    )

    get_engineered_stock_data_s3 = PythonOperator(
        task_id='get_engineered_stock_data_s3',
        python_callable=get_engineered_stock_data_from_s3,
    )

    combine_data_for_ml = PythonOperator(
        task_id='combine_data_for_ml', python_callable=combine_data_for_ml
    )

    store_ml_data_in_s3 = PythonOperator(
        task_id='store_ml_data_in_s3', python_callable=store_ml_data_s3
    )

    run_ml_workload = PythonOperator(
        task_id='run_ml_workload', python_callable=run_ml
    )

    store_ml_model_in_s3 = PythonOperator(
        task_id='store_ml_model_in_s3', python_callable=store_ml_model_s3
    )

    get_engineered_news_sentiment_data_s3 >> combine_data_for_ml
    get_engineered_stock_data_s3 >> combine_data_for_ml
    combine_data_for_ml >> store_ml_data_in_s3
    combine_data_for_ml >> run_ml_workload >> store_ml_model_in_s3
