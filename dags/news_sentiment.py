from bronze.news_sentiment_data_elt import extract_features_from_news_data
from bronze.stock_data_elt import (
    clean_news_sentiment_data,
    fetch_news_sentiment_data,
)
from utils.s3_helper import (
    get_news_sentiment_data_from_s3,
    store_engineered_sentiment_data_in_s3,
    store_news_sentiment_data_in_s3,
)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

with DAG(
    'News-Sentiment-Pipeline',
    start_date=days_ago(2),
    schedule_interval=None,  # Adjust schedule as needed
    catchup=False,
) as dag:

    fetch_news_sentiment_data = PythonOperator(
        task_id='fetch_news_sentiment_data',
        python_callable=fetch_news_sentiment_data,
    )

    clean_news_sentiment_data = PythonOperator(
        task_id='clean_news_sentiment_data',
        python_callable=clean_news_sentiment_data,
    )

    store_news_sentiment_data_s3 = PythonOperator(
        task_id='store_news_sentiment_data_s3',
        python_callable=store_news_sentiment_data_in_s3,
    )

    get_sentiment_data_s3 = PythonOperator(
        task_id='get_sentiment_data_s3',
        python_callable=get_news_sentiment_data_from_s3,
    )

    engineer_sentiment_features = PythonOperator(
        task_id='engineer_sentiment_features',
        python_callable=extract_features_from_news_data,
    )

    store_engineered_sentiment_data_s3 = PythonOperator(
        task_id='store_engineered_sentiment_data_s3',
        python_callable=store_engineered_sentiment_data_in_s3,
    )

    (
        fetch_news_sentiment_data
        >> clean_news_sentiment_data
        >> store_news_sentiment_data_s3
    )
    store_news_sentiment_data_s3 >> get_sentiment_data_s3
    (
        get_sentiment_data_s3
        >> engineer_sentiment_features
        >> store_engineered_sentiment_data_s3
    )
