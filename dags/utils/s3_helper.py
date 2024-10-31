import boto3
import botocore
import pandas as pd
import os
from botocore.config import Config

boto_config = Config(region_name='eu-west-2')

bucket_name = os.getenv("AWS_S3_BUCKET_NAME")

# Task ids for data sharing
source_data_task_stock = 'clean_stock_data'
source_data_task_insider_transaction = 'clean_insider_transaction_data'
source_data_task_technical_indicators = 'clean_technical_indicators_data'
source_data_task_news_sentiment = 'clean_news_sentiment_data'
source_data_engineer_ml_features = 'engineer_ml_features'
source_data_task_engineered_sentiment = 'engineer_sentiment_features'
source_data_task_ml_data = 'combine_data_for_ml'

# Filenames on s3
stock_data_file_name = 'bronze/stock_data.parquet'
insider_transaction_data_file_name = 'bronze/insider_transaction_data.parquet'
technical_indicators_data_file_name = (
    'bronze/technical_indicators_data.parquet'
)
news_sentiment_data_file_name = 'bronze/news_sentiment_data.parquet'
engineered_stock_data_file_name = 'silver/engineered_stock_data.parquet'
engineered_sentiment_data_file_name = (
    'silver/engineered_sentiment_data.parquet'
)
engineered_insider_transaction_data_file_name = (
    'silver/engineered_insider_transaction_data.parquet'
)
ml_data_file_name = 'gold/ml_data.parquet'
hgb_model_file_name = 'gold/models/histgradientboostingregressor_model.pkl'
random_forest_model_file_name = 'gold/models/random_forest_model.pkl'


def get_s3_client():
    return boto3.client('s3', config=boto_config)


def save_to_s3(df, file_name):
    local_path = f'/opt/airflow/data/{file_name}'
    df.to_csv(
        local_path.replace('.parquet', '.csv'), index=False
    )  # save csv to examine the data inline
    df.to_parquet(local_path)
    s3 = get_s3_client()
    s3.upload_file(local_path, bucket_name, file_name)


def get_from_s3(file_name):
    print(f'Downloading {file_name} from S3')
    s3 = get_s3_client()
    local_path = f'/opt/airflow/data/{file_name}'
    try:
        s3.download_file(bucket_name, file_name, local_path)
        return pd.read_parquet(local_path)

    except botocore.exceptions.ClientError as error:
        print(error.response['Error']['Code'])
        print(error.response['Error']['Message'])
        raise error

    except botocore.exceptions.ParamValidationError as error:
        raise ValueError(
            'The parameters you provided are incorrect: {}'.format(error)
        )


# Functions to load data into S3
def store_stock_data_in_s3(ti):
    df = ti.xcom_pull(task_ids=source_data_task_stock)
    save_to_s3(df, stock_data_file_name)


def store_insider_transaction_data_in_s3(ti):
    df = ti.xcom_pull(task_ids=source_data_task_insider_transaction)
    save_to_s3(df, insider_transaction_data_file_name)


def store_technical_indicators_data_in_s3(ti):
    df = ti.xcom_pull(task_ids=source_data_task_technical_indicators)
    save_to_s3(df, technical_indicators_data_file_name)


def store_news_sentiment_data_in_s3(ti):
    df = ti.xcom_pull(task_ids=source_data_task_news_sentiment)
    save_to_s3(df, news_sentiment_data_file_name)


# Functions to fetch data from S3
def get_stock_data_from_s3():
    return get_from_s3(stock_data_file_name)


def get_insider_transaction_data_from_s3():
    return get_from_s3(insider_transaction_data_file_name)


def get_technical_indicators_data_from_s3():
    return get_from_s3(technical_indicators_data_file_name)


def get_news_sentiment_data_from_s3():
    return get_from_s3(news_sentiment_data_file_name)


# merged and feature engineered stock data
def store_engineered_data_in_s3(ti):
    df = ti.xcom_pull(task_ids=source_data_engineer_ml_features)
    save_to_s3(df, engineered_stock_data_file_name)


# sentiment data
def store_engineered_sentiment_data_in_s3(ti):
    df = ti.xcom_pull(task_ids=source_data_task_engineered_sentiment)
    save_to_s3(df, engineered_sentiment_data_file_name)


# ml data
def get_engineered_news_sentiment_data_from_s3():
    return get_from_s3(engineered_sentiment_data_file_name)


def get_engineered_stock_data_from_s3():
    return get_from_s3(engineered_stock_data_file_name)


def store_ml_data_s3(ti):
    df = ti.xcom_pull(task_ids=source_data_task_ml_data)
    save_to_s3(df, ml_data_file_name)


def store_ml_model_s3(ti):
    s3 = get_s3_client()

    # Upload Random Forest model
    local_path = f'/opt/airflow/data/{random_forest_model_file_name}'
    s3.upload_file(local_path, bucket_name, random_forest_model_file_name)

    # Upload HistGradientBoostingRegressor model
    local_path = f'/opt/airflow/data/{hgb_model_file_name}'
    s3.upload_file(local_path, bucket_name, hgb_model_file_name)
