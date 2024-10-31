import pandas as pd
from sklearn.preprocessing import MinMaxScaler


def join_and_merge_datasets(ti):
    stock_data = ti.xcom_pull(task_ids='get_stock_data_s3')
    insider_transaction_data = ti.xcom_pull(
        task_ids='get_insider_transactions_s3'
    )
    technical_indicators_data = ti.xcom_pull(
        task_ids='get_technical_indicators_s3'
    )

    # Convert ticker column to symbol, sort, then merge on 'symbol' and the closest date
    insider_transaction_data['symbol'] = insider_transaction_data[
        'ticker'
    ].str.upper()
    insider_transaction_data.drop(columns=['ticker'], inplace=True)
    insider_transaction_data = insider_transaction_data.sort_values(
        by='transaction_date'
    )
    insider_transaction_data = insider_transaction_data.dropna(
        subset=['transaction_date']
    )

    merged_df = pd.merge_asof(
        stock_data,
        insider_transaction_data,
        left_on='timestamp',
        right_on='transaction_date',
        by='symbol',
        direction='nearest',
        tolerance=pd.Timedelta('1D'),
    )

    technical_indicators_data = technical_indicators_data.sort_values(
        by='date'
    )
    technical_indicators_data = technical_indicators_data.dropna(
        subset=['date']
    )

    # Merge the merged DataFrame with technical indicators
    final_df = pd.merge_asof(
        merged_df,
        technical_indicators_data,
        left_on='timestamp',
        right_on='date',
        by='symbol',
        direction='nearest',
        tolerance=pd.Timedelta('1D'),
    )
    return final_df


def engineer_features(ti):
    joined_data = ti.xcom_pull(task_ids='join_and_merge')
    print(joined_data.head(10))

    joined_data['year'] = joined_data['date'].dt.year
    joined_data['month'] = joined_data['date'].dt.month
    joined_data['day'] = joined_data['date'].dt.day
    joined_data['day_of_week'] = joined_data['date'].dt.dayofweek

    # Normalize numerical features (example using Min-Max scaling)
    scaler = MinMaxScaler()
    numerical_cols = [
        'open',
        'high',
        'low',
        'close',
        'volume',
        'shares',
        'share_price',
    ]
    joined_data[numerical_cols] = scaler.fit_transform(
        joined_data[numerical_cols]
    )

    # Encode categorical features (example using one-hot encoding)
    categorical_cols = [
        'executive',
        'security_type',
        'acquisition_or_disposal',
    ]
    joined_data = pd.get_dummies(joined_data, columns=categorical_cols)

    # Impute missing values
    joined_data.fillna(
        method='ffill', inplace=True
    )  # Forward-fill missing values
    return joined_data
