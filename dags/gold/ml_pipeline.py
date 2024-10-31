import pandas as pd
from sklearn.ensemble import (
    HistGradientBoostingRegressor,
    RandomForestRegressor,
)
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split


def preprocess_data(stock_data, news_data):
    # Drop unnecessary columns from stock data
    stock_data = stock_data.drop(
        columns=['transaction_date', 'timestamp', 'shares', 'share_price']
    )
    # drop every column that starts with executive_
    stock_data = stock_data.drop(
        columns=[
            col for col in stock_data.columns if col.startswith('executive_')
        ]
    )

    # rename ticker column to symbol
    news_data = news_data.rename(columns={'ticker': 'symbol'})
    # convert time_published to datetime and then rename to timestamp
    news_data['date'] = pd.to_datetime(news_data['time_published'])
    # Drop unnecessary columns from news data
    news_data = news_data.drop(
        columns=[
            'title',
            'url',
            'time_published',
            'authors',
            'summary',
            'cleaned_summary',
        ]
    )

    # Merge data based on ticker and date
    merged_data = pd.merge(
        stock_data, news_data, on=['symbol', 'date'], how='left'
    )

    # convert symbol to categorical using dummies
    merged_data = pd.get_dummies(merged_data, columns=['symbol'])
    # drop date column
    merged_data = merged_data.drop(columns=['date'])

    # Handle missing values
    merged_data.fillna(method='ffill', inplace=True)

    return merged_data


def train_model(data):
    # Define features and target variable
    X = data.drop('close', axis=1)
    y = data['close']

    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Train a Random Forest Regression model
    model_rf = RandomForestRegressor(n_estimators=100, random_state=42)
    model_rf.fit(X_train, y_train)

    # Train a HistGradientBoostingRegressor model (replacing LinearRegression)
    model_hgb = HistGradientBoostingRegressor(random_state=42)
    model_hgb.fit(X_train, y_train)

    # Make predictions
    y_pred_rf = model_rf.predict(X_test)
    y_pred_hgb = model_hgb.predict(X_test)

    # Evaluate models
    mse_rf = mean_squared_error(y_test, y_pred_rf)
    mse_hgb = mean_squared_error(y_test, y_pred_hgb)

    print('Mean Squared Error for Random Forest Regression:', mse_rf)
    print('Mean Squared Error for HistGradientBoostingRegressor:', mse_hgb)

    # write models to file
    import pickle

    with open(
        '/opt/airflow/data/gold/models/random_forest_model.pkl', 'wb'
    ) as f:
        pickle.dump(model_rf, f)
        print('Random Forest Regression model saved to file')
    with open(
        '/opt/airflow/data/gold/models/histgradientboostingregressor_model.pkl',
        'wb',
    ) as f:
        pickle.dump(model_hgb, f)
        print('HistGradientBoostingRegressor model saved to file')
    return mse_rf, mse_hgb


def combine_data_for_ml(ti):
    stock_data = ti.xcom_pull(task_ids='get_engineered_stock_data_s3')
    news_sentiment_data = ti.xcom_pull(
        task_ids='get_engineered_news_sentiment_data_s3'
    )
    merged_data = preprocess_data(stock_data, news_sentiment_data)
    return merged_data


def run_ml(ti):
    merged_data = ti.xcom_pull(task_ids='combine_data_for_ml')
    model_lr, model_rf = train_model(merged_data)
    return model_lr, model_rf
