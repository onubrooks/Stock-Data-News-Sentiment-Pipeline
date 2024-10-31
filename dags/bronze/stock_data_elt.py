import io

import pandas as pd
import requests

API_KEY = '06X745AP7A7Z7ZBR'  # 06X745AP7A7Z7ZBR' # '9PT41Q4ROVCUT4OA'
SYMBOLS = ['IBM', 'AAPL', 'GOOGL']
INDICATORS = ['SMA', 'EMA', 'WMA', 'DEMA']
DATATYPE = 'csv'
OUTPUTSIZE = 'full'
INTERVAL = 'daily'  # 'daily'
BASE_URL = 'https://www.alphavantage.co/query'


def fetch_stock_data():
    all_stock_data = []
    for symbol in SYMBOLS:
        stock_url = f'{BASE_URL}?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}&datatype={DATATYPE}&outputsize={OUTPUTSIZE}'
        response = requests.get(stock_url)
        if response.status_code == 200:
            csv_data = response.text
            df = pd.read_csv(io.StringIO(csv_data))
            df['symbol'] = symbol
            all_stock_data.append(df)
        else:
            print(f"Error fetching data for {symbol}: {response.status_code}")
    return pd.concat(all_stock_data, ignore_index=True)


def clean_stock_data(ti):
    """
    Cleans stock data from a DataFrame.

    Args:
        ti (str): Task Instance.

    Returns:
        pandas.DataFrame: Cleaned DataFrame.
    """
    df = ti.xcom_pull(task_ids='fetch_stock_data')
    # Handle missing values
    df.dropna(inplace=True)

    # Convert data types
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['open'] = pd.to_numeric(df['open'])
    df['high'] = pd.to_numeric(df['high'])
    df['low'] = pd.to_numeric(df['low'])
    df['close'] = pd.to_numeric(df['close'])
    df['volume'] = pd.to_numeric(df['volume'])

    # Handle outliers. For example, remove rows with volume etc less than 0
    df = df[df['volume'] >= 0]
    df = df[df['open'] >= 0]
    df = df[df['high'] >= 0]
    df = df[df['low'] >= 0]
    df = df[df['close'] >= 0]

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Sort by timestamp
    df.sort_values(by='timestamp', inplace=True)

    return df


def fetch_insider_transaction_data():
    all_insider_transaction_data = []
    for symbol in SYMBOLS:
        it_url = f'{BASE_URL}?function=INSIDER_TRANSACTIONS&symbol={symbol}&apikey={API_KEY}'
        response = requests.get(it_url)
        if response.status_code == 200:
            data = response.json()
            print(data)
            df = pd.DataFrame(data['data'])
            all_insider_transaction_data.append(df)
        else:
            print(f"Error fetching data for {symbol}: {response.status_code}")
    return pd.concat(all_insider_transaction_data, ignore_index=True)


def clean_insider_transaction_data(ti):
    """
    Cleans the given stock transaction data.

    Args:
        ti (str): Task Instance to fetch the data representing stock transactions.

    Returns:
        A cleaned pandas DataFrame.
    """

    df = ti.xcom_pull(task_ids='fetch_insider_transaction_data')

    # Convert numeric columns to numeric data types
    numeric_cols = ['shares', 'share_price']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Handle missing values
    df.dropna(inplace=True)  # Remove rows with missing values

    # Validate 'acquisition_or_disposal' values
    valid_values = ['A', 'D']
    df = df[df['acquisition_or_disposal'].isin(valid_values)]

    # Convert 'transaction_date' to datetime
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    return df


def fetch_technical_indicators_data():
    all_technical_indicators_data = []
    for symbol in SYMBOLS:
        for indicator in INDICATORS:
            ti_url = f'{BASE_URL}?function={indicator}&symbol={symbol}&interval={INTERVAL}&time_period=10&series_type=open&apikey={API_KEY}'
            response = requests.get(ti_url)
            if response.status_code == 200:
                data = response.json()
                df = extract_indicator_data(data, indicator)
                all_technical_indicators_data.append(df)
            else:
                print(
                    f"Error fetching data for {symbol}: {response.status_code}"
                )
    combined_df = pd.concat(all_technical_indicators_data, ignore_index=True)
    # Group by 'date' and 'symbol', then aggregate using 'first' to select the first non-null value for each indicator.
    grouped_df = combined_df.groupby(['date', 'symbol']).first()
    # Reset the index to make 'date' and 'symbol' columns
    grouped_df.reset_index(inplace=True)
    return grouped_df


def extract_indicator_data(response_data, indicator):
    """Extracts indicator data from API response.

    Args:
        response_data: JSON response from the API.

    Returns:
        A pandas DataFrame containing the extracted indicator data.
    """

    indicator_data = response_data[f'Technical Analysis: {indicator}']
    df = pd.DataFrame.from_dict(indicator_data, orient='index')
    df.reset_index(inplace=True)
    df.columns = ['date', indicator]
    df['symbol'] = response_data['Meta Data']['1: Symbol']
    df = df[['date', 'symbol', indicator]]
    return df


def clean_technical_indicators_data(ti):
    """Cleans and validates the stock data DataFrame.

    Args:
        ti (str): Task Instance to fetch the data representing technical indicators.

    Returns:
        A cleaned DataFrame.
    """

    df = ti.xcom_pull(task_ids='fetch_technical_indicators_data')
    # Check for missing values
    if df.isnull().values.any():
        print("Warning: DataFrame contains missing values.")
        # Consider strategies like imputation or removal

    # Check for invalid data types
    for col in ['SMA', 'EMA', 'DEMA']:
        if not pd.api.types.is_numeric_dtype(df[col]):
            print(f"Warning: Column '{col}' is not numeric.")
            # Convert to numeric, handle potential errors
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # convert to datetime and sort
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by='date', inplace=True)

    return df


def fetch_news_sentiment_data():
    news_url = f'{BASE_URL}?function=NEWS_SENTIMENT&apikey={API_KEY}'
    response = requests.get(news_url)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data['feed'])
    else:
        print(f"Error fetching data: {response.status_code}")
        return pd.DataFrame()


def clean_news_sentiment_data(ti):
    """Cleans and prepares a list of news articles.

    Args:
    ti (str): Task Instance to fetch the data representing news sentiment data.

    Returns:
    pd.DataFrame: A pandas DataFrame containing the cleaned data.
    """

    df = ti.xcom_pull(task_ids='fetch_news_sentiment_data')

    df['time_published'] = pd.to_datetime(
        df['time_published'].str[:8] + ' ' + df['time_published'].str[-6:]
    )

    # Clean authors column (combine and remove unnecessary entries)
    df['authors'] = df['authors'].apply(
        lambda x: ', '.join([a for a in x if a != 'Inc.'])
    )

    # Clean summary column (remove unnecessary characters)
    df['summary'] = df['summary'].str.replace(r'[()\-]', '', regex=True)

    # Extract relevant information from topics list
    df['main_topic'] = df['topics'].apply(
        lambda x: x[0]['topic'] if x.any() else None
    )
    df.drop('topics', axis=1, inplace=True)  # Remove original topics column

    # Handle potential issues with ticker_sentiment (assuming single ticker)
    df['ticker'] = df['ticker_sentiment'].apply(
        lambda x: x[0]['ticker'] if x.any() else None
    )
    df['ticker_sentiment'] = df['ticker_sentiment'].apply(
        lambda x: x[0]['ticker_sentiment_score'] if x.any() else None
    )

    # Select relevant columns
    df = df[
        [
            'title',
            'url',
            'time_published',
            'authors',
            'summary',
            'banner_image',
            'source',
            'source_domain',
            'main_topic',
            'ticker',
            'ticker_sentiment',
        ]
    ]

    return df
