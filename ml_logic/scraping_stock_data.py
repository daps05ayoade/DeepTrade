import pandas as pd
import requests

def get_data_from_url(url: str):
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception if the request was unsuccessful
    return response.json()

def get_technical_data(symbol: str, feature: str, api_key: str):
    url = f'https://www.alphavantage.co/query?function={feature}&symbol={symbol}&interval=daily&time_period=10&series_type=close&apikey={api_key}'
    data = get_data_from_url(url)
    key = 'Technical Analysis: ' + feature

    # Check if the response contains an error
    if "Error Message" in data:
        raise ValueError(f"Error retrieving {feature} for {symbol}: {data['Error Message']}")
    if key not in data:
        raise ValueError(f"Unexpected API response structure when fetching {feature} for {symbol}")

    if feature == 'BBANDS':
        df = pd.DataFrame(data[key]).T
        df = df.rename(columns={
            'Real Upper Band': 'upper_band',
            'Real Middle Band': 'middle_band',
            'Real Lower Band': 'lower_band'
        })
        for col in ['upper_band', 'middle_band', 'lower_band']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    elif feature == 'MACD':
        df = pd.DataFrame(data[key]).T
        df = df.rename(columns={
            'MACD': 'macd',
            'MACD_Signal': 'macd_signal',
            'MACD_Hist': 'macd_hist'
        })
        for col in ['macd', 'macd_signal', 'macd_hist']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    elif feature == 'STOCH':
        df = pd.DataFrame(data[key]).T
        df = df.rename(columns={
            'SlowK': 'slowk',
            'SlowD': 'slowd'
        })
        for col in ['slowk', 'slowd']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    else:
        df = pd.DataFrame(data[key]).T

    return df

def get_stock_data(symbol: str, api_key: str):
    api_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&apikey={api_key}'

    # Obtain and process stock data
    data = get_data_from_url(api_url)
    df = pd.DataFrame(data['Time Series (Daily)']).T

    # Assign new column names
    column_names = ['open','high','low','close','adj_close','volume', 'dividend', 'split_coeff']
    df.columns = column_names

    # Convert columns to numeric
    for col in column_names:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Obtain technical data and merge
    features = ['EMA', 'MACD', 'BBANDS', 'RSI', 'STOCH', 'ATR']
    for feature in features:
        tech_df = get_technical_data(symbol, feature, api_key)
        df = df.join(tech_df, rsuffix=f'_{feature}')

    # Convert all column names to lower case
    df.columns = df.columns.str.lower()

    # Set the index to datetime
    df.index = pd.to_datetime(df.index)

    # Sort Index in ascending order
    df = df.sort_index(ascending=True)

    df['returns'] = df['adj_close'].pct_change()

    df['rolling_std'] = df['returns'].rolling(10).std()

    # Create target column
    df['target'] = df['adj_close'].shift(-1)

    # Drop NA values
    df.dropna(inplace=True)

    return df
