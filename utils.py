import pandas as pd

def clean_text_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes text, removes junk, and converts financial symbols
    to readable text (USD, BTC, etc.).
    """
    string_cols = df.select_dtypes(include=['object']).columns

    # Define a mapping for common symbols to clean strings
    symbol_map = {
        r'\$': 'usd ',
        r'€': 'eur ',
        r'£': 'gbp ',
        r'₿': 'btc ',
        r'Ξ': 'eth ',
        r'_{2,}': '',     # Matches 2 or more underscores (___ or __)
        r'\.{2,}': '',    # Matches 2 or more literal dots (...)
        r'\s{2,}': ' '    # Collapses double spaces into one
    }

    for col in string_cols:
        # Convert to string/lower/strip first
        series = df[col].astype(str).str.lower().str.strip()
        
        # Apply all symbol replacements in one loop
        for symbol, replacement in symbol_map.items():
            series = series.str.replace(symbol, replacement, regex=True)
            
        df[col] = series
        
    return df


def clean_datetime_cols(df: pd.DataFrame, date_cols: list = None) -> pd.DataFrame:
    """
    Converts specified columns to datetime and formats them to YYYY-MM-DD.
    If no columns are provided, it skips.
    """
    if date_cols is None:
        return df

    for col in date_cols:
        if col in df.columns:
            # errors='coerce' turns unparseable dates into NaT instead of crashing
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
            
    return df


if __name__ == '__main__':
    print('---Functions Loaded---')