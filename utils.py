import pandas as pd
import re

def clean_text_cols(df: pd.DataFrame, exclude_cols: list = None) -> pd.DataFrame:
    if exclude_cols is None:
        exclude_cols = ['event_id', 'series_id', 'match_key']

    all_string_cols = df.select_dtypes(include=['object']).columns
    target_cols = [c for c in all_string_cols if c not in exclude_cols]
    
    symbol_map = {
        # 1. Month Expansion (The Fix for 'mar' -> 'march')
        r'\bjan\b': 'january',
        r'\bfeb\b': 'february',
        r'\bmar\b': 'march',
        r'\bapr\b': 'april',
        # 'may' is already a word, usually left alone
        r'\bjun\b': 'june',
        r'\bjul\b': 'july',
        r'\baug\b': 'august',
        r'\bsep\b': 'september',
        r'\boct\b': 'october',
        r'\bnov\b': 'november',
        r'\bdec\b': 'december',

        # 2. Alphanumeric Splitter (tx-18)
        r'([a-z])[-_]([0-9])': r'\1 \2', 
        r'([0-9])[-_]([a-z])': r'\1 \2',
        
        # 3. Currency & Crypto
        r'\$': 'usd ',
        r'₿': 'btc ',
        
        # 4. Prediction Market Question Stripping
        r'^will a\s+': '',
        r'^will the\s+': '',
        r'^will\s+': '',
        r'^who will\s+': '',
        r'\?': '',         
        
        # 5. Final Cleanup
        r'[^a-z0-9\s]': ' ', 
        r'\s{2,}': ' '     
    }

    for col in target_cols:
        # Pre-process: lowercase and strip before regex loop
        series = df[col].astype(str).str.lower().str.strip()
        
        for pattern, replacement in symbol_map.items():
            series = series.str.replace(pattern, replacement, regex=True)
            
        df[col] = series.str.strip()
        
    return df

def clean_datetime_cols(df: pd.DataFrame, date_cols: list = None) -> pd.DataFrame:
    """
    Converts specified columns to datetime, handling UTC/ISO formats,
    and formats them to YYYY-MM-DD strings.
    """
    if date_cols is None:
        return df

    for col in date_cols:
        if col in df.columns:
            temp_dt = pd.to_datetime(df[col], utc=True, errors='coerce')
            df[col] = temp_dt.dt.strftime('%Y-%m-%d')
            df.loc[df[col] == 'NaT', col] = None
            
    return df


if __name__ == '__main__':
    print('---Functions Loaded---')