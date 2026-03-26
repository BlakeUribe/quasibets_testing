import pandas as pd
import re

def clean_text_cols(df: pd.DataFrame, exclude_cols: list = None) -> pd.DataFrame:
    """
    Standardizes text and expands alphanumeric identifiers (like tx-18) 
    so the anchor-based matching system can 'see' the individual tokens.
    """
    if exclude_cols is None:
            exclude_cols = ['event_id', 'series_id', 'match_key']

    # Get all object columns EXCEPT the ones in our exclude list
    all_string_cols = df.select_dtypes(include=['object']).columns
    target_cols = [c for c in all_string_cols if c not in exclude_cols]
    
    # Updated mapping to handle "tx-18" style splits
    symbol_map = {
        # 1. Alphanumeric Splitter (The Fix for 'tx-18')
        # This inserts a space between letters and numbers connected by hyphens/underscores
        r'([a-z])[-_]([0-9])': r'\1 \2', 
        r'([0-9])[-_]([a-z])': r'\1 \2',
        
        # 2. Currency & Crypto
        r'\$': 'usd ',
        r'€': 'eur ',
        r'£': 'gbp ',
        r'₿': 'btc ',
        r'Ξ': 'eth ',
        
        # 3. Noise artifacts
        r'_{2,}': '',      
        r'\.{2,}': '',     
        
        # 4. Prediction Market Question Stripping
        r'^will a\s+': '',
        r'^will the\s+': '',
        r'^will\s+': '',
        r'^who will\s+': '',
        r'\?': '',         
        
        # 5. Final Cleanup: Collapses multiple spaces and non-alphanumeric noise
        r'[^a-z0-9\s]': ' ', # Replaces remaining punctuation with space
        r'\s{2,}': ' '     
    }

    for col in target_cols:
        series = df[col].astype(str).str.lower().str.strip()
        
        for pattern, replacement in symbol_map.items():
            series = series.str.replace(pattern, replacement, regex=True)
            
        df[col] = series.str.strip()
        
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