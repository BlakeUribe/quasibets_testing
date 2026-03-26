import pandas as pd

import pandas as pd

def clean_text_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes text, removes financial noise, and strips prediction 
    market 'question' prefixes to leave the core subject for matching.
    """
    string_cols = df.select_dtypes(include=['object']).columns

    # The mapping handles symbols, noise, and question-style prefixes
    symbol_map = {
        # Currency & Crypto
        r'\$': 'usd ',
        r'€': 'eur ',
        r'£': 'gbp ',
        r'₿': 'btc ',
        r'Ξ': 'eth ',
        
        # Polymarket & Noise artifacts
        r'_{2,}': '',      # Matches ___ or __
        r'\.{2,}': '',     # Matches ... or ..
        
        # Prediction Market Question Stripping (Kalshi style)
        r'^will a\s+': '',
        r'^will the\s+': '',
        r'^will\s+': '',
        r'^who will\s+': '',
        r'\?': '',         # Removes all question marks
        
        # Cleanup
        r'\s{2,}': ' '     # Collapses multiple spaces into one
    }

    for col in string_cols:
        # Initial lower and strip
        series = df[col].astype(str).str.lower().str.strip()
        
        # Apply all logic in the symbol map
        for pattern, replacement in symbol_map.items():
            series = series.str.replace(pattern, replacement, regex=True)
            
        # Final strip to catch anything left by the prefix removal
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