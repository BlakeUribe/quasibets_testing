import pandas as pd
import re

def clean_text_cols(df: pd.DataFrame, exclude_cols: list = None) -> pd.DataFrame:
    df = df.copy() # Avoid SettingWithCopyWarning
    if exclude_cols is None:
        exclude_cols = ['event_id', 'series_id', 'match_key']

    all_string_cols = df.select_dtypes(include=['object']).columns
    target_cols = [c for c in all_string_cols if c not in exclude_cols]
    
    # Order matters: Clean symbols FIRST, then expand months
    for col in target_cols:
        s = df[col].astype(str).str.lower().str.strip()
        
        # 1. Handle Currencies and specific symbols
        s = s.replace({'\$': 'usd ', '₿': 'btc '}, regex=True)
        
        # 2. Split alphanumeric (tx-18 -> tx 18)
        s = s.str.replace(r'([a-z])[-_]([0-9])', r'\1 \2', regex=True)
        s = s.str.replace(r'([0-9])[-_]([a-z])', r'\1 \2', regex=True)
        
        # 3. Strip "Will/Who" prefixes
        s = s.str.replace(r'^(will the|will a|will|who will)\s+', '', regex=True)
        
        # 4. Remove all non-alphanumeric (Clean punctuation BEFORE month expansion)
        s = s.str.replace(r'[^a-z0-9\s]', ' ', regex=True)
        
        # 5. Month Expansion (Now \b works perfectly because punctuation is gone)
        months = {
            r'\bjan\b': 'january', r'\bfeb\b': 'february', r'\bmar\b': 'march',
            r'\bapr\b': 'april', r'\bjun\b': 'june', r'\bjul\b': 'july', #skip may
            r'\baug\b': 'august', r'\bsep\b': 'september', r'\boct\b': 'october',
            r'\bnov\b': 'november', r'\bdec\b': 'december'
        }
        for pat, rep in months.items():
            s = s.str.replace(pat, rep, regex=True)
            
        # 6. Final whitespace collapse
        df[col] = s.str.replace(r'\s{2,}', ' ', regex=True).str.strip()
        
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


KEYWORD_DF = pd.read_csv('data/keywords.csv')

def create_match_keys(df, text_cols, keyword_df=KEYWORD_DF):
    """
    Pass a DataFrame and a list of columns. 
    Returns the DataFrame with a new 'match_key' column.
    """
    # 1. Prepare Anchor Set (Do this once per call)
    flattened_anchors = set()
    for kw in keyword_df['keyword'].dropna().unique():
        flattened_anchors.update(str(kw).lower().split())

    # 2. Combine all requested text columns into one temporary search string
    # We join with a space to prevent words from sticking together
    combined_text = df[text_cols[0]].fillna('').astype(str).str.lower()
    for col in text_cols[1:]:
        combined_text = combined_text + " " + df[col].fillna('').astype(str).str.lower()

    # 3. Internal helper for the row-level logic
    def _process_row(text, event_id):
        # Extract Year
        year_match = re.search(r'20\d{2}', text)
        if year_match:
            year = year_match.group(0)
        else:
            # Check event_id for YY format (e.g., -26)
            id_year = re.search(r'-(\d{2})', str(event_id))
            year = f"20{id_year.group(1)}" if id_year else "9999"

        # Clean and Tokenize
        clean_text = re.sub(r'[^a-z0-9\s]', '', text)
        tokens = set(clean_text.split())
        
        # Match against anchors
        found_elements = sorted(list(tokens & flattened_anchors))
        
        if not found_elements:
            return f"unknown_{year}"
        
        return f"{'_'.join(found_elements)}_{year}"

    # 4. Apply the logic
    # We zip the combined text and event_id for speed
    df['match_key'] = [
        _process_row(txt, eid) 
        for txt, eid in zip(combined_text, df['event_id'])
    ]
    
    return df


if __name__ == '__main__':
    print('---Functions Loaded---')