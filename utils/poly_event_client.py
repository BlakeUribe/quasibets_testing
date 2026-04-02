import requests
import pandas as pd
import time
from .helpers import clean_text_cols

class PolymarketEventClient:
    def __init__(self, event_limit=500):
        """
        event_limit: Total number of active events to fetch.
        """
        self.base_url = "https://gamma-api.polymarket.com/events"
        self.event_limit = event_limit
        self.df_events = None

    def fetch_events(self):
        """Fetches raw event data from Gamma API with pagination control."""
        all_events = []
        offset = 0
        limit_per_page = 100
        
        print(f"Fetching up to {self.event_limit} Polymarket events...")
        
        while len(all_events) < self.event_limit:
            params = {
                "active": "true",
                "closed": "false",
                "limit": min(limit_per_page, self.event_limit - len(all_events)),
                "offset": offset,
                "order": "id",
                "ascending": "false"
            }
            
            try:
                response = requests.get(self.base_url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data or len(data) == 0:
                    break
                    
                all_events.extend(data)
                offset += len(data)
                time.sleep(0.3) # Politeness delay
                
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                break

        self.df_events = pd.DataFrame(all_events)
        return self.df_events

    def _process_tags(self, df):
        """Flattens the nested tags list into a comma-separated string."""
        if 'tags' not in df.columns:
            return pd.Series(index=df.index, dtype='object')
            
        def extract_tags(tag_list):
            if isinstance(tag_list, list):
                return ", ".join([t.get('label', '') for t in tag_list if isinstance(t, dict)])
            return ""
            
        return df['tags'].apply(extract_tags)

    def transform_events(self):
        """Standardizes columns and cleans text for matching."""
        if self.df_events is None or self.df_events.empty:
            return pd.DataFrame()

        # 1. Map Columns
        event_map = {
            'id': 'event_id',
            'title': 'event_title',
            # 'description': 'description', # too redundant with title, can be added back if needed
        }
        
        df = self.df_events[list(event_map.keys())].copy()
        df = df.rename(columns=event_map)
        
        # 2. Add Tags and Platform
        df['tags'] = self._process_tags(self.df_events)
        df['platform'] = 'poly'

        # 3. Clean Text using updated utils
        # We clean 'event_title' and 'description' for better matching
        df = clean_text_cols(df, exclude_cols=['event_id'])
        
        return df

# --- Usage ---
if __name__ == "__main__":
    # Control the fetch size here
    client = PolymarketEventClient(event_limit=500)
    
    raw_data = client.fetch_events()
    poly_events = client.transform_events()
    
    print(f"Successfully processed {len(poly_events)} Polymarket events.")
    # poly_events.to_csv('data/events/poly_events.csv', index=False)