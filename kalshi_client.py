import requests
import pandas as pd
import time
import io
from utils import clean_text_cols

class KalshiEventClient:
    def __init__(self, event_limit=200):
        """
        event_limit: Total number of events to fetch across all pages.
        """
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.event_limit = event_limit
        self.df_events = None

    def fetch_events(self):
        """Fetches only open events with pagination control."""
        all_events = []
        cursor = None
        remaining = self.event_limit
        
        print(f"Fetching up to {self.event_limit} open events from Kalshi...")

        while remaining > 0:
            # Kalshi max limit per page is 200
            fetch_count = min(remaining, 200)
            params = {
                "limit": fetch_count, 
                "status": "open",
                "cursor": cursor
            }
            
            response = requests.get(f"{self.base_url}/events", params=params)
            
            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break
                
            data = response.json()
            batch = data.get('events', [])
            all_events.extend(batch)
            
            # Update loop controls
            remaining -= len(batch)
            cursor = data.get('cursor')
            
            if not cursor or len(batch) == 0:
                break
                
            time.sleep(0.5) # Respect rate limits

        self.df_events = pd.DataFrame(all_events)
        return self.df_events

    def transform_events(self):
        """Standardizes columns and cleans text for matching."""
        if self.df_events is None or self.df_events.empty:
            return pd.DataFrame()

        # Map to your standard schema
        event_col_map = {
            'event_ticker': 'event_id',
            'category': 'tags',
            'title': 'event_title',
            'sub_title': 'event_sub_title'
        }

        # Select only what's needed for event matching
        available_cols = [c for c in event_col_map.keys() if c in self.df_events.columns]
        df = self.df_events[available_cols].rename(columns=event_col_map)
        
        df['platform'] = 'kalshi'

        # Apply your custom cleaning function
        df = clean_text_cols(df, exclude_cols=['event_id'])
        
        return df

# --- Usage ---
if __name__ == "__main__":
    # Control exactly how many events you want here
    client = KalshiEventClient(event_limit=500) 
    
    raw_data = client.fetch_events()
    kalshi_events = client.transform_events()
    
    print(f"Successfully processed {len(kalshi_events)} Kalshi events.")
    # kalshi_events.to_csv('data/events/kalshi_events.csv', index=False)