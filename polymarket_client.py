import requests
import pandas as pd
import time
from utils import clean_text_cols, clean_datetime_cols

class PolymarketClient:
    def __init__(self, target_count=5000, limit_per_page=1000):
        self.base_url = "https://gamma-api.polymarket.com/events"
        self.target_count = target_count
        self.limit_per_page = limit_per_page
        self.raw_data = None
        self.master_df = None

    def fetch_events(self):
        """Fetches raw event data from the Gamma API with pagination."""
        all_events = []
        offset = 0
        
        print(f"Fetching Polymarket events...")
        while len(all_events) < self.target_count:
            params = {
                "active": "true",
                "closed": "false",
                "limit": self.limit_per_page,
                "offset": offset,
                "order": "id",
                "ascending": "false"
            }
            
            try:
                response = requests.get(self.base_url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if not data:
                    break
                    
                all_events.extend(data)
                offset += self.limit_per_page
                time.sleep(0.5)
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                break

        self.raw_data = pd.DataFrame(all_events)
        return self.raw_data

    def _process_tags(self):
        """Internal helper to flatten and group tags."""
        tags_exploded = self.raw_data[['id', 'tags']].explode('tags').dropna()
        tag_labels = pd.json_normalize(tags_exploded['tags'])
        tag_labels['event_id'] = tags_exploded['id'].values
        
        grouped_tags = tag_labels.groupby('event_id')['label'].apply(lambda x: ', '.join(x)).reset_index()
        grouped_tags.columns = ['event_id', 'tags']
        return grouped_tags

    def transform_data(self):
        """Processes raw JSON into market and event DataFrames."""
        if self.raw_data is None or self.raw_data.empty:
            raise ValueError("No data found. Run fetch_events() first.")

        # 1. Process Tags & Events
        grouped_tags = self._process_tags()
        event_cols_map = {'id': 'event_id', 'title': 'event_title', 'description': 'description', 'startDate': 'start_date'}
        
        poly_events = self.raw_data[list(event_cols_map.keys())].rename(columns=event_cols_map)
        poly_events = pd.merge(poly_events, grouped_tags, on='event_id', how='left')

        # 2. Process Markets
        print(f"Processing Polymarket markets...")
        markets_exploded = self.raw_data[['id', 'markets']].explode('markets').dropna()
        markets_df = pd.json_normalize(markets_exploded['markets'])
        
        market_map = {
            'id': 'market_id',
            'question': 'market_subtitle',
            'bestBid': 'yes_bid',
            'bestAsk': 'yes_ask',
            'endDateIso': 'expiration'
        }
        
        markets_df['event_id'] = markets_exploded['id'].values
        markets_df = markets_df.rename(columns=market_map)
        
        # Filter for schema-compliant columns
        final_cols = [c for c in market_map.values() if c in markets_df.columns] + ['event_id']
        markets_df = markets_df[final_cols].copy()

        # 3. Final Merge and Cleaning
        self.master_df = pd.merge(markets_df, poly_events, on='event_id', how='left')
        self.master_df = clean_text_cols(self.master_df)
        self.master_df = clean_datetime_cols(self.master_df, date_cols=['start_date', 'expiration'])
        
        self.master_df['platform'] = 'poly'
        self.master_df = self.master_df.sort_values(['event_id', 'market_id']).reset_index(drop=True)
        
        return self.master_df

    def get_separated_dfs(self):
        """Returns both the full markets DF and the unique events DF."""
        if self.master_df is None:
            self.transform_data()
            
        # event_cols = ['event_id', 'event_title', 'description', 'platform'] # need to fix start date
        event_cols = ['event_id', 'event_title', 'description', 'platform']
        
        existing_cols = [c for c in event_cols if c in self.master_df.columns]
        
        events_df = self.master_df[existing_cols].drop_duplicates(subset=['event_id']).copy()
        events_df = clean_text_cols(events_df)
        
        return self.master_df, events_df



if __name__ == "__main__":
# --- Usage Example ---
    client = PolymarketClient()
    # client.fetch_events()
    # markets_df, events_df = client.get_separated_dfs()

    # # Save results
    # markets_df.to_csv('data/markets/poly_markets.csv', index=False)
    # events_df.to_csv('data/events/poly_events.csv', index=False)

    # print("Transformation complete. Events Sample:")
    # print(events_df.head())