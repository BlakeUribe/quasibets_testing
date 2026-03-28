import requests
import pandas as pd
import time
from utils import clean_text_cols, clean_datetime_cols

class KalshiClient:
    def __init__(self, max_event_pages=10, max_market_pages=5):
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2"
        self.max_event_pages = max_event_pages
        self.max_market_pages = max_market_pages
        self.raw_events = None
        self.raw_markets = None
        self.master_df = None

    def _fetch_paginated(self, endpoint, limit=200, params=None):
        """Generic paginator for Kalshi API endpoints."""
        all_data = []
        cursor = None
        url = f"{self.base_url}/{endpoint}"
        
        # Determine how many pages to fetch based on endpoint
        pages = self.max_event_pages if endpoint == 'events' else self.max_market_pages
        
        for page in range(pages):
            current_params = {"limit": limit, "cursor": cursor}
            if params:
                current_params.update(params)
            
            response = requests.get(url, params=current_params)
            if response.status_code != 200:
                print(f"Error {response.status_code} on {endpoint}")
                break
                
            data = response.json()
            batch = data.get(endpoint, [])
            all_data.extend(batch)
            
            cursor = data.get('cursor')
            if not cursor:
                break
            time.sleep(0.5)
            
        return pd.DataFrame(all_data)

    def fetch_data(self):
        """Fetches both events and markets."""
        print("Fetching Kalshi events...")
        self.raw_events = self._fetch_paginated('events', params={"status": "open"})
        
        print("Fetching Kalshi markets...")
        market_params = {'mve_filter': 'exclude', 'status': 'open'}
        self.raw_markets = self._fetch_paginated('markets', limit=1000, params=market_params)
        
        return self.raw_events, self.raw_markets

    def _backfill_missing_events(self, market_df, event_df):
        """Identifies markets without events and fetches them individually."""
        all_market_tickers = market_df['event_ticker'].unique()
        known_event_tickers = event_df['event_ticker'].unique() if not event_df.empty else []
        missing_tickers = [t for t in all_market_tickers if t not in known_event_tickers]

        if not missing_tickers:
            return event_df
        
        if len(missing_tickers) > 500:
            missing_tickers = missing_tickers[:500]
            print(f"Adjusting missing events to 500 to avoid excessive API calls.")

        print(f"Backfilling {len(missing_tickers)} missing events from Kalshi...")
        backfilled = []
        for ticker in missing_tickers:
            res = requests.get(f"{self.base_url}/events/{ticker}")
            if res.status_code == 200:
                backfilled.append(res.json().get('event', {}))
            time.sleep(0.1)
        
        if backfilled:
            new_events_df = pd.DataFrame(backfilled)
            return pd.concat([event_df, new_events_df], ignore_index=True)
        return event_df

    def transform_data(self):
        if self.raw_markets is None or self.raw_markets.empty:
            raise ValueError("No market data found. Run fetch_data() first.")

        # 1. Map Events
        event_col_map = {
            'series_ticker': 'series_id',
            'event_ticker': 'event_id',
            'category': 'tags',
            'title': 'event_title',
            'sub_title': 'event_sub_title'
        }
        
        # Handle Backfill logic
        full_events = self._backfill_missing_events(self.raw_markets, self.raw_events)
        full_events = full_events[[c for c in event_col_map.keys() if c in full_events.columns]].rename(columns=event_col_map)

        # 2. Map Markets
        # Create description from rules
        m_df = self.raw_markets.copy()
        m_df['description'] = m_df.get('rules_primary', '') + m_df.get('rules_secondary', '')

        market_col_map = {
            'ticker': 'market_id',
            'event_ticker': 'event_id',
            'yes_ask_dollars': 'yes_ask',
            'yes_bid_dollars': 'yes_bid',
            'close_time': 'close_time',
            'expiration_time': 'expiration'
        }

        m_df = m_df[[c for c in market_col_map.keys() if c in m_df.columns] + ['description']].rename(columns=market_col_map)

        # 3. Merge
        # Get unique columns from events that aren't already in markets
        cols_to_use = full_events.columns.difference(m_df.columns).tolist()
        cols_to_use.append('event_id')

        self.master_df = pd.merge(m_df, full_events[cols_to_use], on='event_id', how='inner')
        self.master_df['platform'] = 'kalshi'

        # 4. Clean using updated utils
        self.master_df = clean_text_cols(self.master_df)
        self.master_df = clean_datetime_cols(self.master_df, date_cols=['close_time', 'expiration'])
        
        return self.master_df

    def get_separated_dfs(self):
        if self.master_df is None:
            self.transform_data()
        
        # Separate unique events for the events.csv
        event_cols = ['event_id', 'event_title', 'event_sub_title',  'platform']
        existing_cols = [c for c in event_cols if c in self.master_df.columns]
        
        events_df = self.master_df[existing_cols].drop_duplicates(subset=['event_id']).copy()
        return self.master_df, events_df

# --- Usage ---
if __name__ == "__main__":
    kalshi = KalshiClient()
    # kalshi.fetch_data()
    # markets, events = kalshi.get_separated_dfs()
    
    # markets.to_csv('data/markets/kalshi_markets.csv', index=False)
    # events.to_csv('data/events/kalshi_events.csv', index=False)
    print("Kalshi processing complete.")