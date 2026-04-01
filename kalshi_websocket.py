import os
import json
import asyncio
import pandas as pd
import websockets
import base64
import time
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding



class KalshiOrdeBookWebsocket:
    def __init__(self, api_id, key_path, ticker_map):
        """
        :param api_id: Kalshi API Key ID
        :param key_path: Path to the private .pem key
        :param ticker_map: { 'KALSHI_TICKER': internal_match_id }
        """
        self.api_id = api_id
        self.ticker_map = ticker_map
        self.market_tickers = list(ticker_map.keys())
        self.ws_url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        self.state = {} # Stores {match_id: {'YES': bid, 'NO': bid}}
        
        # Load Private Key for Auth
        with open(key_path, "rb") as f:
            self._priv = serialization.load_pem_private_key(f.read(), password=None)

    # --- AUTHENTICATION ---
    def _sign(self, msg):
        sig = self._priv.sign(
            msg.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def _get_headers(self):
        ts = str(int(time.time() * 1000))
        sig = self._sign(ts + "GET" + "/trade-api/ws/v2")
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.api_id,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    # --- DATA PROCESSING ---
    def _get_best_price(self, book_side):
        """Extracts max bid from Kalshi's [[price, qty], ...] format."""
        if not book_side:
            return None
        return max([float(level[0]) for level in book_side])

    async def start(self):
        headers = self._get_headers()

        async with websockets.connect(self.ws_url, additional_headers=headers) as ws:
            print(f"📡 Connected to Kalshi! Subscribing to {len(self.market_tickers)} tickers.")

            subscribe_msg = {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": self.market_tickers,
                },
            }
            await ws.send(json.dumps(subscribe_msg))

            while True:
                        try:
                            raw = await ws.recv()
                            data = json.loads(raw)
                            
                            msg_type = data.get('type')
                            if msg_type in ['orderbook_snapshot', 'orderbook_delta']:
                                msg = data.get('msg', {})
                                ticker = msg.get('market_ticker')
                                match_id = self.ticker_map.get(ticker, "UNKNOWN")

                                # 1. Extract raw prices from the message
                                new_yes = self._get_best_price(msg.get('yes_dollars_fp', []))
                                new_no = self._get_best_price(msg.get('no_dollars_fp', []))

                                # 2. Determine which side updated for your logic trigger
                                updated_side = "NONE"
                                if new_yes is not None and new_no is not None:
                                    updated_side = "BOTH"
                                elif new_yes is not None:
                                    updated_side = "YES"
                                elif new_no is not None:
                                    updated_side = "NO"

                                # 3. Update internal state
                                if match_id not in self.state:
                                    self.state[match_id] = {"YES": None, "NO": None}
                                
                                if new_yes is not None: self.state[match_id]["YES"] = new_yes
                                if new_no is not None: self.state[match_id]["NO"] = new_no

                                # 4. UNIFIED JSON OUTPUT
                                # Note: We use 'bid' here to match the Poly schema, 
                                # as Kalshi's 'yes_dollars_fp' represents the buy-side (bids).
                                output = {
                                    "platform": "KALSHI",
                                    "match_id": match_id,
                                    "ticker": ticker,
                                    "last_updated_side": updated_side,
                                    "data": {
                                        "YES": {"bid": self.state[match_id]["YES"], "ask": None}, # Kalshi API v2 ask requires separate calc/field
                                        "NO": {"bid": self.state[match_id]["NO"], "ask": None}
                                    }
                                }

                                # Output as JSON string
                                print(json.dumps(output))

                        except Exception as e:
                            print(f"Kalshi Loop Error: {e}")
                            break


if __name__ == "__main__":
    from dotenv import load_dotenv
    
    arb_candidates = pd.read_json('data/arb_candidates.json')
    market_to_match_map = {row['market_ids']['kalshi']: row['match_id'] for _, row in arb_candidates.iterrows()}

    load_dotenv()
    KALSHI_API_ID = os.environ["KALSHI_API_KEY_ID"]
    KALSHI_API_KEY_PATH = os.environ["KALSHI_API_KEY_PATH"]
    # WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"

    collector = KalshiOrdeBookWebsocket(KALSHI_API_ID, KALSHI_API_KEY_PATH, market_to_match_map)
    asyncio.run(collector.start())



