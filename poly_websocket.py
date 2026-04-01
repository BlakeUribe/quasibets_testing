import asyncio
import websockets
import json
import aiohttp

class PolyOrderBookWebsocket:
    def __init__(self, match_map):
        """
        :param match_map: { 'poly_market_id': internal_match_id }
        """
        self.match_map = {str(k): v for k, v in match_map.items()}
        self.token_map = {}  # Will be populated by prepare_tokens()
        self.state = {}      # Stores latest prices: {match_id: {'yes': bid, 'no': bid}}
        self.poly_ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    async def prepare_tokens(self):
        """Automatically fetches CLOB tokens for all markets in your match_map."""
        print(f"Fetching token IDs for {len(self.match_map)} markets...")
        async with aiohttp.ClientSession() as session:
            for poly_id in self.match_map.keys():
                url = f"https://gamma-api.polymarket.com/markets/{poly_id}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        tokens = json.loads(data.get('clobTokenIds', '[]'))
                        if tokens:
                            self.token_map[poly_id] = {"tokens": tokens}
                        else:
                            print(f"⚠️ Warning: No tokens found for Poly ID {poly_id}")
        print("Token map initialized.")

    def _get_best_prices(self, bids, asks):
        """Extracts the highest bid and lowest ask (Top of Book)."""
        best_bid = max([float(x['price']) for x in bids]) if bids else None
        best_ask = min([float(x['price']) for x in asks]) if asks else None
        return best_bid, best_ask

    async def start(self):
        """Connects to the WebSocket and displays unified data."""
        if not self.token_map:
            await self.prepare_tokens()

        all_asset_ids = []
        for info in self.token_map.values():
            all_asset_ids.extend(info['tokens'])

        async with websockets.connect(self.poly_ws_url) as ws:
            print("📡 Connected to Polymarket WebSocket!")
            
            subscribe_msg = {
                "type": "subscribe", 
                "assets_ids": all_asset_ids,
                "channels": ["book"]
            }
            await ws.send(json.dumps(subscribe_msg))

            while True:
                try:
                    raw = await ws.recv()
                    if raw == "PING":
                        await ws.send("PONG")
                        continue
                    
                    data = json.loads(raw)
                    messages = data if isinstance(data, list) else [data]

                    for msg in messages:
                        if msg.get('event_type') == 'book':
                            asset_id = msg.get('asset_id')
                            
                            # 1. Identify the market and side
                            poly_id = next((k for k, v in self.token_map.items() if asset_id in v['tokens']), None)
                            if not poly_id: 
                                continue
                            
                            match_id = self.match_map.get(poly_id)
                            side = "YES" if asset_id == self.token_map[poly_id]['tokens'][0] else "NO"
                            
                            # 2. Extract best prices using your helper
                            bid, ask = self._get_best_prices(msg.get('bids', []), msg.get('asks', []))

                            # 3. Update internal state
                            if match_id not in self.state:
                                self.state[match_id] = {
                                    "YES": {"bid": None, "ask": None}, 
                                    "NO": {"bid": None, "ask": None}
                                }
                            
                            self.state[match_id][side]["bid"] = bid
                            self.state[match_id][side]["ask"] = ask

                            # 4. JSON / DICT OUTPUT
                            # We wrap the match data in a consistent schema
                            output = {
                                "platform": "POLY",
                                "match_id": match_id,
                                "poly_id": poly_id,
                                "last_updated_side": side,
                                "data": self.state[match_id]
                            }

                            # To output as a dictionary:
                            # print(output)

                            # To output as a clean JSON string (best for logging/piping):
                            print(json.dumps(output))

                except Exception as e:
                    print(f"Loop Error: {e}")
                    break

if __name__ == "__main__":
    import pandas as pd

    arb_candidates = pd.read_json('data/arb_candidates.json')
    market_to_match_map = {row['market_ids']['poly']: row['match_id'] for _, row in arb_candidates.iterrows()}


    collector = PolyOrderBookWebsocket(market_to_match_map)
    asyncio.run(collector.start())

    


