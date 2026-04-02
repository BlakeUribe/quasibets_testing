import asyncio
import websockets
import json
import aiohttp

class PolyOrderBookWebsocket:
    # 1. Add 'on_update' parameter to __init__
    def __init__(self, match_map, on_update=None):
        """
        :param match_map: { 'poly_market_id': internal_match_id }
        :param on_update: An async function to call whenever data arrives
        """
        self.match_map = {str(k): v for k, v in match_map.items()}
        self.token_map = {}
        self.state = {}
        self.on_update = on_update # Store the callback function
        self.poly_ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    async def prepare_tokens(self):
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
        print("Token map initialized.")

    def _get_best_prices(self, bids, asks):
        best_bid = max([float(x['price']) for x in bids]) if bids else None
        best_ask = min([float(x['price']) for x in asks]) if asks else None
        return best_bid, best_ask

    async def start(self):
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
                            poly_id = next((k for k, v in self.token_map.items() if asset_id in v['tokens']), None)
                            if not poly_id: continue
                            
                            match_id = self.match_map.get(poly_id)
                            side = "YES" if asset_id == self.token_map[poly_id]['tokens'][0] else "NO"
                            bid, ask = self._get_best_prices(msg.get('bids', []), msg.get('asks', []))

                            if match_id not in self.state:
                                self.state[match_id] = {
                                    "YES": {"bid": None, "ask": None}, 
                                    "NO": {"bid": None, "ask": None}
                                }
                            
                            self.state[match_id][side]["bid"] = bid
                            self.state[match_id][side]["ask"] = ask

                            output = {
                                "platform": "POLY",
                                "match_id": match_id,
                                "poly_id": poly_id,
                                "last_updated_side": side,
                                "data": self.state[match_id]
                            }

                            # 2. TRIGGER THE CALLBACK
                            if self.on_update:
                                # We 'await' it because it's an async function
                                await self.on_update(output)
                            else:
                                # Fallback if no callback was provided
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

    


