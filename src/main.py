import asyncio
import websockets
import json
import os
import requests
from utils.helpers import save_data_sample


uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'

project_directory = os.path.dirname(os.path.abspath('__main__'))
path = os.makedirs((project_directory+'/data/ws_updates'), exist_ok=True)
print(project_directory)

def get_order_book_snapshot():
    snapshot = requests.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20').json()
    with open ((project_directory+'/data/initial_snapshot.json'), 'w') as file:
        json.dump(snapshot, file)

get_order_book_snapshot()

ws_buffer=[]
    
async def run_code():
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to server")
            # Subscribing to a particular channel
            await websocket.send(
            json.dumps({
                "method": "SUBSCRIBE",
                "params": [
                    "btcusdt@depth@100ms"
                ],
                "id": 1
                }))
            
            response = await websocket.recv() # The first response from the server is the subscription confirmation not the actual data
            print (type(response))
            p = json.loads(response)
            print (type(p))
            print(p)
           
            subscrip_confirm_exp_keys = {'result','id'}
            if p.keys() == subscrip_confirm_exp_keys:
                print(f'Subscription is confirmed: {p}')
            elif p['e'] == "depthUpdate":
                ws_buffer.append(response)
                print (f'Data message sent before the subscription acknowledgement')
            else:
                print('Wrong response from the server')
 

            # Logic - in this case saving a sample of server requests
            await save_data_sample(websocket)
            print('All done')
    except Exception as e:
        print(f"Something went wrong: {e}")
    finally:
        print("WebSocket connection closed")

asyncio.run(run_code())



# Establish a websocket connection to a crypto exchange and ensure it closes properly ✅ 
# Subscribe to a relevant channel ✅ 
# Save a local copy of the order book

# Define methods to process information
# Impement cheksum func to ensure the correctness of the local order book
