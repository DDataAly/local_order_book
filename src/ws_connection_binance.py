# Establish a websocket connection to a crypto exchange and ensure it closes properly
# Save a local copy of the order book
# Subscribe to a relevant channel
# Define methods to process information
# Impement cheksum func to ensure the correctness of the local order book

import asyncio
import websockets
import json
uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'


async def save_data_sample(websocket):
    print('cats')
    cnt = 0 
    while cnt < 3:
        response = await websocket.recv()
        with open ('data/sample.json','a') as file:
            json.dump(response, file)
        cnt = cnt+1

    print ('Loop is finished')
    


async def hello():
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
            print(f'This is the first response: {response}')

            # Logic - in this case saving a sample of server requests
            await save_data_sample(websocket)
            print('All done')
    except Exception as e:
        print(f"Something went wrong: {e}")
    finally:
        print("WebSocket connection closed")

asyncio.run(hello())