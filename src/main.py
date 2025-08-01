import asyncio
import websockets
import json
import requests
from utils.helpers import path_initial_shapshot


uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'
ws_buffer=[]


async def ws_ingestion(queue, websocket):
    # Infinite ingestion function
    while True:
        response = await websocket.recv() # The first response from the server is the subscription confirmation not the actual data
        ws_stream = json.loads(response)
        await queue.put(ws_stream)

async def to_do_processing_logic(ws_stream):
    pass

async def ws_processing(queue):
    # Infinite processing function
    while True:
        ws_stream =  await queue.get()
        await to_do_processing_logic(ws_stream) #tbc it we should await here - depends on inner logic

async def subscribe_websocket(websocket):
    await websocket.send(
        json.dumps({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@depth@100ms"], 
            "id": 1}))
    print ("Subscribed to the channel")


async def run_code():
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to server")
            #Run outside the queue to guarantee the ordering
            await subscribe_websocket(websocket)

            queue =  asyncio.Queue()
            ws_ingestion_task = asyncio.create_task(ws_ingestion(queue,websocket))
            ws_processing_task = asyncio.create_task(ws_processing(queue))
            
            # Run coroutines for 5 sec - prevents an infinite loop by raising a TimeOut Error
            try: 
                 await asyncio.wait_for(
                      asyncio.gather(ws_ingestion_task, ws_processing_task), 
                      timeout = 5
                 )
            except asyncio.TimeoutError:
                 print('Runtime time out')

            # Cancel infinite coroutines as wait_for does't cancel them
            await asyncio.sleep(2)
            ws_ingestion_task.cancel()
            ws_processing_task.cancel()

            #Wait for tasks completion - in this case TimeOut error
            await asyncio.gather(ws_ingestion_task, ws_processing_task, return_exceptions=True)
            
            print('All done')

    except Exception as e:
        print(f"Something went wrong: {e}")
    finally:
        print("WebSocket connection closed")


asyncio.run(run_code())


def get_order_book():
    snapshot = requests.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20').json()
    with open ((path_initial_shapshot), 'w') as file:
        json.dump(snapshot, file)
    print('Order book is saved')

get_order_book()     





# # Establish a websocket connection to a crypto exchange and ensure it closes properly ✅ 
# # Subscribe to a relevant channel ✅ 
# # Save a local copy of the order book ✅ 

# # Define methods to process information
# # Impement cheksum func to ensure the correctness of the local order book


            # subscr_confirm_exp_keys = {'result','id'}
            # if p.keys() == subscr_confirm_exp_keys:
            #     print(f'Subscription is confirmed: {p}')
            # elif p['e'] == "depthUpdate":
            #     ws_buffer.append(response)
            #     print (f'Data message sent before the subscription acknowledgement')
            # else:
            #     print('Wrong response from the server')
 



# project_directory = os.path.dirname(os.path.abspath('__main__'))
# path = os.makedirs((project_directory+'/data/ws_updates'), exist_ok=True)
# print(project_directory)
