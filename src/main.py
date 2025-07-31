import asyncio
from datetime import timedelta, datetime
import websockets
import json
import os
import requests
from utils.helpers import save_data_sample

uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'
ws_buffer=[]
dir_path = (os.path.dirname(os.path.abspath('__main__')))

async def ws_ingestion(queue, websocket):
    while True:
        response = await websocket.recv() # The first response from the server is the subscription confirmation not the actual data
        ws_stream = json.loads(response)
        await queue.put(ws_stream)

async def to_do_processing_logic(ws_stream):
    pass

async def ws_processing(queue):
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
            await subscribe_websocket(websocket)

            queue =  asyncio.Queue()
            ws_ingestion_task = asyncio.create_task(ws_ingestion(queue,websocket))
            ws_processing_task = asyncio.create_task(ws_processing(queue))
            
            await asyncio.sleep(2)
            ws_ingestion_task.cancel()
            ws_processing_task.cancel()
            await asyncio.gather(ws_ingestion_task, ws_processing_task, return_exceptions=True)
            
            print('All done')

    except Exception as e:
        print(f"Something went wrong: {e}")
    finally:
        print("WebSocket connection closed")


async def run_for_duration(duration_seconds = 1):

    start_time = datetime.now()
    end_time = start_time + timedelta(duration_seconds)
    while datetime.now() < end_time:
        await run_code()
    print ('Running is done for the test time')



asyncio.run(run_for_duration())


# def get_order_book_snapshot():
#     snapshot = requests.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20').json()
#     with open ((dir_path +'/data/initial_snapshot.json'), 'w') as file:
#         json.dump(snapshot, file)
#     print('Job done')

# get_order_book_snapshot()     




# async def main():
#     queue = asyncio.Queue()
#     ws_ingestion_task = asyncio.create.task(ws_ingestion(queue))
#     ws_processing_task = asyncio.create_task(ws_processing(queue))
#     await asyncio.gather(ws_ingestion_task, ws_processing_task)





# get_order_book_snapshot()


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
 
 
# async def pop_earliest_stream(ws_buffer):
#     try:
#         return ws_buffer[0], ws_buffer[1:]
#     except IndexError:
#         print('The buffer is empty')


# while queue:
#     item, queue = pop_first(queue)
#     process(item)




# project_directory = os.path.dirname(os.path.abspath('__main__'))
# path = os.makedirs((project_directory+'/data/ws_updates'), exist_ok=True)
# print(project_directory)
