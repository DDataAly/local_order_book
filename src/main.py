import asyncio
import websockets
import json
import requests
from utils.helpers import path_initial_shapshot


uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'

async def send_subscription_request(websocket):
    await websocket.send(
        json.dumps({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@depth@100ms"], 
            "id": 1}))
    response = await websocket.recv()
    return (response)


async def is_subscription_confirmed(response) -> bool:
    response_dict = json.loads(response)
    if response_dict.keys() == {'result','id'}:
        return True
    elif response_dict.get('e') == "depthUpdate":
        return True
    return False


async def ws_ingestion(queue, websocket):
    # Infinite ingestion function
    while True:
        print ('Trying to ingest')
        response = await websocket.recv() 
        # print('Trying to parse')
        # ws_stream = json.loads(response)
        print('Trying to put in the queue')
        await queue.put(response)



async def is_ws_in_order_book(response, queue, replace_order_book = True, order_book_ts = None) -> bool:
    parsed = json.loads(response)
    ws_first_update_id = parsed['U']
    ws_final_update_id = parsed ['u']
    if replace_order_book:
        order_book_ts = get_order_book()
    is_aligned = False 

    while is_aligned == False:
        if ws_first_update_id <= order_book_ts < ws_final_update_id:
            is_aligned = True
    
        if order_book_ts < ws_first_update_id:
            print('Replacing order book')
            return await is_ws_in_order_book(response, queue, replace_order_book = True)

        if ws_final_update_id <= order_book_ts:
            print('Requesting a new ws')
            response =  await queue.get() 
            return await is_ws_in_order_book(response, queue, replace_order_book = False, order_book_ts=order_book_ts)     
    return is_aligned


async def to_do_processing_logic():
    pass


async def ws_processing(queue):
    # Infinite processing function
    continue_search = True
    while continue_search:
        ws_stream =  await queue.get()
        is_aligned = await is_ws_in_order_book(ws_stream, queue)
        print (is_aligned)
        if is_aligned == True:
            print ('The match is found')
            continue_search = False
        else:
            continue

    await to_do_processing_logic()


async def run_code():
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to server")
            #Run outside the queue to guarantee the ordering
            response = await send_subscription_request(websocket)
            if await is_subscription_confirmed(response):
                print ('Subscription is confirmed')


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



def get_order_book() -> int:
    snapshot = requests.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20').json()
    with open ((path_initial_shapshot), 'w') as file:
        json.dump(snapshot, file)
    print('Order book is saved')
    order_book_timestamp = snapshot.get("lastUpdateId")
    return order_book_timestamp

get_order_book()     

asyncio.run(run_code())





# # Establish a websocket connection to a crypto exchange and ensure it closes properly ✅ 
# # Subscribe to a relevant channel ✅ 
# # Save a local copy of the order book ✅ 

# # Define methods to process information
# # Impement cheksum func to ensure the correctness of the local order book






# project_directory = os.path.dirname(os.path.abspath('__main__'))
# path = os.makedirs((project_directory+'/data/ws_updates'), exist_ok=True)
# print(project_directory)
