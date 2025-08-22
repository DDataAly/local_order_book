import asyncio
import websockets
import json
import aiohttp
from collections import deque
from order_book.order_book_class import OrderBook, EmptyOrderBookException
from utils.helpers import path_initial_shapshot

uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'

#region
async def send_subscription_request(websocket) -> str:
    """
    Sends a subscription request to the Binance WebSocket for depth updates.
    Args:
        websocket (websockets.WebSocketClientProtocol): The WebSocket connection to Binance.
    Returns:
        A JSON-formatted response string from Binance, which is either:
        - subscription confirmation {"result": null, "id": 1}
        - depth update message 
            {
            "e": "depthUpdate", // Event type
            "E": 1672515782136, // Event time
            "s": "BNBBTC",      // Symbol
            "U": 157,           // First update ID in event
            "u": 160,           // Final update ID in event
            "b": [              // Bids to be updated
                [
                "0.0024",       // Price level to be updated
                "10"            // Quantity
                ]
            ],
            "a": [              // Asks to be updated
                [
                "0.0026",       // Price level to be updated
                "100"           // Quantity
                ]
            ]
            }
    """
    await websocket.send(
        json.dumps({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@depth@100ms"], 
            "id": 1}))
    response = await websocket.recv()
    return (response)


async def is_subscription_confirmed(response) -> bool:
    """
    Parses the response string and checks that expected dictionary keys are present 
    Args:
        response: string returned by send_subscription_request(websocket)
    Returns:
        bool - True if keys are present otherwise False   
    """
    try:
        response_dict = json.loads(response)
    except json.JSONDecodeError as e:
        print (f'Invalid format of server response: {e}') 
        return False  
         
    if response_dict.keys() == {'result','id'}:
        return True
    elif response_dict.get('e') == "depthUpdate":
        return True
    return False


async def run_the_subscriber(websocket):
    """
    Repeatedly sends a subscription request to the Binance WebSocket for depth updates 
    until the subscription is confirmed.
    Args:
        websocket (websockets.WebSocketClientProtocol): The WebSocket connection to Binance.
    Returns:
        None   
    """
    response = await send_subscription_request(websocket)
    while not await is_subscription_confirmed(response):
        await asyncio.sleep(0.1)
        response = await send_subscription_request(websocket)
    print ('Subscription is confirmed')  


async def ws_ingestion(websocket: websockets.WebSocketClientProtocol,buffer: deque[str]):
    """
    Infinite function which receives order book prices and quantity updates and adds them to buffer
    Args:
        websocket (websockets.WebSocketClientProtocol): The WebSocket connection to Binance
        buffer: a deque to keep incoming WebSocket stream messages
    Returns:
        None 
    """
    while True:
        print ('Continue ingestion')
        response = await websocket.recv() 
        buffer.append(response)

async def get_first_depth_update_id(buffer: deque[str]) -> int:
    """
    Loops through messages in the buffer till finds a valid depth update message.
    Extracts the ID of the first update in the first valid depth update message,
    which is used to synchronise the WebSocket stream with the API order book snapshot.
    Args:
        buffer: a deque to keep incoming WebSocket stream messages
    Returns:
        int - The 'U' value (first update ID) from the first valid depth update message.
    """
    while not buffer:
        await asyncio.sleep(0.01)
    for message in buffer:    
        try:
            parsed = json.loads(message)
        except json.JSONDecodeError:
            continue    
        if 'U' in parsed:
            print(f'First depth update message in the stream is {parsed}')
            return parsed["U"]
        else:
            print(f'Skipping non-depthUpdate message: {parsed}')
        await asyncio.sleep(0.01)

#endregion

async def get_order_book() -> int:
    """
    Sends a request to get a copy of the order book from Binance REST API using aiohttp.ClientSession()
    to avoid blocking the event loop. This allows ws_ingestion to run simultaneously with this function.
    Saves the received JSON locally at the path specified by 'path_initial_shapshot'.
    Extracts and returns the 'lastUpdateId' of the saved order book copy.
    This ID is used to synchronize the order book with the WebSocket depth stream.

    Returns:
        int - The last update ID from the order book copy 
    """
    async with aiohttp.ClientSession() as session:
        async with session.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20') as response:
            snapshot = await response.json()
    order_book_last_update_id = snapshot.get("lastUpdateId")
    return snapshot, order_book_last_update_id


async def fetch_order_book_snapshot(buffer) -> None:
    while True:
        await asyncio.sleep (0.1)
        snapshot, order_book_last_update_id = await get_order_book()  
        first_received_message_id = await get_first_depth_update_id(buffer)

        if order_book_last_update_id >= first_received_message_id:
            print(f'A valid snapshot of the order book is found')
            return snapshot, order_book_last_update_id  


async def find_matching_messsage(order_book_last_update_id, buffer) -> None:
    """
    Continuously checks the buffer for the earliest depth update message with the 'u' value 
    (last update ID) greater than the 'lastUpdateId' value from the order book snapshot.
    Once found, returns this message.

    Args:
        order_book_last_update_id (int): the last update ID from the REST API snapshot of the order book
        buffer (collections.deque[str]): incoming WebSocket stream messages waiting to be processed

    Returns:
        dict - the first matching message found in the buffer
    """

    while True:
        await asyncio.sleep (0.1)
        while buffer:
            message = buffer[0]
           
            try:
                parsed = json.loads(message)
            except json.JSONDecodeError:
                buffer.popleft()
                continue
           
            if 'u' not in parsed:
                buffer.popleft()
                continue

            message_final_update_id = parsed ['u']
            if message_final_update_id > order_book_last_update_id:
                print(f'Match is found: {parsed}')
                return parsed
            else:
                buffer.popleft()
        print('No matching message found in the buffer yet.')

        
async def to_do_processing_logic():
    await asyncio.sleep (0.1)


async def ws_processing(buffer):
    # TODO
    # Infinite processing function
    while True:
        print ('Continue processing')
        await to_do_processing_logic()
    


async def orchestrator(websocket):
    buffer = deque([])
    ws_ingestion_task = None
    ws_processing_task = None
    order_book = None

    ws_ingestion_task = asyncio.create_task(ws_ingestion(websocket, buffer))

    try:
        snapshot, order_book_last_update_id = await asyncio.wait_for(fetch_order_book_snapshot(buffer), timeout=5)
        print('Suitable order book fetched, saving it now....')

    except asyncio.TimeoutError:
        print('No suitable order book fetched, can\'t proceed')
        ws_ingestion.cancel()
        await asyncio.gather(ws_ingestion_task, return_exceptions=True)
        raise  

    try:
        matching_message = await asyncio.wait_for(find_matching_messsage(order_book_last_update_id, buffer), timeout = 5)  
        print(f'Order book snapshot is fetched. Matching message is found {matching_message}. Starting processing') 
    except asyncio.TimeoutError:
        print('No suitable Websocket stream message fetched, can\'t proceed')
        ws_ingestion.cancel()
        await asyncio.gather(ws_ingestion_task, return_exceptions=True)
        raise   

    order_book = OrderBook(snapshot) 
    print(f'Order book object has been initialised with order book with the last update ID {order_book_last_update_id}')   
    with open ((path_initial_shapshot), 'w') as file:
        json.dump(snapshot, file)
    print('Order book copy is saved locally')

    ws_processing_task = asyncio.create_task(ws_processing(buffer))
    return ws_ingestion_task, ws_processing_task, order_book
    



async def run_code():
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to server")

            try:
                await asyncio.wait_for(run_the_subscriber(websocket), timeout = 5)
            except asyncio.TimeoutError:
                print('Can\'t subscribe to the requested channel')
                return
            
            ws_ingestion_task, ws_processing_task, order_book = await orchestrator(websocket)
                        
            try: 
                tasks =[ws_ingestion_task, ws_processing_task]
                await asyncio.wait_for(asyncio.gather(*tasks), timeout = 3)

            except asyncio.TimeoutError:
                print('Run time has ended')
                for task in tasks:
                    task.cancel()
                #Wait for tasks completion - in this case TimeOut error
                await asyncio.gather(ws_ingestion_task, ws_processing_task, return_exceptions=True)
            
            print('All done')

    except Exception as e:
        print(f"Something went wrong: {e}")
    finally:
        print("WebSocket connection closed")
        
asyncio.run(run_code()) #Creates the event loop and runs coroutines  









# # Establish a websocket connection to a crypto exchange and ensure it closes properly ✅ 
# # Subscribe to a relevant channel ✅ 
# # Save a local copy of the order book ✅ 

# # Define methods to process information
# # Impement cheksum func to ensure the correctness of the local order book






# project_directory = os.path.dirname(os.path.abspath('__main__'))
# path = os.makedirs((project_directory+'/data/ws_updates'), exist_ok=True)
# print(project_directory)


# Recursion version
# async def is_ws_in_order_book(response, queue, replace_order_book = True, order_book_ts = None) -> bool:
#     parsed = json.loads(response)
#     ws_first_update_id = parsed['U']
#     ws_final_update_id = parsed ['u']
#     if replace_order_book:
#         order_book_ts = get_order_book()
#     is_aligned = False 

#     while is_aligned == False:
#         if ws_first_update_id <= order_book_ts < ws_final_update_id:
#             is_aligned = True
    
#         if order_book_ts < ws_first_update_id:
#             print('Replacing order book')
#             return await is_ws_in_order_book(response, queue, replace_order_book = True)

#         if ws_final_update_id <= order_book_ts:
#             print('Requesting a new ws')
#             response =  await queue.get() 
#             return await is_ws_in_order_book(response, queue, replace_order_book = False, order_book_ts=order_book_ts)     
#     return is_aligned

# Event coordination
# async def fetch_order_book_snapshot(buffer, event) -> None:
#     while not event.is_set():
#         await asyncio.sleep (0.1)
#         order_book_last_update_id = await get_order_book()  
#         first_received_message_id = await get_first_depth_update_id(buffer)

#         if order_book_last_update_id >= first_received_message_id:
#             event.set()
#             print(f'A valid snapshot of the order book is found')
#             return order_book_last_update_id  

# Event coordination
# async def find_matching_messsage(order_book_last_update_id, buffer, event) -> None:
#     """
#     Continuously checks the buffer for the earliest depth update message with the 'u' value 
#     (last update ID) greater than the 'lastUpdateId' value from the order book snapshot.
#     Once found, saves this message in a match_content variable and sets the Asyncio event
#     to signal the event loop that it can proceed with other coroutines.


#     Args:
#         order_book_last_update_id (int): the last update ID from the REST API snapshot of the order book
#         buffer (collections.deque[str]): incoming WebSocket stream messages waiting to be processed
#         event (asyncio.Event): an event object set when the match is found

#     Returns:
#         None, the function exists once the match is found and the event was set
#     """

#     while not event.is_set():
#         await asyncio.sleep (0.1)
#         while buffer:
#             message = buffer[0]
           
#             try:
#                 parsed = json.loads(message)
#             except json.JSONDecodeError:
#                 buffer.popleft()
#                 continue
           
#             if 'u' not in parsed:
#                 buffer.popleft()
#                 continue

#             message_final_update_id = parsed ['u']
#             if message_final_update_id > order_book_last_update_id:
#                 match_content = parsed
#                 event.set()
#                 print(f'Match is found: {match_content}')
#                 return   # Not returning anything, using an event as a flag to confirm the match
#             else:
#                 buffer.popleft()
#         print('No matching message found in the buffer yet.')
 