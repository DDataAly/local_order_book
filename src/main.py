import asyncio
import websockets
import json
import aiohttp
from utils.helpers import path_initial_shapshot
from collections import deque

uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'

#region
async def send_subscription_request(websocket) -> str:
    """
    Sends a subscription request to the Binance WebSocket for depth updates.
    Args:
        websocket: The WebSocket connection to Binance.
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

async def ws_ingestion(websocket: websockets.WebSocketClientProtocol,buffer: deque[str]):
    """
    Infinite function which receives order book prices and quantity updates and adds them to buffer
    Args:
        websocket: The WebSocket connection to Binance
        buffer: a deque to keep incoming WebSocket stream messages
    Returns:
        Nothing 
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
    NEED TO UPDATE
    Sends a request to get a copy of the order book from Binance REST API.
    Saves the received JSON locally at the path specified by 'path_initial_shapshot'.
    Extracts and returns the 'lastUpdateId' of the saved order book copy.
    This id is used to synchronise the order book with WebSocket depth stream.

    Returns:
        int - The last update ID from the order book copy 
    """
    async with aiohttp.ClientSession() as session:
        async with session.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20') as response:
            snapshot = await response.json()
    with open ((path_initial_shapshot), 'w') as file:
        json.dump(snapshot, file)
    print('Order book is saved')
    order_book_last_id = snapshot.get("lastUpdateId")
    return order_book_last_id


async def fetch_order_book_snapshot(buffer, event) -> None:
    """
    Does smth - #TODO

    Args:
        event (asyncio.Event): an event object set when the match is found

    Returns:
        None, the function exists once the match is found and the event was set
    """

    while not event.is_set():
        await asyncio.sleep (0.1)
        order_book_last_update_id = await get_order_book()  
        first_received_message_id = await get_first_depth_update_id(buffer)

        if order_book_last_update_id >= first_received_message_id:
            event.set()
            print(f'A valid snapshot of the order book is found')
            return    




async def find_matching_messsage(order_book_last_update_id, buffer, event) -> None:
    """
    Continuously checks the buffer for the earliest depth update message with the 'u' value 
    (last update ID) greater than the 'lastUpdateId' value from the order book snapshot.
    Once found, saves this message in a match_content variable and sets the Asyncio event
    to signal the event loop that it can proceed with other coroutines.


    Args:
        order_book_last_update_id (int): the last update ID from the REST API snapshot of the order book
        buffer (collections.deque[str]): incoming WebSocket stream messages waiting to be processed
        event (asyncio.Event): an event object set when the match is found

    Returns:
        None, the function exists once the match is found and the event was set
    """

    while not event.is_set():
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
                match_content = parsed
                event.set()
                print(f'Match is found: {match_content}')
                return   # Not returning anything, using an event as a flag to confirm the match
            else:
                buffer.popleft()
        print('No matching message found in the buffer yet.')

        
async def to_do_processing_logic():
    pass


async def ws_processing(buffer):
    # Infinite processing function
    await to_do_processing_logic()


async def run_code():
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to server")
            #Run outside the queue to guarantee the ordering
            response = await send_subscription_request(websocket)
            if await is_subscription_confirmed(response):
                print ('Subscription is confirmed')
            
            buffer = deque([])

            
            ws_ingestion_task = asyncio.create_task(ws_ingestion(websocket, buffer))
            order_book_last_update_id = await get_order_book()  
            first_received_message_id = await get_first_depth_update_id(buffer)
            ws_processing_task = None
        

            while order_book_last_update_id < first_received_message_id:
                order_book_last_update_id = await get_order_book() 

            match_event = asyncio.Event()
            
            find_match_task = asyncio.create_task(find_matching_messsage(order_book_last_update_id,buffer, match_event))
            await match_event.wait()

            ws_processing_task = asyncio.create_task(ws_processing(buffer))  

            # Run coroutines for 5 sec - prevents an infinite loop by raising a TimeOut Error
            try: 
                tasks =[ws_ingestion_task]
                if ws_processing_task:
                    tasks.append (ws_processing_task)
                if find_match_task:
                    tasks.append (find_match_task)
                await asyncio.wait_for(
                    asyncio.gather(*tasks), 
                    timeout = 10
                )
                       
            except asyncio.TimeoutError:
                 print('Runtime time out')

            # Cancel infinite coroutines as wait_for does't cancel them
            await asyncio.sleep(2)
            ws_ingestion_task.cancel()
            find_match_task.cancel()
            if ws_processing_task:
                ws_processing_task.cancel()

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


 