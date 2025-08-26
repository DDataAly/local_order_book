import asyncio
import websockets
from order_book.orchestrator import orchestrator
from wb_sockets import run_the_subscriber

uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'

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
                await asyncio.wait_for(asyncio.gather(*tasks), timeout = 1)

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
 