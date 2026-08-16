import aiohttp
import asyncio
import json
from collections import deque

async def get_first_depth_update_id(buffer: deque[str]) -> int:
    """
    Loops through messages in the buffer till finds a valid depth update message.
    Extracts the ID of the first update in the first valid depth update message,
    which is used to synchronise the WebSocket stream with the API order book snapshot.
    Args:
        buffer (collections.deque[str]) - incoming WebSocket stream messages waiting to be processed
    Returns:
        order_book_last_update_id (int): the last update ID from the REST API snapshot of the order book
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

async def get_order_book() -> dict:
    """
    Sends a request to get a copy of the order book from Binance REST API using aiohttp.ClientSession()
    to avoid blocking the event loop. This allows ws_ingestion to run simultaneously with this function.
    Saves the received JSON locally at the path specified by 'path_initial_shapshot'.
    Returns:
            snapshot (dict): parsed JSON order book snapshot from Binance REST API
    """
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=10') as response:
                response.raise_for_status()
                snapshot = await response.json()
        except aiohttp.ContentTypeError as e:
            print(f'The server response file is not a valid json: {e}') 
            raise
        except aiohttp.ClientError as e:
            print (f'Error fetching the order book snapshot: {e}') 
            raise
        except Exception as e:
            print(f'An error occurred fetching the order book copy: {e}')
            raise
    return snapshot



def validate_snapshot(snapshot: dict) -> bool:
    try:
        assert all([item in snapshot.keys() for item in ['lastUpdateId', 'asks', 'bids']])
        assert all(len(snapshot[key])!=0 for key in ['asks', 'bids'])
        assert isinstance(snapshot['lastUpdateId'], int) 
        assert snapshot['lastUpdateId']!= 0
        
        for side in ['bids', 'asks']:
            for price,qty in snapshot[side]:
                assert not(float(qty) == 0 or float(price) == 0)
        print('All good!')
    except Exception as e:
        print("Invalid snapshot received, retrying")
        return False
    return True


async def fetch_order_book_snapshot(buffer) -> dict:
    """
    Validates that the snapshot is of expected format and not stale by checking it overlaps with the stream.

    Continuously requests a copy of the order book from the Binance REST API and compares
    its "lastUpdateId" with the 'U' value (first update ID) from the earliest valid
    depth update message in the WebSocket buffer.

    Once the snapshot's "lastUpdateId" is greater than or equal to the first 'U' value,
    a valid snapshot has been found and is returned.
    Args:
        buffer (collections.deque[str]) - incoming WebSocket stream messages waiting to be processed
    Returns:
        tuple:
            snapshot (dict): parsed JSON order book snapshot from Binance REST API
    """
    while True:
        await asyncio.sleep (0.1)
        try:
            snapshot = await get_order_book()
            if validate_snapshot(snapshot): 
                order_book_last_update_id = snapshot["lastUpdateId"]
                first_received_message_id = await get_first_depth_update_id(buffer)
                if order_book_last_update_id >= first_received_message_id:
                    print(f'A valid snapshot of the order book is found')
                    return snapshot   
        except Exception:
            continue    




async def find_matching_message(order_book_last_update_id, buffer) -> None:
    """
    Finds the first message that contains updates not yet reflected in the snapshot.

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



# sandbox
# Original function:
# def validate_snapshot(snapshot: dict) -> bool:
    # try:
    #     assert all([item in snapshot.keys() for item in ['lastUpdateId', 'asks', 'bids']])
    #     assert all([float(snapshot[side][0][0]) for side in['bids', 'asks']])
    #     assert all([float(snapshot[side][0][1]) for side in['bids', 'asks']])
    # except Exception as e:
    #     print("Invalid snapshot received, retrying")
    #     return False
    # return True

# def validation_practice(snapshot:dict) -> bool:
#     for side in ['bids', 'asks']:
#         for item in snapshot[side]:
#             print(float(item[0]))
#     for side in ['bids', 'asks']:
#         for item in snapshot[side]:
#             print(float(item[1]))

# def validation_practice2(snapshot: dict) -> bool:
#     try: 
#         assert all ([float(item[1]) for item in snapshot[side]] for side in ['bids', 'asks'])
#         print('happy cats')
#     except Exception:
#         print('Invalid snapshot received')    

# test_snapshot = {"lastUpdateId": 123, 
#                  "bids": [['1.0','1.0'], ['1.1', '2.1']], 
#                  'asks': [['4.1', '4.0'], ['3.1', '3.0']]}

# # validation_practice(test_snapshot)
# print('cats')
# # validation_practice2(test_snapshot)

# def validation_practice3(snapshot:dict) -> bool:
#     for side in ['bids', 'asks']:
#         for item in snapshot[side]:
#             if float(item[0])!=0 and float(item[1])!=0:
#                 print(True)
#             else:
#                 print (False)

# # validation_practice3(test_snapshot)

# def validation_practice4(snapshot:dict):
#     try:
#         assert all([True if (float(item[0])!=0 and float(item[1])!=0) else False for item in snapshot[side]] for side in ['bids', 'asks'])
#         print ('Valid')
#     except Exception:
#         print ('Not valid')

# # validation_practice4(test_snapshot)

# def validation_practice5(snapshot:dict) -> bool:
#     new_list=[]
#     for side in ['bids', 'asks']:
#         for qty,price in snapshot[side]:
#             if float(qty) ==0 or float(price) ==0:
#                 new_list.append('zero value found')
#                 assert len(new_list) == 0

# # validation_practice5(test_snapshot)

# def validation_practice6(snapshot:dict) -> bool:
#     for side in ['bids', 'asks']:
#         for price,qty in snapshot[side]:
#             assert not(float(qty) ==0 or float(price) ==0)

# def validation_practice7(snapshot):
#     assert all ((float(item[0])!=0 and float(item[1])!=0) for side in ['bids', 'asks'] for item in snapshot[side])
#     print('Wohoo!')

# test_snapshot = {"lastUpdateId": 123, 
#                  "bids": [['1.0','1.0'], ['1.1', '2.1']], 
#                  'asks': [['emma', '4.0'], ['3.1', '3.0']]}


# validation_practice7(test_snapshot)

