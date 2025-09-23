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

async def get_order_book() -> tuple [dict, int]:
    """
    Sends a request to get a copy of the order book from Binance REST API using aiohttp.ClientSession()
    to avoid blocking the event loop. This allows ws_ingestion to run simultaneously with this function.
    Saves the received JSON locally at the path specified by 'path_initial_shapshot'.
    Extracts and returns the 'lastUpdateId' of the saved order book copy.
    This ID is used to synchronize the order book with the WebSocket depth stream.

    Returns:
        tuple:
            snapshot (dict): parsed JSON order book snapshot from Binance REST API
            order_book_last_update_id (int): the last update ID from the REST API snapshot of the order book
    """
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20') as response:
                response.raise_for_status()
                snapshot = await response.json()
        except aiohttp.ClientError as e:
            print (f'Error fetching the order book snapshot: {e}') 
            raise
        except aiohttp.ContentTypeError as e:
            print(f'The server response file is not a valid json: {e}') 
            raise
        except Exception as e:
            print(f'An error occurred fetching the order book copy: {e}')
            raise
    return snapshot, snapshot.get("lastUpdateId")


async def fetch_order_book_snapshot(buffer) -> tuple [dict, int]:
    """
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
            order_book_last_update_id (int): the "lastUpdateId" of that snapshot
    """
    while True:
        await asyncio.sleep (0.1)
        snapshot, order_book_last_update_id = await get_order_book()  
        first_received_message_id = await get_first_depth_update_id(buffer)

        if order_book_last_update_id >= first_received_message_id:
            print(f'A valid snapshot of the order book is found')
            return snapshot, order_book_last_update_id  


async def find_matching_message(order_book_last_update_id, buffer) -> None:
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

