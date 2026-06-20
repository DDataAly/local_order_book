import asyncio
from collections import deque
from wb_sockets import ws_ingestion, fetch_order_book_snapshot, find_matching_message, ws_processing
from order_book.order_book_production import create_order_book


async def initialise_order_book_stream(websocket):
    buffer = deque([])
    # Initilise tasks as None for the case when the error is raised before they're defined
    # This is useful as it allows to cancel them neatly at the finally block of run_code()
    # irrespectively of the point where error occurs
    ws_ingestion_task = None
    ws_processing_task = None
    
    ws_ingestion_task = asyncio.create_task(ws_ingestion(websocket, buffer))

    try:
        snapshot = await asyncio.wait_for(fetch_order_book_snapshot(buffer), timeout=5)
        print('Suitable order book fetched, saving it now....')
    except asyncio.TimeoutError:
        print('No suitable order book fetched, can\'t proceed')
        raise  

    try:
        matching_message = await asyncio.wait_for(find_matching_message(snapshot["lastUpdateId"], buffer), timeout = 5)  
        print(f' Matching message is found {matching_message}. Starting processing') 
    except asyncio.TimeoutError:
        print('No suitable Websocket stream message fetched, can\'t proceed')
        raise

    order_book = await create_order_book(snapshot)

    ws_processing_task = asyncio.create_task(ws_processing(order_book, buffer))
    
    return ws_ingestion_task, ws_processing_task


