import asyncio
from collections import deque
from wb_sockets import ws_ingestion, fetch_order_book_snapshot, find_matching_message, ws_processing
from order_book.order_book_production import create_and_save_local_order_book
from order_book.order_book_class import OrderBook


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
        matching_message = await asyncio.wait_for(find_matching_message(order_book_last_update_id, buffer), timeout = 5)  
        print(f'Order book snapshot is fetched. Matching message is found {matching_message}. Starting processing') 
    except asyncio.TimeoutError:
        print('No suitable Websocket stream message fetched, can\'t proceed')
        ws_ingestion.cancel()
        await asyncio.gather(ws_ingestion_task, return_exceptions=True)
        raise 

    order_book = await create_and_save_local_order_book(snapshot, order_book_last_update_id)

    ws_processing_task = asyncio.create_task(ws_processing(order_book, buffer))
    return ws_ingestion_task, ws_processing_task, order_book
    