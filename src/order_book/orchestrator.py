import asyncio
from collections import deque
from wb_sockets import ws_ingestion, fetch_order_book_snapshot, find_matching_message, ws_processing, MissingMessageInIngestedStream
from order_book.order_book_production import create_and_save_local_order_book

# Defines number of attempted re-tries of restarting ws_processing in case a gap has been detected between adjacent WebSocket messages
MAX_RETRIES = 3



async def orchestrator(websocket, max_retries = MAX_RETRIES):
    buffer = deque([])

    ws_ingestion_task = asyncio.create_task(ws_ingestion(websocket, buffer))

    for attempt in range (0, max_retries):
        try:
            snapshot, order_book_last_update_id = await asyncio.wait_for(fetch_order_book_snapshot(buffer), timeout=5)
            print('Suitable order book fetched, saving it now....')

        except asyncio.TimeoutError:
            print('No suitable order book fetched, can\'t proceed')
            ws_ingestion_task.cancel()
            await asyncio.gather(ws_ingestion_task, return_exceptions=True)
            raise  

        try:
            matching_message = await asyncio.wait_for(find_matching_message(order_book_last_update_id, buffer), timeout = 5)  
            print(f'Order book snapshot is fetched. Matching message is found {matching_message}. Starting processing') 
        except asyncio.TimeoutError:
            print('No suitable Websocket stream message fetched, can\'t proceed')
            ws_ingestion_task.cancel()
            await asyncio.gather(ws_ingestion_task, return_exceptions=True)
            raise 

        order_book = await create_and_save_local_order_book(snapshot, order_book_last_update_id)

        try:
            ws_processing_task = asyncio.create_task(ws_processing(order_book, buffer))
            return ws_ingestion_task, ws_processing_task, order_book
        
        except MissingMessageInIngestedStream:
            continue

    if MissingMessageInIngestedStream:
        raise ('There is an issue with continuity of Websocket messages. Maximum number of retries is exhausted.')   
    else:
        raise Exception ('There is an error in processing logic not related to the message continuity')       
