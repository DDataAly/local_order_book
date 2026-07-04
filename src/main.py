import asyncio
import time
import websockets
from order_book.initialise_order_book_stream import initialise_order_book_stream
from wb_sockets import run_the_subscriber

uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'

async def run_code(subscription_timeout):
    # Initilise tasks as None for the case when the error is raised before they're defined
    # This is useful as it allows to cancel them neatly at the finally block of run_code()
    # irrespectively of the point where error occurs
    ws_ingestion_task = None
    ws_processing_task = None

    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to server")

            try:
                await asyncio.wait_for(run_the_subscriber(websocket), timeout = subscription_timeout)
            except asyncio.TimeoutError:
                print('Can\'t subscribe to the requested channel')
                raise
            except Exception: 
                print('Error at the subscription stage')
                raise
            
            try:
                ws_ingestion_task, ws_processing_task = await initialise_order_book_stream(websocket)
            except Exception:
                print('Error at the initialisation stage')
                raise
                    
             
            tasks =[ws_ingestion_task, ws_processing_task]
            await asyncio.gather(*tasks)

    finally:
        if ws_ingestion_task:
            ws_ingestion_task.cancel()
            await asyncio.gather(ws_ingestion_task, return_exceptions=True)
        
        if ws_processing_task:
            ws_processing_task.cancel()
            await asyncio.gather(ws_processing_task, return_exceptions=True)

        print("WebSocket connection closed")
        

async def run_for_duration(runtime, subscription_timeout = 5):
    start_time = time.monotonic()
    while time.monotonic() < start_time + runtime:
        time_till_timeout = runtime - (time.monotonic() - start_time)
        try:
            await asyncio.wait_for(run_code(subscription_timeout), timeout = time_till_timeout)
        except asyncio.TimeoutError:
            print('Program has been stopped due to the execution timeout') 
            break   
        except Exception as e:
            print(f'Restarting due to error: {e}')
            continue


if __name__ == '__main__':
    asyncio.run(run_code()) #Creates the event loop and runs coroutines  


# Sandbox code 

def this_always_fails():
      print('Hello from cats')
      raise Exception

max_sync_retry = 3

for _ in range (max_sync_retry):
        try:
            ws_ingestion_task = this_always_fails()
            break
        except Exception as e:
            print(f'round {_}')
            continue
else:
    print('Error at the order book stream initiation')
    raise ('Stop trying. Ask cats')