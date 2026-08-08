import asyncio
import time
import websockets
from order_book.initialise_order_book_stream import initialise_order_book_stream
from wb_sockets.verifying import run_verification
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
            
            match_found = asyncio.Event()
            verification_snapshot_timestamp = [None]
            stop_fetching_verification_snapshots = asyncio.Event()

            try:
                order_book, ws_ingestion_task, ws_processing_task = await initialise_order_book_stream(websocket, match_found, verification_snapshot_timestamp, stop_fetching_verification_snapshots)
            except Exception:
                print('Error at the initialisation stage')
                raise
                    
             
            tasks =[ws_ingestion_task, ws_processing_task]
            await asyncio.gather(*tasks)

    finally:
        if both_tasks_running(ws_ingestion_task, ws_processing_task):
            try:
                verification_passed=await asyncio.wait_for(run_verification(
                        match_found,
                        stop_fetching_verification_snapshots,
                        verification_snapshot_timestamp,
                        order_book.ob_bids,
                        order_book.ob_asks), 
                        timeout=10)
                if verification_passed:
                    print('Verification is successful')
                else:
                    print("Verification has failed")
            except asyncio.TimeoutError:
                print('Verification has timed out without completion')
            except Exception:
                print('An error occurred at verification stage')

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


def both_tasks_running(ws_ingestion_task, ws_processing_task):
    if ws_ingestion_task and ws_processing_task:
        if not ws_processing_task.done() and not ws_ingestion_task.done():
            return True
    return False

if __name__ == '__main__':
    asyncio.run(run_for_duration(2))


# Sandbox code 

# def this_always_fails():
#       print('Hello from cats')
#       raise Exception

# max_sync_retry = 3

# for _ in range (max_sync_retry):
#         try:
#             ws_ingestion_task = this_always_fails()
#             break
#         except Exception as e:
#             print(f'round {_}')
#             continue
# else:
#     print('Error at the order book stream initiation')
#     raise ('Stop trying. Ask cats')
