import asyncio
import logging
import time
import websockets
from order_book.initialise_order_book_stream import initialise_order_book_stream
from wb_sockets.verifying import run_verification
from wb_sockets import run_the_subscriber
from utils import logger as logger_config

uri = 'wss://stream.binance.com:9443/ws/btcusdt@depth'
order_book_depth = 10

logger_config.setup_logger()
logger = logging.getLogger(__name__)

async def run_code(subscription_timeout, order_book_depth):
    # Initilise tasks as None for the case when the error is raised before they're defined
    # This is useful as it allows to cancel them neatly at the finally block of run_code()
    # irrespectively of the point where error occurs
    ws_ingestion_task = None
    ws_processing_task = None

    async with websockets.connect(uri) as websocket:
        logger.info("Connected to server")
        try:
            try:
                await asyncio.wait_for(run_the_subscriber(websocket), timeout = subscription_timeout)
            except asyncio.TimeoutError:
                logger.error('Can\'t subscribe to the requested channel')
                raise
            except Exception: 
                logger.error('Error at the subscription stage')
                raise
            
            match_found = asyncio.Event()
            verification_snapshot_timestamp = [None]
            stop_fetching_verification_snapshots = asyncio.Event()

            try:
                order_book, ws_ingestion_task, ws_processing_task = await initialise_order_book_stream(websocket, match_found, verification_snapshot_timestamp, stop_fetching_verification_snapshots)
            except Exception:
                logger.error('Error at the initialisation stage')
                raise
                    
            
            tasks =[ws_ingestion_task, ws_processing_task]
            await asyncio.gather(asyncio.shield(*tasks))

        finally:
            logger.debug('Finally block actually runs')
            if both_tasks_running(ws_ingestion_task, ws_processing_task):
                logger.debug('Both tasks are running')
                # await asyncio.gather(*tasks)
                try:
                    verification_passed=await asyncio.wait_for(run_verification(
                            match_found,
                            stop_fetching_verification_snapshots,
                            verification_snapshot_timestamp,
                            order_book, 
                            order_book_depth), 
                            timeout=5)
                    if verification_passed:
                        logger.info('Verification is successful')
                    else:
                        logger.error("Verification has failed")
                except asyncio.TimeoutError:
                    logger.error('Verification has timed out without completion')
                except Exception:
                    logger.error('An error occurred at verification stage')

            if ws_ingestion_task:
                    ws_ingestion_task.cancel()
                    await asyncio.gather(ws_ingestion_task, return_exceptions=True)
                
            if ws_processing_task:
                    ws_processing_task.cancel()
                    await asyncio.gather(ws_processing_task, return_exceptions=True)

        logger.info("WebSocket connection closed")

        

async def run_for_duration(runtime, subscription_timeout = 5):
    start_time = time.monotonic()
    while time.monotonic() < start_time + runtime:
        time_till_timeout = runtime - (time.monotonic() - start_time)
        try:
            await asyncio.wait_for(run_code(subscription_timeout, order_book_depth), timeout = time_till_timeout)
        except asyncio.TimeoutError:
            logger.info('Program has been stopped due to the execution timeout') 
            break   
        except Exception as e:
            logger.warning(f'Restarting due to error: {e}')
            continue


def both_tasks_running(ws_ingestion_task, ws_processing_task):
    if ws_ingestion_task and ws_processing_task:
        logger.debug('Both tasks are defined')
        logger.debug(f'Ingestion task exists {ws_ingestion_task}')
        logger.debug(f'Ingesting is running: {not ws_ingestion_task.done()}')
        logger.debug(f'Processing task exists {ws_processing_task}')
        logger.debug(f'Processing is running: {not ws_processing_task.done()}')
        if not ws_processing_task.done() and not ws_ingestion_task.done():
            logger.debug('Helper returned T')
            return True
    logger.debug('Helper returned F')
    return False

if __name__ == '__main__':
    asyncio.run(run_for_duration(10))
