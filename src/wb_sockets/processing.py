import asyncio
import json
from collections import deque
import logging

logger = logging.getLogger(__name__)


class MissingMessageInIngestedStream(Exception):
    """Raised when there is a gap between the update IDs in the stream of ingested depth updates"""


async def to_do_processing_logic(order_book, message):
    """
    Applies a single Binance depth update message to the local copy of the order book.
    This function mutates the given OrderBook instance in place by:
    - Updating bid and ask dictionaries (`ob_bids`, `ob_asks`).
    - Updating corresponding price lists (`ob_bids_prices`, `ob_asks_prices`)
    to ensure consistency between all order book attributes.
    Args:
            order_book (OrderBook): The local copy of the order book to update
            message (dict): A WebSocket depth update message received from Binance
    Returns:
            None
    """
    logger.debug("Trying to execute the logic")
    order_book.ob_bids, order_book.ob_asks = await order_book.update_order_book(message)
    order_book.ob_bids_prices, order_book.ob_asks_prices = (
        await order_book.update_price_lists(message)
    )
    logger.debug("Processing is done")


async def is_continuous(curr_msg, buffer:deque) -> bool:
    target_id = int(curr_msg["u"]) + 1

    # If buffer is empty, wait for a new message being added by ws_ingestion
    while len(buffer) < 1:
        await asyncio.sleep(0.1)

    try:
        next_msg_first_id = int(json.loads(buffer[0])["U"])
        logger.debug(f"Last update current {curr_msg['u']}, first update next {next_msg_first_id}")

        # If there is no id gaps between messages
        if next_msg_first_id == target_id:
            logger.debug("Condition is met")
            return True
    except Exception as e:
        logger.debug(f"Message in the buffer can not be processed: {e}")

    logger.error(f"Continuity check failed")
    return False

async def ws_processing(order_book, buffer, match_found: asyncio.Event, snapshot_timestamp: list, stop_fetching_verification_snapshot: asyncio.Event):
    logger.info('Processing has started')
    # Infinite processing function
    num_processed_msg = 0
    while True:
        logger.debug(f'This is the verification_snapshot_timestamp: {snapshot_timestamp[0]}')
        if len(buffer) < 1:
            await asyncio.sleep(0.1)
            continue

        try:
            logger.debug("Processing in progress")
            msg_str = buffer.popleft()
            curr_msg = json.loads(msg_str)

            if await is_continuous(curr_msg, buffer):
                logger.debug(f"Printing the message I am going to process: {curr_msg}")
                await to_do_processing_logic(order_book, curr_msg)
                num_processed_msg = num_processed_msg +1
            else:
                raise MissingMessageInIngestedStream(
                    "The message stream is not continuous. Launching re-sync"
                )

            # Verification stage - this block runs when run_verification returns snapshot_timestamp
            if not snapshot_timestamp[0]==None:
                msg_timestamp = curr_msg["u"] 
                logger.debug(f'Running ws_processing in the verification mode, current msg_timestamp is {msg_timestamp}')

                if snapshot_timestamp[0] > msg_timestamp:
                    # curr_msg timestamp is behind the snapshot_timestamp - the snapshot is potentially suitable, 
                    # keep processing messages from the buffer and compare with the same snapshot_timestamp[0]
                    logger.debug ('Verification snapshot might be suitable, continue processing')
                    stop_fetching_verification_snapshot.set()
                    pass
               
                elif snapshot_timestamp[0] == msg_timestamp:
                    # curr_msg timestamp equal snapshot_timestamp - local order book copy is in sync with the snaphshot
                    # we can stop ws_processing and start verification
                    logger.info ('Match found')
                    match_found.set()
                    break
                else:
                    # curr_msg timestamp is ahead snapshot_timestamp - we need to fetch a new snapshot
                    logger.debug ('Verification snapshot is not suitable, will need to get a new one')
                    snapshot_timestamp[0]=None
                    stop_fetching_verification_snapshot.clear()
  
            logger.debug(f'We processed {num_processed_msg} messages')
            await asyncio.sleep(0.1)

        except MissingMessageInIngestedStream:
            raise    

        except Exception as e:
            buffer.appendleft(curr_msg)
            await asyncio.sleep(0.1)
            logger.error(f"Something went wrong with processing when trying to process message with timestamp {curr_msg['u']}: {e}")
            raise

