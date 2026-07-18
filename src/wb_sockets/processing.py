import asyncio
import json
from collections import deque


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
    print("Trying to execute the logic")
    order_book.ob_bids, order_book.ob_asks = await order_book.update_order_book(message)
    order_book.ob_bids_prices, order_book.ob_asks_prices = (
        await order_book.update_price_lists(message)
    )
    print("Processing is done")


async def is_continuous(curr_msg, buffer:deque) -> bool:
    target_id = int(curr_msg["u"]) + 1

    # If buffer is empty, wait for a new message being added by ws_ingestion
    while len(buffer) < 1:
        await asyncio.sleep(0.1)

    try:
        next_msg_first_id = int(json.loads(buffer[0])["U"])
        print(f"Last update current {curr_msg['u']}, first update next {next_msg_first_id}")

        # If there is no id gaps between messages
        if next_msg_first_id == target_id:
            print("Condition is met")
            return True
    except Exception as e:
        print(f"Message in the buffer can not be processed: {e}")

    print(f"Condition is not met")
    return False


async def ws_processing(order_book, buffer):
    # Infinite processing function
    while True:
        if len(buffer) < 1:
            await asyncio.sleep(0.1)
            continue

        try:
            print("Continue processing")
            print(f"This is buffer (len={len(buffer)}): {buffer}")
            msg_str = buffer.popleft()
            print(f"After popleft (len={len(buffer)}): {buffer}")
            curr_msg = json.loads(msg_str)
            print(f"Parsed JSON OK. This is curr_msg: {curr_msg}")


            if await is_continuous(curr_msg, buffer):
                print(f"Printing the message I am going to process: {curr_msg}. It has type {type(curr_msg)}")
                await to_do_processing_logic(order_book, curr_msg)
            else:
                raise MissingMessageInIngestedStream(
                    "The message stream is not continuous. Launching re-sync"
                )
            
            await asyncio.sleep(0.1)

        except MissingMessageInIngestedStream:
            raise    

        except Exception as e:
            buffer.appendleft(curr_msg)
            await asyncio.sleep(0.1)
            print(f"Something is wrong with processing: {e}")
            raise