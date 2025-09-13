import asyncio
import json


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


async def is_continuous(curr_msg, buffer):
    curr_msg_last_id = curr_msg["u"]
    next_msg_first_id = json.loads(buffer[0])["U"]
    print(
        f"Last update current {curr_msg_last_id}, first update next {next_msg_first_id}"
    )
    if int(curr_msg_last_id) + 1 == next_msg_first_id:
        return True
    return False


async def ws_processing(order_book, buffer):
    # Infinite processing function
    while True:
        if not buffer:
            await asyncio.sleep(0.1)
            continue

        try:
            print("Continue processing")
            curr_msg = json.loads(buffer.popleft())

            if not buffer:
                pass
                # Need to implement some logic to distinguish 2 cases:
                # 1) ingestion is stopped as we're at checksum stage -> we need to apply last message in the buffer to the order book state without checking the continuity
                # 2) ingestion is stuck or slow -> we need to wait for the next message before resuming processing

            if await is_continuous(curr_msg, buffer):
                print(
                    f"Printing the message I am going to process: {curr_msg}. It has type {type(curr_msg)}"
                )
                await to_do_processing_logic(order_book, curr_msg)
            else:
                raise MissingMessageInIngestedStream(
                    "There is a gap between update IDs between the processed and following message. Current message haven't been processed and will stay in the buffer for retry."
                )
            await asyncio.sleep(0.1)

        except MissingMessageInIngestedStream as e:
            buffer.appendleft(curr_msg)
            print(f"Continuity gap detected: {e}")
            raise

        except Exception as e:
            buffer.appendleft(curr_msg)
            print(f"Something is wrong with processing: {e}")
            raise
