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


async def is_continuous(curr_msg, buffer, max_num_skipped_msg = 2):
    target_id = int(curr_msg["u"]) + 1
    shelf = []

    while len(shelf) <= max_num_skipped_msg:
        # If buffer is empty, wait for a new message being added by ws_ingestion
        while len(buffer) < 1:
            await asyncio.sleep(0.1)

        next_msg_first_id = int(json.loads(buffer[0])["U"])
        print(f"Last update current {curr_msg['u']}, first update next {next_msg_first_id}")

        # If there is no id gaps between messages
        if next_msg_first_id == target_id:
            print("Condition is met")
            return True

        # If there is a gap between msgs, we pop the problematic msg and put it on the shelf
        skipped_msg = buffer.popleft()
        shelf.append(skipped_msg)
        print(f"Skipped a message. Shelf size: {len(shelf)}")

    print(f"Condition is not met after skipping {max_num_skipped_msg} messages")
    return False

# Updating in progress - don't need len(buffer) < 2 check here since is_continuous handling this
async def ws_processing(order_book, buffer):
    # Infinite processing function
    while True:
        if len(buffer) < 2:
            await asyncio.sleep(0.1)
            continue

        try:
            print("Continue processing")
            print(f"This is buffer (len={len(buffer)}): {buffer}")
            # msg_str = buffer.popleft()
            print(f"After popleft (len={len(buffer)}): {buffer}")
            curr_msg = json.loads(msg_str)
            print("Parsed JSON OK")
            if buffer:
                print("Cats")

            if buffer:
                print("Cats")
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
            await asyncio.sleep(0.1)
            print(f"Continuity gap detected: {e}")
            raise

        except Exception as e:
            buffer.appendleft(curr_msg)
            await asyncio.sleep(0.1)
            print(f"Something is wrong with processing: {e}")
            raise


            #  pass
            #     # Need to implement some logic to distinguish 2 cases:
            #     # 1) ingestion is stopped as we're at checksum stage -> we need to apply last message in the buffer to the order book state without checking the continuity
            #     # 2) ingestion is stuck or slow -> we need to wait for the next message before resuming processing




# Not working version - some adjusments to handle errors in the WebSockets stream
# import asyncio
# import json


# class MissingMessageInIngestedStream(Exception):
#     """Raised when there is a gap between the update IDs in the stream of ingested depth updates"""


# async def to_do_processing_logic(order_book, message):
#     """
#     Applies a single Binance depth update message to the local copy of the order book.
#     This function mutates the given OrderBook instance in place by:
#     - Updating bid and ask dictionaries (`ob_bids`, `ob_asks`).
#     - Updating corresponding price lists (`ob_bids_prices`, `ob_asks_prices`)
#     to ensure consistency between all order book attributes.
#     Args:
#             order_book (OrderBook): The local copy of the order book to update
#             message (dict): A WebSocket depth update message received from Binance
#     Returns:
#             None
#     """
#     print("Trying to execute the logic")
#     order_book.ob_bids, order_book.ob_asks = await order_book.update_order_book(message)
#     order_book.ob_bids_prices, order_book.ob_asks_prices = (
#         await order_book.update_price_lists(message)
#     )
#     print("Processing is done")


# async def is_continuous(curr_msg, buffer):
#     curr_msg_last_id = curr_msg["u"]
#     next_msg_first_id = json.loads(buffer[0])["U"]
#     print(
#         f"Last update current {curr_msg_last_id}, first update next {next_msg_first_id}"
#     )
#     if int(curr_msg_last_id) + 1 == next_msg_first_id:
#         print("Condition is met")
#         return True
#     print("Condition is not met")
#     return False


# async def ws_processing(order_book, buffer):
#     # Infinite processing function
#     while True:
#         if not buffer:
#             await asyncio.sleep(0.1)
#             continue

#         try:
#             print("Continue processing")
#             print(f"This is buffer (len={len(buffer)}): {buffer}")
#             # msg_str = buffer.popleft()
#             print(f"After popleft (len={len(buffer)}): {buffer}")
#             curr_msg = json.loads(msg_str)
#             print("Parsed JSON OK")
#             if buffer:
#                 print("Cats")

#             if buffer:
#                 print("Cats")
#                 if await is_continuous(curr_msg, buffer):
#                     print(
#                         f"Printing the message I am going to process: {curr_msg}. It has type {type(curr_msg)}"
#                     )
#                     await to_do_processing_logic(order_book, curr_msg)
#                 else:
#                     raise MissingMessageInIngestedStream(
#                         "There is a gap between update IDs between the processed and following message. Current message haven't been processed and will stay in the buffer for retry."
#                     )
#                 await asyncio.sleep(0.1)

#         except MissingMessageInIngestedStream as e:
#             buffer.appendleft(curr_msg)
#             await asyncio.sleep(0.1)
#             print(f"Continuity gap detected: {e}")
#             raise

#         except Exception as e:
#             buffer.appendleft(curr_msg)
#             await asyncio.sleep(0.1)
#             print(f"Something is wrong with processing: {e}")
#             raise









# Main brunch working version - before websockets inconsistencies were detected
# import asyncio
# import json

# class MissingMessageInIngestedStream(Exception):
#     """Raised when there is a gap between the update IDs in the stream of ingested depth updates"""   

# async def to_do_processing_logic(order_book, message):
#         print('Trying to execute the logic')
#         order_book.ob_bids, order_book.ob_asks = await order_book.update_order_book(message)
#         order_book.ob_bids_prices, order_book.ob_asks_prices = await order_book.update_price_lists(message)   
#         print('Processing is done')   
#         await asyncio.sleep (0.1)

# # Need to refactor
# async def ws_processing(order_book, buffer):
#     # Infinite processing function
#     while True:
#         if not buffer:
#             await asyncio.sleep(0.1)
#             continue

#         try:
#             print ('Continue processing')
#             message = json.loads(buffer.popleft())
#             print(f'Printing the message I am going to process: {message}. It has type {type(message)}')
#             await to_do_processing_logic(order_book, message)
#             print('Job done, let me check if the next message is valid...')
#             last_update_current_message = message['u']
#             first_update_next_message =  json.loads(buffer[0])['U']
#             print (f'Last update current {last_update_current_message}, first update next {first_update_next_message}')
#             if int(last_update_current_message) + 1 == first_update_next_message:
#                 print("All good with the next message")
#             else:
#                 print('There is some issue with the next message') 
#                 raise MissingMessageInIngestedStream ("There is a gap between update IDs between the processed and following message. Buffer is not valid.") 
#                 # Here should be some logic around re-fetching order book and doing sync again - perhaps as a part of the orchestrator or main event loop      
#             await asyncio.sleep (0.1)
#         except Exception as e:
#             print(f'Something is wrong with processing: {e}')