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
        print('Trying to execute the logic')
        order_book.ob_bids, order_book.ob_asks = await order_book.update_order_book(message)
        order_book.ob_bids_prices, order_book.ob_asks_prices = await order_book.update_price_lists(message)   
        print('Processing is done')   


async def message_continuity_checker (message_to_process, buffer):
    last_update_message_to_process = message_to_process['u']
    first_update_next_message =  json.loads(buffer[0])['U']
    print (f'Last update current {last_update_message_to_process}, first update next {first_update_next_message}')
    if int(last_update_message_to_process) + 1 == first_update_next_message:
        return True
    return False
   

async def ws_processing(order_book, buffer):
    # Infinite processing function
    while True:
        if not buffer:
            await asyncio.sleep(0.1)
            continue

        try:
            print ('Continue processing')
            message_to_process = json.loads(buffer.popleft())
            print(f'Printing the message I am going to process: {message_to_process}. It has type {type(message_to_process)}')
            await to_do_processing_logic(order_book, message_to_process)
            print('Job done, let me check if the next message is valid...')

            if await message_continuity_checker(message_to_process, buffer):
                print("All good with the next message")
            else:
                raise MissingMessageInIngestedStream ("There is a gap between update IDs between the processed and following message. Buffer is not valid.") 
            await asyncio.sleep (0.1)

        except MissingMessageInIngestedStream as e: 
            print(f'Continuity gap detected: {e}')
            raise

        except Exception as e:
            print(f'Something is wrong with processing: {e}')
            raise