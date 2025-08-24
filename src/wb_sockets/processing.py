import asyncio
import json

class MissingMessageInIngestedStream(Exception):
    """Raised when there is a gap between the update IDs in the stream of ingested depth updates"""   

async def to_do_processing_logic(order_book, message):
        print('Trying to execute the logic')
        order_book.ob_bids, order_book.ob_asks = await order_book.update_order_book(message)
        order_book.ob_bids_prices, order_book.ob_asks_prices = await order_book.update_price_lists(message)   
        print('Processing is done')   
        await asyncio.sleep (0.1)

# Need to refactor
async def ws_processing(order_book, buffer):
    # Infinite processing function
    while True:
        if not buffer:
            await asyncio.sleep(0.1)
            continue

        try:
            print ('Continue processing')
            message = json.loads(buffer.popleft())
            print(f'Printing the message I am going to process: {message}. It has type {type(message)}')
            await to_do_processing_logic(order_book, message)
            print('Job done, let me check if the next message is valid...')
            last_update_current_message = message['u']
            first_update_next_message =  json.loads(buffer[0])['U']
            print (f'Last update current {last_update_current_message}, first update next {first_update_next_message}')
            if int(last_update_current_message) + 1 == first_update_next_message:
                print("All good with the next message")
            else:
                print('There is some issue with the next message') 
                raise MissingMessageInIngestedStream ("There is a gap between update IDs between the processed and following message. Buffer is not valid.") 
                # Here should be some logic around re-fetching order book and doing sync again - perhaps as a part of the orchestrator or main event loop      
            await asyncio.sleep (0.1)
        except Exception as e:
            print(f'Something is wrong with processing: {e}')