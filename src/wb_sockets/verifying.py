import asyncio
from wb_sockets import get_order_book, validate_snapshot
from order_book.order_book_class import OrderBook

async def run_comparison (verification_snapshot: dict,
                    order_book: OrderBook,
                    order_book_depth: int) -> bool:
    verification_order_book=OrderBook()
    verification_order_book.ob_bids, verification_order_book.ob_asks = await verification_order_book.extract_order_book_bids_asks(verification_snapshot)

    await order_book.prepare_local_copy_for_validation(order_book_depth)
    if order_book.ob_bids == verification_order_book.ob_bids: 
        if order_book.ob_asks == verification_order_book.ob_asks:
            print('Local order book was compared with the fresh copy - and they match!')
            return True 

    return False



async def run_verification(match_found: asyncio.Event,
                           stop_fetching_verification_snapshots: asyncio.Event,
                           snapshot_timestamp: list,
                           order_book: OrderBook,
                           order_book_depth: int):
    print('Verification stage started')
    while not stop_fetching_verification_snapshots.is_set():

        verification_snapshot = await get_order_book()
        if not validate_snapshot(verification_snapshot):
            continue
        snapshot_timestamp[0]=verification_snapshot["lastUpdateId"] 
        print(f'New verification snapshot timestamp is {snapshot_timestamp[0]}')
        await asyncio.sleep(0.1)
        print('I am up!')

        if match_found.is_set():
            print(f'Verification and local copies aligned, will run value comparison now...')
            records_match = await run_comparison(verification_snapshot,order_book, order_book_depth)
            return records_match


# async def run_verification_till_timeout(
#                         match_found: asyncio.Event,
#                         stop_fetching_verification_snapshots: asyncio.Event,
#                         snapshot_timestamp: list,
#                         local_ob_bids: dict,
#                         local_ob_ask:dict,
#                         max_verification_time =10):
#     try:
#         records_match = await asyncio.wait_for(run_verification
#             (match_found,
#             stop_fetching_verification_snapshots,
#             snapshot_timestamp,
#             local_ob_bids,
#             local_ob_ask), 
#             timeout=max_verification_time)
#         if records_match:
#             print('Verification passed')
#             return True # TBC if we want to return anything
#         print ('Verification failed')
#         return False
#     except asyncio.TimeoutError:
#         print('Verification was not completed due to timeout')
#     except Exception as e:
#         print(f'Something went wrong. Verification can not be run; {e}')


