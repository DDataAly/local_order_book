import asyncio
from wb_sockets import get_order_book, validate_snapshot
from order_book.order_book_class import OrderBook
import logging

logger= logging.getLogger(__name__)

async def run_comparison (verification_snapshot: dict,
                    order_book: OrderBook,
                    order_book_depth: int) -> bool:
    verification_order_book=OrderBook()
    verification_order_book.ob_bids, verification_order_book.ob_asks = await verification_order_book.extract_order_book_bids_asks(verification_snapshot)

    await order_book.prepare_local_copy_for_validation(order_book_depth)
    if order_book.ob_bids == verification_order_book.ob_bids: 
        if order_book.ob_asks == verification_order_book.ob_asks:
            logger.debug('Local order book was compared with the fresh copy - and they match!')
            return True 

    return False



async def run_verification(match_found: asyncio.Event,
                           stop_fetching_verification_snapshots: asyncio.Event,
                           snapshot_timestamp: list,
                           order_book: OrderBook,
                           order_book_depth: int):
    logger.info('Verification stage started')
    while not stop_fetching_verification_snapshots.is_set():

        verification_snapshot = await get_order_book()
        if not validate_snapshot(verification_snapshot):
            continue
        snapshot_timestamp[0]=verification_snapshot["lastUpdateId"] 
        logger.debug(f'New verification snapshot timestamp is {snapshot_timestamp[0]}')
        await asyncio.sleep(0.1)

        if match_found.is_set():
            logger.debug(f'Verification and local copies aligned, will run value comparison now...')
            records_match = await run_comparison(verification_snapshot,order_book, order_book_depth)
            return records_match



