from order_book.order_book_class import OrderBook
import json
import os
import logging

logger= logging.getLogger(__name__)

async def create_order_book(snapshot) -> OrderBook:
    try: 
        save_snapshot(snapshot)
    except Exception:
        logger.debug("Error saving initial snapshot")
    
    try:
        order_book = OrderBook() 
        order_book.ob_bids, order_book.ob_asks = await order_book.extract_order_book_bids_asks(snapshot)
        logger.info(f'Order book object has been created based on the snapshot with the last update ID {snapshot["lastUpdateId"]}')   
        return order_book
    except Exception:
        logger.warning('An error occurred creating order book object')
        raise

# TODO - use Pathlib for below
def save_snapshot(snapshot: dict) -> None:
    # Find the project directory path 
    PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Create data folder in the project directory if doesn't exist
    snapshot_directory = os.path.join(PROJECT_DIR, "ob_snapshots")
    os.makedirs(snapshot_directory, exist_ok = True)

    # Create path to the file where we save the snapshot to
    full_file_path = os.path.join(snapshot_directory, 'ob_initial_snapshot.json')

    with open ((full_file_path), 'w') as file:
        json.dump(snapshot, file)
    logger.info(f'Initial snapshot of the order book is saved at {full_file_path}')

