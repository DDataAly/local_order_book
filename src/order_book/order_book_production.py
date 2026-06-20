from order_book.order_book_class import OrderBook
import json
import os

async def create_order_book(snapshot) -> OrderBook:
    try: 
        save_snapshot(snapshot)
    except Exception:
        print ("Error saving initial snapshot")
    
    try:
        order_book = OrderBook(snapshot) 
        order_book.ob_bids, order_book.ob_asks = await order_book.extract_order_book_bids_asks()
        order_book.ob_bids_prices, order_book.ob_asks_prices =  await order_book.extract_order_book_prices()
        print(f'Order book object has been created based on the snapshot with the last update ID {snapshot["lastUpdateId"]}')   
        return order_book
    except Exception:
        print('An error occurred creating order book object')
        raise


def save_snapshot(snapshot: dict) -> None:
    # Find the project directory path 
    PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    print(f"This is the project directory: {PROJECT_DIR}")

    # Create data folder in the project directory if doesn't exist
    snapshot_directory = os.path.join(PROJECT_DIR, "data")
    os.makedirs(snapshot_directory, exist_ok = True)
    print(f"This is the path to the directory where we want to save the snapshot: {snapshot_directory}") 

    # Create path to the file where we save the snapshot to
    full_file_path = os.path.join(snapshot_directory, 'ob_initial_snapshot.json')
    print(f"This is the path of the file where we will write the snapshot to: {full_file_path}")
    with open ((full_file_path), 'w') as file:
        json.dump(snapshot, file)
    print('Initial snapshot of the order book is saved locally')




# async def save_local_copy_ready_for_validation(order_book):
#     await order_book.prepare_local_copy_for_validation(order_book)

# def inner_layer():
#     raise ValueError

# def mid_layer():
#     print('cats')
#     inner_layer()

# def outer_layer():
#     print ("Fabulous")
#     try:
#         mid_layer()
#     except Exception as e:
#         print('Here we are!')
#         raise e

# outer_layer()
