from order_book.order_book_class import OrderBook
import json
import os

#Need to refactor 
async def create_and_save_local_order_book(snapshot, order_book_last_update_id):
    order_book = OrderBook(snapshot) 
    order_book.ob_bids, order_book.ob_asks = await order_book.extract_order_book_bids_asks()
    order_book.ob_bids_prices, order_book.ob_asks_prices =  await order_book.extract_order_book_prices()
    print(f'Order book object has been initialised with order book with the last update ID {order_book_last_update_id}')   
    
    print(f"This is the path: {os.getcwd()}") # This returns path to main.py since we call this func from main.py
    parent_directory = os.getcwd()
    snapshot_directory ='data'
    full_path_directories = os.path.join(parent_directory, snapshot_directory)
    os.makedirs(full_path_directories, exist_ok = True) # This checks if the data directory exists and creates it if it doesn't

    print(f"This is the full snapshot path: {full_path_directories}") 
    full_file_path = os.path.join(full_path_directories, 'initial.json')
    print(f"This is the actual absolute path of the output: {full_file_path}")

    with open ((full_file_path), 'w') as file:
        json.dump(snapshot, file)
    print('Order book copy is saved locally')

    return order_book

