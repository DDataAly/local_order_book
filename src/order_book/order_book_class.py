import json
import bisect
import logging

logger = logging.getLogger(__name__)

print(logger)

class EmptyOrderBookException(Exception):
    """Raised when the order book snapshot has no asks or bids values"""
    

class OrderBook:
    def __init__(self, content:dict = None):
        self.content = content
        self.ob_bids = {}
        self.ob_asks = {}
        self.ob_bids_prices = []
        self.ob_asks_prices = []

        
    # Maintainig order book

    async def extract_order_book_bids_asks (self) -> tuple[dict, dict]:
        try:
            self.ob_bids = {float(price):float(qty) for [price,qty] in self.content['bids']}
            self.ob_asks = {float(price):float(qty) for [price,qty] in self.content['asks']}
            if len(self.ob_bids) == 0 or len(self.ob_asks) == 0:
                logger.critical ('Order book snapshot doesn\'t contain bids or asks and can\'t be processed')
                # Need to raise error as the execution of the code can't be continued
                raise EmptyOrderBookException ("No bids or asks in the order book snapshot")
        except (TypeError, KeyError, ValueError) as e:
            # Need to raise error as the execution of the code can't be continued 
            logger.critical (f'Order book snapshot is not valid and can\'t be processed: {e}')
            raise 
        return (self.ob_bids, self.ob_asks)    
    

    async def update_order_book_side(self, message: dict, book_side: str) -> dict:
        dispatch_table =  {
            'b' : self.ob_bids,
            'a' : self.ob_asks    
            }
        print(dispatch_table)
        side = dispatch_table[book_side]
        try:    
            message_side = {float(price):float(qty) for [price,qty] in message.get(book_side,[])}
            print('Updating in progress')
            for price, qty in message_side.items():
                if qty == 0:
                    print(f'Deleting {price} from order book if present')
                    side.pop(price,None)
                else:
                    side[price] = qty
                    print(f'Updating/adding {price} in the order book')
        except (TypeError, KeyError, ValueError) as e:
            # No need to raise error as we want to continue execution and simply move to the next message
            logger.warning(f'Bad message received, skipping: {e}')    
        return side
    

    async def update_order_book(self, message: dict) -> dict:
        for book_side in ['a','b']:
            await self.update_order_book_side (message, book_side)






# print('Updated order book bids')
# for price, qty in order_book_bids_dict.items():
#     print(f'{price:.8f}, {qty:.8f}')

# print('Updated order book bids = sorted')
# order_book_sorted_bids = dict(sorted(order_book_bids_dict.items()))
# for price, qty in order_book_sorted_bids.items():
#     print(f'{price:.8f}, {qty:.8f}')

# print('\n')
# print('Local copy of the book in json format')
# local_book = {"lastUpdateId": message['u'],
#               "bids":[[f'{price:.8f}', f'{qty:.8f}'] for price,qty in sorted(order_book_sorted_bids.items(), reverse = True)]
#             }
# print(local_book)

     
# # Maintainig order book
# order_book_bids_dict = {float(price):float(qty) for [price,qty] in order_book['bids']}
# print("Order book bids")
# for price, qty in order_book_bids_dict.items():
#     print(f'{price:.8f}, {qty:.8f}')

# message_bids_dict = {float(price):float(qty) for [price,qty] in message['b']}
# print('Message bids')
# for price, qty in message_bids_dict.items():
#     print(f'{price:.8f}, {qty:.8f}')

# print('Updating in progress')
# for price in message_bids_dict.keys():
#     if message_bids_dict[price] == 0:
#         print(f'Deleting {price} from order book if present')
#         order_book_bids_dict.pop(price,None)
#     else:
#         order_book_bids_dict[price] = message_bids_dict[price]
#         print(f'Updating/adding {price} in the order book')

# print('Updated order book bids')
# for price, qty in order_book_bids_dict.items():
#     print(f'{price:.8f}, {qty:.8f}')

# print('Updated order book bids = sorted')
# order_book_sorted_bids = dict(sorted(order_book_bids_dict.items()))
# for price, qty in order_book_sorted_bids.items():
#     print(f'{price:.8f}, {qty:.8f}')


# def return_order_book(self):
# print('\n')
# print('Local copy of the book in json format')
# local_book = {"lastUpdateId": message['u'],
#               "bids":[[f'{price:.8f}', f'{qty:.8f}'] for price,qty in sorted(order_book_sorted_bids.items(), reverse = True)]
#             }
# print(local_book)


# # Maintaining price list
# # Bids only 
# order_book_prices = [float(x[0]) for x in order_book['bids']]
# order_book_prices.reverse()  # Everything to be done on the reversed order book to maintain asc order otherwise bisect works incorrectly
# print (f'Reversed order book prices:{order_book_prices}')
# print('\n')

# prices_to_keep=[] # Includes both cases where qty changes and new price levels are added
# prices_to_remove=[] # Includes records with zero qty
# message_bids =[[float(x[0]), float(x[1])] for x in message['b']]
# for message in message_bids:
#     if message[1] == 0:
#         prices_to_remove.append(message[0])
#     else:
#         prices_to_keep.append(message[0])
# print(f'Prices to keep {prices_to_keep}')
# print(f'Prices to remove {prices_to_remove}')


# for price in prices_to_keep:
#     index = bisect.bisect_left(order_book_prices, price)
#     if order_book_prices[index ] == price:
#         print(f'{price} already in the book')
#         continue
#     else:
#         bisect.insort_left(order_book_prices, price)
# print(order_book_prices)
# print('\n')

# # for price in prices_to_keep:
# #     print(price in order_book_prices)


# for price in prices_to_remove:
#     #print(f'{price}')
#     #print(price in order_book_prices)
#     index = bisect.bisect_left(order_book_prices, price)
#     #print(index)
#     #print(len(order_book_prices))
#     if index in range (1,len(order_book_prices)):
#         order_book_prices.pop(index)
#         print (f'Price {price} has been removed')
#     else:
#         print (f'Price {price} is outside the order book range')
#         continue
# print(order_book_prices)

# #for price in prices_to_remove:
# #    print(price in order_book_prices)




# ws_stream = "{\"e\":\"depthUpdate\",\"E\":1753887116314,\"s\":\"BTCUSDT\",\"U\":73718086122,\"u\":73718086156,\"b\":[[\"118467.65000000\",\"1.60662000\"],[\"118467.64000000\",\"0.00020000\"],[\"118459.20000000\",\"0.30390000\"],[\"118458.78000000\",\"0.00000000\"],[\"118457.78000000\",\"0.01693000\"],[\"118457.29000000\",\"0.00010000\"],[\"118429.19000000\",\"0.00000000\"],[\"118428.94000000\",\"0.04230000\"],[\"118186.88000000\",\"0.02538000\"],[\"106620.00000000\",\"0.00020000\"]],\"a\":[[\"118467.66000000\",\"4.62422000\"],[\"118470.00000000\",\"0.03682000\"],[\"118470.60000000\",\"0.00000000\"],[\"118472.24000000\",\"0.30404000\"],[\"118494.15000000\",\"0.00000000\"],[\"118499.01000000\",\"0.00000000\"]]}"

# def get_order_book():
#     snapshot = requests.get('https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20').json()
#     with open ((path_initial_shapshot), 'w') as file:
#         json.dump(snapshot, file)
#     print('Order book is saved')
#     order_book_timestamp = snapshot.get("lastUpdateId")
#     return order_book_timestamp

     
# get_order_book()      

