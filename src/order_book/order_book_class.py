import json
import bisect
import logging

logger = logging.getLogger(__name__)

order_book = {"lastUpdateId": 74105025813, "bids": [["113678.85000000", "7.25330000"], ["113678.84000000", "0.77360000"], ["113678.21000000", "0.01775000"], ["113678.00000000", "0.03190000"], ["113677.99000000", "0.00005000"], ["113677.94000000", "0.00005000"], ["113677.93000000", "0.29045000"], ["113677.59000000", "0.06521000"], ["113677.42000000", "0.08374000"], ["113676.92000000", "0.00010000"], ["113676.60000000", "0.04058000"], ["113676.00000000", "0.00005000"], ["113675.73000000", "0.00007000"], ["113675.59000000", "0.06669000"], ["113675.57000000", "0.02510000"], ["113675.54000000", "0.00005000"], ["113675.11000000", "0.00005000"], ["113675.10000000", "0.31680000"], ["113674.77000000", "0.07648000"], ["113674.27000000", "0.00007000"]], "asks": [["113678.86000000", "1.93563000"], ["113678.87000000", "0.00713000"], ["113679.34000000", "0.00005000"], ["113679.35000000", "0.03677000"], ["113680.00000000", "0.00005000"], ["113681.32000000", "0.00005000"], ["113681.41000000", "0.00005000"], ["113681.42000000", "0.04418000"], ["113682.30000000", "0.00005000"], ["113683.06000000", "0.00085000"], ["113683.82000000", "0.00005000"], ["113684.00000000", "0.00080000"], ["113685.38000000", "0.00200000"], ["113685.71000000", "0.00025000"], ["113686.27000000", "0.00005000"], ["113686.28000000", "0.06912000"], ["113686.32000000", "0.00005000"], ["113686.33000000", "0.12770000"], ["113686.87000000", "0.00005000"], ["113686.88000000", "0.06029000"]]}
message = {'e': 'depthUpdate', 'E': 1754423900214, 's': 'BTCUSDT', 'U': 74105025814, 'u': 74105025843, 'b': [['113678.84000000', '0.1'], ['113644.65000000', '0.04336000'], ['103395.52000000', '0.02640000'], ['114000.57000000', '0.00000000'], ['112809.57000000', '0.00000000'], ['113678.21000000', '0.00000000']], 'a': [['113678.86000000', '1.93563000'], ['113678.87000000', '0.00698000'], ['113689.58000000', '0.00023000'], ['113689.59000000', '0.35536000'], ['113689.88000000', '0.00005000'], ['113689.89000000', '0.29047000'], ['113690.71000000', '0.48505000'], ['113691.75000000', '0.00000000'], ['113705.35000000', '0.31667000'], ['113799.05000000', '0.00000000']]}

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
        except (TypeError, KeyError, ValueError) as e:
            logger.critical (f'Order book snapshot is not valid and can\'t be processed: {e}')
        return (self.ob_bids, self.ob_asks)    
    

    async def update_order_book_side(self, message: dict, book_side: str) -> dict:
        dispatch_table =  {
            'b' : self.ob_bids,
            'a' : self.ob_asks    
            }
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

