import json
import bisect
import logging
import asyncio
from collections import namedtuple

logger = logging.getLogger(__name__)

PriceChange = namedtuple('PriceChange', ['prices_to_add_or_update', 'prices_to_remove'])

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
        side = dispatch_table[book_side]
        try:    
            message_side = {float(price):float(qty) for [price,qty] in message.get(book_side,[])}
            for price, qty in message_side.items():
                if qty == 0:
                    side.pop(price,None)
                else:
                    side[price] = qty
        except (TypeError, KeyError, ValueError) as e:
            # No need to raise error as we want to continue execution and simply move to the next message
            logger.warning(f'Bad message received, skipping: {e}')    
        return side
    

    async def update_order_book(self, message: dict) -> dict:
        dispatch_table =  {
            'b' : self.ob_bids,
            'a' : self.ob_asks    
            }
        for side in dispatch_table:
            side_records = await self.update_order_book_side (message, side[0])
            dispatch_table[side] = [
                [f'{price:.8f}', f'{qty:.8f}'] for price,qty in side_records.items()
                                 ]
        # Need to separate sorting logic into separate func to avoid sorting every time after we add a message    
        self.content ['bids'] = sorted(dispatch_table['b'], key = lambda x: float(x[0]), reverse=True)
        self.content ['asks'] = sorted(dispatch_table['a'], key = lambda x: float(x[0]))
        return self.content
    
    async def trim_order_book(self, num_records: int =5000) -> dict:
        # Binance order book snapshot contains 5000 records
        # We need to trim the order book as we have reliable data only for 5000 records
        self.content['bids'] = self.content['bids'][0:num_records]
        self.content['asks'] = self.content['asks'][0:num_records]
        return self.content


# Maintaining price list - not strictly required for order book
# Going to use at the later stages of the project    

    async def extract_ob_prices (self) -> tuple[list,list]:
        self.ob_bids, self.ob_asks = await self.extract_order_book_bids_asks()
        self.ob_bids_prices = [price for price in self.ob_bids.keys()]
        self.ob_bids_prices.reverse()
        self.ob_asks_prices = [price for price in self.ob_asks.keys()]
        return (self.ob_bids_prices, self.ob_asks_prices)
    
    async def get_prices_from_message(self, message: dict, side: str) -> PriceChange:
        prices_to_keep =[] # Includes both cases where qty changes and new price levels are added
        prices_to_remove =[] # Includes records with zero qty
        try:
            message_price_qty =[[float(price), float(qty)] for price,qty in message[side]]
            for price,qty in message_price_qty:
                if qty == 0:
                    prices_to_remove.append(price)
                else:
                    prices_to_keep.append(price)
        except (TypeError, KeyError, ValueError) as e:
            logger.warning(f'Bad message received, skipping: {e}')  
        return PriceChange(prices_to_add_or_update = prices_to_keep, 
                            prices_to_remove = prices_to_remove)
    
    async def update_ob_prices (self, PriceChange: namedtuple, side:str) -> list:
        dispatch_table =  {
            'b' : self.ob_bids_prices,
            'a' : self.ob_asks_prices    
            }
        
        for price in PriceChange.prices_to_add_or_update:
            index = bisect.bisect_left(dispatch_table[side], price)
            if index < len(dispatch_table[side]) and dispatch_table[side][index] == price:
                print(f'{price} already in the book')
                continue
            else:
                bisect.insort_left(dispatch_table[side], price)
        print(f'This is the list the adder/updater finishes with {dispatch_table[side]}')

    
        for price in PriceChange.prices_to_remove:
            print(f'This is the list the remover starts with {dispatch_table[side]}')
            print(f'This is what we\'re going to remove: {PriceChange.prices_to_remove}')
            print('\n')
            index = bisect.bisect_left(dispatch_table[side], price)
            print(index)
            if index < len(dispatch_table[side]) and price == dispatch_table[side][index]:
                dispatch_table[side].pop(index)
                print (f'Price {price} has been removed')
            else:
                print (f'Price {price} is outside the order book range or not present in the order book')
                continue
        return(dispatch_table[side])
    

    async def update_order_book_prices(self, message:dict) -> dict:
        dispatch_table =  {
            'b' : self.ob_bids_prices,
            'a' : self.ob_asks_prices    
            }
        for side in dispatch_table:
            price_change = await self.get_prices_from_message(message, side[0])
            side_records = await self.update_ob_prices (price_change, side[0])
            dispatch_table[side] = [
                f'{price:.8f}' for price in side_records
                                 ]
        return (self.ob_bids_prices, self.ob_asks_prices)



# #for price in prices_to_remove:
# #    print(price in order_book_prices)
    
     
        
content = {"lastUpdateId": 74105025813, 
                    "bids": [["113678.85000000", "7.25330000"], 
                            ["113678.84000000", "0.77360000"], 
                            ["113677.94000000", "0.00005000"], 
                            ["113676.92000000", "0.00010000"], 
                            ["113675.73000000", "0.00007000"], 
                            ["113674.77000000", "0.07648000"], 
                            ["113674.27000000", "0.00007000"]
                        ], 
                    "asks": [["113678.86000000", "1.93563000"], 
                            ["113679.35000000", "0.03677000"], 
                            ["113681.32000000", "0.00005000"], 
                            ["113682.30000000", "0.00005000"], 
                            ["113683.82000000", "0.00005000"], 
                            ["113684.00000000", "0.00080000"], 
                            ["113685.38000000", "0.00200000"]
                            ]
                        }

message = {'e': 'depthUpdate', 
            'E': 1754423900214, 
            's': 'BTCUSDT', 
            'U': 74105025814, 
            'u': 74105025843, 
            'b': 
            [    
                ['114000.57000000', '0.00000000'], # delete the item which is bigger than the biggest bid in the order book
                ['113688.21000000', '0.40000000'],  # add bid better than the best bid in the order book
                ['113676.92000000', '1.00000000'], # change qty for the item present in the order book
                ['113674.77000000', '0.00000000'], # delete the item which is present in the local order book
                ['113670.84000000', '0.10000000'], # add bid lower than the best bid int the order book
                ['113000.00000000', '0.00000000'] # delete the item which is smaller than the smallest bid in the order book
            ],
            'a': 
            [
                ['113678.86000000', '1.93563000'], 
                ['113678.87000000', '0.00698000'], 
                ['113689.58000000', '0.00023000'], 
                ['113689.59000000', '0.35536000'], 
                ['113689.88000000', '0.00005000'], 
                ['113689.89000000', '0.29047000'], 
                ['113690.71000000', '0.48505000']
                 ]
            }

async def run_code():
    order_book = OrderBook(content)
    result = await order_book.extract_order_book_bids_asks()
    print(f'Prices and qtys {result}')
    result_new = await order_book.extract_ob_prices()
    print(f'These are price lists: {result_new}')

    ob_bids_prices, ob_asks_prices = await order_book.update_order_book_prices (message)
    print (f'These are the bids with all updates {ob_bids_prices}')
    print (f'These are the asks with all updates {ob_asks_prices}')
asyncio.run(run_code())



