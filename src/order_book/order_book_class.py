import bisect
import logging
from collections import namedtuple

logger = logging.getLogger(__name__)

PriceChange = namedtuple('PriceChange', ['prices_to_add_or_update', 'prices_to_remove'])

class EmptyOrderBookException(Exception):
    """Raised when the order book snapshot has no asks or bids values"""   

class OrderBook:
    def __init__(self, content:dict = None):
        self.content = content
        self.ob_bids: dict[float, float] = {} 
        self.ob_asks: dict[float, float] = {} 
        self.ob_bids_prices: list[float] = [] # always sorted ASC
        self.ob_asks_prices: list[float] = [] # always sorted ASC
        
    # Maintaining order book

    async def extract_order_book_bids_asks (self) -> tuple[list[float], list[float]]:
        try:
            self.ob_bids = {float(price):float(qty) for price,qty in self.content['bids']}
            self.ob_asks = {float(price):float(qty) for price,qty in self.content['asks']}
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
        side = self.ob_bids if book_side == 'b' else self.ob_asks
        try:    
            message_side = {float(price):float(qty) for price,qty in message.get(book_side,[])}
            for price, qty in message_side.items():
                if qty == 0:
                    side.pop(price,None)
                else:
                    side[price] = qty
        except (TypeError, KeyError, ValueError) as e:
            # No need to raise error as we want to continue execution and simply move to the next message
            logger.warning(f'Bad message received, skipping: {e}')    
        return side
    

    async def update_order_book(self, message: dict) -> tuple[dict, dict]:
        for side_key in ['b', 'a']:
            await self.update_order_book_side (message, side_key)
        return self.ob_bids, self.ob_asks
    

    async def sort_updated_order_book(self) -> dict:
        ob_bids_list = sorted(self.ob_bids.items(),reverse=True)
        ob_asks_list = sorted(self.ob_asks.items())
        self.content ['bids'] = [[f'{bid[0]:.8f}', f'{bid[1]:.8f}'] for bid in ob_bids_list]
        self.content ['asks'] = [[f'{ask[0]:.8f}', f'{ask[1]:.8f}'] for ask in ob_asks_list]
        return self.content                 

    
    async def trim_order_book(self, num_records: int =5000) -> dict:
        # Binance order book snapshot contains 5000 records
        # We need to trim the order book as we have reliable data only for 5000 records
        self.content['bids'] = self.content['bids'][0:num_records]
        self.content['asks'] = self.content['asks'][0:num_records]
        return self.content
    

    # Maintaining price list - not strictly required for order book
    # Going to use at the later stages of the project    


    async def extract_order_book_prices (self) -> tuple[list[float],list[float]]:
        #Doing sorting just in case there is some error in the snapshot and sorting is wrong
        #Since method used once, sorting should not create a massive overhead
        self.ob_bids, self.ob_asks = await self.extract_order_book_bids_asks()
        self.ob_bids_prices = sorted(self.ob_bids.keys())
        self.ob_asks_prices = sorted(self.ob_asks.keys())
        return self.ob_bids_prices, self.ob_asks_prices
    

    async def parse_price_changes_from_message(self, message: dict, side: str) -> PriceChange:
        # Records with qty = 0 should be deleted if present; if qty!=0 records should be updated if present or added if not
        prices_to_keep, prices_to_remove =[],[] 
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
    

    async def update_price_list_side (self, price_change: PriceChange, side_key:str) -> list:
        price_list = self.ob_bids_prices if side_key == 'b' else self.ob_asks_prices 
       
        for price in price_change.prices_to_add_or_update:
            index = bisect.bisect_left(price_list, price)
            if index == len(price_list) or price_list[index] != price:
                bisect.insort_left(price_list, price)
        
        for price in price_change.prices_to_remove:
            index = bisect.bisect_left(price_list, price)
            if index < len(price_list) and price == price_list[index]:
                price_list.pop(index)       
        return price_list
    

    async def update_price_lists(self, message:dict) -> tuple[list[float],list[float]]:
        for side_key in ('b','a'):
            price_change = await self.parse_price_changes_from_message(message, side_key)
            await self.update_price_list_side (price_change, side_key)
        return self.ob_bids_prices, self.ob_asks_prices


    async def trim_price_lists(self, num_records: int = 5000) -> tuple[list[float],list[float]]:
        self.ob_bids_prices = self.ob_bids_prices[-num_records:]
        self.ob_asks_prices = self.ob_asks_prices[0:num_records]
        return self.ob_bids_prices, self.ob_asks_prices


    async def format_price_lists (self) -> tuple[list[str],list[str]]:   
        bids_prices = [f'{price:.8f}' for price in reversed(self.ob_bids_prices)]
        asks_prices = [f'{price:.8f}' for price in self.ob_asks_prices]
        return bids_prices, asks_prices
    


