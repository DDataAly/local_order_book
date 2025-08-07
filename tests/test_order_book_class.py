import pytest_asyncio
import pytest
import logging
from src.order_book.order_book_class import OrderBook

@pytest.fixture
def small_order_book():
    content = {"lastUpdateId": 74105025813, 
                        "bids": [["113678.85000000", "7.25330000"], 
                                ["113678.84000000", "0.77360000"]
                            ], 
                        "asks": [["113678.86000000", "1.93563000"], 
                                ["113679.35000000", "0.03677000"]
                                ]
                            }
    order_book = OrderBook(content)
    return order_book

@pytest.fixture
def big_order_book():
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
    order_book = OrderBook(content)
    return order_book

@pytest.fixture
def short_message():
    return {'e': 'depthUpdate', 
            'E': 1754423900214, 
            's': 'BTCUSDT', 
            'U': 74105025814, 
            'u': 74105025843, 
            'b': [['114000.57000000', '0.30000000'], ['113688.21000000', '0.40000000']],
            'a': [['113678.86000000', '1.93563000'], ['113690.71000000', '0.48505000']]
            }


@pytest.fixture
def long_message():
    return {'e': 'depthUpdate', 
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
            ['113000.00000000', '0.00000000']], # delete the item which is smaller than the smallest bid in the order book
            

            'a': 
            [['113678.86000000', '1.93563000'], 
                ['113678.87000000', '0.00698000'], 
                ['113689.58000000', '0.00023000'], 
                ['113689.59000000', '0.35536000'], 
                ['113689.88000000', '0.00005000'], 
                ['113689.89000000', '0.29047000'], 
                ['113690.71000000', '0.48505000']
                 ]}


@pytest.mark.describe('Order book tests')
class TestOrderBookInit:
    @pytest.mark.it('should intitialise an order book object')
    def test_initialise_object(self):
        order_book = OrderBook()
        assert isinstance(order_book, OrderBook)
    
    @pytest.mark.it('should initialise with correct values')
    def test_initalise_values(self):
        order_book = OrderBook({"lastUpdateId": 74105025813, 
                                "bids": [["113678.85000000", "7.25330000"], 
                                        ["113674.27000000", "0.00007000"]],
                                "asks": [["113678.85000000", "7.25330000"],
                                        ["113685.38000000", "0.00200000"]]})
        assert order_book.content == {"lastUpdateId": 74105025813, 
                                "bids": [["113678.85000000", "7.25330000"], 
                                        ["113674.27000000", "0.00007000"]],
                                "asks": [["113678.85000000", "7.25330000"],
                                        ["113685.38000000", "0.00200000"]]}

class TestUpdateOrderBook:
    @pytest.mark.it('should return a dict when called')
    @pytest.mark.asyncio
    async def test_returns_dict(self, small_order_book):
        message = {}
        result = await small_order_book.update_order_book_bids(message)
        assert isinstance(result, dict)

    @pytest.mark.it('correctly parse bid price and qty from the order book snapshot')
    @pytest.mark.asyncio
    async def test_parses_snapshot(self, small_order_book):
        message = {}
        result = await small_order_book.update_order_book_bids(message)
        assert result == {113678.85000000 : 7.25330000, 113678.84000000: 0.77360000}


    @pytest.mark.it('correctly parse bid price and qty from the WebSocket stream messages')
    @pytest.mark.asyncio
    async def test_parses_message(self, short_message):
        order_book = OrderBook({"lastUpdateId": 74105025813, 
                        "bids": [], 
                        "asks": []})
        result = await order_book.update_order_book_bids(short_message)
        assert result == {114000.57000000: 0.30000000, 113688.21000000 : 0.40000000}


    @pytest.mark.it('logs errors if WebSocket stream message is of the wrong type or format')
    @pytest.mark.asyncio
    async def test_logs_errors(self, small_order_book, caplog):
        message = {'e': 'depthUpdate', 
            'E': 1754423900214, 
            's': 'BTCUSDT', 
            'U': 74105025814, 
            'u': 74105025843, 
            'b': [['some_text', '0.30000000']],
            'a': []
            }
        caplog.set_level(logging.WARNING, logger="src.order_book.order_book_class")
        result = await small_order_book.update_order_book_bids(message)
        assert 'Bad message received, skipping' in caplog.text 

    @pytest.mark.it('removes bids from the order book if qty for this bid in the WebSocket stream message is zero')
    @pytest.mark.asyncio
    async def test_removes_zero_qty_bids(self, small_order_book):
        message = {'e': 'depthUpdate', 
            'E': 1754423900214, 
            's': 'BTCUSDT', 
            'U': 74105025814, 
            'u': 74105025843, 
            'b': [["113678.84000000", "0.00000000"]],
            'a': []
            }  
        result = await small_order_book.update_order_book_bids (message)
        assert result == {113678.85000000: 7.25330000}

    @pytest.mark.it('ignores bids in the WebSocket stream message with zero qty if they\'re not in the order book')
    @pytest.mark.asyncio
    async def test_ignores_non_present_zero_qty_bids(self, small_order_book):
        # Bids with prices higher than order book max are possible e.g. if the bid was placed and closed immediately
        message = {'e': 'depthUpdate', 
            'E': 1754423900214, 
            's': 'BTCUSDT', 
            'U': 74105025814, 
            'u': 74105025843, 
            'b': [["113680.84000000", "0.00000000"],["113300.84000000", "0.00000000"]],
            'a': []
            }  
        result = await small_order_book.update_order_book_bids (message)
        assert result == {113678.85000000: 7.25330000, 113678.84000000 : 0.77360000} 

    @pytest.mark.it('if bid is present in the order book and WebSocket stream message  the qty in the order book becomes equal to qty in the message')
    @pytest.mark.asyncio
    async def test_updates_bids_qty(self, small_order_book):
        message = {'e': 'depthUpdate', 
            'E': 1754423900214, 
            's': 'BTCUSDT', 
            'U': 74105025814, 
            'u': 74105025843, 
            'b': [["113678.85000000", "2.00000000"]],
            'a': []
            }  
        result = await small_order_book.update_order_book_bids (message)
        assert result == {113678.85000000: 2.00000000,
                          113678.84000000: 0.77360000}   

    @pytest.mark.it('adds bids in the WebSocket stream message with non-zero qty if they\'re not in the order book')
    @pytest.mark.asyncio
    async def test_adds_non_present_non_zero_qty_bids(self, small_order_book):
        # Bids with prices higher than order book max are possible e.g. if the bid was placed and closed immediately
        message = {'e': 'depthUpdate', 
            'E': 1754423900214, 
            's': 'BTCUSDT', 
            'U': 74105025814, 
            'u': 74105025843, 
            'b': [["113680.84000000", "4.00000000"],["113300.84000000", "0.50000000"]],
            'a': []
            }  
        result = await small_order_book.update_order_book_bids (message)
        assert result == {113678.85000000: 7.25330000, 
                          113678.84000000 : 0.77360000,
                          113680.84000000 : 4.00000000,
                          113300.84000000 : 0.50000000} 
        
    @pytest.mark.it('processes messages with different types of updates correctly')
    @pytest.mark.asyncio
    async def test_processes_multiple_updates(self, big_order_book, long_message):
        result = await big_order_book.update_order_book_bids (long_message)
        assert result == {113678.85000000 : 7.25330000, 
                        113678.84000000 : 0.77360000, 
                        113677.94000000 : 0.00005000,
                        113676.92000000 : 1.00000000, 
                        113675.73000000 : 0.00007000,
                        113674.27000000 : 0.00007000,
                        113688.21000000 : 0.40000000,
                        113670.84000000 : 0.10000000}

