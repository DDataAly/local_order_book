import pytest_asyncio
import pytest
import logging
from src.order_book.order_book_class import OrderBook, EmptyOrderBookException

@pytest.fixture
def small_order_book():
    content = {"lastUpdateId": 74105025813, 
                        "bids": [["113678.85000000", "7.25330000"], 
                                ["113678.84000000", "0.77360000"]
                            ], 
                        "asks": [["113678.86000000", "1.93563000"], 
                                ["113900.35000000", "0.13677000"]
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
            'b': [['114000.57000000', '0.30000000']],
            'a': [['113666.20000000', '2.93563000']]
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


class TestExtractOrderBookBidsAsks:
    @pytest.mark.it('should return a tuple with two dict when called')
    @pytest.mark.asyncio
    async def test_returns_dict(self, small_order_book):
        message = {}
        result = await small_order_book.extract_order_book_bids_asks()
        assert isinstance(result, tuple)
        assert isinstance(result[0], dict)
        assert isinstance(result[1], dict)
    
    @pytest.mark.it('correctly parses prices and qtys from the order book')
    @pytest.mark.parametrize("index, expected_output",                            
                            [
                                (0, {113678.85000000 : 7.25330000, 113678.84000000: 0.77360000}),
                                (1, {113678.86000000 : 1.93563000, 113900.35000000 : 0.13677000})  
                            ],
                            ids = ['parses bids',
                                'parses asks']
                        )           
    @pytest.mark.asyncio
    async def test_parses_snapshot_correctly(self,small_order_book, index, expected_output):
            result = await small_order_book.extract_order_book_bids_asks()
            assert result[index] == expected_output    


    @pytest.mark.it('catches invalid order book: raises and logs errors')
    @pytest.mark.parametrize("content, expected_error, expected_error_message, expected_log",                            
                                [
                                    (
                                    {"lastUpdateId": 74105025813, 
                                     "bids": [["113678.85000000", "7.25330000"], ["113678.84000000", "0.77360000"]]}, 
                                    KeyError,
                                    'asks', 
                                    'Order book snapshot is not valid' 
                                    ),
                                                                        (
                                    {"lastUpdateId": 74105025813, 
                                     "asks": [["113685.38000000", "0.00200000"]]}, 
                                    KeyError,
                                    'bids', 
                                    'Order book snapshot is not valid' 
                                    ),
                                     (
                                    {"lastUpdateId": 74105025813, 
                                    "bids": [],
                                    "asks":  [["113685.38000000", "0.00200000"]]}, 
                                    EmptyOrderBookException,
                                    "No bids or asks in the order book snapshot",
                                    "Order book snapshot doesn\'t contain bids or asks"
                                    ),
                                    (
                                    {"lastUpdateId": 74105025813, 
                                    "bids": [["113678.85000000", "7.25330000"]],
                                    "asks":  []}, 
                                    EmptyOrderBookException,
                                    "No bids or asks in the order book snapshot",
                                    "Order book snapshot doesn\'t contain bids or asks"
                                    ),
                                    (
                                    {"lastUpdateId": 74105025813, 
                                    "bids": [["113678.85000000", "7.25330000"], ["113678.84000000", "0.77360000"]],
                                    "asks":  [["some_text", "7.25330000"],["113685.38000000", "0.00200000"]]},
                                    ValueError,
                                    "could not convert string to float",
                                    'Order book snapshot is not valid'
                                    ),
                                    (
                                    {"lastUpdateId": 74105025813, 
                                    "bids": [["113678.85000000", "text"], ["113678.84000000", "0.77360000"]],
                                    "asks":  [["113685.38000000", "0.00200000"]]},
                                    ValueError,
                                    "could not convert string to float",
                                    'Order book snapshot is not valid'
                                    ),
                                    (
                                    {"lastUpdateId": 74105025813, 
                                    "bids": [144, ["113678.85000000", "7.25330000"]],
                                    "asks":  [["113685.98000000", "7.25330000"],["113685.38000000", "0.00200000"]]},
                                    TypeError,
                                    'cannot unpack non-iterable',
                                    'Order book snapshot is not valid'
                                    )                                 
                                ],
                            ids = ['missing asks key',
                                   'missing bids key',
                                   'missing bids values',
                                   'missing asks values',
                                   'invalid price format',
                                   'invalid qty format',
                                   'invalid price-qty pair format'
                                ]    
                            )
    @pytest.mark.asyncio
    async def test_raises_error(self, content, expected_error, expected_error_message, expected_log, caplog):
        order_book = OrderBook()
        order_book.content = content
        caplog.set_level(logging.CRITICAL, logger="src.order_book.order_book_class")
        with pytest.raises (expected_error) as e:
            await order_book.extract_order_book_bids_asks()
        assert expected_error_message in str(e.value)  
        assert expected_log in caplog.text 


class TestUpdateOrderBookSide:
    @pytest.mark.it('should return a dict when called')
    @pytest.mark.asyncio
    async def test_returns_dict(self, small_order_book):
        await small_order_book.extract_order_book_bids_asks()
        result = await small_order_book.update_order_book_side(message ={}, book_side ='b')
        assert isinstance(result, dict)


    @pytest.mark.it('correctly parse bid price and qty from the WebSocket stream message')
    @pytest.mark.parametrize('index, expected_output',
                             [
                                ('b', {113678.85000000 : 7.25330000, 113678.84000000 : 0.77360000, 114000.57000000 : 0.30000000}),
                                ('a', {113678.86000000 : 1.93563000, 113900.35000000 : 0.13677000, 113666.20000000 : 2.93563000})
                              ],
                             ids = ['parses bids side', 'parses asks side'])
    @pytest.mark.asyncio
    async def test_parses_message(self, small_order_book, short_message, index, expected_output):
        await small_order_book.extract_order_book_bids_asks()
        result = await small_order_book.update_order_book_side(short_message, book_side= index)
        assert result == expected_output


    @pytest.mark.it('catches invalid WebSocket stream messages: logs errors')
    @pytest.mark.parametrize('index, side_value, expected_log',
                             [
                                 ('b',['some_text', '0.30000000'],'Bad message received'),
                                 ('b',['113900.35000000', 'some_text'],'Bad message received'),
                                 ('b', {113678.85000000 : 7.25330000}, 'Bad message received'),
                                 ('a',['some_text', '0.80000000'],'Bad message received'),
                                 ('a',['114952.35000000', 'some_text'],'Bad message received'),
                                 ('a', {113678.86000000 : 1.93563000},'Bad message received')                    
                             ],
                             ids = ['invalid price format in bids',
                                    'invalid qty format in bids',
                                    'invalid price-qty pair format in bids',
                                    'invalid price format in asks',
                                    'invalid qty format in asks', 
                                    'invalid price-qty pair format in asks']
                            )        
         
    @pytest.mark.asyncio                        
    async def test_logs_invalid_message_errors(self, small_order_book, short_message, index, side_value, expected_log, caplog):
        short_message [index] = side_value
        # We need to isolate the test case by making sure the other side doesn't cause the error
        if index == 'a':
            short_message['b'] = []
        else: 
            short_message['a'] = []
        caplog.set_level(logging.WARNING, logger="src.order_book.order_book_class")
        await small_order_book.extract_order_book_bids_asks()
        await small_order_book.update_order_book_side(short_message, book_side= index)
        assert expected_log in caplog.text 


    @pytest.mark.it('correctly processes individual updates')
    # Bids with prices higher than order book max are possible e.g. if the bid was placed and closed immediately
    @pytest.mark.parametrize('index, side_value, expected_output',
                             [
                                ('b', [["113678.84000000", "0.00000000"]], {113678.85000000: 7.25330000}),
                                ('b', [["113680.84000000", "0.00000000"],["113300.84000000", "0.00000000"]], 
                                 {113678.85000000: 7.25330000, 113678.84000000 : 0.77360000}),
                                ('b', [["113678.85000000", "2.00000000"]], {113678.85000000: 2.00000000, 113678.84000000: 0.77360000}),
                                ('b', [["113680.84000000", "4.00000000"],["113300.84000000", "0.50000000"]], 
                                    {113678.85000000: 7.25330000, 
                                    113678.84000000 : 0.77360000,
                                    113680.84000000 : 4.00000000,
                                    113300.84000000 : 0.50000000}),
                                ('a', [["113678.86000000", "0.00000000"]], {113900.35000000 : 0.13677000}),
                                ('a', [["113900.84000000", "0.00000000"],["113300.84000000", "0.00000000"]], 
                                 {113678.86000000 : 1.93563000, 113900.35000000 : 0.13677000}), 
                                ('a', [["113678.86000000", "2.00000000"]], {113678.86000000: 2.00000000, 113900.35000000 : 0.13677000}),
                                ('a', [["114680.84000000", "4.00000000"],["113300.84000000", "0.50000000"]], 
                                    {113678.86000000 : 1.93563000, 
                                    113900.35000000 : 0.13677000,
                                    114680.84000000 : 4.00000000,
                                    113300.84000000 : 0.50000000})  
                             ],
                             ids = [
                             'removes bid from order book - qty for bid in the message is zero',
                             'ignores bids with zero qty in the message if they\'re not in the order book',    
                             'updates bid qty in the order book with its qty from the message',
                             'adds new bids in the order book from the message',
                             'removes ask from order book - qty for ask in the message is zero',
                             'ignores asks with zero qty in the message if they\'re not in the order book',
                             'updates ask qty in the order book with its qty from the message',
                             'adds new asks in the order book from the message']   
                            )
    @pytest.mark.asyncio
    async def test_updates(self, short_message, small_order_book, index, side_value, expected_output):
        short_message [index] = side_value
        await small_order_book.extract_order_book_bids_asks()
        result = await small_order_book.update_order_book_side (short_message, book_side = index)
        assert result == expected_output

        
    @pytest.mark.it('processes messages with different types of updates correctly')
    @pytest.mark.asyncio
    async def test_processes_multiple_updates(self, big_order_book, long_message):
        await big_order_book.extract_order_book_bids_asks()
        result = await big_order_book.update_order_book_side (long_message, book_side ='b')
        assert result == {113678.85000000 : 7.25330000, 
                        113678.84000000 : 0.77360000, 
                        113677.94000000 : 0.00005000,
                        113676.92000000 : 1.00000000, 
                        113675.73000000 : 0.00007000,
                        113674.27000000 : 0.00007000,
                        113688.21000000 : 0.40000000,
                        113670.84000000 : 0.10000000}
        
class TestUpdateOrderBook:
    @pytest.mark.it('returns a dictionary with correct keys')
    @pytest.mark.asyncio
    async def test_returns_correct_dict(self, small_order_book, short_message):
        await small_order_book.extract_order_book_bids_asks()
        result = await small_order_book.update_order_book(short_message)
        assert isinstance(result, dict)
        assert {'lastUpdateId', 'bids','asks'}.issubset(set(result.keys()))
        assert len(result) == 3 


    @pytest.mark.it('correctly updates order book content - message with bids only')
    @pytest.mark.asyncio
    async def test_updates_ob_bids_only(self, big_order_book, long_message):
        message = long_message
        message['a'] = [] 
        await big_order_book.extract_order_book_bids_asks()
        result = await big_order_book.update_order_book(message)
        assert result == {"lastUpdateId": 74105025813, 
                          "bids" : [
                            ['113688.21000000' , '0.40000000'],
                            ['113678.85000000' , '7.25330000'], 
                            ['113678.84000000' , '0.77360000'], 
                            ['113677.94000000' , '0.00005000'],
                            ['113676.92000000' , '1.00000000'], 
                            ['113675.73000000' , '0.00007000'],
                            ['113674.27000000' , '0.00007000'],
                            ['113670.84000000' , '0.10000000']
                            ],
                        "asks" : 
                            [
                            ["113678.86000000", "1.93563000"], 
                            ["113679.35000000", "0.03677000"], 
                            ["113681.32000000", "0.00005000"], 
                            ["113682.30000000", "0.00005000"], 
                            ["113683.82000000", "0.00005000"], 
                            ["113684.00000000", "0.00080000"], 
                            ["113685.38000000", "0.00200000"]
                            ]
                        }
        

    @pytest.mark.it('correctly updates order book content - message with asks only')
    @pytest.mark.asyncio
    async def test_updates_ob_asks_only(self, big_order_book, long_message):
        message = long_message
        message['b'] = [] 
        await big_order_book.extract_order_book_bids_asks()
        result = await big_order_book.update_order_book(message)
        assert result == {"lastUpdateId": 74105025813, 
                          "bids" : [
                            ["113678.85000000", "7.25330000"], 
                            ["113678.84000000", "0.77360000"], 
                            ["113677.94000000", "0.00005000"], 
                            ["113676.92000000", "0.00010000"], 
                            ["113675.73000000", "0.00007000"], 
                            ["113674.77000000", "0.07648000"], 
                            ["113674.27000000", "0.00007000"]
                            ],
                        "asks" : 
                            [ 
                            ["113678.86000000", "1.93563000"],     
                            ['113678.87000000', '0.00698000'], 
                            ["113679.35000000", "0.03677000"], 
                            ["113681.32000000", "0.00005000"], 
                            ["113682.30000000", "0.00005000"], 
                            ["113683.82000000", "0.00005000"], 
                            ["113684.00000000", "0.00080000"], 
                            ["113685.38000000", "0.00200000"],
                            ['113689.58000000', '0.00023000'], 
                            ['113689.59000000', '0.35536000'], 
                            ['113689.88000000', '0.00005000'], 
                            ['113689.89000000', '0.29047000'], 
                            ['113690.71000000', '0.48505000']
                            ]
                        }
        
    @pytest.mark.it('correctly updates order book content - message with bids and asks')
    @pytest.mark.asyncio
    async def test_updates_ob_bids_and_asks(self, big_order_book, long_message):
        await big_order_book.extract_order_book_bids_asks()
        result = await big_order_book.update_order_book(long_message)
        assert result == {"lastUpdateId": 74105025813, 
                          "bids" : [
                            ['113688.21000000' , '0.40000000'],
                            ['113678.85000000' , '7.25330000'], 
                            ['113678.84000000' , '0.77360000'], 
                            ['113677.94000000' , '0.00005000'],
                            ['113676.92000000' , '1.00000000'], 
                            ['113675.73000000' , '0.00007000'],
                            ['113674.27000000' , '0.00007000'],
                            ['113670.84000000' , '0.10000000']
                            ],
                        "asks" : 
                            [ 
                            ["113678.86000000", "1.93563000"],     
                            ['113678.87000000', '0.00698000'], 
                            ["113679.35000000", "0.03677000"], 
                            ["113681.32000000", "0.00005000"], 
                            ["113682.30000000", "0.00005000"], 
                            ["113683.82000000", "0.00005000"], 
                            ["113684.00000000", "0.00080000"], 
                            ["113685.38000000", "0.00200000"],
                            ['113689.58000000', '0.00023000'], 
                            ['113689.59000000', '0.35536000'], 
                            ['113689.88000000', '0.00005000'], 
                            ['113689.89000000', '0.29047000'], 
                            ['113690.71000000', '0.48505000']
                            ]
                        }    

class TestTrimOrderBook:
    @pytest.mark.it('trims the order book to required num of records')
    @pytest.mark.asyncio
    async def test_trims_book(self, big_order_book, long_message):
        await big_order_book.extract_order_book_bids_asks()
        await big_order_book.update_order_book(long_message)   
        result = await big_order_book.trim_order_book(3)
        assert result == {"lastUpdateId": 74105025813, 
                          "bids" : [
                            ['113688.21000000' , '0.40000000'],
                            ['113678.85000000' , '7.25330000'], 
                            ['113678.84000000' , '0.77360000']
                            ],
                        "asks" : 
                            [ 
                            ["113678.86000000", "1.93563000"],     
                            ['113678.87000000', '0.00698000'], 
                            ["113679.35000000", "0.03677000"]
                            ]
                        }    

                
