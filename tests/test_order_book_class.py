import pytest
import logging
import copy
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

    @pytest.mark.it('returns a tuple of dictionaries')
    @pytest.mark.asyncio
    async def test_returns_correct_dict(self, small_order_book, short_message):
        await small_order_book.extract_order_book_bids_asks()
        result = await small_order_book.update_order_book(short_message)
        assert isinstance(result, tuple)
        assert isinstance(result[0], dict)
        assert isinstance(result[1], dict)
        

    @pytest.mark.it('updates both sides of the order book correctly for a message with bids only, asks only or both bids and asks')
    @pytest.mark.parametrize('message, expected_output',
                        [
                            ({'e': 'depthUpdate', 'E': 1754423900214, 's': 'BTCUSDT', 
                            'U': 74105025814, 'u': 74105025843, 
                            'b': [['114000.57000000', '0.30000000']],'a': []},
                                (
                                    {113678.85000000 : 7.25330000, 
                                    113678.84000000 : 0.77360000,
                                    114000.57000000 : 0.30000000},
                                    {113678.86000000 : 1.93563000, 
                                    113900.35000000 : 0.13677000}
                                )
                            ),    
                            ({'e': 'depthUpdate','E': 1754423900214, 's': 'BTCUSDT', 
                            'U': 74105025814, 'u': 74105025843, 
                            'b': [],'a': [['113666.20000000', '2.93563000']]}, 
                                (
                                    {113678.85000000 : 7.25330000, 
                                    113678.84000000 : 0.77360000},
                                    {113678.86000000 : 1.93563000, 
                                    113900.35000000 : 0.13677000,
                                    113666.20000000 : 2.93563000}
                                )
                            ),
                            ({'e': 'depthUpdate', 'E': 1754423900214, 's': 'BTCUSDT', 
                            'U': 74105025814, 'u': 74105025843, 
                            'b': [['114000.57000000', '0.30000000']],'a': [['113666.20000000', '2.93563000']]},
                                (
                                    {113678.85000000 : 7.25330000, 
                                    113678.84000000 : 0.77360000,
                                    114000.57000000 : 0.30000000},
                                    {113678.86000000 : 1.93563000, 
                                    113900.35000000 : 0.13677000,
                                    113666.20000000 : 2.93563000}
                                ) 
                              )
                        ],
                        ids = ['bids only',
                               'asks only',
                               'bids and asks'])
    @pytest.mark.asyncio
    async def test_updates_both_sides_correctly(self, small_order_book, message, expected_output):
        await small_order_book.extract_order_book_bids_asks()
        result = await small_order_book.update_order_book(message)
        assert result == expected_output
  

class TestSortUpdatedOrderBook:
    @pytest.mark.it('returns a dictionary with correct keys')
    @pytest.mark.asyncio
    async def test_returns_correct_dict(self, small_order_book, short_message):
        await small_order_book.extract_order_book_bids_asks()
        await small_order_book.update_order_book(short_message)
        await small_order_book.sort_updated_order_book()
        assert isinstance(small_order_book.content, dict)
        assert len(small_order_book.content.keys()) == 3
        assert set(['lastUpdateId','bids','asks']) == set(small_order_book.content.keys())


    @pytest.mark.it('sorts bids in desc and asks in asc order')
    @pytest.mark.asyncio
    async def test_returns_sorts_bids_asks(self, big_order_book, long_message):
        await big_order_book.extract_order_book_bids_asks()
        await big_order_book.update_order_book(long_message)
        await big_order_book.sort_updated_order_book()
        assert all(big_order_book.content['bids'][i] > big_order_book.content['bids'][i + 1] for i in range(0, (len(big_order_book.content['bids'])) - 1))
        assert all(big_order_book.content['asks'][i] < big_order_book.content['asks'][i + 1] for i in range(0, (len(big_order_book.content['asks'])) - 1))
  

    @pytest.mark.it('returns prices and qtys as lists containing strings of numbers with 8 digits after floating point')
    @pytest.mark.asyncio
    async def test_returns_qty_prices_correct_format(self, small_order_book, short_message):
        await small_order_book.extract_order_book_bids_asks()
        await small_order_book.update_order_book(short_message)
        await small_order_book.sort_updated_order_book()
        assert all(isinstance(small_order_book.content['bids'][i],list) for i in range(0,(len(small_order_book.content['bids'])-1)))
        assert all(isinstance(small_order_book.content['bids'][i][0],str) for i in range(0,(len(small_order_book.content['bids'])-1)))
        assert all(isinstance(small_order_book.content['bids'][i][1],str) for i in range(0,(len(small_order_book.content['bids'])-1)))
        assert all(len(small_order_book.content['bids'][i][0].split('.')[1]) == 8 for i in range(0,(len(small_order_book.content['bids'])-1)))


class TestTrimOrderBook:

    @pytest.mark.it('trims the order book to required num of records')
    @pytest.mark.asyncio
    async def test_trims_book(self, big_order_book, long_message):
        await big_order_book.extract_order_book_bids_asks()
        await big_order_book.update_order_book(long_message)  
        await big_order_book.sort_updated_order_book()  
        await big_order_book.trim_order_book(3)
        assert big_order_book.content == {"lastUpdateId": 74105025813, 
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

class TestPriceListMaintenance:

    @pytest.mark.it('price lists matches price-qty pairs in the order book after a single update')
    @pytest.mark.asyncio
    async def test_matching_full_version_single_update(self, big_order_book, long_message):
        big_order_book_1 = copy.deepcopy(big_order_book)
        await big_order_book_1.extract_order_book_bids_asks()
        await big_order_book_1.update_order_book(long_message)  
        await big_order_book_1.sort_updated_order_book() 
        order_book_1_bids = [bid[0] for bid in big_order_book_1.content['bids']]
        order_book_1_asks = [ask[0] for ask in big_order_book_1.content['asks']]
        price_lists_order_book =(order_book_1_bids, order_book_1_asks)
        
        await big_order_book.extract_order_book_prices()
        await big_order_book.update_price_lists(long_message) 
        price_lists = await big_order_book.format_price_lists()
    
        assert price_lists_order_book == price_lists


    @pytest.mark.it('price lists matches price-qty pairs in the order book after multiple updates')
    @pytest.mark.parametrize("trim", [False, True])
    @pytest.mark.asyncio
    async def test_matching_full_version_multiple_updates(self, big_order_book, long_message, short_message, trim, num_records = 3):
        big_order_book_1 = copy.deepcopy(big_order_book)
        await big_order_book_1.extract_order_book_bids_asks()
        await big_order_book_1.update_order_book(long_message) 
        await big_order_book_1.update_order_book(short_message) 
        await big_order_book_1.sort_updated_order_book() 
        if trim:
            await big_order_book_1.trim_order_book(num_records)
        order_book_1_bids = [bid[0] for bid in big_order_book_1.content['bids']]
        order_book_1_asks = [ask[0] for ask in big_order_book_1.content['asks']]
        price_lists_order_book =(order_book_1_bids, order_book_1_asks)
        
        await big_order_book.extract_order_book_prices()
        await big_order_book.update_price_lists(long_message) 
        await big_order_book.update_price_lists(short_message) 
        if trim:
            await big_order_book.trim_price_lists(num_records)                
        price_lists = await big_order_book.format_price_lists()

        assert price_lists_order_book == price_lists


