import pytest
import json
import asyncio
from collections import deque
from src.wb_sockets.processing import is_continuous, ws_processing
from src.order_book.order_book_class import OrderBook

@pytest.fixture()
def curr_msg():
    curr_msg = {"e":"depthUpdate",
    "E":1753786825814,
    "s":"BTCUSDT",
    "U":73652024499,
    "u":73652024512,
    "b":[["118300.00000000", "1.73150000"]],
    "a":[["118304.00000000","1.77750000"]]
    }
    return curr_msg

@pytest.fixture
def order_book():
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

class TestIsContinuous():
    @pytest.mark.describe('Tests to ensure correct buffer continuity validation')
    @pytest.mark.asyncio
    async def test_returns_true_for_buffer_no_gaps(self,curr_msg):
        next_msg = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":73652024513,
                "u":73652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        buffer = deque([json.dumps (next_msg)])
        print(buffer)

        assert await is_continuous (curr_msg, buffer) 

    @pytest.mark.asyncio    
    async def test_returns_true_for_buffer_with_one_invalid_msg(self,curr_msg):
        next_msg = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":53652024513,
                "u":53652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        
        following_msg = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":73652024513,
                "u":73652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        buffer = deque([json.dumps(next_msg), json.dumps(following_msg)])
        print(buffer)

        assert await is_continuous (curr_msg, buffer) 

    @pytest.mark.asyncio    
    async def test_returns_true_for_buffer_with_two_invalid_msgs(self,curr_msg):
        next_msg = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":53652024513,
                "u":53652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        
        following_msg_invalid = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":63652024513,
                "u":63652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        
        following_msg_valid = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":73652024513,
                "u":73652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        buffer = deque([json.dumps(next_msg), json.dumps(following_msg_invalid), json.dumps(following_msg_valid)])
        print(buffer)

        assert await is_continuous (curr_msg, buffer)    

    @pytest.mark.asyncio    
    async def test_returns_false_for_buffer_with_three_invalid_msgs(self,curr_msg):
        next_msg = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":53652024513,
                "u":53652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        
        following_msg_invalid_1 = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":63652024513,
                "u":63652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        
        following_msg_invalid_2 = {"e":"depthUpdate",
                "E":1753786825814,
                "s":"BTCUSDT",
                "U":64652024513,
                "u":64652024517,
                "b":[["114300.00000000", "0.73150000"]],
                "a":[["108304.00000000","2.77750000"]]}
        buffer = deque([json.dumps(next_msg), json.dumps(following_msg_invalid_1), json.dumps(following_msg_invalid_2)])
        print(buffer)

        assert not await is_continuous (curr_msg, buffer) 

# Need to change to assert on bids and asks directly as content doesn't change till the production stage
class TestWsProcessing():
        @pytest.mark.describe('Tests to ensure ws_processing runs correctly')
        @pytest.mark.asyncio
        async def test_updates_order_book_continuous_buffer(self,curr_msg, order_book):
                next_msg = {"e":"depthUpdate",
                        "E":1753786825814,
                        "s":"BTCUSDT",
                        "U":73652024513,
                        "u":73652024517,
                        "b":[["114300.00000000", "0.73150000"]],
                        "a":[["108304.00000000","2.77750000"]]}
                buffer = deque([json.dumps(curr_msg), json.dumps(next_msg)])
                
                order_book.ob_bids, _ = await order_book.extract_order_book_bids_asks()
                print(f'This is ob.bids before: {order_book.ob_bids}')

                task = asyncio.create_task(ws_processing(order_book, buffer)) 
                await asyncio.sleep(0.5)
                print(f'This is ob.bids after: {order_book.ob_bids}')
                
                assert order_book.content == {"lastUpdateId": 73652024512, 
                        "bids": [["113678.85000000", "7.25330000"], 
                                ["113678.84000000", "0.77360000"],
                                ["118300.00000000", "1.73150000"]
                            ], 
                        "asks": [["113678.86000000", "1.93563000"], 
                                ["113900.35000000", "0.13677000"],
                                ["118304.00000000","1.77750000"]
                                ]
                            }
                
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                        await task
                
