import pytest
import json
from collections import deque
from src.wb_sockets.processing import is_continuous

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
            