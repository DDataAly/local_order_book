from unittest.mock import AsyncMock, patch
from src.wb_sockets.syncing import get_order_book
import pytest


@pytest.mark.asyncio
class TestGetOrderBook:
    @pytest.mark.it("Returns a mock to simulate API call")
    @patch('src.wb_sockets.syncing.aiohttp.ClientSession')
    async def test_mock_fetch (self, fake_aiohttp_client_session_class):
        order_book_sample = {"lastUpdateId": 75310524787, 
                                           "bids": [["110055.50000000", "3.26691000"], 
                                                    ["110054.47000000", "0.00012000"]], 
                                            "asks": [["110055.51000000", "3.79950000"], 
                                                     ["110055.52000000", "0.04587000"]]
                                            }
        url = 'https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20'
        fake_session = AsyncMock()
        fake_response = AsyncMock()
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = order_book_sample

        fake_session.__aenter__.return_value = fake_session
        fake_session.get.return_value.__aenter__.return_value = fake_response
        fake_aiohttp_client_session_class.return_value = fake_session


        snapshot, order_book_last_update_id = await get_order_book()
        assert snapshot == order_book_sample
        assert order_book_last_update_id == 75310524787
        fake_response.raise_for_status.assert_called_once()

