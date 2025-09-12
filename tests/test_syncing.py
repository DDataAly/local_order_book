from unittest.mock import AsyncMock, patch
from wb_sockets.syncing import get_order_book


class TestGetOrderBook:
    @pytest.mark.it ()