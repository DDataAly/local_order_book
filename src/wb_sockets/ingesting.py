from collections import deque
import websockets 

async def ws_ingestion(websocket: websockets.WebSocketClientProtocol,buffer: deque[str]):
    """
    Infinite function which receives order book prices and quantity updates and adds them to buffer
    Args:
        websocket (websockets.WebSocketClientProtocol): The WebSocket connection to Binance
        buffer: a deque to keep incoming WebSocket stream messages
    Returns:
        None 
    """
    while True:
        print ('Continue ingestion')
        response = await websocket.recv() 
        print(response)
        buffer.append(response)