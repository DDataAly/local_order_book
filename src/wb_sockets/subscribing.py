import json
import asyncio

async def _send_subscription_request(websocket) -> str:
    """
    Sends a subscription request to the Binance WebSocket for depth updates.
    Args:
        websocket (websockets.WebSocketClientProtocol): The WebSocket connection to Binance.
    Returns:
        A JSON-formatted response string from Binance, which is either:
        - subscription confirmation {"result": null, "id": 1}
        - depth update message 
            {
            "e": "depthUpdate", // Event type
            "E": 1672515782136, // Event time
            "s": "BNBBTC",      // Symbol
            "U": 157,           // First update ID in event
            "u": 160,           // Final update ID in event
            "b": [              // Bids to be updated
                [
                "0.0024",       // Price level to be updated
                "10"            // Quantity
                ]
            ],
            "a": [              // Asks to be updated
                [
                "0.0026",       // Price level to be updated
                "100"           // Quantity
                ]
            ]
            }
    """
    await websocket.send(
        json.dumps({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@depth@100ms"], 
            "id": 1}))
    response = await websocket.recv()
    return (response)


async def _is_subscription_confirmed(response) -> bool:
    """
    Parses the response string and checks that expected dictionary keys are present 
    Args:
        response: string returned by send_subscription_request(websocket)
    Returns:
        bool - True if keys are present otherwise False   
    """
    try:
        response_dict = json.loads(response)
    except json.JSONDecodeError as e:
        print (f'Invalid format of server response: {e}') 
        return False  
         
    if response_dict.keys() == {'result','id'}:
        return True
    elif response_dict.get('e') == "depthUpdate":
        return True
    return False


async def run_the_subscriber(websocket):
    """
    Repeatedly sends a subscription request to the Binance WebSocket for depth updates 
    until the subscription is confirmed.
    Args:
        websocket (websockets.WebSocketClientProtocol): The WebSocket connection to Binance.
    Returns:
        None   
    """
    response = await _send_subscription_request(websocket)
    while not await _is_subscription_confirmed(response):
        await asyncio.sleep(0.1)
        response = await _send_subscription_request(websocket)
    print ('Subscription is confirmed')  