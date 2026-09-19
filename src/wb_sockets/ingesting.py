from collections import deque
import websockets 
import json

# async def ws_ingestion(websocket: websockets.WebSocketClientProtocol,buffer: deque[str]):
#     """
#     Infinite function which receives order book prices and quantity updates and adds them to buffer
#     Args:
#         websocket (websockets.WebSocketClientProtocol): The WebSocket connection to Binance
#         buffer: a deque to keep incoming WebSocket stream messages
#     Returns:
#         None 
#     """
#     while True:
#         print ('Continue ingestion')
#         response = await websocket.recv() 
#         # print(response)
#         a=json.loads(response)
#         print(a['U'])
#         print(a['u'])
#         buffer.append(response)




async def ws_ingestion(websocket: websockets.WebSocketClientProtocol,buffer: deque[str]):
    faulty_tracker = 0
    end_timestamp = 0
    num_arrived_msg =0
    while faulty_tracker <=10:
        print('Continue ingestion')
        response = await websocket.recv() 
        num_arrived_msg= num_arrived_msg+1
        msg = json.loads(response)
        print(f'This is Websockets message {msg}')
        beg_timestamp = int(msg['U'])
        print(f'This is end of previous Websockets msg: {end_timestamp}')
        print(f'This is the beginning of current Websockets msg: {beg_timestamp}')

        if end_timestamp == 0:
            print('We are processing first Websockets message')
            buffer.append(response)
            end_timestamp = int(msg['u'])
        else:
            if beg_timestamp - end_timestamp ==1:
                print('Continuity passed')
                buffer.append(response)
                end_timestamp = int(msg['u'])
            else:
                print('Continuity failed')
                faulty_tracker= faulty_tracker +1
        print(f'We received {num_arrived_msg} messages and encountered {faulty_tracker} breakages')




# Sandbox
# def continuity_checker(buffer, msg_list):
#     faulty_tracker = 0
#     end_timestamp = 0
#     while len(msg_list)>0 and faulty_tracker <=2:
#         msg = msg_list.popleft()
#         print(f'This is message {msg}')
#         beg_timestamp = int(msg[0])
#         print(f'This is end of previous msg: {end_timestamp}')
#         print(f'This is the beginning of current msg: {beg_timestamp}')

#         if end_timestamp == 0:
#             print('We are processing first element')
#             buffer.append(msg)
#             end_timestamp = int(msg[1])
#         else:
#             if beg_timestamp - end_timestamp ==1:
#                 print('Continuity passed')
#                 buffer.append(msg)
#                 end_timestamp = int(msg[1])
#             else:
#                 print('Continuity failed')
#                 faulty_tracker= faulty_tracker +1
#                 print(f'We encountered {faulty_tracker} breakages')

#     print(buffer)


# buffer =[]
# msg_list=deque([(4,8), (9,12), (10,12), (13,18), (14,18), (19,22), (23,25), (24,25), (26,41)])
# print(type(msg_list))

# continuity_checker(buffer, msg_list)