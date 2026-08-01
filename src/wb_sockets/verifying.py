
from websockets import asyncio
from syncing import get_order_book, validate_snapshot
import time

def run_comparison (verification_snapshot: dict,
                    local_ob_bids: dict, 
                    local_ob_ask: dict) -> bool:
    return True


async def run_verification(match_found: asyncio.Event,
                           stop_fetching_verification_snapshots: asyncio.Event,
                           snapshot_timestamp: list,
                           local_ob_bids: dict,
                           local_ob_ask:dict):
 
    while not stop_fetching_verification_snapshots.is_set():

        verification_snapshot = await get_order_book()
        if not validate_snapshot(verification_snapshot):
            continue
        snapshot_timestamp[0]=verification_snapshot["lastUpdateId"] 
        await asyncio.sleep(0.1)

        if match_found.is_set():
            records_match = run_comparison(verification_snapshot,local_ob_ask, local_ob_ask)
            return records_match


# async def run_verification_till_timeout(
#                         match_found: asyncio.Event,
#                         stop_fetching_verification_snapshots: asyncio.Event,
#                         snapshot_timestamp: list,
#                         local_ob_bids: dict,
#                         local_ob_ask:dict,
#                         max_verification_time =10):
#     try:
#         records_match = await asyncio.wait_for(run_verification
#             (match_found,
#             stop_fetching_verification_snapshots,
#             snapshot_timestamp,
#             local_ob_bids,
#             local_ob_ask), 
#             timeout=max_verification_time)
#         if records_match:
#             print('Verification passed')
#             return True # TBC if we want to return anything
#         print ('Verification failed')
#         return False
#     except asyncio.TimeoutError:
#         print('Verification was not completed due to timeout')
#     except Exception as e:
#         print(f'Something went wrong. Verification can not be run; {e}')




