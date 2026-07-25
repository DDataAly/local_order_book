
from websockets import asyncio


def run_verification(timeout = 10, match_found: asyncio.Event, snapshot_timestamp = None):
    # use asincio event to trigger the verification once the match is found
    # while await match_found.wait() or timeout:
    # - fetch a snapshop and extract the lastUpdateId as snapshot_timestamp
    # if match_found: break the loop and do verification
    # - once verification run exit the execution and log an appropriate message
    pass

