# 📈 Local Order Book – Binance WebSockets Stream

## 🔰 Overview

An asynchronous Python project that fetches a live copy of Binance’s BTC/USDT order book and maintains it in sync with the real Binance order book for a configurable duration using WebSocket depth update streams. Once the runtime ends, the system automatically verifies its local copy by fetching a fresh snapshot from Binance, finding the exact synchronisation point, and comparing values.

## 🔧 Tech Stack
**Language**  
- Python 3.13

**Core Libraries**  
- asyncio – concurrent ingestion and processing via a single event loop
- websockets – connection to Binance WebSocket depth streams
- aiohttp – asynchronous REST API requests for order book snapshots
- bisect – maintaining sorted price lists efficiently
- logging – structured event and error tracking
- pytest – unit testing of order book and processing logic

## 📊 How It Works

1. Subscribe to Binance WebSocket depth update stream
2. Sync – fetch a REST API snapshot and find the matching WebSocket message to establish a consistent starting point
3. Ingest – continuously receive depth update messages into a local buffer
4. Process – apply updates to the local OrderBook object, maintaining bid/ask dictionaries in sync with the live market
5. Verify – once runtime ends, fetch a fresh snapshot, find the exact boundary where local state matches the snapshot’s lastUpdateId, and compare values

## Key Design Decisions

**Async architecture** – asyncio allows ingestion and processing to run concurrently without threads, switching between tasks at natural await points. This is essential for a system where messages must be collected and applied in real time.

**Validation at the boundary** – both the initial snapshot and incoming stream messages are validated before being applied to the local order book. This separates the concern of “is this data usable?” from “apply this data”, making the processing logic cleaner.

**Automatic re-launch** – if a continuity gap is detected in the message stream, the system raises a structured exception that propagates to the top-level orchestrator, which restarts the full sync cycle automatically. This makes the system self-recovering without manual intervention.

**Verification by exact boundary matching** – rather than approximating, the system waits for the exact message whose final update ID matches the verification snapshot’s lastUpdateId, ensuring the comparison is meaningful.

**Task lifecycle management** – background tasks (ws_ingestion, ws_processing) are shielded from external cancellation during verification and explicitly cancelled in a finally block to ensure clean shutdown regardless of how the program exits.


## ▶️ Usage Instructions
Run the program by calling run_for_duration() from main.py with the desired runtime in seconds:
```bash
asyncio.run(run_for_duration(runtime=30))
```
CLI argument parsing is a planned improvement — see Known Issues below.

## Project Status

The project runs end to end. Verification passes in the majority of runs. The following areas are known work in progress:

Results confidence — verification occasionally times out. Investigation needed to determine whether the timeout window is too short or whether there is a genuine logic issue.

Ingestion robustness — the continuity check at ingestion needs refinement to correctly distinguish between stray out-of-order messages (which can be recovered from) and genuine stream gaps (which require re-sync).

Planned improvements

- Message validation (currently only snapshot validation is implemented)
- CLI argument parsing via argparse
- Remove hardcoded values (e.g. verification timeout, order book depth)
- Expand test coverage to async flows
- Investigate latency optimisations — sorted price lists vs dictionary updates
- Switch to poetry for dependency management

## 📁 File Structure
Run the main event loop:
```
local_order_book/
│
├── src/
│   ├── main.py                      # Entry point and top-level orchestration
│   │
│   ├── wb_sockets/                  # WebSocket pipeline
│   │   ├── subscribing.py           # Subscribe to Binance depth stream
│   │   ├── ingesting.py             # Receive messages and buffer them (with continuity check)
│   │   ├── syncing.py               # Match snapshot to stream for initial sync
│   │   ├── processing.py            # Apply depth updates to OrderBook
│   │   └── verifying.py             # Verification logic after runtime ends
│   │
│   ├── order_book/                  # Order book domain
│   │   ├── order_book_class.py      # OrderBook class and update methods
│   │   ├── order_book_production.py # Creates local order book from snapshot
│   │   └── initialise_order_book_stream.py  # Orchestrates sync and task creation
│   │
│   └── utils/
│       └── logger.py                # Logging configuration
│
├── tests/
│   ├── test_order_book_class.py     # Core OrderBook logic
│   ├── test_processing.py           # Buffer and message handling
│   └── test_syncing.py              # API snapshot mocking
│
├── .gitignore
├── requirements.txt
└── README.md
```

## 🖥️ Demo Output - tests

![Core logic tests output](docs/tests_output_order_book_class.png)
