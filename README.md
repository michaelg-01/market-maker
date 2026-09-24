# MM

A fast Rust-based market maker for Binance Spot that hedges on decentralized exchanges.

## How It Works

* **Quoting:** The Rust core calculates a fair value and places wide limit orders on Binance.

* **Hedging:** When a Binance order fills, an opposing order is sent.

* **Batching:** It waits until small fills add up to a minimum size (`MIN_HEDGE_QTY`) before firing the hedge.

* *Note:* Hedging is currently disabled in the code (`HEDGING_ENABLED = false`).

## Setup

1. **Rust Core:** Build with `cargo build --release`.

2. **Python Sidecars:** Use Python 3.11+.

   ```bash
   python3.11 -m venv venv
   source venv/bin/activate
   pip install aiohttp websockets eth-account eth-utils
   pip install git+https://github.com/elliottech/lighter-python.git
   ```

## Environment Variables

You need these to run the system:

**Binance**

* `BINANCE_API_KEY`

* `BINANCE_ED25519_KEY_PATH`

* `BINANCE_FIX_SENDER`

**Aster (`aster_signer.py`)**

* `ASTER_USER`, `ASTER_SIGNER`, `ASTER_PRIVATE_KEY`

**Lighter (`lighter_signer.py`)**

* `LIGHTER_BASE_URL`, `LIGHTER_API_KEY_PRIVATE_KEY`, `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_INDEX`
