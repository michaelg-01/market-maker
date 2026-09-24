# MM

A fast Rust-based market maker for Binance Spot that hedges on decentralized exchanges. The decentralized exchange should have a tighter spread than the corresponding asset on Binance Spot for it to work.

## How It Works

* **Quoting:** Calculates a fair value and places wide limit orders on Binance.

* **Hedging:** When a Binance order fills, an opposing market order is sent on the decentralized exchange.


## Setup

1. **Rust Core:** Build with `cargo build --release`.

2. **Python Sidecars:** Use Python 3.11+.

   ```bash
   python3.11 -m venv venv
   source venv/bin/activate
   pip install aiohttp websockets eth-account eth-utils
   pip install git+https://github.com/elliottech/lighter-python.git
   ```
