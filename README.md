# MarketMaker Core

This repository contains a high-performance market making system designed to provide liquidity on Binance Spot (via FIX-SBE) while seamlessly routing cross-venue hedges through asynchronous Python sidecars. The architecture relies on a fast Rust core for primary market data and order entry, paired with dedicated subprocesses to handle the specific cryptographic and API requirements of external decentralized exchanges.

## Architecture & Components

The system is split into a low-latency Rust core and flexible Python sidecars. They communicate via a strict NDJSON protocol over `stdin`/`stdout`.

*   **Rust Core (`main.rs`, `fix.rs`, `sbe.rs`):** 
    *   Manages TLS-wrapped TCP FIX sessions to Binance.
    *   Implements a custom SBE (Simple Binary Encoding) codec for zero-allocation message parsing.
    *   Maintains the local order book, fair value estimates, and risk models.
    *   Dispatches hedge requests to the sidecars upon filling primary orders.
*   **Aster Sidecar (`aster_signer.py`):** 
    *   A drop-in hedge execution layer for Aster. 
    *   Constructs and signs EIP-712 REST payloads for market orders.
    *   Listens to the Aster `userDataStream` via WebSockets for guaranteed fill confirmations.
*   **Lighter Sidecar (`lighter_signer.py`):**
    *   A dedicated hedge execution layer for the Lighter DEX.
    *   Manages account nonces and signs WebSocket transactions.
    *   Listens to Lighter's account channels to reconcile partial and full fills.

## Trading Logic & Risk Management

The engine employs a defensive, fair-value-driven pricing model designed to survive highly volatile market conditions. 

*   **Fair Value Pricing:** The system anchors its base price to the "non-self mid"—the midpoint of the best bid and ask on the primary venue, explicitly filtering out its own resting orders to avoid self-referential pricing loops.
*   **Volatility Scaling:** Spread edge and step-behind parameters are dynamically multiplied when trailing volatility spikes, widening quotes to compensate for adverse selection risk.
*   **Impulse & Jump Detectors:** The system continuously monitors an anchor market (e.g., the highly liquid ETHUSDT pair) for sudden price jumps or heavily one-sided taker flow. If a threshold is breached, it immediately pulls quotes until the market stabilizes.
*   **Directional Imbalance Protection:** If the top-of-book size ratio becomes excessively skewed (e.g., massive ask size vs. minimal bid size), the system will cancel the threatened side of its quote to avoid being run over by the impending momentum.

## Setup & Build Instructions

### 1. Build the Rust Core
Ensure you have the latest stable Rust toolchain installed.
```bash
# Build the highly optimized release binary
cargo build --release
```

### 2. Configure the Python Environment
The Python sidecars require Python 3.11+ and specific asynchronous/cryptographic dependencies.
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install aiohttp websockets eth-account eth-utils
# Install the Lighter SDK (required if using lighter_signer.py)
pip install git+https://github.com/elliottech/lighter-python.git
```

## Environment Variables

The system relies entirely on environment variables for configuration and secret management. 

### Core Configuration (Rust)
*   `BINANCE_API_KEY`: Your Binance API key string (sent as Username during FIX Logon).
*   `BINANCE_ED25519_KEY_PATH`: Absolute path to the PKCS#8 PEM private key used to sign the FIX Logon payload.
*   `BINANCE_FIX_SENDER`: The base `SenderCompID` for your FIX sessions. The engine appends suffixes (e.g., `OE`, `MS`) to distinguish Order Entry and Market Data sessions.

### Aster Sidecar (`aster_signer.py`)
*   `ASTER_USER`: Your user wallet address (0x-prefixed).
*   `ASTER_SIGNER`: Your designated signer wallet address.
*   `ASTER_PRIVATE_KEY`: The private key corresponding to the signer address.
*   `ASTER_SYMBOL`: The target market symbol (Default: `ETHUSD1`).
*   `ASTER_REST_URL`: The Aster REST API endpoint (Default: `https://fapi.asterdex.com`).
*   `ASTER_WS_URL`: The Aster WebSocket endpoint (Default: `wss://fstream.asterdex.com/ws`).
*   `ASTER_QTY_DECIMALS` / `ASTER_PRICE_DECIMALS`: Precision mapping for the target market (Defaults: 3 and 2, respectively).
*   `ASTER_FILL_TIMEOUT`: Maximum seconds to wait for a WebSocket fill confirmation (Default: `10.0`).

### Lighter Sidecar (`lighter_signer.py`)
*   `LIGHTER_BASE_URL`: The Lighter REST API endpoint.
*   `LIGHTER_API_KEY_PRIVATE_KEY`: Your Lighter API private key for transaction signing.
*   `LIGHTER_ACCOUNT_INDEX`: Your Lighter account index integer.
*   `LIGHTER_API_KEY_INDEX`: Your Lighter API key index integer.
*   `LIGHTER_MARKET_SYMBOL`: The target market symbol (Default: `ETH`).
*   `LIGHTER_MAX_SLIPPAGE`: Allowed slippage for market orders as a decimal percentage (Default: `0.01`).
*   `LIGHTER_FILL_TIMEOUT`: Maximum seconds to wait for a transaction acknowledgment and fill (Default: `10.0`).

## Running the Application

Before starting, ensure you have updated the hardcoded paths in `main.rs` (such as the path to the desired Python sidecar script) to match your deployment environment. 

```bash
# Example execution mapping the Lighter sidecar vars
export BINANCE_API_KEY="your_api_key"
export BINANCE_ED25519_KEY_PATH="/path/to/key.pem"
export BINANCE_FIX_SENDER="MYORG"

export LIGHTER_BASE_URL="https://api.lighter.xyz"
export LIGHTER_API_KEY_PRIVATE_KEY="your_private_key"
export LIGHTER_ACCOUNT_INDEX="0"
export LIGHTER_API_KEY_INDEX="0"

./target/release/marketmaker
```