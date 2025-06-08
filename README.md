# Strategy Trader

A Python framework for scanning Binance funding rates and auto-trading with Telegram alerts.

## Features

- **FundingRateScanner**: scans on-chain funding rates and filters by threshold  
- **BinanceFuturesTrader**: places market orders with SL/TP logic  
- **TelegramBot**: sends trade alerts to your Telegram chat  

## Quickstart

```bash
git clone https://github.com/luftmensc/Funding_Trader.git
cd Funding_Trader

# 1. Create & activate virtualenv
python3 -m venv venv
source venv/bin/activate

# 2. Install deps
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your API keys and chat ID

# 4. Run
python src/main.py
