# Redpanda Index Demo

An end-to-end demo that streams SPY ETF quotes into Redpanda, ingests them to Snowflake for analytics and derives windowed price movements for realtime volatility analysis. 

<p align="center">
  <img src="./redpanda-index-demo-diagram.png" alt="Pipeline data flow" width="720">
</p>

---

## ✨ What's in this repo

- `redpanda_index_quotes_ingest.yaml` — Job to ingests SPY trades into Redpanda from the [Alpaca Market Data API](https://docs.alpaca.markets/docs/about-market-data-api). 
- `redpanda_index_candles.yaml` — Job to samples price high and low in continuous 10s windows from the data feed and write to a new topic. 
- `redpanda_index_snowflake.yaml` — Streams SPY trades to Snowflake for analytics. 
- `redpanda_index_prices-value.avsc` - AVRO schema for the SPY trade data.
- `redpanda-index-demo-diagram.png` — Architecture diagram.
- `redpanda-index-visualizer` - This folder contains the code to run the volatility analysis visualization application

---

## 🧱 Quickstart Prerequisites

- Docker
- `rpk` (Redpanda CLI) [Download](https://docs.redpanda.com/current/get-started/rpk-install/)
- A Snowflake account + database/schema/warehouse
- Alpaca [API Key](https://docs.alpaca.markets/docs/about-market-data-api#authentication) - free plan allows 200 requests per minute.
- Node.js and `npm`

---

## 🚀 Quickstart

```bash
git clone https://github.com/bfbarkhouse/redpanda-index-demo && cd redpanda-index-demo
```
```bash
rpk container start -n 1
rpk topic create redpanda_index_prices redpanda_index_snow_dlq redpanda_index_candles
rpk registry schema create redpanda_index_prices-value --schema redpanda_index_prices-value.avsc
rpk connect run ./redpanda_index_quotes_ingest.yaml
rpk connect run ./redpanda_index_snowflake.yaml
rpk connect run ./redpanda_index_candles.yaml
cd redpanda-index-visualizer
npm install
npm start
```
Open a browser to [http://localhost:3001](http://localhost:3001) to view the real-time volatility analysis
