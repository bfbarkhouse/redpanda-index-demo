# Redpanda Index Demo

An end-to-end demo that streams SPY ETF quotes into Redpanda, streams them to Snowflake for analytics and derives windowed price movements for realtime volatility analysis. 

<p align="center">
  <img src="./redpanda-index-demo-diagram.png" alt="Pipeline data flow" width="720">
</p>

---

## ✨ What's in this repo

- `redpanda_index_quotes_ingest.yaml` — Job that ingests SPY trades into Redpanda from the Alpaca markets API. :contentReference[oaicite:1]{index=1}  
- `redpanda_index_candles.yaml` — Job that samples price high and low in 10s windows from the data feed and writes to a new topic. :contentReference[oaicite:2]{index=2}  
- `redpanda_index_snowflake.yaml` — Streams SPY trades to Snowflake for analytics. :contentReference[oaicite:3]{index=3}
- `redpanda_index_prices-value.avsc` - AVRO schema for the SPY trade data
- `redpanda-index-demo-diagram.png` — Architecture diagram. :contentReference[oaicite:4]{index=4}

> **Note:** Update credentials, topic names, and any transforms to match your environment.

---

## 🧱 Prerequisites

- Docker
- `rpk` (Redpanda CLI)
- A Snowflake account + database/schema/warehouse
- Credentials/secrets for any external data providers (Alpaca in this case)

---

## 🚀 Quickstart

Clone:

```bash
git clone https://github.com/bfbarkhouse/redpanda-index-demo
cd redpanda-index-demo
rpk container start -n 3
rpk topic create redpanda_index_prices redpanda_index_snow_dlq redpanda_index_candles
rpk registry schema create redpanda_index_prices-value --schema redpanda_index_prices-value.avsc
rpk connect run ./redpanda_index_quotes_ingest.yaml
rpk connect run ./redpanda_index_snowflake.yaml
rpk connect run ./redpanda_index_candles.yaml
