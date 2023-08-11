#!/bin/bash

# Simple script to download all symbols from https://api.binance.com/api/v3/exchangeInfo https://dapi.binance.com/dapi/v1/exchangeInfo
# jq is required

curl -s -H 'Content-Type: application/json'  https://api.binance.com/api/v3/exchangeInfo | jq -r '.symbols | sort_by(.symbol) | .[] | .symbol' > spot_symbol.txt
curl -s -H 'Content-Type: application/json'  https://dapi.binance.com/dapi/v1/exchangeInfo | jq -r '.symbols | sort_by(.symbol) | .[] | .symbol' > cm_symbol.txt
curl -s -H 'Content-Type: application/json'  https://fapi.binance.com/fapi/v1/exchangeInfo | jq -r '.symbols | sort_by(.symbol) | .[] | .symbol' > um_symbol.txt
