from __future__ import annotations

import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import uvicorn
from fastapi import FastAPI

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.memory_store import SnapshotStore


BASE_URL = "https://api.upstox.com/v2"
TOKEN_STORE_FILE = PROJECT_ROOT / "token_store.json"
HTTP_TIMEOUT = 20
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "2"))

SYMBOL_CONFIG = {
    "NIFTY": {
        "instrument": os.getenv("NIFTY_INSTRUMENT", "NSE_INDEX|Nifty 50"),
        "expiry": os.getenv("NIFTY_EXPIRY", "2026-04-13"),
        "table": "nifty_option_chain",
    },
    "SENSEX": {
        "instrument": os.getenv("SENSEX_INSTRUMENT", "BSE_INDEX|SENSEX"),
        "expiry": os.getenv("SENSEX_EXPIRY", "2026-04-16"),
        "table": "sensex_option_chain",
    },
}

store = SnapshotStore(max_snapshots=100)
app = FastAPI(title="Option Chain Collector Service")


def _load_access_token() -> str:
    env_token = os.getenv("UPSTOX_ACCESS_TOKEN", "").strip()
    if env_token:
        return env_token

    if TOKEN_STORE_FILE.exists():
        with TOKEN_STORE_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)

        file_token = str(data.get("access_token") or "").strip()
        if file_token:
            return file_token

        raise ValueError(f"access_token key is missing or empty in {TOKEN_STORE_FILE}")

    raise FileNotFoundError(
        f"Token file not found: {TOKEN_STORE_FILE}. "
        "Set UPSTOX_ACCESS_TOKEN or provide token_store.json."
    )


def _headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {_load_access_token()}",
    }


def _check_token_validity() -> tuple[bool, str | None]:
    try:
        url = f"{BASE_URL}/market-quote/ohlc"
        params = {
            "instrument_key": "NSE_INDEX|Nifty 50",
            "interval": "1d",
        }
        response = requests.get(url, headers=_headers(), params=params, timeout=HTTP_TIMEOUT)

        if response.status_code == 401:
            return False, "Upstox token is invalid or expired."

        response.raise_for_status()
        return True, None
    except Exception as exc:
        return False, str(exc)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _possible_instrument_keys(instrument: str) -> list[str]:
    keys = [instrument]
    if "|" in instrument:
        keys.append(instrument.replace("|", ":"))
    if ":" in instrument:
        keys.append(instrument.replace(":", "|"))

    out: list[str] = []
    seen: set[str] = set()
    for key in keys:
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _extract_ohlc_block(data: dict[str, Any], instrument: str) -> dict[str, Any]:
    if not isinstance(data, dict) or not data:
        return {}

    possible_keys = _possible_instrument_keys(instrument)

    for key in possible_keys:
        candidate = data.get(key)
        if isinstance(candidate, dict):
            return candidate

    for _, value in data.items():
        if isinstance(value, dict):
            token = value.get("instrument_token") or value.get("instrument_key")
            if token in possible_keys:
                return value

    first_value = next(iter(data.values()), None)
    if isinstance(first_value, dict):
        return first_value

    return {}


def _extract_ohlc_values(block: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(block, dict):
        return {"open": None, "high": None, "low": None, "close": None}

    ohlc = block.get("ohlc") or block.get("live_ohlc") or {}
    return {
        "open": _safe_float(ohlc.get("open")),
        "high": _safe_float(ohlc.get("high")),
        "low": _safe_float(ohlc.get("low")),
        "close": _safe_float(ohlc.get("close")),
    }


def _fetch_ohlc(instrument: str) -> dict[str, Any]:
    url = f"{BASE_URL}/market-quote/ohlc"
    params = {
        "instrument_key": instrument,
        "interval": "1d",
    }

    response = requests.get(url, headers=_headers(), params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data", {})
    block = _extract_ohlc_block(data, instrument)
    return _extract_ohlc_values(block)


def _normalize_leg(leg: dict[str, Any], side: str) -> dict[str, Any]:
    side = side.upper()

    market_data = leg.get("market_data", {}) if isinstance(leg.get("market_data"), dict) else {}
    greeks = leg.get("option_greeks", {}) if isinstance(leg.get("option_greeks"), dict) else {}

    oi = _safe_int(market_data.get("oi"), 0)
    prev_oi = _safe_int(market_data.get("prev_oi"), 0)
    chg_oi = 0
    if oi is not None and prev_oi is not None:
        chg_oi = oi - prev_oi

    return {
        f"{side}_INSTRUMENT_KEY": leg.get("instrument_key"),
        f"{side}_IV": _safe_float(greeks.get("iv")),
        f"{side}_GAMMA": _safe_float(greeks.get("gamma")),
        f"{side}_THETA": _safe_float(greeks.get("theta")),
        f"{side}_DELTA": _safe_float(greeks.get("delta")),
        f"{side}_CHG_OI": _safe_int(chg_oi, 0),
        f"{side}_OI": oi,
        f"{side}_VOLUME": _safe_int(market_data.get("volume"), 0),
        f"{side}_LTP": _safe_float(market_data.get("ltp")),
        f"{side}_BID": _safe_float(market_data.get("bid_price")),
        f"{side}_ASK": _safe_float(market_data.get("ask_price")),
    }


def _normalize_chain_items(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], float | None, float | None]:
    rows: list[dict[str, Any]] = []
    total_ce_oi = 0.0
    total_pe_oi = 0.0
    latest_spot: float | None = None

    for item in items:
        if latest_spot is None:
            latest_spot = _safe_float(item.get("underlying_spot_price"))

        strike = _safe_float(item.get("strike_price"))
        if strike is None:
            continue

        call_leg = item.get("call_options") if isinstance(item.get("call_options"), dict) else {}
        put_leg = item.get("put_options") if isinstance(item.get("put_options"), dict) else {}

        ce = _normalize_leg(call_leg, "CE")
        pe = _normalize_leg(put_leg, "PE")

        ce_oi = float(ce.get("CE_OI") or 0)
        pe_oi = float(pe.get("PE_OI") or 0)

        total_ce_oi += ce_oi
        total_pe_oi += pe_oi

        row_pcr = _safe_float(item.get("pcr"))
        if row_pcr is None and ce_oi:
            row_pcr = round(pe_oi / ce_oi, 2)

        rows.append(
            {
                **ce,
                "STRIKE": strike,
                "PCR": row_pcr,
                **pe,
            }
        )

    rows.sort(key=lambda x: float(x["STRIKE"]))
    latest_pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi else None
    return rows, latest_spot, latest_pcr


def _fetch_option_chain(instrument: str, expiry: str) -> tuple[list[dict[str, Any]], float | None, float | None]:
    url = f"{BASE_URL}/option/chain"
    params = {
        "instrument_key": instrument,
        "expiry_date": expiry,
    }

    response = requests.get(url, headers=_headers(), params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    items = payload.get("data", [])
    if not isinstance(items, list):
        raise ValueError(f"Unexpected option chain response format: {payload}")

    return _normalize_chain_items(items)


def _collect_symbol(symbol: str) -> None:
    config = SYMBOL_CONFIG[symbol]
    instrument = config["instrument"]
    expiry = config["expiry"]
    table = config["table"]

    while True:
        now = datetime.now()
        try:
            rows, chain_spot, pcr = _fetch_option_chain(instrument, expiry)
            ohlc = _fetch_ohlc(instrument)

            snapshot = {
                "symbol": symbol,
                "instrument": instrument,
                "expiry": expiry,
                "table": table,
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                "latest_spot": chain_spot if chain_spot is not None else ohlc.get("close"),
                "latest_pcr": pcr,
                "latest_open": ohlc.get("open"),
                "latest_high": ohlc.get("high"),
                "latest_low": ohlc.get("low"),
                "latest_prev_close": ohlc.get("close"),
                "latest_df": rows,
                "snapshot_count": store.count(symbol) + 1,
                "db_connected": True,
                "rows_inserted": len(rows),
                "last_db_update": now.strftime("%H:%M:%S"),
                "fetch_error": None if rows else "Option chain returned no rows.",
                "bootstrapped_once": True,
            }
            store.append(symbol, snapshot)

        except Exception as exc:
            previous = store.latest(symbol)
            snapshot = {
                "symbol": symbol,
                "instrument": instrument,
                "expiry": expiry,
                "table": table,
                "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                "latest_spot": previous.get("latest_spot"),
                "latest_pcr": previous.get("latest_pcr"),
                "latest_open": previous.get("latest_open"),
                "latest_high": previous.get("latest_high"),
                "latest_low": previous.get("latest_low"),
                "latest_prev_close": previous.get("latest_prev_close"),
                "latest_df": previous.get("latest_df", []),
                "snapshot_count": store.count(symbol),
                "db_connected": False,
                "rows_inserted": len(previous.get("latest_df", [])),
                "last_db_update": now.strftime("%H:%M:%S"),
                "fetch_error": str(exc),
                "bootstrapped_once": bool(previous),
            }
            store.append(symbol, snapshot)

        time.sleep(REFRESH_SECONDS)


@app.on_event("startup")
def startup_event() -> None:
    for symbol in ("NIFTY", "SENSEX"):
        thread = threading.Thread(target=_collect_symbol, args=(symbol,), daemon=True)
        thread.start()


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "message": "Trading Backend Running",
        "health_url": "/health",
        "latest_nifty_url": "/latest/NIFTY",
        "latest_sensex_url": "/latest/SENSEX",
    }


@app.get("/health")
def health() -> dict[str, Any]:
    nifty_latest = store.latest("NIFTY")
    sensex_latest = store.latest("SENSEX")

    token_present = bool(os.getenv("UPSTOX_ACCESS_TOKEN", "").strip() or TOKEN_STORE_FILE.exists())
    token_valid = False
    auth_message = None

    if token_present:
        token_valid, auth_message = _check_token_validity()
    else:
        auth_message = "Upstox token is missing."

    return {
        "ok": True,
        "symbols": store.symbols(),
        "nifty_count": store.count("NIFTY"),
        "sensex_count": store.count("SENSEX"),
        "nifty_latest_ts": nifty_latest.get("timestamp"),
        "sensex_latest_ts": sensex_latest.get("timestamp"),
        "nifty_error": nifty_latest.get("fetch_error"),
        "sensex_error": sensex_latest.get("fetch_error"),
        "token_present": token_present,
        "token_valid": token_valid,
        "auth_required": not token_valid,
        "auth_message": auth_message,
    }


@app.get("/latest/{symbol}")
def latest(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    if symbol not in ("NIFTY", "SENSEX"):
        return {"error": f"Unsupported symbol: {symbol}"}
    return store.latest(symbol)


@app.get("/snapshots/{symbol}")
def snapshots(symbol: str) -> dict[str, Any]:
    symbol = symbol.upper()
    if symbol not in ("NIFTY", "SENSEX"):
        return {"error": f"Unsupported symbol: {symbol}"}
    return {
        "symbol": symbol,
        "count": store.count(symbol),
        "snapshots": store.snapshots(symbol),
    }


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8765"))
    uvicorn.run(app, host="0.0.0.0", port=port)
