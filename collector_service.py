def _check_token_validity() -> tuple[bool, str | None]:
    try:
        headers = _headers()
        url = f"{BASE_URL}/market-quote/ohlc"
        params = {
            "instrument_key": "NSE_INDEX|Nifty 50",
            "interval": "1d",
        }
        response = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)

        if response.status_code == 401:
            return False, "Upstox token is invalid or expired."

        response.raise_for_status()
        return True, None
    except Exception as exc:
        return False, str(exc)
