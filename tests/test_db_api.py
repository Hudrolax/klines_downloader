from main import get_min_max_dates_for_symbol_tf, add_klines
import requests
from main import url


def test_get_min_max_dates_for_symbol_tf():
    # test get min_max date for unknown symbol
    result = get_min_max_dates_for_symbol_tf(
        symbol='UNKNOWN_SYMBOL', tf='unknown')
    assert result == (None, None)

    result = get_min_max_dates_for_symbol_tf(symbol='BTCUSDT', tf='15m')
    assert isinstance(result, tuple)
    assert isinstance(result[0], int)
    assert isinstance(result[1], int)


def test_add_klines():
    klines = [
        [
            1499740000000,
            "0.01634790",
            "0.80000000",
            "0.01575800",
            "0.01577100",
            "148976.11427815",
            1499844799999,
            "2434.19055334",
            309,
            "1756.87402397",
            "28.46694368",
            "0"
        ],
        [
            1499740000001,
            "0.01634790",
            "0.80000000",
            "0.01575800",
            "0.01577100",
            "148976.11427815",
            1499844799999,
            "2434.19055334",
            310,
            "1756.87402397",
            "28.46694368",
            "0"
        ]
    ]
    response = add_klines('BTCUSDT', '15m', klines)
    assert response.status_code == 201
    response = requests.get(url, params=dict(symbol='BTCUSDT', timeframe='15m', start_date=1499740000000, limit_first=1))
    assert response.json()[0][0] == 1499740000001
