from binance_api import get_symbols, download_klines


def test_get_symbols():
    symbols = get_symbols()
    assert isinstance(symbols, list)
    assert len(symbols) > 0
    assert 'BTCUSDT' in symbols

def test_get_BTCUSDT_klines():
    klines = download_klines(symbol='BTCUSDT', interval='15m', start_date=1499040000000, limit=1)
    assert isinstance(klines, list)
    assert len(klines) > 0
    assert isinstance(klines[0][0], int)