from binance_api import get_symbols, download_klines
import logging
from time import sleep
import os
import requests
from itertools import product
from datetime import datetime

logging.basicConfig(format='%(asctime)s: %(message)s',
                    datefmt='%d/%m/%y %H:%M:%S', level=logging.INFO)

SERVER_HOST = os.environ.get('SERVER_HOST', 'api')
SERVER_PORT = os.environ.get('SERVER_PORT', 8000)

url = f'http://{SERVER_HOST}:{SERVER_PORT}/klines'

logger = logging.getLogger(__name__)

timeframes_in_minutes = {
    # '1m': 1,
    # '5m': 5,
    '15m': 15,
    # '1h': 60,
    # '4h': 240,
    # '1d': 1440,
    # '1w': 10080,
    # '1M': 43200,
}


def get_min_max_dates_for_symbol_tf(symbol: str, tf: str) -> tuple[int, int] | tuple[None, None]:
    """Function gets min and max dates is already downloaded klines

    Args:
        symbol (str): symbol
        tf (str): timeframe

    Returns:
        tuple[int, int] | None: Tuple with min/max dates on DB (min_date, max_date)
    """
    try:
        # get first kline
        params = {'symbol': symbol, 'timeframe': tf, 'limit_first': 1}
        first_date = requests.get(url, params=params, timeout=3).json()[0][0]

        # get last kline
        params = {'symbol': symbol, 'timeframe': tf, 'limit': 1}
        last_date = requests.get(url, params=params, timeout=3).json()[0][0]
    except (requests.ConnectTimeout, ConnectionRefusedError, IndexError):
        return None, None

    return (first_date, last_date)


def add_klines(symbol: str, tf: str, raw_klines: list[list]) -> requests.Response:
    """Adding klines to DB

    Args:
        symbol (str): symbol
        tf (str): timeframe
        klines (list[list]): raw list of klines from broker API

    Returns:
        requests.Response: response from DB API
    """
    klines_dict = []
    for kline in raw_klines:
        kline_dict = dict(
            symbol=symbol,
            timeframe=tf,
            open_time=kline[0],
            close_time=kline[6],
            open=kline[1],
            high=kline[2],
            low=kline[3],
            close=kline[4],
            volume=kline[5],
            trades=kline[8],
        )
        klines_dict.append(kline_dict)

    return requests.post(url, json=klines_dict)


def main_loop():
    while True:
        try:
            symbols = get_symbols()
            k = 1
            whole_length = len(symbols) * len(timeframes_in_minutes.keys())
            for symbol, tf in product(symbols, timeframes_in_minutes.keys()):
                logger.info(f'Download history for {symbol}_{tf}')

                min_date, max_date = get_min_max_dates_for_symbol_tf(
                    symbol, tf)

                def download_klines_data(
                    symbol: str,
                    tf: str,
                    date_kwarg: str,
                    k: int,
                    whole_length: int,
                    min_date: int | None,
                    max_date: int | None,
                ):
                    while True:
                        try:
                            break_date = int(datetime.strptime(
                                '01.01.2020', "%d.%m.%Y").timestamp() * 1000)
                            if min_date is not None and date_kwarg == 'min_date' and min_date <= break_date:
                                break

                            kwargs = {}
                            if date_kwarg == 'min_date':
                                kwargs['end_date'] = min_date
                            elif date_kwarg == 'max_date':
                                kwargs['start_date'] = max_date
                            else:
                                raise ValueError(
                                    f'Unexpected param name: {date_kwarg}')

                            raw_klines = download_klines(symbol, tf, **kwargs)
                            if len(raw_klines) == 0:
                                break
                            add_klines(symbol, tf, raw_klines)

                            if date_kwarg == 'min_date':
                                min_date = raw_klines[0][0]
                            elif date_kwarg == 'max_date':
                                max_date = raw_klines[-1][0]

                            date_f = datetime.fromtimestamp(
                                raw_klines[0][0] / 1000).strftime('%d.%m.%Y %H:%M:%S')
                            logger.info(
                                f'added {len(raw_klines)} klines of {symbol}_{tf} date {date_f} ({k}/{whole_length})')
                        except (OSError) as ex:
                            logger.error(ex)

                        sleep(0.5)

                # download older klines
                logger.info('download older klines...')
                download_klines_data(symbol, tf, 'min_date', k, whole_length, min_date, max_date)

                # download newest klines
                logger.info('download newest klines...')
                download_klines_data(symbol, tf, 'max_date', k, whole_length, min_date, max_date)

                k += 1
        except OSError as ex:
            logger.error(ex)

        logger.info('All klines downloaded.')


if __name__ == '__main__':
    main_loop()
