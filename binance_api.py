from binance.spot import Spot
import logging


logger = logging.getLogger(__name__)


def get_symbols() -> list[str]:
    client = Spot()
    info = client.exchange_info()
    return [symbol['symbol'] for symbol in info['symbols'] if symbol['status'] == 'TRADING']


def download_klines(
    symbol: str,
        interval: str,
        start_date: int | None = None,
        end_date: int | None = None,
        limit: int = 500,
) -> list[list]:
    """Function downloads klines from broker api

    Args:
        symbol (str): symbol
        interval (str): timeframe interval
        start_date (int | None, optional): start date. Defaults to None.
        end_date (int | None, optional): end date. Defaults to None.
        limit (int, optional): limit of klines. Defaults to 500.

    Returns:
        list[list]: list of klines.
            Response:
                [
                    [
                        1499040000000,      // Kline open time
                        "0.01634790",       // Open price
                        "0.80000000",       // High price
                        "0.01575800",       // Low price
                        "0.01577100",       // Close price
                        "148976.11427815",  // Volume
                        1499644799999,      // Kline Close time
                        "2434.19055334",    // Quote asset volume
                        308,                // Number of trades
                        "1756.87402397",    // Taker buy base asset volume
                        "28.46694368",      // Taker buy quote asset volume
                        "0"                 // Unused field, ignore.
                    ]
                ]
    """
    client = Spot()
    kwargs = {}
    if start_date:
        kwargs['startTime'] = start_date+1
    if end_date:
        kwargs['endTime'] = end_date-1

    try:
        klines = client.klines(
            symbol,
            interval,
            limit=limit,
            **kwargs
        )
    except Exception as ex:
        logger.error(f'symbol {symbol}, tf {interval}, kwargs: {kwargs}')
        raise ex
    return klines
