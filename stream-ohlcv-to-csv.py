# -*- coding: utf-8 -*-
import csv
import ccxt
import pandas as pd
from pathlib import Path
from argparse import ArgumentParser

MAX_RETRIES = 5


def fetch_ohlcv_retry(exchange, max_retries, symbol, timeframe, since, limit):
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
        return ohlcv
    except Exception as e:
        if max_retries > 1:
            return fetch_ohlcv_retry(exchange, max_retries - 1, symbol,
                                     timeframe, since, limit)
        else:
            raise e


def stream_ohlcv_to_csv(exchange, max_retries, symbol,
                        timeframe, since, limit, csv_file):
    now = exchange.milliseconds()
    all_ohlcv = []
    fetch_since = since
    while fetch_since < now:
        try:
            ohlcv = fetch_ohlcv_retry(
                exchange, max_retries, symbol, timeframe, fetch_since, limit)
            ohlcv = exchange.filter_by_since_limit(
                ohlcv, fetch_since, None, key=0)

            if ohlcv:
                all_ohlcv = all_ohlcv + ohlcv
                from_date = exchange.iso8601(all_ohlcv[0][0])
                to_date = exchange.iso8601(all_ohlcv[-1][0])
                print(len(all_ohlcv), 'candles in total from',
                      from_date, 'to', to_date)
                write_to_csv(csv_file, ohlcv)
                fetch_since = ohlcv[-1][0] + 1
            else:
                print(
                    f'no data received since {exchange.iso8601(fetch_since)}')
                break
        except KeyboardInterrupt:
            break


def write_to_csv(path, data):
    with open(path, mode='a') as output_file:
        csv_writer = csv.writer(output_file, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(data)
        print(f'Wrote {len(data)} rows to {path}')


def write_ohlcv_header(path):
    columns = [['timestamp', 'open', 'high',
               'low', 'close', 'volume']]
    write_to_csv(path, columns)


if __name__ == '__main__':
    ap = ArgumentParser()
    ap.add_argument('--exchange', default='binance', type=str)
    ap.add_argument('--symbol', default='BTC/USDT', type=str)
    ap.add_argument('--timeframe', default='1h', type=str)
    ap.add_argument('--limit', default=100, type=int)
    ap.add_argument('-d', '--directory', type=str, default='./ohlcv-data',
                    help='directory to write data')
    ap.add_argument(
        '-f', '--file', type=str, default='',
        help='filname for output (will be determined automatically if empty)')
    args = ap.parse_args()

    print(args.__dict__)

    file = args.file
    if not file:
        # infer filepath from arguments
        file = f'{args.exchange}-{args.symbol}-{args.timeframe}.csv'
        file = file.replace('/', '-')

    outpath = Path(args.directory) / file
    if not outpath.exists():
        print(f'\nCreate new .csv file at {outpath}')
        write_ohlcv_header(outpath)

    print(f'\nLoad data from {outpath}')
    df = pd.read_csv(outpath)
    print(df.info())

    # sanity check the data
    # timestamps should be sorted and monotonically increasing
    assert df.timestamp.is_unique
    assert df.timestamp.is_monotonic_increasing

    # we want to fetch data from after the last timestamp
    try:
        fetch_since = int(df.timestamp.values[-1]) + 1
    except IndexError:
        fetch_since = 0
    print('\nFetch data since (unix timestamp): ', fetch_since)

    # convert since from string to milliseconds integer if needed
    exchange = getattr(ccxt, args.exchange)({'enableRateLimit': True})
    if isinstance(fetch_since, str):
        fetch_since = exchange.parse8601(fetch_since)

    # preload all markets from the exchange
    exchange.load_markets()
    # fetch all candles
    stream_ohlcv_to_csv(exchange, MAX_RETRIES, args.symbol,
                        args.timeframe, fetch_since, args.limit, outpath)
