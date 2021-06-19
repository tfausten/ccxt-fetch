""" Converts ohlcv data to zipline format """
import pandas as pd
from argparse import ArgumentParser

ap = ArgumentParser()
ap.add_argument('-i', '--input', type=str, required=True)
ap.add_argument('-o', '--output', type=str)
args = ap.parse_args()

# read and transform
df = pd.read_csv(args.input)
df.timestamp = pd.to_datetime(df.timestamp, unit='ms')
print(df.info())
print(df.head())

# write
if args.output:
    assert args.input != args.output
    df.to_csv(args.output, index=False)
