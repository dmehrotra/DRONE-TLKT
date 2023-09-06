import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
import requests
import datetime as dt
from tqdm import tqdm

logger = logging.getLogger(__name__)


@click.command("join")
@click.pass_context


@click.option(
    "-i",
    "--in_path",
    help="Path to file with previously scraped locations",
    default="/Volumes/easystore/Drones",
)
@click.option(
    "-k",
    "--kind",
    help="Path to file with previously scraped locations",
    default="kml",
)
@click.option(
    "-l",
    "--limit",
    help="Path to file with previously scraped locations",
    default="none",
)
@timeit

def join(ctx, in_path, kind, limit):
    merge_file = f"{in_path}/compiled-flight-data-{kind}.csv"
    geocode_file = f"{in_path}/geocoded.csv"
    df = pd.concat([chunk for chunk in tqdm(pd.read_csv(merge_file, chunksize=100000, dtype=str), desc='Loading data')])
    
    logger.error(f"{merge_file} contains {df.shape[0]} rows")
    
    for c in ["latitude", "longitude"]:
        df[c] = df[c].astype(float)
        df[c] = df[c].apply(lambda x: round(x, 4))

    if limit == "S":
        df['sequence'] = pd.to_datetime(df['sequence']).round("S")

    df = df.drop_duplicates()

    logger.error(f"Now {merge_file} contains {df.shape[0]} rows at a lower resolution")

    geocoded_flight_data = pd.read_csv(geocode_file,dtype=str)
    for c in ["latitude", "longitude"]:
        geocoded_flight_data[c] = geocoded_flight_data[c].astype(float)

    merged = pd.merge(df,geocoded_flight_data,on=['latitude','longitude'],how='left')
    merged.to_csv(f"{in_path}/{kind}-with-census-data.csv",index=False)
