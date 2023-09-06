import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
import datetime as dt
from tqdm import tqdm

logger = logging.getLogger(__name__)


@click.command("xls2csv")
@click.pass_context


@click.option(
    "-i",
    "--in_path",
    help="Path to file with previously scraped locations",
    default="/Volumes/easystore/Drones/",
)

@click.option(
    "-k",
    "--kind",
    help="Path to file with previously scraped locations",
    default="calls-for-service",
)
@click.option(
    "-g",
    "--geocode_folder",
    help="Path to file with previously scraped locations",
    default="geocodio",
)

@timeit

def xls2csv(ctx, in_path, kind, geocode_folder):
    df = pd.read_excel(f"{in_path}/{kind}/{kind}.xlsx")
    logger.error(f"Total Calls for Service: {df.shape[0]}")

    if kind == "calls-for-service":
        df['Address'] = df['Block Location'] + ", Chula Vista, CA, " + df['Zip Code']
        logger.error(f"Total Addresses: {df['Address'].drop_duplicates().shape[0]}")
        df.to_csv(f"{in_path}/{kind}/{kind}.csv",index=False)
        df['Address'].drop_duplicates().to_csv(f"{in_path}/{geocode_folder}/{kind}-addresses.csv",index=False)


