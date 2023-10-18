import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
import requests
import datetime as dt
from tqdm import tqdm

logger = logging.getLogger(__name__)


@click.command("join3p")
@click.pass_context


@click.option(
    "-i",
    "--in_path",
    help="Path to file with previously scraped locations",
    default="/Volumes/easystore/Drones",
)
@click.option(
    "-c",
    "--column",
    help="Path to file with previously scraped locations",
    default="Address",
)
@click.option(
    "-g",
    "--geocode_file",
    help="Path to file with previously scraped locations",
    default="geocodio",
)
@timeit

def join3p(ctx, in_path, column, geocode_file):
    calls_for_service = pd.concat([chunk for chunk in tqdm(pd.read_csv(f"{in_path}", chunksize=100000, dtype=str), desc='Loading data')])
    addresses = pd.read_csv(f"{geocode_file}",dtype=str)
    df = pd.merge(calls_for_service,addresses,on=column)
    df.to_csv('/Volumes/easystore/Drones/calls-for-service-with-census-data.csv',index=False)
