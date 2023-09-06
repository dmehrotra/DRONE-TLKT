import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
from datetime import datetime, timedelta
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import json

mys=[]
logger = logging.getLogger(__name__)


def enumerate_months(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%m%Y")[1:]  # Remove the leading zero from the month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)




@click.command("get_flights")
@click.pass_context

@click.option(
    "-d",
    "--data_path",
    help="Path to file with previously scraped locations",
    default="data",
)

@click.option(
    "-start",
    "--start_time",
    help="",
    default="8/1/2022",
)
@click.option(
    "-end",
    "--end_time",
    help="",
    default="8/1/2023",
)

@timeit


def get_flights(ctx, data_path, start_time, end_time):
    
    all_flights = pd.read_csv(f"{data_path}/all-flights-manifest.csv")
    logger.error(f" Flights in Manifest: {all_flights.shape[0]}")

    st = list(map(lambda x: int(x), start_time.split('/')))
    et = list(map(lambda x: int(x), end_time.split('/')))
    
    start_date = datetime(st[-1], st[0], st[1])
    end_date = datetime(et[-1], et[0], et[1])
    
    month_year = []
    for month in enumerate_months(start_date, end_date):
        month_year.append(month)


    df_container = []
    departments = ["cvpd","spduas","bothellpd","chicopd"]
    
    for department in tqdm(departments,total=len(departments)):
        for month in month_year:
            params = {
                'month': month,
                'mapmode': 'large',
                'address': '',
                'lat': '',
                'long': '',
                'radius': '0.5',
            }

            try:
                response = requests.get(f'https://app.airdata.com/u/{department}', params=params)
                soup = BeautifulSoup(response.content, "html.parser")
                a = soup.find_all('div',{'id':'main_clustermap_data_ajax'})[0].text.replace('\t','').replace('\n','').strip()
                b = pd.DataFrame(json.loads(a))
                b['department'] = department
                b.columns = ['lat_map','lon_map','time','id','date','time_s','address_map','incident_id','type','department']
                df_container.append(b)

            except Exception as error:
                # handle the exception
                logger.error("An exception occurred:", error)

    flights = pd.concat(df_container)
    flights = flights.drop_duplicates()

    
    flights.drop_duplicates().to_csv(f"{data_path}/all-flights-manifest.csv",index=False)
