import click
import os
import aiohttp
import asyncio
import aiofiles
from tqdm import tqdm
import pandas as pd
import logging
import datetime as dt

logger = logging.getLogger(__name__)


async def fetch(session, url, nid, kml_path, kind):
    async with session.get(url, timeout=100) as response:
        rtext = await response.text()
        if "gpx" in kind:
            async with aiofiles.open(f"{kml_path}/{nid}.gpx", "w") as f:
                await f.write(rtext)
        else:
            async with aiofiles.open(f"{kml_path}/{nid}.kml", "w") as f:
                await f.write(rtext)


async def fetch_all(session, ids, kml_path, kind):
    tasks = []

    for nid in ids:
        url = f"http://app.airdata.com/{kind}?flight={nid}"
        task = asyncio.create_task(fetch(session, url, nid, kml_path, kind))
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    return results


async def main(ids, kml_path,kind):
    async with aiohttp.ClientSession() as session:
        resp = await fetch_all(session, ids, kml_path, kind)



@click.command("scrape_flight_details")

@click.pass_context

@click.option(
    "-d",
    "--data_path",
    help="Path to file with previously scraped locations",
    default="data",
)

@click.option(
    "-k",
    "--kml_storage_path",
    default="/Volumes/easystore/Drones",
    help=""
    )
@click.option(
    "-t",
    "--kind",
    default="kml",
    help=""
    )


def scrape_flight_details(ctx, data_path, kml_storage_path, kind):
    manifest_file = f"{data_path}/all-flights-manifest.csv"
    kml_path = f"{kml_storage_path}/flights/{kind}"

    all_flights = pd.read_csv(manifest_file)
    
    files=[]
    for filename in os.listdir(f"{kml_path}"):
        f = os.path.join(f"{kml_path}", filename)
        if f.endswith('kml'):
            files.append(f.split('/')[-1].replace('.kml',''))
        elif f.endswith('gpx'):
            files.append(f.split('/')[-1].replace('.kml',''))
 
    logger.warning(f"Total Flight Details Scraped: {len(files)}")

    to_run = all_flights[~all_flights["id"].isin(files)]

    logger.warning(f"Total Logs To Run: {to_run.shape[0]} ")

    to_run = to_run["id"].sample(frac=1).drop_duplicates().values

    chunk_size = 5
    
    for i in tqdm(range(0, len(to_run), chunk_size)):
        chunk = to_run[i : i + chunk_size]
        asyncio.get_event_loop().run_until_complete(main(chunk,kml_path,kind))

    # for index, row in all_incidents.iterrows():
    #     url = f"https://data.sp0n.io/v1/incident/{row['incidentId']}"
    #     updates = requests.get(url).json()
    #     for uid in updates["updates"]:
    #         update = updates["updates"][uid]
    #         update["cid"] = uid
    #         update["key"] = updates["key"]
    #         update["address"] = updates["address"]
    #         update["latitude"] = updates["latitude"]
    #         update["longitude"] = updates["longitude"]
    #         update["neighborhood"] = updates["neighborhood"]
    #         update["title"] = updates["title"]
    #         update["incident_ts"] = updates["ts"]
    #         update["police"] = updates["police"]
    #         gun_fire_incident_logs.append(update)

