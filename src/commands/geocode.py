import click
from ..core.utils import timeit
import os
import aiohttp
import asyncio
import aiofiles
from tqdm import tqdm
import pandas as pd
import random
import logging
logger = logging.getLogger(__name__)


async def write_geocodes(out_file_path, data):
    async with aiofiles.open(out_file_path, "a") as f:
        for d in data:
            try:
                await f.write(
                    f"{d['success']},{d['latitude']},{d['longitude']},{d['geoid']},{d['block']},{d['block_group']},{d['tract']},{d['county']},{d['state']},{d['county_name']},{d['state_name']},{d['population']},{d['housing']}\n"
                    )
            except:
                logger.error(d)


async def fetch(session, url, lat, lon):
    async with session.get(url, timeout=100) as response:

        try:
            body = await response.json()
            obj = {}
            block_level = body["result"]["geographies"]["Census Blocks"][0]
            states_level = body["result"]["geographies"]["States"][0]
            counties_level = body["result"]["geographies"]["Counties"][0]
            obj["latitude"] = lat
            obj["longitude"] = lon
            obj["geoid"] = block_level["GEOID"]
            obj["block"] = block_level["BLOCK"]
            obj["block_group"] = block_level["BLKGRP"]
            obj["tract"] = block_level["TRACT"]
            obj["county"] = block_level["COUNTY"]
            obj["state"] = block_level["STATE"]
            obj["county_name"] = counties_level["NAME"]
            obj["state_name"] = states_level["NAME"]
            obj["population"] = block_level["POP100"]
            obj["housing"] = block_level["HU100"]
            obj["success"] = True

            return obj

        except Exception as identifier:
            obj = {}
            obj["latitude"] = lat
            obj["longitude"] = lon
            obj["geoid"] = ""
            obj["block"] = ""
            obj["block_group"] = ""
            obj["tract"] = ""
            obj["county"] = ""
            obj["state"] = ""
            obj["county_name"] = ""
            obj["state_name"] = ""
            obj["success"] = False
            return obj



async def fetch_all(session, coords):
    geocode_tasks = []

    for lat, lon in coords:
        url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lon}&y={lat}&benchmark=4&vintage=420&format=json"
        task = asyncio.create_task(fetch(session, url, lat, lon))
        geocode_tasks.append(task)

    geocoded_results = await asyncio.gather(*geocode_tasks)

    return geocoded_results




async def main(coords, out_file_path):
    async with aiohttp.ClientSession() as session:
        resp = await fetch_all(session, coords)
        await write_geocodes(out_file_path, resp)


header = "success,latitude,longitude,geoid,block,block_group,tract,county,state,county_name,state_name\n"


@click.command("geocode")
@click.pass_context
@click.option(
    "-i",
    "--in_path",
    help="File with coordinates",
)
@click.option(
    "-i",
    "--out_path",
    help="File with coordinates",
)

@timeit
def geocode(ctx, in_path, out_path):
    """Get census geocode info for the coordinates"""

    out_file_path = out_path
    
    coordinates = pd.concat([chunk for chunk in tqdm(pd.read_csv(in_path, chunksize=1000), desc='Loading data')])
    # coordinates.sample(160).to_csv('./data/testing.csv',index=False)
    coordinates = coordinates[['latitude','longitude']].drop_duplicates()

    for c in ["latitude", "longitude"]:
        coordinates[c] = coordinates[c].astype(float)
        coordinates[c] = coordinates[c].apply(lambda x: round(x, 4))

    coordinates = coordinates.drop_duplicates()
    
    coordinates['idx'] = coordinates['latitude'].astype(str) + "," + coordinates['longitude'].astype(str)
    
    logger.error(f"Total Coordinates: {coordinates.shape[0]}")

    geocoded_results = pd.read_csv(out_file_path)
    geocoded_results = geocoded_results[geocoded_results['success']==True].copy()
    logger.error(f"Geocoded: {geocoded_results.shape[0]}")
    geocoded_results.to_csv(out_file_path,index=False)

    geocoded_results['idx'] = geocoded_results['latitude'].astype(str) + "," + geocoded_results['longitude'].astype(str)


    to_run = coordinates[~
        coordinates['idx'].isin(geocoded_results['idx'].values)
    ]
    logger.error(f"To Run: {to_run.shape[0]}")

    to_run = to_run.sample(frac=1)[["latitude", "longitude"]].drop_duplicates().values



    chunk_size = 10
    for i in tqdm(range(0, len(to_run), chunk_size)):
        chunk = to_run[i : i + chunk_size]
        asyncio.get_event_loop().run_until_complete(main(chunk, out_file_path))
