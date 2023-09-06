import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
from tqdm import tqdm
from datetime import datetime
import json
import geopandas as gpd
import fiona
import gpxpy

logger = logging.getLogger(__name__)


@click.command("compile_flights")
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
    help="Path to file with previously scraped locations",
    default="",
)
@click.option(
    "-o",
    "--out_path",
    help="Path to file with previously scraped locations",
    default="",
)
@click.option(
    "-t",
    "--kind",
    help="",
    default="kml"
)
@timeit

def compile_flights(ctx, data_path, kml_storage_path, out_path, kind):
    
    fiona.drvsupport.supported_drivers['kml'] = 'rw'
    fiona.drvsupport.supported_drivers['KML'] = 'rw'

    kml_path = f"{kml_storage_path}/flights/{kind}"
    
    drone_manifest = pd.read_csv(f'{data_path}/all-flights-manifest.csv')
    
    if "gpx" in kind:
        drone_manifest['kml_file'] = drone_manifest['id'].apply(lambda x: f"{kml_path}/{x}.gpx")
    else:
        drone_manifest['kml_file'] = drone_manifest['id'].apply(lambda x: f"{kml_path}/{x}.kml")

    total = len(os.listdir(kml_path))
    logger.warning(f"Flights to parse from {kml_path}: {total}")

    compiled_flights = []

    for _, row in tqdm(drone_manifest.iterrows(),total = drone_manifest.shape[0]):
        try:
            if "gpx" in kind:
                with open(row['kml_file']) as f:
                    gpx = gpxpy.parse(f)  
                
                points = []
                for segment in gpx.tracks[0].segments:
                    for p in segment.points:
                        points.append({
                            'sequence': p.time,
                            'longitude': p.longitude,
                            'latitude': p.latitude,
                            'altitude': p.elevation,
                        })

                flight_path = pd.DataFrame.from_records(points)

            else:
                
                df = gpd.read_file(row['kml_file'], driver='KML')
                flight_path = pd.DataFrame(df['geometry'][1].coords)
                flight_path = flight_path.reset_index()
                flight_path.columns=['sequence','longitude','latitude','altitude']

            flight_path['id'] = row['id']
            flight_path['type'] = row['type']
            flight_path['incident_id'] = row['incident_id']
            flight_path['address_map'] = row['address_map']
            flight_path.to_csv(f"{kml_path}/{row['id']}-coordinates.csv",index=False)
            compiled_flights.append(flight_path)

        except Exception as error:
            logger.error(f"{error}: {row['kml_file']}")

        

    
    
    compiled_flights = pd.concat(compiled_flights)

    compiled_flights.to_csv(f"{out_path}/compiled-flight-data-{kind}.csv",index=False)


