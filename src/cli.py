
import click


from .commands.get_flights import get_flights
from .commands.scrape_flight_details import scrape_flight_details
from .commands.compile_flights import compile_flights
from .commands.geocode import geocode
from .commands.join import join
from .commands.xls2csv import xls2csv

# from .commands.scrape_incidents import scrape_incidents
# from .commands.compile_logs import compile_logs
# from .commands.check_audio_progress import check_audio_progress
# from .commands.pull_dispatch import pull_dispatch
# from .commands.transcribe_dispatch import transcribe_dispatch


import logging

logger = logging.getLogger(__name__)

@click.group()
@click.option("--base", default="data", help="Path to the data directory")
@click.pass_context

def cli(ctx, base):
    """
    citizen-toolkit: run tasks related to scraping citizens
    """

    ctx.obj["base"] = click.format_filename(base)
    FORMAT = "[%(asctime)s][%(levelname)s][%(name)s] %(module)s:%(funcName)s:%(lineno)d - %(message)s"
    logging.basicConfig(
        format=FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


cli.add_command(get_flights)
cli.add_command(scrape_flight_details)
cli.add_command(compile_flights)
cli.add_command(geocode)
cli.add_command(join)
cli.add_command(xls2csv)

# cli.add_command(scrape_map)
# cli.add_command(scrape_incidents)
# cli.add_command(compile_logs)
# cli.add_command(check_audio_progress)
# cli.add_command(pull_dispatch)
# cli.add_command(transcribe_dispatch)