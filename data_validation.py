from prefect import task, flow, get_run_logger
import time as ttime
from tiled.client import from_uri
from bluesky_tiled_plugins.writing.validator import validate
from dotenv import load_dotenv
import os

BEAMLINE_OR_ENDSTATION = "qas"


def get_api_key_from_env(api_key=None):
    with open("/srv/container.secret", "r") as secrets:
        load_dotenv(stream=secrets)
    api_key = os.environ["TILED_API_KEY"]
    return api_key


@task(retries=2, retry_delay_seconds=10)
def get_run(uid, api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{BEAMLINE_OR_ENDSTATION}/raw"][uid]
    return run


# SQL database-backed - remove if this does not exist on the beamline
@task(retries=2, retry_delay_seconds=10)
def get_run_migration(uid, api_key=None):  # TODO remove after migration is complete
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{BEAMLINE_OR_ENDSTATION}/migration"][uid]
    return run


@task(retries=2, retry_delay_seconds=10)
def get_run_processed(uid, api_key=None):
    if not api_key:
        api_key = get_api_key_from_env()
    cl = from_uri("https://tiled.nsls2.bnl.gov", api_key=api_key)
    run = cl[f"{BEAMLINE_OR_ENDSTATION}/processed"][uid]
    return run


@task(retries=2, retry_delay_seconds=10)
def read_stream(run, stream):
    return run[stream].read()


# currently configured to run only one of BTP validation or read all streams checks
@flow
def data_validation(uid, api_key=None, dry_run=False):
    logger = get_run_logger()
    run_client = get_run_migration(
        uid, api_key=api_key
    )  # replace with get_run() if no SQL database
    logger.info(f"Validating uid {run_client.start['uid']}")
    start_time = ttime.monotonic()
    try:
        # the following calls to validate() only work for SQL database-backed catalogs - remove if not available
        if dry_run:
            validate(
                run_client, fix_errors=False, try_reading=True, raise_on_error=True
            )
        else:
            validate(run_client, fix_errors=True, try_reading=True, raise_on_error=True)
    except AttributeError:
        # check by reading data if not SQL database-backed
        run_client = get_run(uid, api_key=api_key)  # remove if no SQL database
        for stream in run_client:
            logger.info(f"{stream}:")
            stream_start_time = ttime.monotonic()
            stream_data = read_stream(run_client, stream)  # noqa: F841
            stream_elapsed_time = ttime.monotonic() - stream_start_time
            logger.info(f"{stream} elapsed_time = {stream_elapsed_time}")
            logger.info(f"{stream} nbytes = {stream_data.nbytes:_}")
    elapsed_time = ttime.monotonic() - start_time
    logger.info(f"{elapsed_time = }")
