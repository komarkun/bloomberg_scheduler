import datetime
import uuid
import json
import os
import shutil
import pandas as pd
import time
import os
from supabase import create_client

from common.session import SESSION
from urllib.parse import urljoin
from common.settings import HOST
from common.logging import LOG
from common.enums import TaskId
from utils.scheduler import parse_group_id


DOWNLOADS_PATH = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../")),
    "downloads",
)


def fetch_scheduled_catalog(ti):
    """Fetch the catalog for scheduled subscriptions."""
    catalogs_url = urljoin(HOST, "/eap/catalogs/")
    response = SESSION.get(catalogs_url)

    # We got back a good response. Let's extract our account number.
    catalogs = response.json()["contains"]
    for catalog in catalogs:
        if catalog["subscriptionType"] == "scheduled":
            # Take the catalog having "scheduled" subscription type,
            # which corresponds to the Data License account number.
            ti.xcom_push(key="catalog", value=catalog)
            break
    else:
        # We exhausted the catalogs, but didn't find a non-'bbg' catalog.
        LOG.error("Scheduled catalog not in %r", response.json()["contains"])
        raise RuntimeError("Scheduled catalog not found")


def fetch_bloomberg_data(ti, request_payload_callable):
    scheduled_catalog = ti.xcom_pull(
        task_ids=TaskId.FETCH_SCHEDULED_CATALOG.value, key="catalog"
    )

    catalog_id = scheduled_catalog["identifier"]

    account_url = urljoin(HOST, "/eap/catalogs/{c}/".format(c=catalog_id))
    LOG.info("Scheduled catalog URL: %s", account_url)

    request_name = "Python309HistoryRequest" + str(uuid.uuid1())[:6]
    request_payload = request_payload_callable(request_name)

    LOG.info("Request component payload:\n%s", json.dumps(request_payload, indent=2))

    # Create a new request resource.
    requests_url = urljoin(account_url, "requests/")
    response = SESSION.post(requests_url, json=request_payload)

    # Extract the URL of the created resource.
    request_location = response.headers["Location"]
    request_url = urljoin(HOST, request_location)

    # Extract the identifier of the created resource.
    request_id = json.loads(response.text)["request"]["identifier"]

    LOG.info(
        "%s resource has been successfully created at %s", request_name, request_url
    )

    SESSION.get(request_url)

    responses_url = urljoin(
        HOST, "/eap/catalogs/{c}/content/responses/".format(c=catalog_id)
    )

    ti.xcom_push(key="responses_url", value=responses_url)
    ti.xcom_push(key="request_name", value=request_name)
    ti.xcom_push(key="request_id", value=request_id)


def download_file_stream(
    ti,
    external_responses_url_callable=None,
    external_param=None,
):

    print(ti)
    group_id, task_name = parse_group_id(ti.task_id)

    if task_name != None:
        group_id += "."

    params = {}
    request_name = None
    request_id = None

    if external_param:
        params = external_param
    else:
        request_name = ti.xcom_pull(
            key="request_name",
            task_ids=group_id + TaskId.FETCH_BLOOMBERG_DATA.value,
        )
        request_id = ti.xcom_pull(
            key="request_id",
            task_ids=group_id + TaskId.FETCH_BLOOMBERG_DATA.value,
        )

        # Filter the required output from the available content by passing the request_name as prefix
        # and request_id as unique requestIdentifier query parameters respectively.
        params = {
            "prefix": request_name,
            "requestIdentifier": request_id,
        }

    scheduled_catalog = ti.xcom_pull(
        key="catalog", task_ids=TaskId.FETCH_SCHEDULED_CATALOG.value
    )

    catalog_id = scheduled_catalog["identifier"]

    responses_url = None
    if external_responses_url_callable:
        responses_url = external_responses_url_callable(catalog_id)
    else:
        responses_url = ti.xcom_pull(
            key="responses_url", task_ids=group_id + TaskId.FETCH_BLOOMBERG_DATA.value
        )

    print(responses_url)
    print("Responses URL")

    # We recommend adjusting the polling frequency and timeout based on the amount of data or the time range requested.
    reply_timeout_minutes = 45
    reply_timeout = datetime.timedelta(minutes=reply_timeout_minutes)
    expiration_timestamp = datetime.datetime.utcnow() + reply_timeout

    while datetime.datetime.utcnow() < expiration_timestamp:
        content_responses = SESSION.get(responses_url, params=params)
        response_contains = json.loads(content_responses.text)["contains"]
        if len(response_contains) > 0:
            output = response_contains[0]
            LOG.info("Response listing:\n%s", json.dumps(output, indent=2))

            output_key = output["key"]
            output_url = urljoin(
                HOST,
                "/eap/catalogs/{c}/content/responses/{key}".format(
                    c=catalog_id, key=output_key
                ),
            )
            output_file_path = os.path.join(DOWNLOADS_PATH, output_key)

            ti.xcom_push(key="output_url", value=output_url)
            ti.xcom_push(key="output_key", value=output_key)

            break
        else:
            # We make a delay between polls to reduce network usage.
            time.sleep(60)
    else:
        LOG.info(
            "Response not received within %s minutes. Exiting.", reply_timeout_minutes
        )

    with SESSION.get(output_url, stream=True) as response:
        output_filename = output_key
        if "content-encoding" in response.headers:
            if response.headers["content-encoding"] == "gzip":
                output_filename = output_filename + ".gz"
            elif response.headers["content-encoding"] == "":
                pass
            else:
                raise RuntimeError(
                    "Unsupported content encoding received in the response"
                )

        output_file_path = os.path.join(DOWNLOADS_PATH, output_filename)

        ti.xcom_push(key="output_file_path", value=output_file_path)

        with open(output_file_path, "wb") as output_file:
            LOG.info("Loading file from: %s (can take a while) ...", output_url)
            shutil.copyfileobj(response.raw, output_file)

        LOG.info("File downloaded: %s", output_filename)
        LOG.debug("File location: %s", output_file_path)


def read_data_frame(ti):

    group_id, task_name = parse_group_id(ti.task_id)

    if task_name != None:
        group_id += "."

    # Read the gzipped JSON file
    output_file_path = ti.xcom_pull(
        key="output_file_path", task_ids=group_id + TaskId.DOWNLOAD_FILE_STREAM.value
    )

    with open(output_file_path, "rb") as file:
        df = pd.read_json(file, compression="gzip")
        ti.xcom_push(key="df", value=df)


def clean_downloads_folder():
    """Clean up the downloads folder."""
    try:
        for file in os.listdir(DOWNLOADS_PATH):
            if file == ".gitkeep":
                continue
            file_path = os.path.join(DOWNLOADS_PATH, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        LOG.info("Downloads folder cleaned successfully")
    except Exception as e:
        # LOG.error("Error cleaning downloads folder: %s", str(e))
        raise RuntimeError(f"Error cleaning downloads folder: ${str(e)}")


def save_to_database(ti, tb_name, upsert=False):

    group_id, task_name = parse_group_id(ti.task_id)

    if task_name != None:
        group_id += "."

    records = ti.xcom_pull(key="records", task_ids=group_id + TaskId.REFAME_DF.value)

    # Initialize Supabase client
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    supabase = create_client(url, key)
    operation = "upsert" if upsert else "insert"

    # Insert data into Supabase
    try:
        if upsert == True:
            result = supabase.table(tb_name).upsert(records).execute()
        else:
            result = supabase.table(tb_name).insert(records).execute()

        LOG.info(
            'Successfully inserted %d records into the "%s" table.',
            len(records),
            tb_name,
        )
        LOG.info("%s result: %s", operation, str(result))

    except Exception as e:
        # LOG.error("Failed to %s records into Supabase: %s", operation, str(e))
        raise RuntimeError(f"Failed to {operation} records into Supabase: {str(e)}")
