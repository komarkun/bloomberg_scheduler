import os
import io
import json
import datetime

from common.logging import LOG

CREDENTIALS_PATH = os.path.join(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")),
    "ps-credential.txt",
)


def get_credentials():
    with io.open(CREDENTIALS_PATH, encoding="utf-8") as credential_file:
        CREDENTIALS = json.load(credential_file)

        EXPIRES_IN = (
            datetime.datetime.fromtimestamp(CREDENTIALS["expiration_date"] / 1000)
            - datetime.datetime.utcnow()
        )
        if EXPIRES_IN.days < 0:
            LOG.warning("Credentials expired %s days ago", EXPIRES_IN.days)
        elif EXPIRES_IN < datetime.timedelta(days=30):
            LOG.warning("Credentials expiring in %s", EXPIRES_IN)

        return CREDENTIALS


CREDENTIALS = get_credentials()
