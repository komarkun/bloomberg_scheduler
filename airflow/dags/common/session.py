from requests_oauthlib import OAuth2Session
from oauthlib.oauth2 import BackendApplicationClient, TokenExpiredError
from common.credentials import CREDENTIALS
from common.logging import LOG


class DLRestApiSession(OAuth2Session):
    """Custom session class for making requests to a DL REST API using OAuth2 authentication."""

    def __init__(self, *args, **kwargs):
        """
        Initialize a DLRestApiSession instance.
        """
        super().__init__(*args, **kwargs)

    def request_token(self):
        """
        Fetch an OAuth2 access token by making a request to the token endpoint.
        """
        oauth2_endpoint = "https://bsso.blpprofessional.com/ext/api/as/token.oauth2"
        self.token = self.fetch_token(
            token_url=oauth2_endpoint, client_secret=CREDENTIALS["client_secret"]
        )

    def request(self, *args, **kwargs):
        """
        Override the parent class method to handle TokenExpiredError by refreshing the token.
        :return: response object from the API request
        """
        try:
            response = super().request(*args, **kwargs)
        except TokenExpiredError:
            self.request_token()
            response = super().request(*args, **kwargs)

        return response

    def send(self, request, **kwargs):
        """
        Override the parent class method to log request and response information.
        :param request: prepared request object
        :return: response object from the API request
        """
        LOG.debug(
            "Request being sent to HTTP server: %s, %s, %s",
            request.method,
            request.url,
            request.headers,
        )

        response = super().send(request, **kwargs)

        LOG.debug("Response status: %s", response.status_code)
        LOG.debug("Response x-request-id: %s", response.headers.get("x-request-id"))

        if response.ok:
            if not kwargs.get("stream"):
                LOG.debug("Response content: %s", response.text)
        else:
            raise RuntimeError(
                "\n\tUnexpected response status code: {c}\nDetails: {r}".format(
                    c=str(response.status_code), r=response.json()
                )
            )

        return response


def init_session():
    CLIENT = BackendApplicationClient(client_id=CREDENTIALS["client_id"])

    SESSION = DLRestApiSession(client=CLIENT)
    # This is a required header for each call to DL Rest API.
    SESSION.headers["api-version"] = "2"
    SESSION.request_token()

    return SESSION


SESSION = init_session()
