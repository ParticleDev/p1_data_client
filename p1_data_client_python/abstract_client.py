"""
An abstract client class for all types of API.

Import as:
import p1_data_client_python.abstract_client as pabstr
"""

import abc
import datetime as dt
import json
import platform
import tqdm
from typing import Any, Dict

import pandas as pd
import requests
import requests.adapters as rq_adapt
import requests.packages.urllib3.util.retry as rq_retry

import p1_data_client_python.exceptions as p1_exc
import p1_data_client_python.version as version


class AbstractClient:
    """
    Base abstract class.
    """

    def __init__(
        self,
        token: str,
        base_url: str = "",
        use_retries: bool = True,
        retries_number: int = 5,
        backoff_factor: float = 0.3,
    ):
        """
        Pass arguments and gets authenticated in the system.

        :param base_url: REST API Server url.
        :param token: Your token for access to the system.
        """
        self.base_url = base_url or self._default_base_url
        self.base_url = self.base_url.rstrip("//")
        self.token = token
        self.use_retries = use_retries
        self.retries_number = retries_number
        self.backoff_factor = backoff_factor
        self._scroll_id = ""
        self.status_forcelist = (500, 502, 504)
        self._last_search_parameters = None
        self.session = self._get_session()
        self.headers = {
            "Authorization": "Token " + self.token,
            "Content-Type": "application/json",
        }

    @classmethod
    def validate_date(cls, date_text: str) -> bool:
        """
        Validate string date.
        """
        try:
            dt.datetime.strptime(date_text, "%Y-%m-%dT%H:%M:%S%z")
        except ValueError:
            raise ValueError("Incorrect data format, "
                             "should be YYYY-MM-DDTHH-MI-SSTZINFO."
                             "Example: 2021-03-05T19:41:02-05:00.")
        return True

    @property
    def client_version(self) -> str:
        """
        Return the client's version.
        """
        return version.VERSION

    @property
    @abc.abstractmethod
    def _api_routes(self) -> Dict[str, str]:
        """
        Abstract property for a dict API routes.

        :return: Dict of API routes like "<ROUTE_NAME>": "ROUTE_PATH"
            Example: "SEARCH": "/data-api/v1/search/"
        """

    @property
    @abc.abstractmethod
    def _default_base_url(self) -> str:
        """
        Abstract property that return base url.

        :return: Default base server url.
        """

    @classmethod
    def _get_dataframe_from_response(
        cls, response: requests.Response
    ) -> pd.DataFrame:
        """
        Retrieve tha dataframe from the json part of a response.

        :param response: Response from a request.
        :return: Dataframe from json.
        """
        try:
            data = pd.DataFrame(response.json()["data"])
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        return data

    def _get_versions(self):
        """
        Get package versions.
        """
        versions = [
            (
                "P1 DATA API Python Client",
                f"{self.client_version} ({platform.platform()})"
            ),
            ("Python", platform.python_version()),
            ("Pandas", pd.__version__),
            ("Requests", requests.__version__),
            ("Tqdm", tqdm.__version__),
        ]
        result = " ".join("/".join(i) for i in versions)
        return result

    def _set_optional_params(
        self, params: Dict[str, Any], **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Settle optional parameters to the params dict for requests running.

        :param params: Dict for parameters.
        :param kwargs: All parameters should be implemented.
        :return: Params dict.
        """
        for param, value in [(k, v) for k, v in kwargs.items()
                             if v is not None]:
            if param.endswith("datetime"):
                self.validate_date(kwargs[param])
            params[param] = value

        return params

    def _get_session(self) -> requests.Session:
        """
        Initialize and return a session allows make retry when some errors
        will raised.
        """
        session = requests.Session()
        retry = rq_retry.Retry(
            total=self.retries_number,
            read=self.retries_number,
            connect=self.retries_number,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
        )
        adapter = rq_adapt.HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        return session

    def _make_request(self, *args: Any, **kwargs: Any) -> requests.Response:
        """
        Single entry point for any request to the REST API.
        """
        if kwargs.get("headers") is None:
            kwargs["headers"] = dict()
        kwargs["headers"]["User-Agent"] = self._get_versions()
        response = self.session.request(*args, **kwargs)
        # Throw exception, if token is not valid.
        if response.status_code == 401:
            raise p1_exc.UnauthorizedException(response.text)
        if response.status_code != 200:
            raise p1_exc.ParseResponseException(
                f"Got next response, from the server: {response.text}"
            )
        return response
