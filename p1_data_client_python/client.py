"""
The core of P1 Data REST API wrapper

Copyright 2020 by Particle.One Inc.
All rights reserved.
This file is part of the ParticleOne and is released under the "MIT". Please
see the license.txt file that should have been included as part of this
package.


Import as

import p1_data_client_python.client as p1_data
"""

import json
from typing import Any, Dict, List

import p1_data_client_python.exceptions as p1_exc
import pandas as pd
import requests
import requests.adapters as rq_adapt
import requests.packages.urllib3.util.retry as rq_retry

DEFAULT_BASE_URL = "https://data.particle.one"


class Client:
    """
    Base class for p1 data REST API operating.
    """

    SEARCH_CHUNK_SIZE = 1000
    API_ROUTES = {
        "AUTH": "/auth-token/",
        "SEARCH": "/data-api/v1/search/",
        "SEARCH_SCROLL": "/data-api/v1/search-scroll/",
        "PAYLOAD": "/data-api/v1/payload/",
    }
    METADATA_ROUTES = {
        "COMMODITIES": "/data-api/v1/commodities/",
        "BUSINESS-CATEGORIES": "/data-api/v1/business-categories/",
        "COUNTRIES": "/data-api/v1/countries/",
        "FREQUENCIES": "/data-api/v1/frequencies/",
    }

    def __init__(
        self,
        token: str,
        base_url: str = DEFAULT_BASE_URL,
        use_retries: bool = True,
        retries_number: int = 5,
        backoff_factor: float = 0.3,
    ):
        """
        Pass arguments and gets authenticated in the system.
        :param base_url: REST API Server url.
        :param token: Your token for access to the system.
        """
        self.base_url = base_url.rstrip("//")
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

    def _get_session(self) -> requests.Session:
        """
        Initialize and return a session allows make retry
        when some errors will raised.
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

    def _make_request(self, *args, **kwargs) -> requests.Response:
        """
        Single entry point for any request to the REST API.
        :return: requests Response.
        """
        response = self.session.request(*args, **kwargs)
        # Throw exception, if token is not valid.
        if response.status_code == 401:
            raise p1_exc.UnauthorizedException(response.text)
        return response

    @property
    def list_of_metadata(self) -> List[str]:
        """
        Retrieve list of metadata keys from METADATA_ROUTES
        """
        return list(self.METADATA_ROUTES.keys())

    def search_pages(self, pages_limit: int = 100) -> pd.DataFrame:
        """
        Get generator which scrolling down through pages
        until end or limit will reached.
        :param pages_limit: Max paged for scrolling.
        """
        current_page_number = 1
        row_count = self._last_total_count

        if self._scroll_id:
            while True:
                if row_count > 0 and current_page_number < pages_limit:
                    payloads = self.search_scroll()
                else:
                    break
                row_count = len(payloads["rows"])
                self._scroll_id = payloads["scroll_id"]
                yield pd.DataFrame(payloads["rows"])
                current_page_number += 1

    def search_scroll(self) -> Dict[Any, Any]:
        """
        Get next chunk(page) of payloads by given scroll_id
        """
        response = self._make_request(
            "GET",
            self.base_url + self.API_ROUTES["SEARCH_SCROLL"],
            headers=self.headers,
            params={"scroll_id": self._scroll_id},
        )
        try:
            next_page = response.json()
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                msg=f"Can't decode response: "
                f"{response.text} as JSON, {repr(e)}",
                doc=e.doc,
                pos=e.pos,
            )
        return next_page

    def _parse_search(self, response: requests.Response) -> pd.DataFrame:
        """
        Parse search response and return pandas Dataframe
        """
        payloads = response.json()
        self._scroll_id = payloads["scroll_id"]
        self._last_total_count = payloads["total_count"]

        return pd.DataFrame(payloads["rows"])

    def search(self, **search_payload) -> pd.DataFrame:
        """
        Get payloads IDs by given search conditions.
        :param text:
        :param commodity:
        :param business_category:
        :param country:
        :param frequency:
        :return: A first page (chunk of a data) that represent.
        pandas dataframe: pd.DataFrame
        """
        # Store search a last search conditions.
        self._last_search_parameters = search_payload
        response = self._make_request(
            "POST",
            self.base_url + self.API_ROUTES["SEARCH"],
            headers=self.headers,
            data=json.dumps(self._last_search_parameters),
        )
        # Parse response to a pandas dataframe, check for errors.
        try:
            search_dataframe = self._parse_search(response)
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        return search_dataframe

    @staticmethod
    def _parse_payload(response: requests.Response) -> pd.DataFrame:
        """
        Parse payload response and return pandas Dataframe.
        """
        payload_response = response.json()
        return pd.DataFrame(payload_response["payload_data"])

    def get_payload(self, payload_id: str) -> pd.DataFrame:
        """
        Get time series data by payload_id.
        :param payload_id: ID of payload from search method.
        """
        response = self._make_request(
            "GET",
            self.base_url + self.API_ROUTES["PAYLOAD"],
            headers=self.headers,
            params={"payload_id": payload_id},
        )
        # Parse response to a pandas dataframe, check for errors.
        try:
            payload_dataframe = self._parse_payload(response)
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        return payload_dataframe

    @staticmethod
    def _parse_metadata_type(
        metadata_type: str, response: requests.Response
    ) -> pd.DataFrame:
        """
        Parse metadata_type response and return pandas Dataframe
        """
        metadata_response = response.json()
        metadata_list = [row["name"] for row in metadata_response["data"]]
        return pd.DataFrame(metadata_list, columns=[metadata_type])

    def get_metadata_type(self, metadata_type: str) -> pd.DataFrame:
        """
        Get list of values for any metadata type.
        All types are listed in METADATA_ROUTES.
        :return: pandas Dataframe with metadata type values on-board
        """
        # Check if metadata_type in the allowed list.
        try:
            metadata_type_path = self.METADATA_ROUTES[metadata_type]
        except KeyError as e:
            raise p1_exc.BadMetaDataTypeException(
                f"{metadata_type} metadata "
                f"type is not supported in the client"
            ) from e
        # make request
        response = self._make_request(
            "GET", self.base_url + metadata_type_path, headers=self.headers
        )
        # Parse response to a pandas dataframe, check for errors.
        try:
            metadata_type_dataframe = self._parse_metadata_type(
                metadata_type, response
            )
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        return metadata_type_dataframe
