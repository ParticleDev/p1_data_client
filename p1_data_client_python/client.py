"""The core of P1 Data REST API wrapper.

Copyright 2020 by Particle.One Inc.
All rights reserved.
This file is part of the ParticleOne and is released under the "MIT". Please
see the license.txt file that should have been included as part of this
package.

Import as:
import p1_data_client_python.client as p1_data
"""

import json
import os
from typing import Any, Dict, List

import pandas as pd
import requests

import p1_data_client_python.abstract_client as p1_abs
import p1_data_client_python.exceptions as p1_exc
import p1_data_client_python.helpers.datetime_ as hdatet

P1_DATA_API_VERSION = os.environ.get("P1_DATA_API_VERSION", "1")


class Client(p1_abs.AbstractClient):
    """Class for p1 data REST API operating."""

    SEARCH_CHUNK_SIZE = 1000

    _URL = f"/data-api/v{P1_DATA_API_VERSION}"

    METADATA_ROUTES = {
        "COMMODITIES": _URL + "/commodities/",
        "BUSINESS-CATEGORIES": _URL + "/business-categories/",
        "COUNTRIES": _URL + "/countries/",
        "FREQUENCIES": _URL + "/frequencies/",
    }

    @property
    def list_of_metadata(self) -> List[str]:
        """Retrieve list of metadata keys from METADATA_ROUTES."""
        return list(self.METADATA_ROUTES.keys())

    def search_pages(self, pages_limit: int = 100) -> pd.DataFrame:
        """Get generator which scrolling down through pages until end or limit
        will reached.

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
        """Get next chunk (page) of payloads by given scroll_id."""
        response = self._make_request(
            "GET",
            self.base_url + self._api_routes["SEARCH_SCROLL"],
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

    def search(self, **search_payload: Any) -> pd.DataFrame:
        """Get payloads IDs by given search conditions.

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
            self.base_url + self._api_routes["SEARCH"],
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

    def get_payload(self, payload_id: str) -> pd.DataFrame:
        """Get time series data by payload_id.

        :param payload_id: ID of payload from search method.
        """
        response = self._make_request(
            "GET",
            self.base_url + self._api_routes["PAYLOAD"],
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

    def get_metadata_type(self, metadata_type: str) -> pd.DataFrame:
        """Get list of values for any metadata type.

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

    @property
    def _default_base_url(self) -> str:
        return "https://data.particle.one"

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "AUTH": "/auth-token/",
            "SEARCH": "/data-api/v1/search/",
            "SEARCH_SCROLL": "/data-api/v1/search-scroll/",
            "PAYLOAD": "/data-api/v1/payload/",
        }

    def _parse_search(self, response: requests.Response) -> pd.DataFrame:
        """Parse search response and return pandas Dataframe."""
        payloads = response.json()
        self._scroll_id = payloads["scroll_id"]
        self._last_total_count = payloads["total_count"]
        return pd.DataFrame(payloads["rows"])

    @staticmethod
    def _parse_payload(response: requests.Response) -> pd.DataFrame:
        """Parse payload response and return pandas Dataframe."""
        payload_response = response.json()
        payload = pd.DataFrame(payload_response["payload_data"])
        payload["period"] = hdatet.to_datetime(payload["original_period"])
        return payload

    @staticmethod
    def _parse_metadata_type(
        metadata_type: str, response: requests.Response
    ) -> pd.DataFrame:
        """Parse metadata_type response and return pandas Dataframe."""
        metadata_response = response.json()
        metadata_list = [row["name"] for row in metadata_response["data"]]
        return pd.DataFrame(metadata_list, columns=[metadata_type])
