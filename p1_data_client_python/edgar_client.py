"""P1 Edgar Data REST API wrapper.

Copyright 2020 by Particle.One Inc.
All rights reserved.
This file is part of the ParticleOne and is released under the "MIT". Please
see the license.txt file that should have been included as part of this
package.

Import as
import p1_data_client_python.client as p1_edg
"""

import json
import sys
from typing import Any, Dict, Optional, Union
import pandas as pd

import p1_data_client_python.abstract_client as p1_abs
import p1_data_client_python.exceptions as p1_exc

PAYLOAD_BLOCK_SIZE = 100
P1_CIK = Union[str, int]
P1_GVK = Union[str, int]


class ItemMapper(p1_abs.AbstractClient):
    """Handler for an item mapping."""

    def get_mapping(self) -> pd.DataFrame:
        """Get all mapping for items.

        :return: Item mapping as dataframe.
        """

        params = {"mapping_type": "items"}
        url = f'{self.base_url}{self._api_routes["MAPPING"]}'
        response = self._make_request(
            "GET", url, headers=self.headers, params=params
        )
        return self._get_dataframe_from_response(response)

    def get_item_from_keywords(self, keywords: str) -> pd.DataFrame:
        """Obtain an item by keywords.

        :param keywords: List of keywords.
        :return: Item code.
        """

        params: Dict[str, Any] = {}
        params = self._set_optional_params(params, keywords=keywords)
        url = f'{self.base_url}{self._api_routes["ITEM"]}'
        response = self._make_request(
            "GET", url, headers=self.headers, params=params
        )
        return self._get_dataframe_from_response(response)

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {"MAPPING": "/metadata/mapping", "ITEM": "/metadata/item"}

    @property
    def _default_base_url(self) -> str:
        return "https://data.particle.one/edgar/v1/"


class GvkCikMapper(p1_abs.AbstractClient):
    """Handler for GVK <-> Cik transformation."""

    def get_gvk_from_cik(
        self, cik: P1_CIK, as_of_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Get GVK by the cik and date.

        :param cik: Company Identification Key as integer.
        :param as_of_date: Date of gvk. Date format is "YYYY-MM-DD".
        Not implemented for now.
        """

        params = {"cik": cik, "as_of_date": as_of_date}
        url = f'{self.base_url}{self._api_routes["GVK"]}'
        response = self._make_request(
            "GET", url, headers=self.headers, params=params
        )
        return self._get_dataframe_from_response(response)

    def get_cik_from_gvk(
        self, gvk: P1_GVK, as_of_date: Optional[str] = None
    ) -> pd.DataFrame:
        """Get Cik by GVK and date.

        :param gvk: Global Company Key(gvk)
        :param as_of_date: Date of gvk, if missed then
        more than one cik may be to be returned.
        """

        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params, gvk=gvk, gvk_date=as_of_date
        )
        url = f'{self.base_url}{self._api_routes["CIK"]}'
        response = self._make_request(
            "GET", url, headers=self.headers, params=params
        )
        return self._get_dataframe_from_response(response)

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "GVK": "/metadata/gvk",
            "CIK": "/metadata/cik",
        }

    @property
    def _default_base_url(self) -> str:
        return "https://data.particle.one/edgar/v1/"


class EdgarClient(p1_abs.AbstractClient):
    """Class for p1 Edgar data REST API operating."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cik_gvk_mapping = None

    def get_payload(
        self,
        form_name: str,
        cik: Union[int, str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        item: Optional[str] = None,
    ) -> pd.DataFrame:
        """Get payload data for a form, and a company.

        :param form_name: Form name.
        :param cik: Company Identification Key as integer.
        :param start_date: Get a data where filing date is
        greater or equal start_date. Date format is "YYYY-MM-DD".
        :param end_date: Get a data where filing date is
        less or equal end_date. Date format is "YYYY-MM-DD".
        :param item: Item for searching.
        :return: Pandas dataframe with payload data.
        """

        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params, start_date=start_date, end_date=end_date, item=item, cik=cik
        )
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}' f"/{form_name}"
        payload_dataframe = pd.DataFrame()
        for df in self._payload_generator(
            "GET", url, headers=self.headers, params=params
        ):
            payload_dataframe = payload_dataframe.append(df, ignore_index=True)
        return payload_dataframe.reset_index(drop=True)

    def get_cik(
        self,
        gvk: Optional[Union[str, int]] = None,
        gvk_date: Optional[str] = None,
        ticker: Optional[str] = None,
        cusip: Optional[str] = None,
        company: Optional[str] = None,
    ) -> pd.DataFrame:
        """Obtain Company Identification Key (cik) by given parameters.

        :param gvk: Global Company Key(gvk)
        :param gvk_date: Date of gvk, if missed then
        more than one cik may be to be returned.
        :param ticker: Company ticker.
        :param cusip: Committee on Uniform Securities
        Identification Procedures number.
        :param company: Company name.
        :return: Pandas dataframe with cik information.
        """
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params,
            gvk=gvk,
            gvk_date=gvk_date,
            ticker=ticker,
            cusip=cusip,
            company=company,
        )
        url = f'{self.base_url}{self._api_routes["CIK"]}'
        response = self._make_request(
            "GET", url, headers=self.headers, params=params
        )
        return self._get_dataframe_from_response(response)

    @property
    def _default_base_url(self) -> str:
        return "https://data.particle.one/edgar/v1/"

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "PAYLOAD": "/data",
            "CIK": "/metadata/cik",
            "ITEM": "/metadata/item",
        }

    def _payload_generator(self, *args, **kwargs) -> pd.DataFrame:
        """Payload generator that output payload data by DataAPI pagination.

        :param args: Positional arguments for making request.
        :param kwargs: Key arguments for making request.
        :return: Pandas dataframe with a current chunk of data.
        """
        current_offset = 0
        count_lines = sys.maxsize
        while current_offset < count_lines:
            kwargs["params"]["offset"] = current_offset
            response = self._make_request(*args, **kwargs)
            try:
                payload_dataframe = pd.DataFrame(response.json()["data"])
                payload_dataframe = payload_dataframe.astype(
                    dtype={"internal_timestamp": "datetime64"})
            except (KeyError, json.JSONDecodeError) as e:
                raise p1_exc.ParseResponseException(
                    "Can't transform server response to a pandas Dataframe"
                ) from e
            count_lines = response.json()["count"]
            yield payload_dataframe
            current_offset += PAYLOAD_BLOCK_SIZE
