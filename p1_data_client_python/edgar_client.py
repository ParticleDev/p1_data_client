"""
P1 Edgar Data REST API wrapper.

Copyright 2020 by Particle.One Inc.
All rights reserved.
This file is part of the ParticleOne and is released under the "MIT". Please
see the license.txt file that should have been included as part of this
package.

Import as:
import p1_data_client_python.client as p1_edg
"""

import json
import os
import sys
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import tqdm

import p1_data_client_python.abstract_client as p1_abs
import p1_data_client_python.exceptions as p1_exc
import p1_data_client_python.helpers.dbg as dbg

FORM8_FIELD_TYPES = {
    "form_publication_timestamp": "datetime64[ns, UTC]",
    "filing_date": "datetime64[ns, UTC]",
    "compustat_timestamp": "datetime64[ns, UTC]",
    "period_of_report": "datetime64[ns, UTC]",
    "creation_timestamp": "datetime64[ns, UTC]",
    "gvk": "int64",
    "item_value": "float64",
}
P1_EDGAR_DATA_API_VERSION = os.environ.get("P1_EDGAR_DATA_API_VERSION", "3")
PAYLOAD_BLOCK_SIZE = 100
P1_CIK = int
P1_GVK = int


class ItemMapper(p1_abs.AbstractClient):
    """Handler for an item mapping."""

    def get_mapping(self) -> pd.DataFrame:
        """
        Get all mapping for items.

        :return: Item mapping as dataframe.
        """
        params = {"mapping_type": "items"}
        url = f'{self.base_url}{self._api_routes["MAPPING"]}'
        response = self._make_request(
            "GET", url, headers=self.headers, params=params
        )
        return self._get_dataframe_from_response(response)

    def get_item_from_keywords(self, keywords: str) -> pd.DataFrame:
        """
        Obtain an item by keywords.

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
        return f"https://data.particle.one/edgar/v{P1_EDGAR_DATA_API_VERSION}/"


class GvkCikMapper(p1_abs.AbstractClient):
    """Handler for GVK <-> Cik transformation."""

    def get_gvk_from_cik(
        self, cik: P1_CIK, as_of_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get GVK by the cik and date.

        :param cik: Central Index Key as integer.
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
        """
        Get Cik by GVK and date.

        :param gvk: Global Company Key(gvk)
        :param as_of_date: Date of gvk, if missed then
        more than one cik may be to be returned.
        """
        params: Dict[str, Any] = {}
        params = self._set_optional_params(params, gvk=gvk, gvk_date=as_of_date)
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
        return f"https://data.particle.one/edgar/v{P1_EDGAR_DATA_API_VERSION}/"


class EdgarClient(p1_abs.AbstractClient):
    """Class for p1 Edgar data REST API operating."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.cik_gvk_mapping = None

    def get_form4_payload(
        self,
        cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        form_type = "form4"
        result = self._get_form4_13_payload(
            form_type,
            cik=cik,
            start_date=start_date,
            end_date=end_date,
        )
        return result

    def get_form8_payload(
        self,
        cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        item: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get payload data for a form 8 and a company.

        :param cik: Central Index Key as integer. It could be a list of P1_CIK
            or just one identifier. None means all CIKs.
        :param start_date: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DD". None means the entire available date range.
        :param end_date: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DD". None means the entire available date range.
        :param item: Item to retrieve. None means all items.
        :return: Pandas dataframe with payload data.
        """
        form_name = "form8k"
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params, start_date=start_date, end_date=end_date, item=item, cik=cik
        )
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}' f"/{form_name}"
        payload_dataframe = pd.DataFrame()
        for df in self._payload_form8_generator(
            "GET", url, headers=self.headers, params=params
        ):
            payload_dataframe = payload_dataframe.append(df, ignore_index=True)
        if (
            not payload_dataframe.empty
            and {
                "filing_date",
                "cik",
                "item_name",
            }.issubset(payload_dataframe.columns)
        ):
            payload_dataframe = payload_dataframe.sort_values(
                ["filing_date", "cik", "item_name"]
            )
        return payload_dataframe.reset_index(drop=True)

    def get_form10_payload(
        self,
        cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get payload data for a form10, and a company.

        :param cik: Central Index Key as integer. Could be list of P1_CIK or
            just one identifier.
        :param start_date: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DD". None means the entire available date range.
        :param end_date: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DD". None means the entire available date range.
        :return: Pandas dataframe with payload data.
        """
        form_name = "form10"
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params, start_date=start_date, end_date=end_date, cik=cik
        )
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}/{form_name}'
        response = self._make_request(
            "GET", url, headers=self.headers, params=params
        )
        return response.json()["data"]

    def get_form13_payload(
        self,
        cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
        cusip: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        form_type = "form13"
        result = self._get_form4_13_payload(
            form_type,
            cik=cik,
            cusip=cusip,
            start_date=start_date,
            end_date=end_date,
        )
        return result

    def get_cik(
        self,
        gvk: Optional[P1_GVK] = None,
        gvk_date: Optional[str] = None,
        ticker: Optional[str] = None,
        cusip: Optional[str] = None,
        company: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Obtain Central Index Key (cik) by given parameters.

        :param gvk: Global Company Key(gvk)
        :param gvk_date: Date of gvk, if missed then more than one cik may be
            to be returned.
        :param ticker: Company ticker.
        :param cusip: Committee on Uniform Securities Identification Procedures
            number.
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
        return f"https://data.particle.one/edgar/v{P1_EDGAR_DATA_API_VERSION}/"

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "PAYLOAD": "/data",
            "CIK": "/metadata/cik",
            "ITEM": "/metadata/item",
        }

    def _get_form4_13_payload(
        self,
        form_type: str,
        cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
        cusip: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get payload data for forms 4 or 13 and a company.

        :param form_type: Form type. Allowed range of values: form4, form13.
        :param cik: Central Index Key as integer. Could be list of P1_CIK or
            just one identifier.
        :param cusip: Committee on Uniform Securities Identification Procedures
            number. Could be list or just one identifier.
        :param start_date: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DD". None means the entire available date range.
        :param end_date: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DD". None means the entire available date range.
        :return: Dict with a data tables.
        """
        dbg.dassert(not (cik is not None and cusip is not None),
                    msg="You cannot pass CIK and CUSIP parameters "
                        "in the same time.")
        dbg.dassert(form_type in ("form13", "form4"),
                    msg="The form_type parameter should be form13 or form4.")
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params, start_date=start_date,
            end_date=end_date, cik=cik, cusip=cusip
        )
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}/{form_type}'
        compound_data: Dict[str, Any] = {}
        for data in self._payload_form4_13_generator(
            "GET", url, headers=self.headers, params=params
        ):
            for key in data:
                if key in compound_data:
                    compound_data[key] += data[key]
                else:
                    compound_data[key] = data[key]
        return compound_data

    def _payload_form4_13_generator(
        self, *args: Any, **kwargs: Any
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Payload generator that output form4/13 payload data
        by DataAPI pagination.

        :param args: Positional arguments for making request.
        :param kwargs: Key arguments for making request.
        :return: 6 dicts of different types of data.
        """
        params = kwargs["params"]
        # Build a list to iterate over. Can be CIK or CUSIP (or neither).
        if "cik" in params:
            iter_name = "CIK"
            iter_list = [params["cik"]]
            if isinstance(params["cik"], list):
                iter_list = params["cik"]
        elif "cusip" in params:
            iter_name = "CUSIP"
            iter_list = [params["cusip"]]
            if isinstance(params["cusip"], list):
                iter_list = params["cusip"]
        else:
            iter_name = "CIK"
            iter_list = [None]
        # Iterate over the list.
        for item in tqdm.tqdm(iter_list, desc=f"{iter_name}: ", position=0):
            current_offset = 0
            count_lines = sys.maxsize
            progress_bar = tqdm.tqdm(desc=f'{item or "Main"}: ', position=1)
            while current_offset < count_lines:
                params["offset"] = current_offset
                # Inject the current parameter from the list.
                if iter_name == "CIK":
                    self._set_optional_params(params, cik=item)
                elif iter_name == "CUSIP":
                    self._set_optional_params(params, cusip=item)
                response = self._make_request(*args, **kwargs)
                data = response.json()["data"]
                count_lines = response.json()["count"]
                yield data
                current_offset += PAYLOAD_BLOCK_SIZE
                # Row number clarification.
                if current_offset > 0 and progress_bar.n == 0:
                    progress_bar.reset(total=count_lines)
                progress_bar.update(
                    PAYLOAD_BLOCK_SIZE
                    if current_offset < count_lines
                    else count_lines - progress_bar.n
                )
            progress_bar.close()

    @classmethod
    def _cast_field_types(
        cls, df: pd.DataFrame, field_types: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Cast fields to the certain types.

        :param df: DataFrame for converting.
        :param field_types: Dict with fields and their types.
        :return: Converted DataFrame.
        """
        field_types = {
            key: field_types[key] for key in field_types if key in df.columns
        }
        for field_name in [
            field_name
            for field_name in field_types
            if field_types[field_name] == "float64"
        ]:
            df[field_name] = df[field_name].apply(
                lambda x: None if x == "" else x
            )
        return df.astype(field_types)

    def _payload_form8_generator(self, *args: Any,
                                 **kwargs: Any) -> pd.DataFrame:
        """
        Payload generator that output payload data by DataAPI pagination.

        :param args: Positional arguments for making request.
        :param kwargs: Key arguments for making request.
        :return: Pandas dataframe with a current chunk of data.
        """
        params = kwargs["params"]
        cik_list = [None]
        if "cik" in params:
            cik_list = (
                [params["cik"]]
                if isinstance(params["cik"], int)
                else params["cik"]
            )
        for cik in tqdm.tqdm(cik_list, desc="Cik: ", position=0):
            current_offset = 0
            count_lines = sys.maxsize
            progress_bar = tqdm.tqdm(desc=f'{cik or "Main"}: ', position=1)
            while current_offset < count_lines:
                params["offset"] = current_offset
                self._set_optional_params(params, cik=cik)
                response = self._make_request(*args, **kwargs)
                try:
                    payload_dataframe = pd.DataFrame(response.json()["data"])
                    payload_dataframe = self._cast_field_types(
                        payload_dataframe, FORM8_FIELD_TYPES
                    )
                except (KeyError, json.JSONDecodeError) as e:
                    raise p1_exc.ParseResponseException(
                        "Can't transform server response to a pandas Dataframe"
                    ) from e
                count_lines = response.json()["count"]
                yield payload_dataframe
                current_offset += PAYLOAD_BLOCK_SIZE
                # Row number clarification.
                if current_offset > 0 and progress_bar.n == 0:
                    progress_bar.reset(total=count_lines)
                progress_bar.update(
                    PAYLOAD_BLOCK_SIZE
                    if current_offset < count_lines
                    else count_lines - progress_bar.n
                )
            progress_bar.close()
