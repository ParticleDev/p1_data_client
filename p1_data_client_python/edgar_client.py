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

import contextlib as clib
import itertools
import time
import logging
import json
import os
import sys
from typing import Any, Dict, List, Optional, Union

import halo
import pandas as pd
import tqdm.auto as tqdm

import p1_data_client_python.abstract_client as p1_abs
import p1_data_client_python.exceptions as p1_exc
import p1_data_client_python.helpers.dbg as dbg

_LOG = logging.getLogger(__name__)
dbg.init_logger(logging.INFO)
CIK_BLOCK_SIZE = 600
FORM8_FIELD_TYPES = {
    "form_publication_timestamp": "datetime64[ns, UTC]",
    "filing_date": "datetime64[ns, UTC]",
    "compustat_timestamp": "datetime64[ns, UTC]",
    "period_of_report": "datetime64[ns, UTC]",
    "creation_timestamp": "datetime64[ns, UTC]",
    "gvk": "int64",
    "item_value": "float64",
}
# Mapping between short form names and form types in the Edgar universe.
FORM_NAMES_TYPES = {
    'form4': ['3', '3/A', '4', '4/A', '5', '5/A'],
    'form8': ['8-K', '8-K/A'],
    'form10': ['10-K', '10-K/A', '10-Q', '10-Q/A'],
    'form13': ['13F-HR', '13F-HR/A'],
}
P1_EDGAR_DATA_API_VERSION = os.environ.get("P1_EDGAR_DATA_API_VERSION", "4")
PAYLOAD_BLOCK_SIZE = {
    'headers': 1000,
    'form4_13': 500,
    'form8': 1000,
}
P1_CIK = int
P1_GVK = int


@clib.contextmanager
def _spinner_exception_handling(spinner: Union[halo.Halo, halo.HaloNotebook]):
    try:
        yield
    finally:
        spinner.stop()


def _check_sorted_unique_param(name: str,
                               value: Union[list, Any]
                               ) -> Union[list, Any]:
    """
    Check an argument for duplicates.

    :param name: Name of parameter.
    :param value: Value of parameter. If list then check for duplicated.
    :return: Processed value.
    """
    if isinstance(value, list):
        sorted_unique_value = sorted(list(set(value)))
        if len(sorted_unique_value) < len(value):
            dbg.dfatal(f"Some values: {value} in the {name} parameter "
                       f"are duplicated.")
    return value


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
        """
        Edgar client init.
        """
        super().__init__(*args, **kwargs)
        self.cik_gvk_mapping = None
        self.is_jupyter = dbg.is_running_in_ipynb()
        if self.is_jupyter:
            self.spinner = halo.HaloNotebook(text='Waiting response size...', spinner='dots')
        else:
            self.spinner = halo.Halo(text='Waiting response size...', spinner='dots')

    def get_form_headers(self,
                         form_type: Union[str, List[str]],
                         cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         output_type: str = 'dataframes',
                         ) -> Union[List[Dict[str, Any]], pd.DataFrame]:
        """
        Get form headers metadata with the following parameters.

        :param form_type: Form type or list of form types. Required.
            Example: form_type=['13F-HR', '4']
        :param cik: Central Index Key as integer. It could be a list of P1_CIK
            or just one identifier. None means all CIKs.
        :param start_date: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DD". None means the entire available date range.
        :param end_date: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DD". None means the entire available date range.
        :param output_type: Output format: 'dict' or 'dataframes'.
        """
        cik = _check_sorted_unique_param('cik', cik)
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params,
            form_type=form_type,
            start_date=start_date,
            end_date=end_date,
            cik=cik
        )
        url = f'{self.base_url}{self._api_routes["HEADERS"]}'
        result = []
        for data in self._payload_form_headers_generator(
            "GET", url, headers=self.headers, params=params
        ):
            result += data
        if output_type == 'dataframes':
            try:
                result = pd.DataFrame(result)
            except (KeyError, json.JSONDecodeError) as e:
                raise p1_exc.ParseResponseException(
                    "Can't transform server response to a Pandas Dataframe"
                ) from e
        else:
            dbg.dfatal(f"Output type {output_type} is not valid.")
        return result

    def get_form4_payload(
        self,
        cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        output_type: str = 'dataframes'
    ) -> Dict[str, List[Dict[str, Any]]]:
        cik = _check_sorted_unique_param('cik', cik)
        form_type = "form4"
        result = self._get_form4_13_payload(
            form_type,
            cik=cik,
            start_date=start_date,
            end_date=end_date,
            output_type=output_type
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
        cik = _check_sorted_unique_param('cik', cik)
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
    ) -> List[Dict[str, Any]]:
        """
        Get payload data for a form10, and a company.

        :param cik: Central Index Key as integer. Could be list of P1_CIK or
            just one identifier.
        :param start_date: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DD". None means the entire available date range.
        :param end_date: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DD". None means the entire available date range.
        :return: List with payload data.
        """
        cik = _check_sorted_unique_param('cik', cik)
        form_name = "form10"
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params, start_date=start_date, end_date=end_date
        )
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}/{form_name}'
        cik_list = [None]
        compound_data = []
        if cik is not None:
            cik_list = ([cik] if isinstance(cik, int) else cik)
        self.spinner.start()
        with _spinner_exception_handling(self.spinner):
            for cik in tqdm.tqdm(cik_list, desc="Processing CIK: "):
                self._set_optional_params(params, cik=cik)
                response = self._make_request(
                    "GET", url, headers=self.headers, params=params
                )
                self.spinner.stop()
                data = response.json()["data"]
                print(f"{cik or 'Summary'} :{len(data)} forms loaded")
                compound_data += response.json()["data"]

        return compound_data

    def get_form13_payload(
        self,
        cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
        cusip: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        output_type: str = 'dataframes'
    ) -> Dict[str, List[Dict[str, Any]]]:
        cik = _check_sorted_unique_param('cik', cik)
        cusip = _check_sorted_unique_param('cusip', cusip)
        form_type = "form13"
        result = self._get_form4_13_payload(
            form_type,
            cik=cik,
            cusip=cusip,
            start_date=start_date,
            end_date=end_date,
            output_type=output_type
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
    def form_types(self) -> List[str]:
        """
        Return form types from the mapping.

        :return: List for form types.
        """
        form_types_list = [form_types for _, form_types
                           in FORM_NAMES_TYPES.items()]
        return list(itertools.chain.from_iterable(form_types_list))

    @property
    def _default_base_url(self) -> str:
        return f"https://data.particle.one/edgar/v{P1_EDGAR_DATA_API_VERSION}/"

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "PAYLOAD": "/data",
            "CIK": "/metadata/cik",
            "ITEM": "/metadata/item",
            "HEADERS": "/data/headers"
        }

    @classmethod
    def _process_form_4_13_10_output(cls,
                                     output: Dict[str, List[Dict[str, Any]]],
                                     output_type: str = 'dataframes',
                                     ) -> Union[Dict[str, list],
                                                Dict[str, pd.DataFrame]]:
        """
        Convert form4 or form13 output from dict to dict of Pandas Dataframes.

        :param output: Output dict for transformation.
        :param output_type: Output format: 'dict' or 'dataframes'.
        :return: The transformed dict of dataframes.
        """
        if output_type == 'dict':
            return output
        elif output_type == 'dataframes':
            try:
                return {table_name: pd.DataFrame(forms)
                        for table_name, forms in output.items()}
            except (KeyError, json.JSONDecodeError) as e:
                raise p1_exc.ParseResponseException(
                    "Can't transform server response to a Pandas Dataframe"
                ) from e
        else:
            dbg.dfatal(f"Output type {output_type} is not valid.")

    def _get_form4_13_payload(
        self,
        form_type: str,
        cik: Optional[Union[P1_CIK, List[P1_CIK]]] = None,
        cusip: Optional[Union[str, List[str]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        output_type: str = 'dataframes'
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
        :param output_type: Output format: 'dict' or 'dataframes'.
        :return: Dict with a data tables.
        """
        dbg.dassert(not (cik is not None and cusip is not None),
                    msg="You cannot pass CIK and CUSIP parameters "
                        "at the same time.")
        dbg.dassert(form_type in ("form13", "form4"),
                    msg="The form_type parameter should be form13 or form4.")
        dbg.dassert(output_type in ("dict", "dataframes"),
                    msg="The output_type parameter should be a dict "
                        "or dataframes.")
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
        return self._process_form_4_13_10_output(compound_data,
                                                 output_type=output_type)

    def _payload_form_headers_generator(
        self, *args: Any, **kwargs: Any
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Payload generator that output form headers
        by DataAPI pagination.

        :param args: Positional arguments for making request.
        :param kwargs: Key arguments for making request.
        :return: 6 dicts of different types of data.
        """
        # Backup CIK
        cik = kwargs['params'].get('cik')
        cik_length = 0
        pb_position = 0
        if isinstance(cik, list):
            cik_length = len(cik)
            if cik_length > CIK_BLOCK_SIZE:
                print(f'Amount of CIK is: {cik_length}', )
        self.spinner.start()
        with _spinner_exception_handling(self.spinner):
            cik_offset = 0
            while (cik_length > cik_offset) \
                    or (cik is None) \
                    or (isinstance(cik, int)):
                current_offset = 0
                count_lines = sys.maxsize
                progress_bar = tqdm.tqdm(desc='Transferring results: ',
                                         position=pb_position)
                time.sleep(2)
                if cik_length > 0:
                    kwargs['params']['cik'] = \
                        cik[cik_offset:cik_offset+CIK_BLOCK_SIZE]
                    if cik_length > CIK_BLOCK_SIZE:
                        progress_bar.set_description_str(
                            desc=f'Downloading CIK from: {cik_offset} '
                                 f'to: {cik_offset + CIK_BLOCK_SIZE}')
                        time.sleep(2)

                while current_offset < count_lines:
                    kwargs['params']["offset"] = current_offset
                    response = self._make_request(*args, **kwargs)
                    self.spinner.stop()
                    data = response.json()["data"]
                    count_lines = response.json()["count"]
                    yield data
                    current_offset += PAYLOAD_BLOCK_SIZE['headers']
                    # Row number clarification.
                    if current_offset > 0 and progress_bar.n == 0:
                        progress_bar.reset(total=count_lines)
                    progress_bar.update(
                        PAYLOAD_BLOCK_SIZE['headers']
                        if current_offset < count_lines
                        else count_lines - progress_bar.n
                    )
                if cik_length == 0:
                    break
                cik_offset += CIK_BLOCK_SIZE
                pb_position += 1
                progress_bar.close()

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
        self.spinner.start()
        with _spinner_exception_handling(self.spinner):
            for item in tqdm.tqdm(iter_list, desc=f"Processing {iter_name}: ",
                                  position=1):
                current_offset = 0
                count_lines = sys.maxsize
                progress_bar = tqdm.tqdm(desc=
                                         f'{item or "Transferring results: "}: ',
                                         position=2)
                while current_offset < count_lines:
                    params["offset"] = current_offset
                    # Inject the current parameter from the list.
                    if iter_name == "CIK":
                        self._set_optional_params(params, cik=item)
                    elif iter_name == "CUSIP":
                        self._set_optional_params(params, cusip=item)
                    response = self._make_request(*args, **kwargs)
                    self.spinner.stop()
                    data = response.json()["data"]
                    count_lines = response.json()["count"]
                    yield data
                    current_offset += PAYLOAD_BLOCK_SIZE['form4_13']
                    # Row number clarification.
                    if current_offset > 0 and progress_bar.n == 0:
                        progress_bar.reset(total=count_lines)
                    progress_bar.update(
                        PAYLOAD_BLOCK_SIZE['form4_13']
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
        for cik in tqdm.tqdm(cik_list, desc="Processing CIKs: ", position=0):
            current_offset = 0
            count_lines = sys.maxsize
            progress_bar = tqdm.tqdm(desc=
                                     f'{cik or "Transferring results"}: ',
                                     position=1)
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
                current_offset += PAYLOAD_BLOCK_SIZE['form8']
                # Row number clarification.
                if current_offset > 0 and progress_bar.n == 0:
                    progress_bar.reset(total=count_lines)
                progress_bar.update(
                    PAYLOAD_BLOCK_SIZE['form8']
                    if current_offset < count_lines
                    else count_lines - progress_bar.n
                )
            progress_bar.close()
