"""
P1 Edgar Data REST API wrapper.

Copyright 2020 by Particle.One Inc.
All rights reserved.
This file is part of the ParticleOne and is released under the "MIT". Please
see the license.txt file that should have been included as part of this
package.

Import as:
import p1_data_client_python.edgar.edgar_client as peedga
"""

import itertools
import json
import logging
from typing import Any, Dict, Iterator, List, Optional, Union

import halo  # type: ignore
import pandas as pd
import tqdm.auto as tauto

import p1_data_client_python.abstract_client as pabstr
import p1_data_client_python.edgar.config as peconf
import p1_data_client_python.edgar.utils as peutil
import p1_data_client_python.exceptions as pexcep
import p1_data_client_python.helpers.dbg as phdbg  # type: ignore

_LOG = logging.getLogger(__name__)
phdbg.init_logger(logging.INFO, force_print_format=True)


class EdgarClient(pabstr.AbstractClient):
    """
    Class for p1 Edgar data REST API operating.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        """
        Edgar client init.
        """
        super().__init__(*args, **kwargs)
        self.cik_gvk_mapping = None
        self.is_jupyter = phdbg.is_running_in_ipynb()
        self.pb_position = 0
        if self.is_jupyter:
            self.spinner = halo.HaloNotebook(
                text="Waiting response size...", spinner="dots"
            )
        else:
            self.spinner = halo.Halo(
                text="Waiting response size...", spinner="dots"
            )

    def get_form_headers(
        self,
        form_type: Optional[Union[str, List[str]]] = None,
        cik: Optional[peconf.CIK_TYPE] = None,
        start_datetime: Optional[str] = None,
        end_datetime: Optional[str] = None,
        date_mode: Optional[str] = None,
        output_type: Optional[str] = "dataframes",
    ) -> Union[List[peconf.SERVER_RESPONSE_TYPE], pd.DataFrame]:
        """
        Get form headers metadata with the following parameters.

        :param form_type: Form type or list of form types.
            Example: form_type=['13F-HR', '4']
        :param cik: Central Index Key as integer. It could be a list of P1_CIK
            or just one identifier. None means all CIKs.
        :param start_datetime: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DDTHH-MI-SS". None means the entire available date range.
        :param end_datetime: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DDTHH-MI-SS". None means the entire available date range.
        :param date_mode: Define whether dates are
            interpreted as publication dates or knowledge dates
        :param output_type: Output format: 'dict' or 'dataframes'.
        """
        peutil.check_date_mode(start_datetime, end_datetime, date_mode)
        peutil.check_form_type(form_type, self.form_types)
        cik = peutil.check_sorted_unique_param("cik", cik)
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params,
            form_type=form_type,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            date_mode=date_mode,
            cik=cik,
        )
        url = f'{self.base_url}{self._api_routes["HEADERS"]}'
        result: Union[List[peconf.SERVER_RESPONSE_TYPE], pd.DataFrame] = []
        for data in self._payload_form_cik_cusip_generator(
            method="GET", url=url, headers=self.headers, params=params
        ):
            result += data
        if output_type == "dataframes":
            try:
                result = pd.DataFrame(result)
            except (KeyError, json.JSONDecodeError) as e:
                raise pexcep.ParseResponseException(
                    "Can't transform server response to a Pandas Dataframe"
                ) from e
        else:
            phdbg.dfatal(f"Output type {output_type} is not valid.")
        return result

    def get_form4_payload(
        self,
        cik: Optional[peconf.CIK_TYPE] = None,
        start_datetime: Optional[str] = None,
        end_datetime: Optional[str] = None,
        date_mode: Optional[str] = None,
        output_type: str = "dataframes",
    ) -> Dict[str, List[peconf.SERVER_RESPONSE_TYPE]]:
        peutil.check_date_mode(start_datetime, end_datetime, date_mode)
        cik = peutil.check_sorted_unique_param("cik", cik)
        form_type = "form4"
        result = self._get_form4_13_payload(
            form_type,
            cik=cik,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            date_mode=date_mode,
            output_type=output_type,
        )
        return result

    def get_form8_payload(
        self,
        cik: Optional[peconf.CIK_TYPE] = None,
        start_datetime: Optional[str] = None,
        end_datetime: Optional[str] = None,
        date_mode: Optional[str] = None,
        item: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Get payload data for a form 8 and a company.

        :param cik: Central Index Key as integer. It could be a list of P1_CIK
            or just one identifier. None means all CIKs.
        :param start_datetime: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DDTHH-MI-SS". None means the entire available date range.
        :param end_datetime: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DDTHH-MI-SS". None means the entire available date range.
        :param date_mode: Define whether dates are
            interpreted as publication dates or knowledge dates
        :param item: Item to retrieve. None means all items.
        :return: Pandas dataframe with payload data.
        """
        peutil.check_date_mode(start_datetime, end_datetime, date_mode)
        cik = peutil.check_sorted_unique_param("cik", cik)
        form_name = "form8"
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            item=item,
            cik=cik,
            date_mode=date_mode,
        )
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}' f"/{form_name}"
        payload_dataframe = pd.DataFrame()
        for df in self._payload_form_cik_cusip_generator(
            method="GET", url=url, headers=self.headers, params=params
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
        cik: Optional[peconf.CIK_TYPE] = None,
        start_datetime: Optional[str] = None,
        end_datetime: Optional[str] = None,
        date_mode: Optional[str] = None,
    ) -> List[peconf.SERVER_RESPONSE_TYPE]:
        """
        Get payload data for a form10, and a company.

        :param cik: Central Index Key as integer. Could be list of P1_CIK or
            just one identifier.
        :param start_datetime: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DDTHH-MI-SS". None means the entire available date range.
        :param end_datetime: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DDTHH-MI-SS". None means the entire available date range.
        :param date_mode: Define whether dates are
            interpreted as publication dates or knowledge dates
        :return: List with payload data.
        """
        peutil.check_date_mode(start_datetime, end_datetime, date_mode)
        cik = peutil.check_sorted_unique_param("cik", cik)
        form_name = "form10"
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params, start_datetime=start_datetime, end_datetime=end_datetime,
            date_mode=date_mode
        )
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}/{form_name}'
        cik_list: List[Union[int, None]] = [None]
        compound_data = []
        if cik is not None:
            cik_list = [cik] if isinstance(cik, int) else cik
        self.spinner.start()
        with peutil.spinner_exception_handling(self.spinner):
            for current_cik in tauto.tqdm(cik_list, desc="Processing CIK: "):
                self._set_optional_params(params, cik=current_cik)
                response = self._make_request(
                    "GET", url, headers=self.headers, params=params
                )
                self.spinner.stop()
                data = response.json()["data"]
                _LOG.info("%s: %s forms loaded",
                          current_cik or "Total",
                          len(data))
                compound_data += response.json()["data"]
        return compound_data

    def get_form10_uuid_payload(
        self,
        uuid: str = None,
    ) -> peconf.SERVER_RESPONSE_TYPE:
        """
        Get payload data for a form10, using uuid.

        :param uuid: Unique form id.
        :return: Form payload data.
        """
        form_name = "form10"
        params: Dict[str, Any] = {"uuid": uuid}
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}/{form_name}/uuid'
        self.spinner.start()
        with peutil.spinner_exception_handling(self.spinner):
            response = self._make_request(
                "GET", url, headers=self.headers, params=params
            )
            self.spinner.stop()
            data = response.json()["data"]
            _LOG.info("Payload for '%s' uuid loaded", uuid)
        return data


    def get_form13_payload(
        self,
        cik: Optional[peconf.CIK_TYPE] = None,
        cusip: Optional[peconf.CUSIP_TYPE] = None,
        start_datetime: Optional[str] = None,
        end_datetime: Optional[str] = None,
        date_mode: Optional[str] = None,
        output_type: str = "dataframes",
    ) -> Dict[str, List[peconf.SERVER_RESPONSE_TYPE]]:
        peutil.check_date_mode(start_datetime, end_datetime, date_mode)
        cik = peutil.check_sorted_unique_param("cik", cik)
        cusip = peutil.check_sorted_unique_param("cusip", cusip)
        form_type = "form13"
        result = self._get_form4_13_payload(
            form_type,
            cik=cik,
            cusip=cusip,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            date_mode=date_mode,
            output_type=output_type,
        )
        return result

    def get_cik(
        self,
        gvk: Optional[peconf.P1_GVK] = None,
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
        form_types_list = [
            form_types for _, form_types in peconf.FORM_NAMES_TYPES.items()
        ]
        return list(itertools.chain.from_iterable(form_types_list))

    @property
    def _default_base_url(self) -> str:
        return f"https://data.particle.one/edgar/v{peconf.P1_EDGAR_DATA_API_VERSION}/"

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "PAYLOAD": "/data",
            "CIK": "/metadata/cik",
            "ITEM": "/metadata/item",
            "HEADERS": "/data/headers",
        }

    @classmethod
    def _process_form_4_13_10_output(
        cls,
        output: Dict[str, List[peconf.SERVER_RESPONSE_TYPE]],
        output_type: str = "dataframes",
    ) -> Union[Dict[str, list], Dict[str, pd.DataFrame]]:
        """
        Convert form4 or form13 output from dict to dict of Pandas Dataframes.

        :param output: Output dict for transformation.
        :param output_type: Output format: 'dict' or 'dataframes'.
        :return: The transformed dict of dataframes.
        """
        if output_type == "dataframes":
            try:
                output = {
                    table_name: pd.DataFrame(forms)
                    for table_name, forms in output.items()
                }
            except (KeyError, json.JSONDecodeError) as e:
                raise pexcep.ParseResponseException(
                    "Can't transform server response to a Pandas Dataframe"
                ) from e
        else:
            phdbg.dfatal(f"Output type {output_type} is not valid.")
        return output

    def _get_form4_13_payload(
        self,
        form_type: str,
        cik: Optional[peconf.CIK_TYPE] = None,
        cusip: Optional[peconf.CUSIP_TYPE] = None,
        start_datetime: Optional[str] = None,
        end_datetime: Optional[str] = None,
        date_mode: Optional[str] = None,
        output_type: str = "dataframes",
    ) -> Dict[str, List[peconf.SERVER_RESPONSE_TYPE]]:
        """
        Get payload data for forms 4 or 13 and a company.

        :param form_type: Form type. Allowed range of values: form4, form13.
        :param cik: Central Index Key as integer. Could be list of P1_CIK or
            just one identifier.
        :param cusip: Committee on Uniform Securities Identification Procedures
            number. Could be list or just one identifier.
        :param start_datetime: Get data where filing date is >= start_date. Date
            format is "YYYY-MM-DDTHH-MI-SS". None means the entire available date range.
        :param end_datetime: Get data where filing date is <= end_date. Date format
            is "YYYY-MM-DDTHH-MI-SS". None means the entire available date range.
        :param date_mode: Define whether dates are
            interpreted as publication dates or knowledge dates
        :param output_type: Output format: 'dict' or 'dataframes'.
        :return: Dict with a data tables.
        """
        phdbg.dassert(
            not (cik is not None and cusip is not None),
            msg="You cannot pass CIK and CUSIP parameters " "at the same time.",
        )
        phdbg.dassert(
            form_type in ("form13", "form4"),
            msg="The form_type parameter should be form13 or form4.",
        )
        phdbg.dassert(
            output_type in ("dict", "dataframes"),
            msg="The output_type parameter should be a dict " "or dataframes.",
        )
        params: Dict[str, Any] = {}
        params = self._set_optional_params(
            params,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            cik=cik,
            cusip=cusip,
            date_mode=date_mode,
        )
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}/{form_type}'
        compound_data: peconf.SERVER_RESPONSE_TYPE = {}
        for data in self._payload_form_cik_cusip_generator(
            method="GET", url=url, headers=self.headers, params=params
        ):
            for key in data:
                if key in compound_data:
                    compound_data[key] += data[key]
                else:
                    compound_data[key] = data[key]
        return self._process_form_4_13_10_output(
            compound_data, output_type=output_type
        )

    def _payload_page_generator(self, **kwargs) -> Iterator[dict]:
        """
        Iterate over the pages with links.
        """
        has_next_link = True
        self.pb_position += 1
        progress_bar = tauto.tqdm(
            desc="Pages: ", position=self.pb_position, leave=False
        )
        while has_next_link:
            response = self._make_request(**kwargs)
            # Parse links.
            links = peutil.Links(response.json()["links"])
            has_next_link = links.has_next_link
            if progress_bar.total is None:
                progress_bar.reset(total=response.json()["count"])
            # Return the data.
            yield response.json()["data"]
            # Update the progress bar.
            if not has_next_link:
                progress_bar.n = progress_bar.total
            else:
                progress_bar.n = links.current_offset
            progress_bar.display()
            # Replace an url for a next page.
            kwargs.pop("params", None)
            kwargs["url"] = links.next_url
        progress_bar.close()
        self.pb_position -= 1

    def _payload_form_cik_cusip_generator(self, **kwargs):
        """
        Iterate through the list of cik.
        """
        self.pb_position = 1
        iter_name = ""
        iter_list = [None]
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
        chunks = list(peutil.chop_list(iter_list, peconf.ITEM_BLOCK_SIZE))
        with peutil.spinner_exception_handling(self.spinner):
            for item in tauto.tqdm(
                chunks,
                desc=f"Processing {iter_name}: ",
                position=self.pb_position,
            ):
                if iter_name == "CIK":
                    self._set_optional_params(params, cik=item)
                elif iter_name == "CUSIP":
                    self._set_optional_params(params, cusip=item)
                yield from self._payload_page_generator(**kwargs)

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
        # Replace NA string with pd.NaN
        df.replace("NA", pd.NA, inplace=True)
        for field_name in peconf.FORM8_DATE_FIELDS:
            if field_name in df.columns:
                df[field_name] = pd.to_datetime(df[field_name])
        try:
            df = df.astype(field_types)
        except Exception as e:
            raise pexcep.CastException(
                "Can't convert fields to certain data types."
            ) from e
        return df
