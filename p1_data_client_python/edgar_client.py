"""
P1 Edgar Data REST API wrapper

Copyright 2020 by Particle.One Inc.
All rights reserved.
This file is part of the ParticleOne and is released under the "MIT". Please
see the license.txt file that should have been included as part of this
package.

Import as
import p1_data_client_python.client as p1_edg
"""

from typing import List, Dict, Optional, Union
import json
import sys
import pandas as pd

import p1_data_client_python.exceptions as p1_exc
import p1_data_client_python.abstract_client as p1_abs

PAYLOAD_BLOCK_SIZE = 100
P1_CIK = Union[str, int]
P1_GVKEY = Union[str, int]


class CompustatItemMapper(p1_abs.AbstractClient):
    """
    Handler for Compustat item mapping.
    """

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "MAPPING": "/metadata/mapping",
            "ITEM": "/metadata/item"
        }

    @property
    def _default_base_url(self) -> str:
        return "https://data.particle.one/edgar/v1/"

    def get_mapping(self):
        """
        Get all mapping for items.
        """

        params = {'mapping_type': 'compustat_items'}
        url = f'{self.base_url}{self._api_routes["MAPPING"]}'
        response = self._make_request(
            "GET",
            url,
            headers=self.headers,
            params=params
        )
        return self._get_dataframe_from_response(response)

    def get_item_from_keywords(self, keywords) -> pd.DataFrame:
        """
        Obtain item by keywords.

        :param keywords: List of keywords.
        :return: Item code.
        """

        params = {}
        params = self._set_optional_params(params,
                                           keywords=keywords)
        url = f'{self.base_url}{self._api_routes["ITEM"]}'
        response = self._make_request(
            "GET",
            url,
            headers=self.headers,
            params=params
        )
        return self._get_dataframe_from_response(response)


class GvkeyCikMapper(p1_abs.AbstractClient):
    """
    Handler for GVKey <-> Cik transformation
    """
    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "GVKEY": "/metadata/gvkey",
            "CIK": "/metadata/cik",
        }

    @property
    def _default_base_url(self) -> str:
        return "https://data.particle.one/edgar/v1/"

    def get_gvkey_from_cik(self,
                           cik: P1_CIK,
                           as_of_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get GVkey by the cik and date.

        :param cik: Company Identification Key as integer.
        :param as_of_date: Date of gvkey. Date format is "YYYY-MM-DD".
        Not implemented for now.
        """

        # TODO(Greg,Vlad): Implement as_of_date.
        params = {'cik': cik}
        url = f'{self.base_url}{self._api_routes["GVKEY"]}'
        response = self._make_request(
            "GET",
            url,
            headers=self.headers,
            params=params
        )
        return self._get_dataframe_from_response(response)

    def get_cik_from_gvkey(self,
                           gvkey: P1_GVKEY,
                           as_of_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get Cik by GVKey and date.

        :param gvkey: Global Company Key(gvkey)
        :param as_of_date: Date of gvkey, if missed then
        more than one cik may be to be returned.
        """

        params = {}
        params = self._set_optional_params(params,
                                           gvkey=gvkey,
                                           gvkey_date=as_of_date)
        url = f'{self.base_url}{self._api_routes["CIK"]}'
        response = self._make_request(
            "GET",
            url,
            headers=self.headers,
            params=params
        )
        return self._get_dataframe_from_response(response)


class EdgarClient(p1_abs.AbstractClient):
    """
    Class for p1 Edgar data REST API operating.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cik_gvkey_mapping = None

    @property
    def _default_base_url(self) -> str:
        return "https://data.particle.one/edgar/v1/"

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "PAYLOAD": "/data",
            "CIK": "/metadata/cik",
            "ITEM": "/metadata/item"
        }

    def _payload_generator(self, *args, **kwargs):
        current_offset = 0
        count_lines = sys.maxsize
        while current_offset < count_lines:
            kwargs['params']['offset'] = current_offset
            response = self._make_request(*args, **kwargs)
            try:
                payload_dataframe = pd.DataFrame(response.json()['data'])
            except (KeyError, json.JSONDecodeError) as e:
                raise p1_exc.ParseResponseException(
                    "Can't transform server response to a pandas Dataframe"
                ) from e
            count_lines = response.json()['count']
            yield payload_dataframe
            current_offset += PAYLOAD_BLOCK_SIZE

    def get_payload(self,
                    form_name: str,
                    cik: Union[int, str],
                    start_date: str = None,
                    end_date: str = None,
                    item: str = None,
                    ) -> pd.DataFrame:
        """
        Get payload data for a form, and a company

        :param form_name: Form name.
        :param cik: Company Identification Key as integer.
        :param start_date: Get a data where filing date is
        greater or equal start_date. Date format is "YYYY-MM-DD".
        :param end_date: Get a data where filing date is
        less or equal end_date. Date format is "YYYY-MM-DD".
        :param item: Item for searching.
        :return: Pandas dataframe with payload data.
        """

        params = {}
        params = self._set_optional_params(params,
                                           start_date=start_date,
                                           end_date=end_date,
                                           item=item)
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}' \
              f'/{form_name}/{cik}'
        payload_dataframe = pd.DataFrame()
        for df in self._payload_generator("GET",
                                          url,
                                          headers=self.headers,
                                          params=params):
            payload_dataframe = payload_dataframe.append(df, ignore_index=True)
        return payload_dataframe

    def get_cik(self, gvkey: Optional[Union[str, int]] = None,
                gvkey_date: Optional[str] = None,
                ticker: Optional[str] = None,
                cusip: Optional[str] = None,
                company: Optional[str] = None,
                ) -> pd.DataFrame:
        """
        Obtain Company Identification Key (cik) by given parameters.

        :param gvkey: Global Company Key(gvkey)
        :param gvkey_date: Date of gvkey, if missed then
        more than one cik may be to be returned.
        :param ticker: Company ticker.
        :param cusip: Committee on Uniform Securities
        Identification Procedures number.
        :param company: Company name.
        :return: Pandas dataframe with cik information.
        """
        params = {}
        params = self._set_optional_params(params,
                                           gvkey=gvkey,
                                           gvkey_date=gvkey_date,
                                           ticker=ticker,
                                           cusip=cusip,
                                           company=company)
        url = f'{self.base_url}{self._api_routes["CIK"]}'
        response = self._make_request(
            "GET",
            url,
            headers=self.headers,
            params=params
        )
        return self._get_dataframe_from_response(response)

    def get_item(self, keywords: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Obtain item by given parameters.

        :param keywords: List of keywords.
        :return: Item code.
        """

        params = {}
        params = self._set_optional_params(params,
                                           keywords=keywords)
        url = f'{self.base_url}{self._api_routes["ITEM"]}'
        response = self._make_request(
            "GET",
            url,
            headers=self.headers,
            params=params
        )
        return self._get_dataframe_from_response(response)

