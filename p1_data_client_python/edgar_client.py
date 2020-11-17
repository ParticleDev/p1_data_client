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
import pandas as pd

import p1_data_client_python.exceptions as p1_exc
import p1_data_client_python.abstract_client as p1_abs


class EdgarClient(p1_abs.AbstractClient):
    """
    Class for p1 Edgar data REST API operating.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # TODO:(Vlad) Drop it when Greg will rework headers.
        self.headers = {
            "accept": "application/json",
            "api_key": "1234567890"
        }
        self.cik_gvkey_mapping = None

    @property
    def _default_base_url(self) -> str:
        return "http://etl.p1:5001"

    @property
    def _api_routes(self) -> Dict[str, str]:
        return {
            "PAYLOAD": "/data",
            "CIK": "/metadata/cik",
            "ITEM": "/metadata/item"
        }

    def get_payload(self,
                    form_name: str,
                    cik: Union[int, str],
                    start_date: str = None,
                    end_date: str = None,
                    items: List[str] = None
                    ) -> pd.DataFrame:
        """
        Get payload data for a form, and a company

        :form_name: Form name.
        :cik: Company Identification Key as integer.
        :start_date: Get a data where filing date greater or equal start_date
        :end_date: Get a data where filing date greater or equal end_date
        :items: List of items for searching.
        """

        params = {}
        params = self._set_optional_params(params,
                                           start_date=start_date,
                                           end_date=end_date,
                                           items=items)
        url = f'{self.base_url}{self._api_routes["PAYLOAD"]}' \
              f'/{form_name}/{cik}'
        response = self._make_request(
            "GET",
            url,
            headers=self.headers,
            params=params
        )
        if response.status_code != 200:
            raise p1_exc.ParseResponseException(
                f"Got next response, from the server: {response.text}"
            )
        try:
            payload_dataframe = pd.DataFrame(response.json()['data'])
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        return payload_dataframe

    def get_cik(self, gvkey: Optional[Union[str, int]] = None,
                gvkey_date: Optional[str] = None,
                ticker: Optional[str] = None,
                cusip: Optional[str] = None,
                company: Optional[str] = None
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
        :return: One or list of cik.
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
        if response.status_code != 200:
            raise p1_exc.ParseResponseException(
                f"Got next response, from the server: {response.text}"
            )
        try:
            cik_dataframe = pd.DataFrame(response.json()['data'])
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        return cik_dataframe

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
        if response.status_code != 200:
            raise p1_exc.ParseResponseException(
                f"Got next response, from the server: {response.text}"
            )
        try:
            item_dataframe = pd.DataFrame(response.json()['data'])
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        return item_dataframe
