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
            "CIK_GVKEY": "/metadata/cik-gvkey"
        }

    def get_payload(self,
                    form_name: str,
                    cik: int,
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
        if items:
            params['items'] = ','.join(items)
        if start_date and self.validate_date(start_date):
            params['start_date'] = start_date
        if end_date and self.validate_date(end_date):
            params['end_date'] = end_date
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
            payload_dataframe = pd.DataFrame(response.json())
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        return payload_dataframe

    def _fill_cik_gvkey_mapping(self):
        url = f'{self.base_url}{self._api_routes["CIK_GVKEY"]}'
        response = self._make_request(
            "GET",
            url,
            headers=self.headers
        )
        if response.status_code != 200:
            raise p1_exc.ParseResponseException(
                f"Got next response, from the server: {response.text}"
            )
        try:
            cik_gvkey_dataframe = pd.DataFrame(response.json()['data'])
        except (KeyError, json.JSONDecodeError) as e:
            raise p1_exc.ParseResponseException(
                "Can't transform server response to a pandas Dataframe"
            ) from e
        self.cik_gvkey_mapping = cik_gvkey_dataframe

    def get_cik_by_gvkey(self, gvkey: Union[str, int], as_of_date: Optional[str]):
        if not self.cik_gvkey_mapping:
            self._fill_cik_gvkey_mapping()
        gvkey = str(gvkey).zfill(6)
        gvkey_indexed = self.cik_gvkey_mapping.set_index("gvkey")
        # Assert if no mapping is found.
        if gvkey not in gvkey_indexed.index:
            pass
            # if not raise_exception:
            #     return None
            # raise UnmappedException("CIK not found for GVKEY='%s'" % gvkey)
        # Select the mapping df subset applicable to the input GVKEY.
        gvkey_df = gvkey_indexed.loc[gvkey]
        if isinstance(gvkey_df, pd.Series):
            gvkey_df = pd.DataFrame(gvkey_df).transpose()
        #
        if as_of_date is None:
            # Return all the possible mappings.
            return list(gvkey_df["cik"].unique())
        cik = ""
        as_of_date = pd.Timestamp(as_of_date)
        # Iterate over the potential mapping candidates.
        for _, row in gvkey_df.iterrows():
            if as_of_date >= pd.Timestamp(row["effdate"]) and (
                row["thrudate"].startswith("9999")
                or as_of_date <= pd.Timestamp(row["thrudate"])
            ):
                # Check whether the mapping applies to the input date.
                cik = row["cik"]
                break
        # Assert if no mapping is found for the input date.
        if not cik:
            return None
            # if not raise_exception:
            #     return None
            # raise UnmappedException(
            #     "CIK not found for GVKEY='%s', date='%s'" % (gvkey, as_of_date)
            # )
        return cik







