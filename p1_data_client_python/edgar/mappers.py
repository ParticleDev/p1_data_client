"""
Helper classes for the Edgar mapping.

Import as: import p1_data_client_python.edgar.mappers as pemapp
"""
import pandas as pd
from typing import Any, Dict, Optional

import p1_data_client_python.abstract_client as pabstr
import p1_data_client_python.edgar.config as peconf


class ItemMapper(pabstr.AbstractClient):
    """
    Handler for an item mapping.
    """

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
        return f"https://data.particle.one/" \
               f"edgar/v{peconf.P1_EDGAR_DATA_API_VERSION}/"


class GvkCikMapper(pabstr.AbstractClient):
    """
    Handler for GVK <-> Cik transformation.
    """

    def get_gvk_from_cik(
        self, cik: peconf.P1_CIK, as_of_date: Optional[str] = None
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
        self, gvk: peconf.P1_GVK, as_of_date: Optional[str] = None
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
        return f"https://data.particle.one/" \
               f"edgar/v{peconf.P1_EDGAR_DATA_API_VERSION}/"
