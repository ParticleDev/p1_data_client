import os

import pandas as pd
import pytest

import p1_data_client_python.helpers.unit_test as hut
import p1_data_client_python.client as p1_data
import p1_data_client_python.exceptions as p1_exc

# TODO: Remove alpha url before production usage
TOKEN_ENV_NAME = "P1_API_TOKEN"
TEST_TIMEOUT_URL = "https://httpstat.us/200?sleep=50000"
BAD_TOKEN = "d238f0111111"
EXAMPLE_METADATA_TYPE = "COMMODITIES"
BAD_METADATA_TYPE = "Stranger Things"


class TestPythonClient(hut.TestCase):
    def set_env_token(self) -> None:
        self.env_token = os.getenv(TOKEN_ENV_NAME)
        if not self.env_token:
            raise p1_exc.TestTokenNotFound(
                f"Token not found in the environment variable {TOKEN_ENV_NAME}"
            )

    def setUp(self) -> None:
        self.set_env_token()
        self.client = p1_data.Client(token=self.env_token)
        super().setUp()

    def test_search(self) -> None:
        # test on good response
        self.client.search(text="Price")
        for page in self.client.search_pages(pages_limit=2):
            self.assertIsInstance(page, pd.DataFrame)
        # test on UnauthorizedException
        with self.assertRaises(p1_exc.UnauthorizedException):
            self.client.token = BAD_TOKEN
            self.client.headers["Authorization"] = "Token " + self.client.token
            self.client.search(text="Price")

    def test_search_scroll(self) -> None:
        self.client.search(text="Price")
        self.assertIsInstance(self.client.search_scroll(), dict)

    def test_payload(self) -> None:
        payloads = self.client.search(text="Price")
        payload_time_series_data = self.client.get_payload(
            payloads.iloc[0].payload_id
        )
        self.assertIsInstance(payload_time_series_data, pd.DataFrame)

    def test_get_metadata_type(self) -> None:
        for metadata_type in self.client.METADATA_ROUTES:
            isinstance(self.client.get_metadata_type(metadata_type), pd.DataFrame)
        with self.assertRaises(p1_exc.UnauthorizedException):
            self.client.token = BAD_TOKEN
            self.client.headers["Authorization"] = "Token " + self.client.token
            self.client.get_metadata_type(EXAMPLE_METADATA_TYPE)
        with pytest.raises(p1_exc.BadMetaDataTypeException):
            self.client.get_metadata_type(BAD_METADATA_TYPE)
