import unittest.mock as mock
from typing import Any

import pandas as pd

import p1_data_client_python.helpers.unit_test as hut
import p1_data_client_python.client as p1_data
import p1_data_client_python.exceptions as p1_exc

TOKEN_ENV_NAME = "P1_API_TOKEN"
EXAMPLE_METADATA_TYPE = "COMMODITIES"
BAD_METADATA_TYPE = "Stranger Things"
SEARCH_ROW_EXAMPLE = {
    "name": "qweqwe",
    "commodity": "qweqweqwe",
    "payload_id": "asdasd",
    "business_category": "asdasd",
    "country": "asdasd",
    "frequency": "asdasd",
    "unit": "zxc",
    "start_date": "zxcff",
}


class SearchOnePageGoodResponse:
    status_code = 200

    @staticmethod
    def json() -> dict:
        return {
            "scroll_id": "qweqwe",
            "total_count": 5555,
            "rows": [SEARCH_ROW_EXAMPLE for _ in range(5555)],
        }


class PayloadGoodResponseMock:
    status_code = 200

    @staticmethod
    def json() -> dict:
        return {
            "payload_data": [
                {
                    "original_period": "010101",
                    "original_value": "1.33",
                    "period": "01-01-01",
                    "value": 1.33,
                },
                {
                    "original_period": "020202",
                    "original_value": "4.33",
                    "period": "02-02-02",
                    "value": 4.33,
                },
            ]
        }


class MessyResponseMock:
    status_code = 200

    @staticmethod
    def json() -> dict:
        return {"message": "strange_message"}


class MetaDataGoodResponseMock:
    status_code = 200

    @staticmethod
    def json() -> dict:
        return {"data": [{"name": "Metadata1"}, {"name": "Metadata2"}]}


class TestPythonClientMock(hut.TestCase):
    def setUp(self) -> None:
        self.client = p1_data.Client(token="goo token")
        super().setUp()

    def test_list_of_metadata(self) -> None:
        self.assertIsInstance(self.client.list_of_metadata, list)

    @mock.patch("requests.Session.request")
    def test_search(self, mock_request: Any) -> None:
        # test on UnauthorizedException
        mock_request.return_value = mock.Mock(status_code=401)
        with self.assertRaises(p1_exc.UnauthorizedException):
            self.client.search(text="Price")
        # test on good response
        mock_request.return_value = SearchOnePageGoodResponse()
        self.client.search(text="Price")
        for page in self.client.search_pages(pages_limit=2):
            self.assertIsInstance(page, pd.DataFrame)

    @mock.patch("requests.Session.request")
    def test_payload(self, mock_request: Any) -> None:
        payload_id = "some very good get_payload ID"
        # test on UnauthorizedException
        mock_request.return_value = mock.Mock(status_code=401)
        with self.assertRaises(p1_exc.UnauthorizedException):
            self.client.get_payload(payload_id)
        # test on good response
        mock_request.return_value = PayloadGoodResponseMock()
        self.assertIsInstance(self.client.get_payload(payload_id), pd.DataFrame)
        # test on ParseResponseException
        mock_request.return_value = MessyResponseMock()
        with self.assertRaises(p1_exc.ParseResponseException):
            self.client.get_payload(payload_id)

    @mock.patch("requests.Session.request")
    def test_get_metadata_type_mock(self, mock_request: Any) -> None:
        # test on UnauthorizedException
        mock_request.return_value = mock.Mock(status_code=401)
        with self.assertRaises(p1_exc.UnauthorizedException):
            self.client.get_metadata_type(EXAMPLE_METADATA_TYPE)
        # test on good response
        mock_request.return_value = MetaDataGoodResponseMock
        self.assertIsInstance(
            self.client.get_metadata_type(EXAMPLE_METADATA_TYPE), pd.DataFrame
        )
        # test on ParseResponseException
        mock_request.return_value = MessyResponseMock
        with self.assertRaises(p1_exc.ParseResponseException):
            self.client.get_metadata_type(EXAMPLE_METADATA_TYPE)
