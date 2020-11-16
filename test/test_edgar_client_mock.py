import unittest.mock as mock

import helpers.unit_test as hut
import p1_data_client_python.edgar_client as p1_edg
import p1_data_client_python.exceptions as p1_exc
import pandas as pd

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


class PayloadGoodResponseMock:
    status_code = 200

    @staticmethod
    def json() -> list:
        return [
            {
                "url": "https://www.sec.gov/Archives/edgar/data/1002910/000162828020015521/000162828020015521/0001628280-20-015521-index.html",
                "cik": 1002910,
                "filing_date": "2020-11-04",
                "internal_timestamp": 1604527607.388826,
                "item": "NIQ",
                "table_row_name": "Net Income",
                "extracted_value": 369.0,
                "period": "2020-11-04T00:00:00"
            },
            {
                "url": "https://www.sec.gov/Archives/edgar/data/1002910/000162828020015521/000162828020015521/0001628280-20-015521-index.html",
                "cik": 1002910,
                "filing_date": "2020-11-04",
                "internal_timestamp": 1604527607.388826,
                "item": "NIQ",
                "table_row_name": "Less: Net Income Attributable to Noncontrolling Interests",
                "extracted_value": 2.0,
                "period": "2020-11-04T00:00:00"
            }
        ]


class CikGoodResponseMock:
    status_code = 200

    @staticmethod
    def json() -> dict:
        return {
            'data': '123'
        }


class ItemGoodResponseMock:
    status_code = 200

    @staticmethod
    def json() -> dict:
        return {
            'data': ['ITEM_CODE']
        }


class MessyResponseMock:
    status_code = 200

    @staticmethod
    def json() -> dict:
        return {"message": "strange_message"}


class TestEdgarPythonClientMock(hut.TestCase):
    def setUp(self) -> None:
        self.client = p1_edg.EdgarClient(token="goo token")
        super().setUp()

    @mock.patch("requests.Session.request")
    def test_payload(self, mock_request) -> None:
        # test on UnauthorizedException
        mock_request.return_value = mock.Mock(status_code=401)
        with self.assertRaises(p1_exc.UnauthorizedException):
            self.client.get_payload('8-K', '123')
        # test on good response
        mock_request.return_value = PayloadGoodResponseMock()
        self.assertIsInstance(self.client.get_payload('8-K', '123'),
                              pd.DataFrame)

    @mock.patch("requests.Session.request")
    def test_get_cik_(self, mock_request) -> None:
        # test on UnauthorizedException
        mock_request.return_value = mock.Mock(status_code=401)
        with self.assertRaises(p1_exc.UnauthorizedException):
            self.client.get_cik(gvkey='123', gvkey_date='2020-01-01')
        # test on good response
        mock_request.return_value = CikGoodResponseMock()
        self.assertIsInstance(self.client.get_cik(gvkey='123',
                                                  gvkey_date='2020-01-01'), str)

    @mock.patch("requests.Session.request")
    def test_get_item(self, mock_request) -> None:
        # test on UnauthorizedException
        mock_request.return_value = mock.Mock(status_code=401)
        with self.assertRaises(p1_exc.UnauthorizedException):
            self.client.get_item(description='qwe')
        # test on good response
        mock_request.return_value = ItemGoodResponseMock()
        self.assertIsInstance(self.client.
                              get_item(description='Some item description',
                                       keywords=['Computers', 'Hardware']),
                              list)
