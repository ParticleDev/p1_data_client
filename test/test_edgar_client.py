import pandas as pd
import helpers.unit_test as hut
import p1_data_client_python.edgar_client as p1_edg


class TestEdgarClient(hut.TestCase):

    def setUp(self) -> None:
        self.client = p1_edg.EdgarClient(token="1234567890")
        super().setUp()

    def test_get_payload(self) -> None:
        payload = self.client.get_payload(form_name='8-K',
                                          cik=1002910,
                                          start_date='2020-01-01',
                                          end_date='2019-01-01',
                                          items=['OIBDPQ', 'NIQ', 'ASDF']
                                          )
        self.assertIsInstance(payload, pd.DataFrame)

    def test__fill_cik_gvkey_mapping(self):
        self.client._fill_cik_gvkey_mapping()
        pass

    def test_get_cik_by_gvkey(self):
        cik = self.client.get_cik_by_gvkey('1800')

