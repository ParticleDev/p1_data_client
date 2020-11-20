import pandas as pd
import helpers.unit_test as hut
import p1_data_client_python.edgar_client as p1_edg

TEST_BASE_URL = 'http://etl.p1:5001/edgar/v1/'


class TestEdgarClient(hut.TestCase):

    def setUp(self) -> None:
        self.client = p1_edg.EdgarClient(token="1234567890",
                                         base_url=TEST_BASE_URL)
        super().setUp()

    def test_get_payload_precise_sampling(self) -> None:
        payload = self.client.get_payload(form_name='8-K',
                                          cik=1002910,
                                          start_date='2020-01-04',
                                          end_date='2021-11-04',
                                          items=['OIBDPQ', 'NIQ']
                                          )
        self.assertIsInstance(payload, pd.DataFrame)

    def test_get_payload_pagination(self) -> None:
        payload = self.client.get_payload(form_name='8-K',
                                          cik=1002910,
                                          )
        self.assertIsInstance(payload, pd.DataFrame)

    def test_get_cik(self) -> None:
        cik = self.client.get_cik(gvkey='004083', gvkey_date='2007-01-18')
        self.assertIsInstance(cik, pd.DataFrame)

    def test_get_item(self) -> None:
        item = self.client.get_item(keywords=['short-term', 'short term'])
        self.assertIsInstance(item, pd.DataFrame)


class TestGvkeyCikMapper(hut.TestCase):
    def setUp(self) -> None:
        self.gvkey_mapper = p1_edg.GvkeyCikMapper(token="1234567890",
                                                  base_url=TEST_BASE_URL)
        super().setUp()

    def test_get_gvkey_from_cik(self):
        gvkey = self.gvkey_mapper.get_gvkey_from_cik(cik='0000940800',
                                                     as_of_date='2007-01-18')
        self.assertIsInstance(gvkey, pd.DataFrame)

    def test_get_cik_from_gvkey(self):
        cik = self.gvkey_mapper.get_cik_from_gvkey(gvkey='061411',
                                                   as_of_date='2007-01-18')
        self.assertIsInstance(cik, pd.DataFrame)


class TestCompustatItemMapper(hut.TestCase):
    def setUp(self) -> None:
        self.item_mapper = \
            p1_edg.CompustatItemMapper(token="1234567890",
                                       base_url=TEST_BASE_URL)
        super().setUp()

    def test_get_item(self) -> None:
        item = self.item_mapper.get_item_from_keywords(
            keywords=['short-term', 'short term'])
        self.assertIsInstance(item, pd.DataFrame)

    def test_get_mapping(self) -> None:
        mapping = self.item_mapper.get_mapping()
        self.assertIsInstance(mapping, pd.DataFrame)





