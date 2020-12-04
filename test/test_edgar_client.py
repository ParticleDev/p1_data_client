import os

import pandas as pd

import helpers.unit_test as hut
import p1_data_client_python.edgar_client as p1_edg

P1_API_URL = os.environ["P1_EDGAR_API_URL"]
P1_API_TOKEN = os.environ["P1_EDGAR_API_TOKEN"]


class TestEdgarClient(hut.TestCase):
    def setUp(self) -> None:
        self.client = p1_edg.EdgarClient(token=P1_API_TOKEN, base_url=P1_API_URL)
        super().setUp()

    def test_get_payload_precise_sampling(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            cik=18498,
            start_date="2020-01-04",
            end_date="2020-12-04",
            item="ACT_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)

    def test_get_payload_without_cik(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            start_date="2020-10-04",
            end_date="2020-12-04",
            item="ACT_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)

    def test_get_payload_pagination(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            cik=18498,
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)

    def test_get_payload_multi_cik(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            cik=[18498]
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)


class TestGvkCikMapper(hut.TestCase):
    def setUp(self) -> None:
        self.gvk_mapper = p1_edg.GvkCikMapper(
            token=P1_API_TOKEN, base_url=P1_API_URL
        )
        super().setUp()

    def test_get_gvk_from_cik(self):
        gvk = self.gvk_mapper.get_gvk_from_cik(
            cik=33115, as_of_date="2007-01-01"
        )
        self.assertIsInstance(gvk, pd.DataFrame)
        self.assertFalse(gvk.empty)

    def test_get_cik_from_gvk(self):
        cik = self.gvk_mapper.get_cik_from_gvk(
            gvk=61411, as_of_date="2007-03-14"
        )
        self.assertIsInstance(cik, pd.DataFrame)
        self.assertFalse(cik.empty)


class TestItemMapper(hut.TestCase):
    def setUp(self) -> None:
        self.item_mapper = p1_edg.ItemMapper(
            token=P1_API_TOKEN, base_url=P1_API_URL
        )
        super().setUp()

    def test_get_item(self) -> None:
        item = self.item_mapper.get_item_from_keywords(
            keywords="short-term short term"
        )
        self.assertIsInstance(item, pd.DataFrame)
        self.assertFalse(item.empty)

    def test_get_mapping(self) -> None:
        mapping = self.item_mapper.get_mapping()
        self.assertIsInstance(mapping, pd.DataFrame)
        self.assertFalse(mapping.empty)
