import os
import pprint

import pandas as pd

import helpers.unit_test as hut
import helpers.io_ as io_

import p1_data_client_python.edgar_client as p1_edg

P1_API_URL = os.environ["P1_EDGAR_API_URL"]
P1_API_TOKEN = os.environ["P1_EDGAR_API_TOKEN"]


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
        self.check_string(hut.convert_df_to_string(gvk))

    def test_get_cik_from_gvk(self):
        cik = self.gvk_mapper.get_cik_from_gvk(
            gvk=61411, as_of_date="2007-03-14"
        )
        self.assertIsInstance(cik, pd.DataFrame)
        self.assertFalse(cik.empty)
        self.check_string(hut.convert_df_to_string(cik))


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
        self.check_string(hut.convert_df_to_string(item))

    def test_get_mapping(self) -> None:
        mapping = self.item_mapper.get_mapping()
        self.assertIsInstance(mapping, pd.DataFrame)
        self.assertFalse(mapping.empty)
        self.check_string(hut.convert_df_to_string(mapping))


class TestEdgarClient(hut.TestCase):
    def setUp(self) -> None:
        self.client = p1_edg.EdgarClient(token=P1_API_TOKEN, base_url=P1_API_URL)
        super().setUp()

    def test_form8_get_payload_precise_sampling(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            cik=18498,
            start_date="2020-01-04",
            end_date="2020-12-04",
            item="ACT_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_form8_get_payload_without_cik(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            start_date="2020-10-04",
            end_date="2020-12-04",
            item="ACT_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_form8_get_payload_pagination(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            cik=18498,
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_form8_get_payload_multi_cik(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            cik=[18498, 319201, 5768]
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_form8_get_payload_empty(self) -> None:
        payload = self.client.get_payload(
            form_name="form8k",
            cik=1212,
            start_date="2020-01-04",
            end_date="2020-12-04",
            item="QWE_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertTrue(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_form10_get_payload(self) -> None:
        payload = self.client.get_form10_payload(
            cik=1002910,
            start_date="2020-05-10",
            end_date="2020-05-12",
        )
        self.assertIsInstance(payload, list)
        self.assertEqual(len(payload), 1)
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload[0].keys()=%s" % payload[0].keys()))
        actual.append(('payload[0]["meta"]=\n%s' % pprint.pformat(payload[0]["meta"])))
        json_str = payload[0]["data"]
        actual.append(pprint.pformat(payload[0]["data"])[:2000])
        actual = "\n".join(actual)
        self.check_string(actual)

    def test_form10_get_payload_empty(self) -> None:
        """
        Verify that nothing is returned for an interval without Form 10*.

        https://www.sec.gov/cgi-bin/browse-edgar?CIK=1002910&owner=exclude
        """
        payload = self.client.get_form10_payload(
            cik=1002910,
            start_date="2020-05-12",
            end_date="2020-05-13",
        )
        self.assertIsInstance(payload, list)
        self.assertEqual(len(payload), 0)
