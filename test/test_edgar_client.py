import logging
import os
import pprint

import pandas as pd

import p1_data_client_python.helpers.unit_test as hut
import p1_data_client_python.edgar_client as p1_edg
import p1_data_client_python.exceptions as p1_exc

_LOG = logging.getLogger(__name__)
P1_API_TOKEN = os.environ["P1_EDGAR_API_TOKEN"]


class TestGvkCikMapper(hut.TestCase):
    def setUp(self) -> None:
        self.gvk_mapper = p1_edg.GvkCikMapper(token=P1_API_TOKEN)
        super().setUp()

    def test_get_gvk_from_cik(self) -> None:
        gvk = self.gvk_mapper.get_gvk_from_cik(cik=33115, as_of_date="2007-01-01")
        self.assertIsInstance(gvk, pd.DataFrame)
        self.assertFalse(gvk.empty)
        self.check_string(hut.convert_df_to_string(gvk))

    def test_get_cik_from_gvk(self) -> None:
        cik = self.gvk_mapper.get_cik_from_gvk(gvk=61411, as_of_date="2007-03-14")
        self.assertIsInstance(cik, pd.DataFrame)
        self.assertFalse(cik.empty)
        self.check_string(hut.convert_df_to_string(cik))


class TestItemMapper(hut.TestCase):
    def setUp(self) -> None:
        self.item_mapper = p1_edg.ItemMapper(token=P1_API_TOKEN)
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
        self.client = p1_edg.EdgarClient(token=P1_API_TOKEN)
        super().setUp()

    @staticmethod
    def _get_df_info(df: pd.DataFrame) -> str:
        ret = []
        for col_name in ["ticker", "item_name", "filing_date"]:
            vals = sorted(df[col_name].unique().astype(str))
            ret.append("col_name=(%d) %s" % (len(vals), ", ".join(vals)))
        return "\n".join(ret)

    def test_form4_get_payload(self) -> None:
        payload = self.client.get_form4_payload(
            cik=58492, start_date="2016-01-26", end_date="2016-01-26",
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(len(payload), 6)
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual)

    def test_form4_get_payload_with_multi_cik(self) -> None:
        payload = self.client.get_form4_payload(
            cik=[880266, 918160, 7789], start_date="2016-01-26", end_date="2016-01-26"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(len(payload), 6)
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual)

    def test_form4_get_payload_with_0_cik(self) -> None:
        with self.assertRaises(p1_exc.ParseResponseException):
            self.client.get_form4_payload(cik=0)

    def test_form4_get_payload_large_response(self) -> None:
        """
        Get the payload for form4. This test is slow.
        """
        payload = self.client.get_form4_payload(
            start_date="2020-12-10", end_date="2020-12-17",
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(len(payload), 6)
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual)

    def test_form4_get_payload_with_bad_dates(self) -> None:
        with self.assertRaises(p1_exc.ParseResponseException):
            self.client.get_form4_payload(
                start_date="2020-10-10", end_date="2020-09-09"
            )

    def test_form8_get_payload_precise_sampling(self) -> None:
        """
        Specify all the parameters.
        """
        payload = self.client.get_form8_payload(
            cik=18498,
            start_date="2020-01-04",
            end_date="2020-12-04",
            item="ACT_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(hut.convert_df_to_string(payload))

    def test_form8_get_payload_without_cik(self) -> None:
        """
        Specify all the parameters excluding CIK.
        """
        payload = self.client.get_form8_payload(
            start_date="2020-10-04", end_date="2020-12-04", item="ACT_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(hut.convert_df_to_string(payload))

    def test_form8_get_payload_pagination(self) -> None:
        """
        Specify only CIK.
        """
        payload = self.client.get_form8_payload(cik=18498,)
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(hut.convert_df_to_string(payload))

    def test_form8_get_payload_multi_cik(self) -> None:
        """
        Specify multiple CIKs.
        """
        payload = self.client.get_form8_payload(cik=[18498, 319201, 5768])
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(hut.convert_df_to_string(payload))

    def test_form8_get_payload_empty(self) -> None:
        payload = self.client.get_form8_payload(
            cik=1212,
            start_date="2020-01-04",
            end_date="2020-12-04",
            item="QWE_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertTrue(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_form10_get_payload(self) -> None:
        """
        Get the payload for form10. This test is slow.
        """
        payload = self.client.get_form10_payload(
            cik=320193, start_date="2017-11-02", end_date="2017-11-04",
        )
        self.assertIsInstance(payload, list)
        self.assertEqual(len(payload), 1)
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload[0].keys()=%s" % payload[0].keys()))
        actual.append(
            ('payload[0]["meta"]=\n%s' % pprint.pformat(payload[0]["meta"]))
        )
        actual.append(pprint.pformat(payload[0]["data"])[:2000])
        actual = "\n".join(actual)
        self.check_string(actual)

    def test_form10_get_payload_multi_cik(self) -> None:
        """
        Get the payload for form10 and a few cik
        """
        payload = self.client.get_form10_payload(
            cik=[1750, 732717],
            start_date="2018-01-01",
            end_date="2018-07-15",
        )
        self.assertIsInstance(payload, list)
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload[0].keys()=%s" % payload[0].keys()))
        actual.append(
            ('payload[0]["meta"]=\n%s' % pprint.pformat(payload[0]["meta"]))
        )
        actual.append(pprint.pformat(payload[0]["data"])[:2000])
        actual = "\n".join(actual)
        self.check_string(actual)

    def test_form10_get_payload_empty(self) -> None:
        """
        Verify that nothing is returned for an interval without Form 10*.

        https://www.sec.gov/cgi-bin/browse-edgar?CIK=1002910&owner=exclude
        """
        payload = self.client.get_form10_payload(
            cik=1002910, start_date="2020-05-12", end_date="2020-05-13",
        )
        self.assertIsInstance(payload, list)
        self.assertEqual(len(payload), 0)

    def test_form13_get_payload(self) -> None:
        payload = self.client.get_form13_payload(
            cik=1259313, start_date="2015-11-16", end_date="2015-11-16",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            hut.convert_df_to_string(payload['information_table']))

    def test_form13_get_payload_multi_cik(self) -> None:
        payload = self.client.get_form13_payload(
            cik=[836372, 859804], start_date="2015-11-16", end_date="2015-11-16",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            hut.convert_df_to_string(payload['information_table']))

    def test_form13_get_payload_cik_cusip(self) -> None:
        with self.assertRaises(AssertionError):
            self.client.get_form13_payload(cik=123, cusip="qwe")

    def test_form13_get_payload_with_cusip(self) -> None:
        payload = self.client.get_form13_payload(
            cusip="01449J204", start_date="2015-11-16", end_date="2015-11-16",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            hut.convert_df_to_string(payload['information_table']))

    def test_form13_get_payload_with_multi_cusip(self) -> None:
        payload = self.client.get_form13_payload(
            cusip=["002824100", "01449J204"],
            start_date="2016-11-15",
            end_date="2016-11-15",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            hut.convert_df_to_string(payload['information_table']))

    def test_form13_get_payload_large_response(self) -> None:
        payload = self.client.get_form13_payload(
            start_date="2020-12-10", end_date="2020-12-17",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            hut.convert_df_to_string(payload['information_table']))

    def test_form_types(self) -> None:
        form_types = self.client.form_types
        actual = "\n".join(form_types)
        self.check_string(actual)

    def test_get_form_headers(self) -> None:
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193],
            start_date='2000-01-01',
            end_date='2020-02-01'
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_get_form_headers_without_cik(self) -> None:
        payload = self.client.get_form_headers(
            form_type='13F-HR',
            start_date='2020-03-01',
            end_date='2020-10-10'
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_get_form_headers_one_form(self) -> None:
        payload = self.client.get_form_headers(
            form_type="13F-HR",
            cik=1404574,
            start_date='2012-11-14',
            end_date='2012-11-14'
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))

    def test_get_form_headers_one_cik(self) -> None:
        payload = self.client.get_form_headers(
            form_type='13F-HR',
            cik=1404574,
            start_date='2012-11-14',
            end_date='2012-11-14'
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(hut.convert_df_to_string(payload))
