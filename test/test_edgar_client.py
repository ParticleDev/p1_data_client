import logging
import os
import pprint

import pandas as pd
import pytest

import p1_data_client_python.edgar.edgar_client as pedgar
import p1_data_client_python.edgar.mappers as p1_edg_map
import p1_data_client_python.exceptions as pexcep
import p1_data_client_python.helpers.unit_test as phunit  # type: ignore

_LOG = logging.getLogger(__name__)
P1_API_TOKEN = os.environ["P1_EDGAR_API_TOKEN"]


class TestGvkCikMapper(phunit.TestCase):
    def setUp(self) -> None:
        self.gvk_mapper = p1_edg_map.GvkCikMapper(token=P1_API_TOKEN)
        super().setUp()

    def test_get_gvk_from_cik(self) -> None:
        """
        Get GVK by the cik and date.
        """
        gvk = self.gvk_mapper.get_gvk_from_cik(cik=33115, as_of_date="2007-01-01")
        self.assertIsInstance(gvk, pd.DataFrame)
        self.assertFalse(gvk.empty)
        self.check_string(phunit.convert_df_to_string(gvk))

    def test_get_cik_from_gvk(self) -> None:
        """
        Get Cik by GVK and date.
        """
        cik = self.gvk_mapper.get_cik_from_gvk(gvk=61411, as_of_date="2007-03-14")
        self.assertIsInstance(cik, pd.DataFrame)
        self.assertFalse(cik.empty)
        self.check_string(phunit.convert_df_to_string(cik))


class TestItemMapper(phunit.TestCase):
    def setUp(self) -> None:
        self.item_mapper = p1_edg_map.ItemMapper(token=P1_API_TOKEN)
        super().setUp()

    def test_get_item(self) -> None:
        """
        Obtain an item by keywords.
        """
        item = self.item_mapper.get_item_from_keywords(
            keywords="short-term short term"
        )
        self.assertIsInstance(item, pd.DataFrame)
        self.assertFalse(item.empty)
        self.check_string(phunit.convert_df_to_string(item))

    def test_get_mapping(self) -> None:
        """
        Get all mapping for items.
        """
        mapping = self.item_mapper.get_mapping()
        self.assertIsInstance(mapping, pd.DataFrame)
        self.assertFalse(mapping.empty)
        self.check_string(phunit.convert_df_to_string(mapping))


class TestEdgarClient(phunit.TestCase):
    def setUp(self) -> None:
        self.client = pedgar.EdgarClient(token=P1_API_TOKEN)
        super().setUp()

    def test_form4_mandatory_date_mode(self):
        with self.assertRaises(AssertionError):
            self.client.get_form4_payload(
                cik=58492, start_date="2016-01-26", end_date="2016-01-26",
            )

    def test_form4_get_payload(self) -> None:
        """
        Get payload data for forms 4 and a company.
        """
        payload = self.client.get_form4_payload(
            cik=58492,
            start_date="2016-01-26",
            end_date="2016-01-26",
            date_mode="publication_date"
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

    def test_form4_get_payload_knowledge_date(self) -> None:
        """
        Get payload data for forms 4 and a company.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form4_payload(
            cik=58492,
            start_date="2021-01-01",
            end_date="2021-01-11",
            date_mode="knowledge_date"
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
        """
        Get payload data for forms 4 and a list of CIKs.
        """
        payload = self.client.get_form4_payload(
            cik=[880266, 918160, 7789],
            start_date="2016-01-26",
            end_date="2016-01-26",
            date_mode="publication_date"
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

    def test_form4_get_payload_with_multi_cik_knowledge_date(self) -> None:
        """
        Get payload data for forms 4 and a list of CIKs.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form4_payload(
            cik=[880266, 58492, 320193],
            start_date="2021-01-01",
            end_date="2021-01-11",
            date_mode="knowledge_date"
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
        """
        Get payload data for forms 4 with CIK=0.
        """
        with self.assertRaises(pexcep.ParseResponseException):
            self.client.get_form4_payload(cik=0)

    @pytest.mark.slow("About 3 minutes.")
    def test_form4_get_payload_large_response(self) -> None:
        """
        Get the payload for form4 with big amount of data.
        """
        payload = self.client.get_form4_payload(
            start_date="2020-12-10",
            end_date="2020-12-17",
            date_mode="publication_date"
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
        """
        Get payload data for forms 4 with bad dates.
        """
        with self.assertRaises(pexcep.ParseResponseException):
            self.client.get_form4_payload(
                start_date="2020-10-10", end_date="2020-09-09",
                date_mode="publication_date"
            )

    def test_form8_get_payload_precise_sampling(self) -> None:
        """
        Get payload data for forms 4 with all parameters.
        """
        payload = self.client.get_form8_payload(
            cik=18498,
            start_date="2020-01-04",
            end_date="2020-12-04",
            item="ACT_QUARTER",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    def test_form8_get_payload_knowledge_date(self) -> None:
        """
        Get payload data for forms 4 with all parameters.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form8_payload(
            cik=320193,
            start_date="2020-07-01",
            end_date="2020-11-01",
            item="ACT_QUARTER",
            date_mode="knowledge_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    def test_form8_get_payload_without_cik(self) -> None:
        """
        Get payload data for forms 4.

        Specify all the parameters excluding CIK.
        """
        payload = self.client.get_form8_payload(
            start_date="2020-10-04",
            end_date="2020-12-04",
            date_mode="publication_date",
            item="ACT_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    def test_form8_get_payload_pagination(self) -> None:
        """
        Get payload data for forms 4.

        Specify only CIK.
        """
        payload = self.client.get_form8_payload(
            cik=18498,
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    def test_form8_get_payload_multi_cik(self) -> None:
        """
        Get payload data for forms 4.

        Specify multiple CIKs.
        """
        payload = self.client.get_form8_payload(cik=[18498, 319201, 5768])
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    def test_form8_get_payload_multi_cik_knowledge_date(self) -> None:
        """
        Get payload data for forms 4 with multiple CIKs.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form8_payload(
            cik=[880266, 58492, 320193],
            start_date="2020-07-01",
            end_date="2020-11-01",
            item="ACT_QUARTER",
            date_mode="knowledge_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    def test_form8_get_payload_empty(self) -> None:
        """
        Get payload data for forms 8.

        Check for an empty response.
        """
        payload = self.client.get_form8_payload(
            cik=1212,
            start_date="2020-01-04",
            end_date="2020-12-04",
            date_mode="publication_date",
            item="QWE_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertTrue(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.slow("About 50 seconds.")
    def test_form10_get_payload(self) -> None:
        """
        Get payload data for forms 10.
        """
        payload = self.client.get_form10_payload(
            cik=320193,
            start_date="2017-11-02",
            end_date="2017-11-04",
            date_mode="publication_date"
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

    @pytest.mark.slow("About 25 seconds.")
    def test_form10_get_payload_knowledge_date(self) -> None:
        """
        Get payload data for forms 10.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form10_payload(
            cik=320193,
            start_date="2020-10-01",
            end_date="2020-11-01",
            date_mode="knowledge_date"
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

    @pytest.mark.slow("About 1.30 minutes.")
    def test_form10_get_payload_multi_cik(self) -> None:
        """
        Get the payload for form10 and a few cik.
        """
        payload = self.client.get_form10_payload(
            cik=[1750, 732717],
            start_date="2018-01-01",
            end_date="2018-07-15",
            date_mode="publication_date"
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

    @pytest.mark.slow("About 45 seconds.")
    def test_form10_get_payload_multi_cik_knowledge_date(self) -> None:
        """
        Get the payload for form10 and a few cik.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form10_payload(
            cik=[58492, 320193],
            start_date="2020-10-01",
            end_date="2020-12-01",
            date_mode="knowledge_date"
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

    @pytest.mark.slow("About 40 seconds.")
    def test_form10_get_payload_empty(self) -> None:
        """
        Verify that nothing is returned for an interval without Form 10*.

        https://www.sec.gov/cgi-bin/browse-edgar?CIK=1002910&owner=exclude
        """
        payload = self.client.get_form10_payload(
            cik=1002910,
            start_date="2020-05-12",
            end_date="2020-05-13",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, list)
        self.assertEqual(len(payload), 0)

    def test_form13_get_payload(self) -> None:
        """
        Get payload data for forms 13.
        """
        payload = self.client.get_form13_payload(
            cik=1259313,
            start_date="2015-11-16",
            end_date="2015-11-16",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_knowledge_date(self) -> None:
        """
        Get payload data for forms 13.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form13_payload(
            cik=1259313,
            start_date="2020-11-01",
            end_date="2020-12-31",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_multi_cik(self) -> None:
        """
        Get payload data for forms 13.

        Check with a few cik as parameter.
        """
        payload = self.client.get_form13_payload(
            cik=[836372, 859804],
            start_date="2015-11-16",
            end_date="2015-11-16",
            date_mode='publication_date'
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_multi_cik_knowledge_date(self) -> None:
        """
        Get payload data for forms 13 for multiple CIKs.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form13_payload(
            cik=[836372, 859804],
            start_date="2020-11-01",
            end_date="2020-12-31",
            date_mode='knowledge_date'
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_cik_cusip(self) -> None:
        """
        Get payload data for forms 13.

        Check for cik and cusip with the same time.
        """
        with self.assertRaises(AssertionError):
            self.client.get_form13_payload(cik=123, cusip="qwe")

    def test_form13_get_payload_with_cusip(self) -> None:
        """
        Get payload data for forms 13.

        Check the cusip filter.
        """
        payload = self.client.get_form13_payload(
            cusip="01449J204",
            start_date="2015-11-16",
            end_date="2015-11-16",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_with_cusip_knowledge_date(self) -> None:
        """
        Get payload data for forms 13.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form13_payload(
            cusip="002824100",
            start_date="2020-12-30",
            end_date="2020-12-30",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_with_multi_cusip(self) -> None:
        """
        Get payload data for forms 13 for multiple CUSIPs.
        """
        payload = self.client.get_form13_payload(
            cusip=["002824100", "01449J204"],
            start_date="2016-11-15",
            end_date="2016-11-15",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_with_multi_cusip_knowledge_date(self) -> None:
        """
        Get payload data for forms 13 for multiple CUSIPs.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form13_payload(
            cusip=["002824100", "928563402"],
            start_date="2021-01-01",
            end_date="2021-01-05",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.slow("Slow Form13 test")
    def test_form13_get_payload_large_response(self) -> None:
        """
        Get payload data for forms 13.

        Check the cusip filter with multiple values.
        """
        payload = self.client.get_form13_payload(
            start_date="2020-12-10",
            end_date="2020-12-17",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_small_response_no_cusip(self) -> None:
        """
        Get payload data for forms 13.

        Test without filtration.
        """
        payload = self.client.get_form13_payload(
            start_date="2020-09-09",
            end_date="2020-09-10",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, dict)
        unique_uuids = payload["information_table"]["uuid"].unique()
        self.assertEqual(len(unique_uuids), 7)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_irrelevant_cusip(self) -> None:
        """
        Get payload data for forms 13.

        Check for irrelevant cusip.
        """
        payload = self.client.get_form13_payload(
            cusip="ffffffffff",
            start_date="2015-11-16",
            end_date="2015-11-16",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.slow("About 20 seconds.")
    def test_form13_get_payload_check_number1(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_date="2018-01-01",
            end_date="2018-01-15",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        unique_uuids = payload["header_data"]["uuid"].unique()
        self.assertEqual(len(unique_uuids), 233)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.slow("About 30 seconds.")
    def test_form13_get_payload_check_number2(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_date="2016-04-01",
            end_date="2016-04-15",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        unique_uuids = payload["header_data"]["uuid"].unique()
        self.assertEqual(len(unique_uuids), 350)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_check_number3(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms in the December gap.
        """
        payload = self.client.get_form13_payload(
            start_date="2020-12-01",
            end_date="2021-01-01",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        unique_uuids = payload["header_data"]["uuid"].unique()
        self.assertEqual(len(unique_uuids), 35)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_check_number4(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_date="2020-10-01",
            end_date="2020-10-05",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_check_number5(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_date="2021-01-15",
            end_date="2021-01-16",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form13_get_payload_no_cusip_universe(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_date="2017-06-29",
            end_date="2017-06-30",
            date_mode="publication_date",
        )
        unique_ciks = payload["header_data"]["cik"].unique()
        self.assertIsInstance(payload, dict)
        expected = [1591379, 1399770, 1559771, 1569638]
        self.assertCountEqual(unique_ciks, expected)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    def test_form_types(self) -> None:
        """
        Mapping between short form names and form types in the Edgar universe.
        """
        form_types = self.client.form_types
        actual = "\n".join(form_types)
        self.check_string(actual)

    def test_headers(self) -> None:
        """
        Get form headers metadata with the following parameters.
        """
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193],
            start_date="2000-01-01",
            end_date="2020-02-01",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    def test_headers_knowledge_date(self) -> None:
        """
        Get form headers metadata with the following parameters.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193],
            start_date="2020-10-01",
            end_date="2020-12-01",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    def test_headers_multi_cik(self) -> None:
        """
        Get form headers metadata with the following parameters.

        Test with multiple CIKs.
        """
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193, 732717],
            start_date="2020-01-01",
            end_date="2020-01-03",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    def test_headers_multi_cik_knowledge_date(self) -> None:
        """
        Get form headers metadata with the following parameters.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193, 732717],
            start_date="2020-10-01",
            end_date="2020-11-01",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    def test_headers_without_cik(self) -> None:
        """
        Get form headers metadata.

        Test without cik parameter.
        """
        payload = self.client.get_form_headers(
            form_type="13F-HR", start_date="2020-03-01", end_date="2020-10-10",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        payload.sort_values(['form_type', 'filing_date', 'cik', 'uuid'],
                            inplace=True)
        self.check_string(phunit.convert_df_to_string(payload))

    def test_headers_one_form(self) -> None:
        """
        Get form headers metadata.

        Test with one form.
        """
        payload = self.client.get_form_headers(
            form_type="13F-HR",
            cik=1404574,
            start_date="2012-11-14",
            end_date="2012-11-14",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    def test_headers_one_cik(self) -> None:
        """
        Get form headers metadata.

        Test with one cik.
        """
        payload = self.client.get_form_headers(
            form_type="13F-HR",
            cik=1404574,
            start_date="2012-11-14",
            end_date="2012-11-14",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    def test_headers_bad_form_types(self) -> None:
        """
        Get form headers with bad form types.

        Test with one cik.
        """
        with self.assertRaises(AssertionError):
            payload = self.client.get_form_headers(
                form_type=["13F-HR", "13-B", "178", "99"],
                cik=1404574,
            )

    def test_headers_without_form_type(self) -> None:
        """
        Get form headers metadata.

        Test with one cik.
        """
        payload = self.client.get_form_headers(
            cik=1404574,
            start_date="2012-11-14",
            end_date="2012-11-14",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @staticmethod
    def _get_df_info(df: pd.DataFrame) -> str:
        ret = []
        for col_name in ["ticker", "item_name", "filing_date"]:
            vals = sorted(df[col_name].unique().astype(str))
            ret.append("col_name=(%d) %s" % (len(vals), ", ".join(vals)))
        return "\n".join(ret)
