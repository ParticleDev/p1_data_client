import logging
import os
import pprint
import pandas as pd
import pytest

import p1_data_client_python as p1cli
import p1_data_client_python.exceptions as pexcep
import p1_data_client_python.helpers.unit_test as phunit  # type: ignore
import p1_data_client_python.edgar.utils as peutil

_LOG = logging.getLogger(__name__)
P1_API_TOKEN = os.environ["P1_EDGAR_API_TOKEN"]


class TestGvkCikMapper(phunit.TestCase):
    def setUp(self) -> None:
        self.gvk_mapper = p1cli.GvkCikMapper(token=P1_API_TOKEN)
        super().setUp()

    def test_get_gvk_from_cik(self) -> None:
        """
        Get GVK by the cik and date.
        """
        gvk = self.gvk_mapper.get_gvk_from_cik(cik=33115,
                                               as_of_date="2007-01-01T00:00:00")
        self.assertIsInstance(gvk, pd.DataFrame)
        self.assertFalse(gvk.empty)
        self.check_string(phunit.convert_df_to_string(gvk))

    def test_get_cik_from_gvk(self) -> None:
        """
        Get Cik by GVK and date.
        """
        cik = self.gvk_mapper.get_cik_from_gvk(gvk=61411,
                                               as_of_date="2007-03-14T00:00:00")
        self.assertIsInstance(cik, pd.DataFrame)
        self.assertFalse(cik.empty)
        self.check_string(phunit.convert_df_to_string(cik))


class TestItemMapper(phunit.TestCase):
    def setUp(self) -> None:
        self.item_mapper = p1cli.ItemMapper(token=P1_API_TOKEN)
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
        self.client = p1cli.EdgarClient(token=P1_API_TOKEN)
        super().setUp()

    def _assert_date_columns_format(self, df: pd.DataFrame) -> None:
        """
        Assert that "created_at" and "release_date" column values are timestamps.

        :param df: input payload dataframe
        :return:
        """
        for col in ["created_at", "release_date"]:
            if col in df.columns:
                all_values_are_timestamps = df[col].str.contains(
                    r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2,}"
                ).all()
                self.assertEqual(True, all_values_are_timestamps)

    def _assert_corner_date_values(
        self,
        df: pd.DataFrame,
        min_created_at: str,
        max_created_at: str,
        min_release_date: str,
        max_release_date: str,
    ) -> None:
        """
        Assert that corner values of date columns are correct.

        :param df: input payload dataframe
        :param min_created_at: expected min "created_at" value
        :param max_created_at: expected max "created_at" value
        :param min_release_date: expected min "release_date"
        :param max_release_date: expected max "release_date"
        :return:
        """
        self.assertEqual(min_created_at, df["created_at"].min())
        self.assertEqual(max_created_at, df["created_at"].max())
        self.assertEqual(min_release_date, df["release_date"].min())
        self.assertEqual(max_release_date, df["release_date"].max())

    @pytest.mark.form4
    def test_form4_no_date_mode(self):
        """
        Check that an error is raised if `date_mode` is not specified.
        """
        with self.assertRaises(AssertionError):
            self.client.get_form4_payload(
                cik=58492, start_datetime="2016-01-26T00:00:00",
                end_datetime="2016-01-26T23:59:59",
            )

    @pytest.mark.form4
    def test_form4_1_cik_publication_date_historical(self) -> None:
        """
        Check payload for 1 CIK with publication date mode, historical.
        """
        payload = self.client.get_form4_payload(
            cik=58492,
            start_datetime="2016-01-26T00:00:00",
            end_datetime="2016-01-27T00:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(11, len(payload["metadata"]))
        self._assert_date_columns_format(payload["metadata"])
        self._assert_corner_date_values(
            df=payload["metadata"],
            min_created_at="2020-06-12T00:00:00",
            max_created_at="2020-06-12T00:00:00",
            min_release_date="2016-01-26T00:00:00",
            max_release_date="2016-01-26T00:00:00",
        )
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    # TODO(*): update after https://github.com/ParticleDev/commodity_research/issues/7488.
    @pytest.mark.form4
    def test_form4_1_cik_publication_date_real_time(self) -> None:
        """
        Check payload for 1 CIK with publication date mode, real time.

        After API v1.7 behaviour is fixed, put
        `self._assert_date_columns_format(payload["metadata"])` and
        `self._assert_corner_date_values()` with recomputed expected
        corner values of "created_at" and "release_date" in the code.
        """
        payload = self.client.get_form4_payload(
            cik=58492,
            start_datetime="2021-02-01T00:00:00",
            end_datetime="2021-02-11T00:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(8, len(payload["metadata"]))
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form4
    def test_form4_1_cik_knowledge_date_historical(self) -> None:
        """
        Check payload for 1 CIK with knowledge date mode, historical.
        """
        payload = self.client.get_form4_payload(
            cik=2969,
            start_datetime="2020-12-01T00:00:00",
            end_datetime="2020-12-01T23:59:59",
            date_mode="knowledge_date"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(9, len(payload["metadata"]))
        self._assert_date_columns_format(payload["metadata"])
        self._assert_corner_date_values(
            df=payload["metadata"],
            min_created_at="2020-12-01T00:00:00",
            max_created_at="2020-12-01T00:00:00",
            min_release_date="2020-04-02T00:00:00",
            max_release_date="2020-04-02T00:00:00",
        )
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    # TODO(*): update after https://github.com/ParticleDev/commodity_research/issues/7488.
    @pytest.mark.form4
    def test_form4_1_cik_knowledge_date_real_time(self) -> None:
        """
        Check payload for 1 CIK with knowledge date mode, real time.

        After API v1.7 behaviour is fixed, put
        `self._assert_date_columns_format(payload["metadata"])` and
        `self._assert_corner_date_values()` with recomputed expected
        corner values of "created_at" and "release_date" in the code.
        """
        payload = self.client.get_form4_payload(
            cik=58492,
            start_datetime="2021-02-01T00:00:00",
            end_datetime="2021-02-11T23:59:59",
            date_mode="knowledge_date"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(8, len(payload["metadata"]))
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form4
    def test_form4_multi_cik_publication_date_historical(self) -> None:
        """
        Check payload for multiple CIKs with publication date mode, historical.
        """
        payload = self.client.get_form4_payload(
            cik=[880266, 918160, 7789],
            start_datetime="2016-01-26T00:00:00",
            end_datetime="2016-01-27T00:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(28, len(payload["metadata"]))
        self._assert_date_columns_format(payload["metadata"])
        self._assert_corner_date_values(
            df=payload["metadata"],
            min_created_at="2020-06-12T00:00:00",
            max_created_at="2020-06-12T00:00:00",
            min_release_date="2016-01-26T00:00:00",
            max_release_date="2016-01-26T00:00:00",
        )
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form4
    def test_form4_multi_cik_publication_date_real_time(self) -> None:
        """
        Check payload for multiple CIKs with publication date mode, real time.
        """
        payload = self.client.get_form4_payload(
            cik=[880266, 918160, 7789],
            start_datetime="2021-02-01T00:00:00",
            end_datetime="2021-02-05T00:00:00",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(27, len(payload["metadata"]))
        self._assert_date_columns_format(payload["metadata"])
        self._assert_corner_date_values(
            df=payload["metadata"],
            min_created_at="2021-02-03T19:44:17.992000",
            max_created_at="2021-02-04T20:37:34.443000",
            min_release_date="2021-02-03T00:00:00",
            max_release_date="2021-02-04T00:00:00",
        )
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form4
    def test_form4_multi_cik_knowledge_date_historical(self) -> None:
        """
        Check payload for multiple CIKs with knowledge date mode, historical.
        """
        payload = self.client.get_form4_payload(
            cik=[320193, 2969],
            start_datetime="2020-12-01T00:00:00",
            end_datetime="2020-12-02T00:00:00",
            date_mode="knowledge_date"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(16, len(payload["metadata"]))
        self._assert_date_columns_format(payload["metadata"])
        self._assert_corner_date_values(
            df=payload["metadata"],
            min_created_at="2020-12-01T00:00:00",
            max_created_at="2020-12-01T00:00:00",
            min_release_date="2020-04-02T00:00:00",
            max_release_date="2020-05-12T00:00:00",
        )
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    # TODO(*): update after https://github.com/ParticleDev/commodity_research/issues/7488.
    @pytest.mark.form4
    def test_form4_multi_cik_knowledge_date_real_time(self) -> None:
        """
        Check payload for multiple CIKs with knowledge date mode, real time.

        After API v1.7 behaviour is fixed, put
        `self._assert_date_columns_format(payload["metadata"])` and
        `self._assert_corner_date_values()` with recomputed expected
        corner values of "created_at" and "release_date" in the code.
        ```
        """
        payload = self.client.get_form4_payload(
            cik=[320193, 2969],
            start_datetime="2021-01-05T00:00:00",
            end_datetime="2021-01-30T23:59:59",
            date_mode="knowledge_date"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(17, len(payload["metadata"]))
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form4
    def test_form4_zero_cik(self) -> None:
        """
        Check that an error is raised if `cik` is 0.
        """
        with self.assertRaises(pexcep.ParseResponseException):
            self.client.get_form4_payload(cik=0)

    # TODO(*): update after https://github.com/ParticleDev/commodity_research/issues/7488.
    @pytest.mark.form4
    @pytest.mark.slow
    def test_form4_large_response(self) -> None:
        """
        Check payload with big amount of data.

        After API v1.7 behaviour is fixed, put
        `self._assert_date_columns_format(payload["metadata"])` and
        `self._assert_corner_date_values()` with recomputed expected
        corner values of "created_at" and "release_date" in the code.
        """
        payload = self.client.get_form4_payload(
            start_datetime="2020-12-16T00:00:00",
            end_datetime="2020-12-17T23:59:59",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, dict)
        self.assertEqual(6, len(payload))
        self.assertEqual(372, len(payload["metadata"]))
        actual = []
        actual.append(("len(payload)=%s" % len(payload)))
        actual.append(("payload.keys()=%s" % payload.keys()))
        for table_name, data in payload.items():
            actual.append(
                f"payload[{table_name}]=" f"{pprint.pformat(data[:100])}"
            )
        actual = "\n".join(actual)
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form4
    def test_form4_invalid_dates(self) -> None:
        """
        Check that an error is raised if datetime arguments have invalid values.
        """
        with self.assertRaises(pexcep.ParseResponseException):
            self.client.get_form4_payload(
                start_datetime="2020-10-10T00:00:00",
                end_datetime="2020-09-09T23:59:59",
                date_mode="publication_date"
            )

    @pytest.mark.form4
    def test_form4_exclusion_right_boundary(self) -> None:
        """
        Test that the end_datetime value is excluded in Forms 3-4-5.

        With date_mode=="publication_date" we should not receive any data
        filed on the end date, no matter the specified time info.
        """
        # Specified time info is 00:00:00.
        df = self.client.get_form4_payload(
            start_datetime="2018-07-17T00:00:00",
            end_datetime="2018-07-18T00:00:00",
            date_mode="publication_date",
        )["metadata"]
        self.assertEqual(len(df["release_date"].unique()), 1)
        self.assertEqual(df.iloc[0]["release_date"], "2018-07-17T00:00:00")
        self.assertEqual(df.shape[0], 303)
        # Specified time info is not 00:00:00.
        df2 = self.client.get_form4_payload(
            start_datetime="2018-07-17T12:00:00",
            end_datetime="2018-07-18T22:00:00",
            date_mode="publication_date",
        )["metadata"]
        self.assertEqual(len(df2["release_date"].unique()), 1)
        self.assertEqual(df2.iloc[0]["release_date"], "2018-07-17T00:00:00")
        self.assertEqual(df2.shape[0], 303)
        self.assertTrue(df.equals(df2))

    @pytest.mark.form8
    def test_form8_get_payload_precise_sampling(self) -> None:
        """
        Get payload data for forms 4 with all parameters.
        """
        payload = self.client.get_form8_payload(
            cik=18498,
            start_datetime="2020-01-04T00:00:00",
            end_datetime="2020-12-05T00:00:00",
            item="ACT_QUARTER",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.form8
    def test_form8_get_payload_knowledge_date(self) -> None:
        """
        Get payload data for forms 4 with all parameters.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form8_payload(
            cik=320193,
            start_datetime="2020-07-01T00:00:00",
            end_datetime="2020-11-02T00:00:00",
            item="ACT_QUARTER",
            date_mode="knowledge_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.form8
    def test_form8_get_payload_without_cik(self) -> None:
        """
        Get payload data for forms 4.

        Specify all the parameters excluding CIK.
        """
        payload = self.client.get_form8_payload(
            start_datetime="2020-10-04T00:00:00",
            end_datetime="2020-12-05T00:00:00",
            date_mode="publication_date",
            item="ACT_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.form8
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

    @pytest.mark.form8
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

    @pytest.mark.form8
    def test_form8_get_payload_multi_cik_knowledge_date(self) -> None:
        """
        Get payload data for forms 4 with multiple CIKs.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form8_payload(
            cik=[880266, 58492, 320193],
            start_datetime="2020-07-01T00:00:00",
            end_datetime="2020-11-02T00:00:00",
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
            start_datetime="2020-01-04T00:00:00",
            end_datetime="2020-12-05T00:00:00",
            date_mode="publication_date",
            item="QWE_QUARTER",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertTrue(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.form8
    def test_form8_publication_date_filtering(self) -> None:
        """
        Test filtering by `filing_date`, i.e. `date_mode=publication_date`.

        Note: filtered payload contains data for the period [start_datetime, end_datetime).
        """
        payload = self.client.get_form8_payload(
            start_datetime="2021-02-16T00:00:00",
            end_datetime="2021-02-18T20:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.assertEqual(payload.shape[0], 868)
        self.assertGreaterEqual(
            pd.to_datetime(payload["filing_date"].min()),
            pd.to_datetime("2021-02-16T00:00:00")
        )
        self.assertLess(
            pd.to_datetime(payload["filing_date"].max()),
            pd.to_datetime("2021-02-18T20:00:00")
        )

    @pytest.mark.form8
    def test_form8_knowledge_date_filtering(self) -> None:
        """
        Test filtering by `creation_timestamp`, i.e. `date_mode=knowledge_date`.

        Note: filtered payload contains data for the period [start_datetime, end_datetime).
        """
        payload = self.client.get_form8_payload(
            start_datetime="2021-02-17T14:00:00",
            end_datetime="2021-02-17T21:57:38",
            date_mode="knowledge_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        self.assertEqual(payload.shape[0], 235)
        self.assertGreaterEqual(
            pd.to_datetime(payload["creation_timestamp"].min()),
            pd.to_datetime("2021-02-17T14:00:00")
        )
        self.assertLess(
            pd.to_datetime(payload["creation_timestamp"].max()),
            pd.to_datetime("2021-02-17T21:57:38")
        )

    @pytest.mark.form8
    def test_form8_missing_form_publication_timestamps(self) -> None:
        """
        Test missing values for the `form_publication_timestamp`.

        The ideal behaviour is no missing values in this column.
        """
        payload = self.client.get_form8_payload(
            start_datetime="2021-02-01T00:00:00",
            end_datetime="2021-02-22T00:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        missing_mask = payload["form_publication_timestamp"].isna()
        n_missing_publication_ts = payload[missing_mask].shape[0]
        # TODO(*): fix the behaviour in the #7524.
        self.assertEqual(n_missing_publication_ts, 307)

    @pytest.mark.form8
    def test_form8_duplicates(self) -> None:
        """
        Test whether there are duplicates per gvk, item, period of report.

        The ideal behavior is no such duplicates.
        """
        payload = self.client.get_form8_payload(
            start_datetime="2021-02-18T00:00:00",
            end_datetime="2021-02-22T00:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        _LOG.debug("info=\n%s", self._get_df_info(payload))
        dups_mask = payload.duplicated(subset=["gvk", "item_name", "period_of_report"], keep=False)
        # TODO(*): fix the behaviour in the #6279.
        self.assertFalse(payload[dups_mask].empty)

    @pytest.mark.form8
    def test_form8_exclusion_right_boundary(self) -> None:
        """
        Test that the end_datetime value is excluded in Forms 8.

        With date_mode=="publication_date" we should not receive any data
        filed on the end date, no matter the specified time info.
        """
        # Specified time info is 00:00:00.
        df = self.client.get_form8_payload(
            start_datetime="2021-02-03T00:00:00",
            end_datetime="2021-02-04T00:00:00",
            date_mode="publication_date",
        )
        self.assertEqual(len(df["filing_date"].unique()), 1)
        self.assertEqual(df.iloc[0]["filing_date"], "2021-02-03T00:00:00")
        self.assertEqual(df.shape[0], 572)
        # Specified time info is not 00:00:00.
        df2 = self.client.get_form8_payload(
            start_datetime="2021-02-03T12:00:00",
            end_datetime="2021-02-04T22:00:00",
            date_mode="publication_date",
        )
        self.assertEqual(len(df2["filing_date"].unique()), 1)
        self.assertEqual(df2.iloc[0]["filing_date"], "2021-02-03T00:00:00")
        self.assertEqual(df2.shape[0], 572)
        self.assertTrue(df.equals(df2))

    @pytest.mark.form10
    @pytest.mark.superslow("About 1 minute.")
    def test_form10_get_payload(self) -> None:
        """
        Get payload data for forms 10.
        """
        payload = self.client.get_form10_payload(
            cik=320193,
            start_datetime="2017-11-02T00:00:00",
            end_datetime="2017-11-05T00:00:00",
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
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form10
    @pytest.mark.superslow("About 1 minute.")
    def test_form10_get_payload_knowledge_date(self) -> None:
        """
        Get payload data for forms 10.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form10_payload(
            cik=320193,
            start_datetime="2020-10-01T00:00:00",
            end_datetime="2020-11-02T00:00:00",
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
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form10
    @pytest.mark.superslow("About 4 minutes.")
    def test_form10_get_payload_multi_cik(self) -> None:
        """
        Get the payload for form10 and a few cik.
        """
        payload = self.client.get_form10_payload(
            cik=[1750, 732717],
            start_datetime="2018-01-01T00:00:00",
            end_datetime="2018-07-16T00:00:00",
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
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form10
    @pytest.mark.superslow("About 4 minutes.")
    def test_form10_get_payload_multi_cik_knowledge_date(self) -> None:
        """
        Get the payload for form10 and a few cik.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form10_payload(
            cik=[58492, 320193],
            start_datetime="2020-10-01T00:00:00",
            end_datetime="2020-12-02T00:00:00",
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
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.form10
    @pytest.mark.superslow("About 1 minute.")
    def test_form10_get_payload_empty(self) -> None:
        """
        Verify that nothing is returned for an interval without Form 10*.

        https://www.sec.gov/cgi-bin/browse-edgar?CIK=1002910&owner=exclude
        """
        payload = self.client.get_form10_payload(
            cik=1002910,
            start_datetime="2020-05-12T00:00:00",
            end_datetime="2020-05-14T00:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, list)
        self.assertEqual(len(payload), 0)

    @pytest.mark.form13
    def test_form13_get_payload(self) -> None:
        """
        Get payload data for forms 13.
        """
        payload = self.client.get_form13_payload(
            cik=1259313,
            start_datetime="2015-11-16T00:00:00",
            end_datetime="2015-11-17T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_knowledge_date(self) -> None:
        """
        Get payload data for forms 13.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form13_payload(
            cik=1259313,
            start_datetime="2020-11-01T00:00:00",
            end_datetime="2020-12-31T23:59:59",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_multi_cik(self) -> None:
        """
        Get payload data for forms 13.

        Check with a few cik as parameter.
        """
        payload = self.client.get_form13_payload(
            cik=[836372, 859804],
            start_datetime="2015-11-16T00:00:00",
            end_datetime="2015-11-17T00:00:00",
            date_mode='publication_date'
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_multi_cik_knowledge_date(self) -> None:
        """
        Get payload data for forms 13 for multiple CIKs.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form13_payload(
            cik=[836372, 859804],
            start_datetime="2020-11-01T00:00:00",
            end_datetime="2021-01-01T00:00:00",
            date_mode='knowledge_date'
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_cik_cusip(self) -> None:
        """
        Get payload data for forms 13.

        Check for cik and cusip with the same time.
        """
        with self.assertRaises(AssertionError):
            self.client.get_form13_payload(cik=123, cusip="qwe")

    @pytest.mark.form13
    def test_form13_get_payload_with_cusip(self) -> None:
        """
        Get payload data for forms 13.

        Check the cusip filter.
        """
        payload = self.client.get_form13_payload(
            cusip="01449J204",
            start_datetime="2015-11-16T00:00:00",
            end_datetime="2015-11-17T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_with_cusip_knowledge_date(self) -> None:
        """
        Get payload data for forms 13.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form13_payload(
            cusip="002824100",
            start_datetime="2020-12-30T00:00:00",
            end_datetime="2020-12-31T00:00:00",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_with_multi_cusip(self) -> None:
        """
        Get payload data for forms 13 for multiple CUSIPs.
        """
        payload = self.client.get_form13_payload(
            cusip=["002824100", "01449J204"],
            start_datetime="2016-11-15T00:00:00",
            end_datetime="2016-11-16T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_with_multi_cusip_knowledge_date(self) -> None:
        """
        Get payload data for forms 13 for multiple CUSIPs.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form13_payload(
            cusip=["002824100", "928563402"],
            start_datetime="2021-01-01T00:00:00",
            end_datetime="2021-01-06T00:00:00",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    @pytest.mark.slow("Slow Form13 test")
    def test_form13_get_payload_large_response(self) -> None:
        """
        Get payload data for forms 13.

        Check the cusip filter with multiple values.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2020-12-10T00:00:00",
            end_datetime="2020-12-18T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_small_response_no_cusip(self) -> None:
        """
        Get payload data for forms 13.

        Test without filtration.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2020-09-09T00:00:00",
            end_datetime="2020-09-11T00:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, dict)
        unique_uuids = payload["information_table"]["uuid"].unique()
        self.assertEqual(len(unique_uuids), 7)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_irrelevant_cusip(self) -> None:
        """
        Get payload data for forms 13.

        Check for irrelevant cusip.
        """
        payload = self.client.get_form13_payload(
            cusip="ffffffffff",
            start_datetime="2015-11-16T00:00:00",
            end_datetime="2015-11-17T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_check_number1(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2018-01-01T00:00:00",
            end_datetime="2018-01-03T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        unique_uuids = payload["header_data"]["uuid"].unique()
        self.assertEqual(len(unique_uuids), 10)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_check_number2(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2016-04-01T00:00:00",
            end_datetime="2016-04-04T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        unique_uuids = payload["header_data"]["uuid"].unique()
        self.assertEqual(len(unique_uuids), 19)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_check_number3(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms in the December gap.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2020-12-20T00:00:00",
            end_datetime="2020-12-26T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, dict)
        unique_uuids = payload["header_data"]["uuid"].unique()
        self.assertEqual(len(unique_uuids), 27)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_check_number4(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2020-10-01T00:00:00",
            end_datetime="2020-10-04T00:00:00",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_check_number5(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2021-01-15T00:00:00",
            end_datetime="2021-01-16T00:00:00",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, dict)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_get_payload_no_cusip_universe(self) -> None:
        """
        Get payload data for forms 13.

        Check the number of the forms.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2017-06-29T00:00:00",
            end_datetime="2017-07-01T00:00:00",
            date_mode="publication_date",
        )
        unique_ciks = payload["header_data"]["cik"].unique()
        self.assertIsInstance(payload, dict)
        expected = [1591379, 1399770, 1559771, 1569638]
        self.assertCountEqual(unique_ciks, expected)
        self.check_string(
            phunit.convert_df_to_string(payload["information_table"])
        )

    @pytest.mark.form13
    def test_form13_exclusion_right_boundary(self) -> None:
        """
        Test that the end_datetime value is excluded in Forms 13.

        With date_mode=="publication_date" we should not receive any data
        filed on the end date, no matter the specified time info.
        """
        # Specified time info is 00:00:00.
        df = self.client.get_form13_payload(
            start_datetime="2015-12-15T00:00:00",
            end_datetime="2015-12-16T00:00:00",
            date_mode="publication_date",
        )["metadata"]
        self.assertEqual(len(df["release_date"].unique()), 1)
        self.assertEqual(df.iloc[0]["release_date"], "2015-12-15T00:00:00")
        self.assertEqual(df.shape[0], 5)
        # Specified time info is not 00:00:00.
        df2 = self.client.get_form13_payload(
            start_datetime="2015-12-15T12:00:00",
            end_datetime="2015-12-16T22:00:00",
            date_mode="publication_date",
        )["metadata"]
        self.assertEqual(len(df2["release_date"].unique()), 1)
        self.assertEqual(df2.iloc[0]["release_date"], "2015-12-15T00:00:00")
        self.assertEqual(df2.shape[0], 5)
        self.assertTrue(df.equals(df2))

    @pytest.mark.form13
    def test_form13_check_datetime_filter1(self) -> None:
        """
        Get payload data for forms 13.

        Check the time filtering for the historical forms with the specific CUSIPs.
        """
        payload = self.client.get_form13_payload(
            cusip=["75574U101", "64828T201", "89677Y100"],
            start_datetime="2020-08-06T00:00:00",
            end_datetime="2020-08-12T00:00:00",
            date_mode="publication_date",
        )
        actual = sorted(payload["metadata"]["release_date"].unique())
        expected = [
            "2020-08-06T00:00:00",
            "2020-08-07T00:00:00",
            "2020-08-10T00:00:00",
            "2020-08-11T00:00:00",
        ]
        self.assertListEqual(actual, expected)

    @pytest.mark.form13
    def test_form13_check_datetime_filter2(self) -> None:
        """
        Get payload data for forms 13.

        Check the time filtering for the historical forms.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2020-07-20T00:00:00",
            end_datetime="2020-07-26T00:00:00",
            date_mode="publication_date",
        )
        actual = sorted(payload["metadata"]["release_date"].unique())
        expected = [
            "2020-07-20T00:00:00",
            "2020-07-21T00:00:00",
            "2020-07-22T00:00:00",
            "2020-07-23T00:00:00",
            "2020-07-24T00:00:00",
        ]
        self.assertListEqual(actual, expected)

    @pytest.mark.form13
    @pytest.mark.superslow("over 7 minutes.")
    def test_form13_check_datetime_filter3(self) -> None:
        """
        Get payload data for forms 13.

        Check the time filtering for the real-time data.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2021-02-16T00:00:00",
            end_datetime="2021-02-19T00:00:00",
            date_mode="publication_date",
        )
        actual = sorted(payload["metadata"]["release_date"].unique())
        expected = [
            "2021-02-16T00:00:00",
            "2021-02-17T00:00:00",
            "2021-02-18T00:00:00",
        ]
        self.assertListEqual(actual, expected)

    @pytest.mark.form13
    def test_form13_check_datetime_filter4(self) -> None:
        """
        Get payload data for forms 13.

        Check the time filtering by knowledge date.
        """
        payload = self.client.get_form13_payload(
            start_datetime="2020-10-17T15:54:00",
            end_datetime="2020-10-19T17:45:53",
            date_mode="knowledge_date",
        )
        actual_min = payload["metadata"]["created_at"].min()
        expected_min = "2020-10-19T10:01:55.894000"
        actual_max = payload["metadata"]["created_at"].max()
        expected_max = "2020-10-19T17:26:58.571000"
        self.assertEqual(actual_min, expected_min)
        self.assertEqual(actual_max, expected_max)

    def test_form_types(self) -> None:
        """
        Mapping between short form names and form types in the Edgar universe.
        """
        form_types = self.client.form_types
        actual = "\n".join(form_types)
        self.check_string(actual, fuzzy_match=True)

    @pytest.mark.headers
    def test_headers(self) -> None:
        """
        Get form headers metadata with the following parameters.
        """
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193],
            start_datetime="2000-01-01T00:00:00",
            end_datetime="2020-02-02T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.headers
    def test_headers_knowledge_date(self) -> None:
        """
        Get form headers metadata with the following parameters.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193],
            start_datetime="2020-10-01T00:00:00",
            end_datetime="2020-12-01T23:59:59",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.headers
    def test_headers_multi_cik(self) -> None:
        """
        Get form headers metadata with the following parameters.

        Test with multiple CIKs.
        """
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193, 732717],
            start_datetime="2020-01-01T00:00:00",
            end_datetime="2020-01-04T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.headers
    def test_headers_multi_cik_knowledge_date(self) -> None:
        """
        Get form headers metadata with the following parameters.

        Check when `date_mode` is "knowledge_date".
        """
        payload = self.client.get_form_headers(
            form_type=["3", "3/A", "4", "4/A", "5", "5/A"],
            cik=[320193, 732717],
            start_datetime="2020-10-01T00:00:00",
            end_datetime="2020-11-02T00:00:00",
            date_mode="knowledge_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.headers
    def test_headers_without_cik(self) -> None:
        """
        Get form headers metadata.

        Test without cik parameter.
        """
        payload = self.client.get_form_headers(
            form_type="13F-HR",
            start_datetime="2020-03-01T00:00:00",
            end_datetime="2020-10-11T00:00:00",
            date_mode="publication_date"
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        payload.sort_values(['form_type', 'filing_date', 'cik', 'uuid'],
                            inplace=True)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.headers
    def test_headers_one_form(self) -> None:
        """
        Get form headers metadata.

        Test with one form.
        """
        payload = self.client.get_form_headers(
            form_type="13F-HR",
            cik=1404574,
            start_datetime="2012-11-14T00:00:00",
            end_datetime="2012-11-15T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.headers
    def test_headers_one_cik(self) -> None:
        """
        Get form headers metadata.

        Test with one cik.
        """
        payload = self.client.get_form_headers(
            form_type="13F-HR",
            cik=1404574,
            start_datetime="2012-11-14T00:00:00",
            end_datetime="2012-11-15T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.headers
    def test_headers_bad_form_types(self) -> None:
        """
        Get form headers with bad form types.

        Test with one cik.
        """
        with self.assertRaises(AssertionError):
            self.client.get_form_headers(
                form_type=["13F-HR", "13-B", "178", "99"],
                cik=1404574,
            )

    @pytest.mark.headers
    def test_headers_without_form_type(self) -> None:
        """
        Get form headers metadata.

        Test with one cik.
        """
        payload = self.client.get_form_headers(
            cik=1404574,
            start_datetime="2012-11-14T00:00:00",
            end_datetime="2012-11-15T00:00:00",
            date_mode="publication_date",
        )
        self.assertIsInstance(payload, pd.DataFrame)
        self.assertFalse(payload.empty)
        self.check_string(phunit.convert_df_to_string(payload))

    @pytest.mark.headers
    def test_headers_length(self):
        """
        Compares output length with list of cik and
        without cik (filtered later)
        """
        from_dttm = '2019-11-01T00:00:00'
        to_dttm = '2019-12-01T00:00:00'
        # Get sp1500 cik list from the file.
        sp1500_cik_list = peutil.get_sp1500_cik_list()
        # Get headers for the given period.
        headers_p1 = self.client.get_form_headers(
            form_type=['4'],
            start_datetime=from_dttm,
            end_datetime=to_dttm,
            date_mode='publication_date'
        )
        # Get headers for the given period filtered by
        # first 400 cik in the sp1500 universe.
        headers_p2 = self.client.get_form_headers(
            form_type=['4'],
            cik=sp1500_cik_list[:400],
            start_datetime=from_dttm,
            end_datetime=to_dttm,
            date_mode='publication_date'
        )
        # Get the length of headers, filtered on a client side.
        client_headers_len = len(headers_p1[headers_p1['cik'].apply(
            lambda x: x in sp1500_cik_list[:400])])
        # Get the length of headers, filtered on a server side.
        server_headers_len = len(headers_p2)
        self.assertEqual(client_headers_len, server_headers_len)

    @pytest.mark.headers
    def test_headers_exclusion_right_boundary(self) -> None:
        """
        Test that the end_datetime value is excluded in Headers.

        With date_mode=="publication_date" we should not receive any data
        filed on the end date, no matter the specified time info.
        """
        # Specified time info is 00:00:00.
        df = self.client.get_form_headers(
            start_datetime="2020-08-10T00:00:00",
            end_datetime="2020-08-11T00:00:00",
            date_mode="publication_date",
        )
        self.assertEqual(len(df["filing_date"].unique()), 1)
        self.assertEqual(df.iloc[0]["filing_date"], "2020-08-10")
        self.assertEqual(df.shape[0], 2968)
        # Specified time info is not 00:00:00.
        df2 = self.client.get_form_headers(
            start_datetime="2020-08-10T12:00:00",
            end_datetime="2020-08-11T22:00:00",
            date_mode="publication_date",
        )
        self.assertEqual(len(df2["filing_date"].unique()), 1)
        self.assertEqual(df2.iloc[0]["filing_date"], "2020-08-10")
        self.assertEqual(df2.shape[0], 2968)
        self.assertTrue(df.equals(df2))

    @staticmethod
    def _get_df_info(df: pd.DataFrame) -> str:
        ret = []
        for col_name in ["ticker", "item_name", "filing_date"]:
            vals = sorted(df[col_name].unique().astype(str))
            ret.append("col_name=(%d) %s" % (len(vals), ", ".join(vals)))
        return "\n".join(ret)
