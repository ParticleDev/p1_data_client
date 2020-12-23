"""
Contains methods for form8 precision/recall computation.

Import as:

import p1_data_client_python.form8_statistics as p1_stats
"""
import datetime
import itertools
import logging
from typing import Any, Optional, Tuple, Union

import helpers.dbg as dbg
import pandas as pd
import tqdm.auto as tqdm

_LOG = logging.getLogger(__name__)


class CompustatValidator:
    """
    Compute precision and recall for the target valid matches.
    """

    def __init__(self, error_threshold: float = 0.01) -> None:
        """
        Initialize the parameters.

        :param error_threshold: comparison error margin, e.g. 0.01 (1%)
        :return:
        """
        self.error_threshold = error_threshold
        dbg.dassert_lte(0.0, error_threshold)
        dbg.dassert_lte(error_threshold, 1.0)

    def compute_recall_data(
        self, precision_data: pd.DataFrame, compustat_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Check whether Compustat values are found in Particle.One matches data.

        :param precision_data: a DataFrame with the extracted values and
            the Compustat validation results
        :param compustat_df: a DataFrame with Compustat data
        :return: a DataFrame with Compustat data and 2 new columns:
            "p1_item_value": P1 extracted value for gvkey, item, period_of_report
            "is_in_p1": True, if Compustat value is found in P1 data, False otherwise
        """
        # Process the Compustat data.
        processed_compustat_df = self._process_compustat_df(compustat_df)
        # Keep only matches that pass the Compustat validation.
        matches_df = precision_data.loc[
            precision_data.is_in_compu.fillna(False)
        ].copy()
        # Select the relevant columns.
        p1_cols_to_keep = [
            "gvkey",
            "item_name",
            "period_of_report",
            "item_value",
            "compu_value",
        ]
        matches_df = matches_df[p1_cols_to_keep]
        # Drop duplicated records.
        compu_df_unique = self._remove_duplicates(processed_compustat_df)
        # Check whether Compustat values are found in Particle.One extracted values.
        merge_cols = ["gvkey", "item_name", "period_of_report"]
        recall_data = pd.merge(
            compu_df_unique,
            matches_df,
            how="left",
            on=merge_cols,
            indicator="is_in_p1",
        )
        indicator_name_mapping = {
            "left_only": False,
            "both": True,
        }
        recall_data["is_in_p1"] = recall_data["is_in_p1"].map(
            indicator_name_mapping
        )
        # Rename the columns.
        recall_data = recall_data.rename(
            columns={
                "item_value_x": "compu_item_value",
                "item_value_y": "p1_item_value",
                "compu_value": "compu_value_matched",
            }
        )
        # If the Compustat value is in P1 data, keep the Compustat value that
        # was matched to P1 value with 1% precision, otherwise keep the Compustat
        # value with the earliest effdate.
        recall_data["compu_item_value"] = recall_data.apply(
            lambda row: row["compu_value_matched"]
            if not pd.isna(row["compu_value_matched"])
            else row["compu_item_value"],
            axis=1,
        )
        # Drop the column as it is not needed anymore.
        recall_data = recall_data.drop("compu_value_matched", axis=1)
        return recall_data

    def compute_precision_data(
        self,
        matches_df: pd.DataFrame,
        compustat_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Compare all matches to Compustat values.

        :param matches_df: a DataFrame with valid matches
        :param compustat_df: a DataFrame with Compustat data
        :return: a DataFrame with valid matches and 2 new columns:
            - "is_in_compu": True, if extracted value matches a Compustat value, False otherwise
            - "compu_value": Compustat value matched
        """
        # Process the Compustat data.
        processed_compustat_df = self._process_compustat_df(compustat_df)
        # Convert to date format to make dates comparable.
        matches_df["period_of_report"] = pd.to_datetime(
            matches_df["period_of_report"]
        ).dt.date
        # Iterate over rows.
        checks = []
        compu_values = []
        for _, row in tqdm.tqdm(matches_df.iterrows()):
            gvkey = row["gvkey"]
            item = row["item_name"]
            value = row["item_value"]
            period_of_report = row["period_of_report"]
            if pd.isna(period_of_report):
                # Store None if there is no period of report.
                _LOG.debug(
                    "No period of report found, storing None "
                    "for the given gvkey %s, item %s, value %s",
                    gvkey,
                    item,
                    value,
                )
                check = None
                compu_value = None
            else:
                # Try to find values in Compustat.
                (check, compu_value,) = self._check_value_against_compustat(
                    processed_compustat_df,
                    gvkey,
                    item,
                    period_of_report,
                    value,
                )
            checks.append(check)
            compu_values.append(compu_value)
        # Store the check results as the DataFrame columns.
        matches_df = matches_df.assign(
            is_in_compu=checks,
            compu_value=compu_values,
        )
        return matches_df

    def _check_value_against_compustat(
        self,
        compustat_df: pd.DataFrame,
        gvkey: Union[str, int],
        item: str,
        period_of_report: datetime.date,
        value: float,
    ) -> Tuple[int, Optional[bool]]:
        """
        Compare extracted value with Compustat values.

        Note:
            - Rescaled extracted values are considered as well
            - Compare values with the precision specified in the constructor

        :param compustat_df: a DataFrame with Compustat data
        :param gvkey: the Compustat unique company identifier
        :param item: financial item, e.g. "NIQ" - net income
        :param period_of_report: reporting period for the corresponding value
        :param value: value that was extracted from a form
        :return: tuple(is_in_compu, compu_value), where
            - is_in_compu: True if a value from a form is in Compustat, False otherwise
            - compu_value: a form value from Compustat if there is one, None otherwise
        """
        gvkey = int(gvkey)
        compustat_df_filtered = self._filter_compustat_df(
            compustat_df, gvkey, item, period_of_report
        )
        if compustat_df_filtered.empty:
            # Store None if there is no Compustat data for the given
            # gvkey, item and period of report.
            _LOG.debug(
                "No Compustat data found, storing None "
                "for the given gvkey %s, item %s and period_of_report %s",
                gvkey,
                item,
                period_of_report,
            )
            is_in_compu = False
            compu_value = None
            return is_in_compu, compu_value
        # Get all Compustat values.
        compu_values = compustat_df_filtered["item_value"].to_list()
        # Look for rescaled values as well, i.e. value*1000 and value/1000.
        extracted_value_candidates = [value, value * 1000, value / 1000]
        # Compare each value with each Compustat value.
        for extracted_value, curr_compu_value in itertools.product(
            extracted_value_candidates, compu_values
        ):
            is_in_compu = self._compare_values(extracted_value, curr_compu_value)
            if is_in_compu:
                # Store the the matched Compustat value.
                compu_value = curr_compu_value
                return is_in_compu, compu_value
        _LOG.debug(
            "No match with Compustat found, storing the earliest Compustat value "
            "for the given gvkey %s, item %s and period_of_report %s",
            gvkey,
            item,
            period_of_report,
        )
        is_in_compu = False
        # Get the earliest value from Compustat using effdate.
        compu_earliest_value = compustat_df_filtered["item_value"].iloc[0]
        # Store the earliest Compustat value.
        compu_value = compu_earliest_value
        return is_in_compu, compu_value

    def _compare_values(self, actual_value: float, ref_value: float) -> bool:
        """
        Compare values with the precision specified in the constructor.

        :param actual_value: value to compare
        :param ref_value: value to compare with
        :return: True if the relative difference between values is smaller
            than or equal to the error threshold, False otherwise
        """
        # Use epsilon to avoid division by zero.
        epsilon = 10 ** -8
        # Take absolute ref_value to correctly compare negative values.
        if abs(ref_value) < epsilon:
            relative_diff = abs(ref_value - actual_value)
        else:
            relative_diff = abs(ref_value - actual_value) / abs(ref_value)
        # If the relative error is smaller than or equal to the threshold,
        # then the values are considered equal.
        is_equal = relative_diff <= self.error_threshold
        return is_equal

    def _process_compustat_df(self, compustat_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the Compustat data before the validation.

        Processing include:
            - items renaming
            - reporting dates to date conversion
            - gvkeys to int conversion
            - columns renaming
            - sorting

        :param compustat_df: a DataFrame with Compustat data
        :return: a DataFrame with processed Compustat data
        """
        # Copy in order not to modify the original df.
        compustat_df = compustat_df.copy()
        # Rename the items to match the valid matches item name pattern.
        # Example: "NIQ" - > "NI_QUARTER".
        compustat_df["item"] = compustat_df["item"].apply(self.rename_item)
        # Convert to date to match the valid matches period of report pattern.
        compustat_df["datadate"] = (
            pd.to_datetime(compustat_df["datadate"], utc=True)
            .dt.tz_convert("US/Eastern")
            .dt.date
        )
        # Convert to int to make values comparable.
        compustat_df["gvkey"] = compustat_df["gvkey"].apply(int)
        # Rename the columns in order to have the same names.
        compustat_df = compustat_df.rename(
            columns={
                "datadate": "period_of_report",
                "item": "item_name",
                "valuei": "item_value",
            }
        )
        # Sort values to use the binary search.
        processed_compustat_df = compustat_df.sort_values(
            by=["gvkey", "item_name", "period_of_report", "effdate"]
        ).reset_index(drop=True)
        return processed_compustat_df

    def _filter_compustat_df(
        self,
        compustat_df: pd.DataFrame,
        gvkey: Union[str, int],
        item: str,
        period_of_report: datetime.date,
    ) -> pd.DataFrame:
        """
        Filter Compustat data by GVKEY, item and period of report.

        :param compustat_df: a DataFrame with Compustat data
        :param gvkey: a company's unique identifier in Compustat to filter by
        :param item: financial item to filter by
        :param period_of_report: reporting period to filter by
        :return: a DataFrame with Compustat data filtered by GVKEY, item and period of report
        """
        # Filter the Compustat data by the gvkey.
        compustat_df_for_gvkey = self._filter_df(compustat_df, "gvkey", gvkey)
        # Filter the Compustat data by the financial item.
        compustat_df_for_gvkey_item = self._filter_df(
            compustat_df_for_gvkey, "item_name", item
        )
        # Filter the Compustat data by the reporting period.
        compustat_df_filtered = self._filter_df(
            compustat_df_for_gvkey_item, "period_of_report", period_of_report
        )
        return compustat_df_filtered

    def compute_statistics(
        self,
        precision_data: pd.DataFrame,
        recall_data: pd.DataFrame,
        print_stats: bool = True,
    ) -> str:
        """
        Compute the high-level statistics about the precision and recall.

        :param precision_data: a DataFrame that is used to compute precision,
            see "compute_precision_data()"
        :param recall_data: a DataFrame that is used to compute recall,
            see "compute_recall_data()"
        :param print_stats: whether to display the statistics or not
        :return: a text statement about precision and recall for target matches
        """
        # Compute the precision stats.
        if precision_data.empty:
            precision_statement = (
                "There is no financial data to compute precision for"
            )
        else:
            # Compute the total number of extracted values that pass
            # the Compustat check as percentage.
            n_valid_values = precision_data["is_in_compu"].sum()
            precision = self.perc(n_valid_values, precision_data.shape[0])
            precision_statement = f"The precision is {precision}"
        # Compute the recall stats.
        if recall_data.empty:
            recall_statement = "There is no financial data to compute recall for"
        else:
            # Compute the number of extracted financial values from
            # Compustat that are found in Particle.One matches data.
            is_in_p1_values = recall_data["is_in_p1"].sum()
            recall = self.perc(is_in_p1_values, recall_data.shape[0])
            recall_statement = f"The recall is {recall}"
        # Combine the stats in one statement.
        overall_stats = "\n".join([precision_statement, recall_statement])
        if print_stats:
            # Display the statistics.
            _LOG.info(overall_stats)
        return overall_stats

    @staticmethod
    def _filter_df(df: pd.DataFrame, col_name: str, value: Any) -> pd.DataFrame:
        """
        Filter a DataFrame by a column's value.

        :param df: a DataFrame to filter
        :param col_name: column name for a column that stores a value to filter by
        :param value: a value to filter by
        :return: a DataFrame filtered by a column's value
        """
        # Look for the slice [value_idx_min, value_idx_max] that stores all the data
        # for the given value.
        value_idx_min = df[col_name].searchsorted(value, side="left")
        value_idx_max = df[col_name].searchsorted(value, side="right")
        df_filtered = df.iloc[value_idx_min:value_idx_max, :].reset_index(
            drop=True
        )
        return df_filtered

    @staticmethod
    def _remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicates from the data.

        :param data: financial data from Compustat or Particle.One
        :return: financial data from Compustat or Particle.One without duplicates
        """
        # Keep only relevant columns.
        cols = ["gvkey", "item_name", "period_of_report", "item_value"]
        data = data[cols]
        # Remove duplicated records.
        dup_cols = [col for col in cols if col != "item_value"]
        data_unique = data.drop_duplicates(subset=dup_cols).reset_index(drop=True)
        # Sort for stability.
        data_unique_sorted = data_unique.sort_values(by=dup_cols)
        return data_unique_sorted

    @staticmethod
    def rename_item(item: str) -> str:
        """
        Replace the -Q/Y ending with _QUARTER/YEAR.

        :param item: the original item name
        :return: the item name with the ending replaced
        """
        if item.endswith("Q"):
            # Replace -Q with _QUARTER.
            item = item.rstrip("Q") + "_QUARTER"
        elif item.endswith("Y"):
            # Replace -Y with _YEAR.
            item = item.rstrip("Y") + "_YEAR"
        return item

    @staticmethod
    def perc(
        a: float,
        b: float,
        only_perc: bool = False,
        invert: bool = False,
        num_digits: int = 2,
        use_thousands_separator: bool = False,
    ) -> str:
        """
        Calculate percentage a / b as a string.

        Asserts 0 <= a <= b. If true, returns a/b to `num_digits` decimal places.

        :param a: numerator
        :param b: denominator
        :param only_perc: return only the percentage, without the original numbers
        :param invert: assume the fraction is (b - a) / b
            This is useful when we want to compute the complement of a count.
        :param num_digits: the number of digits to display
        :param use_thousands_separator: report the numbers using thousands separator
        :return: string with a/b
        """
        dbg.dassert_lte(0, a)
        dbg.dassert_lte(a, b)
        if use_thousands_separator:
            a_str = str("{0:,}".format(a))
            b_str = str("{0:,}".format(b))
        else:
            a_str = str(a)
            b_str = str(b)
        if invert:
            a = b - a
        dbg.dassert_lte(0, num_digits)
        if only_perc:
            fmt = "%." + str(num_digits) + "f%%"
            ret = fmt % (float(a) / b * 100.0)
        else:
            fmt = "%s / %s = %." + str(num_digits) + "f%%"
            ret = fmt % (a_str, b_str, float(a) / b * 100.0)
        return ret
