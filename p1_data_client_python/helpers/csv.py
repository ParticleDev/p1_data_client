import logging

import helpers.dbg as dbg
import pandas as pd
import ast

_LOG = logging.getLogger(__name__)


def from_typed_csv(file_name: str) -> pd.DataFrame:
    """
    Loads csv file into dataframe and applies the original types of columns,
    in order to open csv in a proper way.

    As a file, which contains types format, it is used 'file_name.types' file,
    if it's exist.

    :param file_name: name of file, which is need to be converted into dataframe
    :return pd.DataFrame: dataframe of pandas format.
    """
    dtypes_filename = file_name + '.types'
    dbg.dassert_exists(dtypes_filename)
    dtypes_file = open(dtypes_filename)
    dtypes_dict = ast.literal_eval(list(dtypes_file)[0])
    df = pd.read_csv(file_name, dtype=dtypes_dict)
    return df
