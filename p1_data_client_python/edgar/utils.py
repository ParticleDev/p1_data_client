"""
Utils for the Edgar API.

Import as: import p1_data_client_python.edgar.utils as peutil
"""
import contextlib as contex
import logging
import urllib.parse as uparse
from typing import Any, Dict, Generator, Iterator, List, Optional, Union

import halo
from edgar.config import DATE_MODE
from helpers import dbg as phdbg

_LOG = logging.getLogger(__name__)


class Links:
    """
    Helpers for the parsing and storing links info.
    """

    def __init__(self, links: Dict[str, Union[int, str]]):
        """
        :param links: Dict with links.
        """
        self.self_link = links["self"]
        self.self_params = uparse.parse_qs(
            uparse.urlparse(self.self_link).query
        )
        self.current_offset = int(self.self_params.get("offset", [0])[0])
        self.has_next_link = "next" in links
        self.next_url = links.get("next")


@contex.contextmanager
def spinner_exception_handling(
    spinner: Union[halo.Halo, halo.HaloNotebook]
) -> Generator:
    """
    Stop a spinner if exceptions happens.

    :param spinner: Spinner object.
    """
    try:
        yield
    finally:
        spinner.stop()


def chop_list(
    lst: List[Union[int, str]], n: int
) -> Iterator[List[Union[int, str]]]:
    """
    List to chop in pieces.

    :param lst: List for chopping.
    :param n: Size of chunk.
    :return: Chunk.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def check_date_mode(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_mode: Optional[str] = None,
) -> None:
    """
    Validate the combination of dates and date_mode field.

    :param start_date: Start date from the client class method.
    :param end_date: Ending date from the client class method.
    :param date_mode: Date mode from the client class method.
    :return:
    """
    if date_mode and not any([start_date, end_date]):
        phdbg.dfatal(
            "The date_mode parameter has to be used with "
            "start_date and end_date."
        )
    if any([start_date, end_date]):
        if date_mode is None:
            phdbg.dfatal(
                "You need to specify date_mode parameter "
                "when you give start/end date."
            )
        if date_mode not in DATE_MODE:
            phdbg.dfatal(f"The date_mode parameter has to be " f"{DATE_MODE}")


def check_form_type(
    form_type: Optional[Union[List[str], str]], valid_types: List[str]
) -> None:
    """
    Make sure that the form types in form_type are valid according to
    valid_form_types.

    :param form_type: Parameter to check.
    :param valid_types: Catalog of the form types.
    """
    phdbg.dassert_isinstance(valid_types, list)
    if form_type is None:
        return
    if not isinstance(form_type, list):
        form_type = [form_type]
    form_diff = set(form_type) - set(valid_types)
    if len(form_diff) > 0:
        phdbg.dfatal(
            f"Form types {form_diff} is not allowed. "
            f"Check list of the allowed form types: "
            f"{valid_types}"
        )


def check_sorted_unique_param(
    name: str, value: Union[list, Any]
) -> Union[list, Any]:
    """
    Check an argument for duplicates.

    :param name: Name of parameter.
    :param value: Value of parameter. If list then check for duplicated.
    :return: Processed value.
    """
    sorted_unique_value = value
    if isinstance(value, list):
        sorted_unique_value = sorted(list(set(value)))
        if len(sorted_unique_value) < len(value):
            _LOG.warning(
                "Some values: %s in the %s parameter, are duplicated.",
                value,
                name,
            )
    return sorted_unique_value


def get_next_step_size(total: int, block_size: int, current_offset: int) -> int:
    """
    Calculate next size of step for a TQDM progress-bar.

    :param total:
    :param block_size:
    :param current_offset:
    :return:
    """
    if current_offset + block_size > total:
        step_size = total - current_offset
    else:
        step_size = current_offset
    return step_size
