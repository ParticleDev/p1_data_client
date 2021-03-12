"""
Store constants for P1 Edgar Data REST API wrapper.

Import as: import p1_data_client_python.edgar.config as peconf
"""

import os
from typing import Any, Dict, List, Union

# Current Data API version that fit current p1 Python client.
CURRENT_EDGAR_DATA_API_VERSION = "7"
# Number of items (CIK, CUSIP) in each request to the server.
# We chunk the items to avoid creating query URL that are too long.
ITEM_BLOCK_SIZE = 500
# List of possible options for date_mode parameter.
# Used to point what type of the field have to be used
# when start_date/end_date given.
DATE_MODE = ["publication_date", "knowledge_date"]
# Field types for form8 cast.
FORM8_FIELD_TYPES = {
    "gvk": "int64",
    "item_value": "float64",
}
# Date fields in form8 for cast.
FORM8_DATE_FIELDS = [
    "form_publication_timestamp",
    "filing_date",
    "compustat_timestamp",
    "period_of_report",
    "creation_timestamp",
]
# Mapping between short form names and form types in the Edgar universe.
FORM_NAMES_TYPES = {
    "form4": ["3", "3/A", "4", "4/A", "5", "5/A"],
    "form8": ["8-K", "8-K/A"],
    "form10": ["10-K", "10-K/A", "10-Q", "10-Q/A"],
    "form13": ["13F-HR", "13F-HR/A"],
}
# Edgar's data API constant passes to Python client class.
P1_EDGAR_DATA_API_VERSION = os.environ.get("P1_EDGAR_DATA_API_VERSION",
                                           CURRENT_EDGAR_DATA_API_VERSION)
# Number of payload in each request to the server.
# This depends on the size of each form.
# E.g., `headers` are typically small and fast to retrieved by the backend,
# while `form4_13` are larger and slower.
PAYLOAD_BLOCK_SIZE = {
    "headers": 1000,
    "form4_13": 500,
    "form8": 1000,
}
P1_CIK = int
P1_GVK = int
SERVER_RESPONSE_TYPE = Dict[str, Any]
CIK_TYPE = Union[P1_CIK, List[P1_CIK]]
CUSIP_TYPE = Union[str, List[str]]
