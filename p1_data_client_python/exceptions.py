"""P1 Data API exceptions.

Import as

import p1_data_client_python.exceptions as p1_exc
"""


class UnauthorizedException(Exception):
    pass


class BadMetaDataTypeException(Exception):
    pass


class ParseResponseException(Exception):
    pass


class CastException(Exception):
    pass


class TestTokenNotFound(Exception):
    pass
