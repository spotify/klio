# Copyright 2019 Spotify AB


class KlioMessageException(Exception):
    """General KlioMessage exception."""


# [batch dev] TODO: maybe add KlioMessage as an attribute here
class KlioVersionMismatch(KlioMessageException):
    """KlioMessage version does not match job's configured version."""


class KlioMessagePayloadException(KlioMessageException):
    """Error handling payload for a KlioMessage."""
