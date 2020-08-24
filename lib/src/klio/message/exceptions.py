# Copyright 2019 Spotify AB


class KlioMessageException(Exception):
    """General KlioMessage exception."""


class KlioMessagePayloadException(KlioMessageException):
    """Error handling payload for a KlioMessage."""
