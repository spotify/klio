# -*- coding: utf-8 -*-
#
# Copyright 2020 Spotify AB


class KlioConfigTemplatingException(Exception):
    """Exception for missing keys when overriding
    klio config parameters
    """

    def __init__(self, key):
        message = "{} missing in key template overrides.".format(key)
        super().__init__(message)
