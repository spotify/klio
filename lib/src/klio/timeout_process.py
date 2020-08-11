# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

from __future__ import absolute_import

import multiprocessing
import traceback
import types


class TimeoutProcess(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None
        self._return_value = None

    def saferun(self):
        if self._target:
            return self._target(*self._args, **self._kwargs)

    def run(self):
        try:
            r_value = self.saferun()
            if isinstance(r_value, types.GeneratorType):
                r_value = next(r_value)
            self._cconn.send(r_value)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            raise e

    @property
    def exception(self):
        if self.exitcode != 0:
            if self._pconn.poll():
                self._exception = self._pconn.recv()
        return self._exception

    @property
    def return_value(self):
        if self.exitcode == 0:
            if self._pconn.poll():
                self._return_value = self._pconn.recv()
        return self._return_value
