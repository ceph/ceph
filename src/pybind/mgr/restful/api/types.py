from __future__ import absolute_import

import re
import abc
import six
import json

from pecan import request


@six.add_metaclass(abc.ABCMeta)
class APIBody(object):

    def __init__(self):
        self._declare = dict()
        for attr in dir(self):
            if attr.startswith("_"):
                continue
            val = getattr(self, attr)
            if isinstance(val, APIType):
                self._declare.setdefault(attr, val)

    @classmethod
    def from_request(cls):
        """
        parse the request body into a python object

        :raise ValueError
        """
        try:
            body = json.loads(request.body)
        except json.decoder.JSONDecodeError:
            raise ValueError("Bad request format")
        self = cls()
        for arg, declare in self._declare.items():
            v = body.pop(arg, None)
            try:
                value = declare(v)
            except ValueError as err:
                raise ValueError("%s: %s" % (arg, err))
            setattr(self, arg, value)

        return self


@six.add_metaclass(abc.ABCMeta)
class APIType(object):

    def __init__(self, default=None, require=False):
        self.default = default
        self.require = require

    def __call__(self, value):
        return self.convert(value)

    def convert(self, value):
        if not value and self.default:
            value = self.default

        if self.require and not value:
            raise ValueError("Missing required argument")

        return self._convert(value)

    @abc.abstractmethod
    def _convert(self, value):
        """
        validate input and return target type, subclasses are responsible
        for implementation. if input is illegal, raise ValueError
        :param value: any
        :raise ValueError
        :return: value
        """
        return value


class StringType(APIType):

    def __init__(self, min_length=None, max_length=None, default=None, require=False, pattern=None):
        super(StringType, self).__init__(default, require)
        self.min_length = min_length
        self.max_length = max_length
        if pattern:
            self.pattern = re.compile(pattern)
        else:
            self.pattern = re.compile(".*")

    def _convert(self, value):
        if not value:
            return ""

        if self.min_length and len(value) < self.min_length:
            raise ValueError("The value should be greater than minimum length %s" % self.min_length)

        if self.max_length and len(value) > self.max_length:
            raise ValueError("The value should be less than maximum length %s" % self.max_length)

        if not self.pattern.search(value):
            raise ValueError("The value should match the pattern: %s" % self.pattern.pattern)

        return str(value)


class IntegerType(APIType):

    def __init__(self, minimum=None, maximum=None, default=None, require=False):
        super(IntegerType, self).__init__(default, require)
        self.minimum = minimum
        self.maximum = maximum

    def _convert(self, value):
        if not value:
            return 0

        value = int(value)
        if self.minimum and value < self.minimum:
            raise ValueError("Value should be greater than minimum %s" % self.minimum)

        if self.maximum and value > self.maximum:
            raise ValueError("Value should be less than maximum %s" % self.maximum)

        return value
