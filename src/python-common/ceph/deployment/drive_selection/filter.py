# -*- coding: utf-8 -*-

import logging

from ceph.deployment.drive_group import DeviceSelection

try:
    from typing import Generator
except ImportError:
    pass

from .matchers import Matcher, SubstringMatcher, AllMatcher, SizeMatcher, EqualityMatcher

logger = logging.getLogger(__name__)


class FilterGenerator(object):
    def __init__(self, device_filter: DeviceSelection) -> None:
        self.device_filter = device_filter

    def __iter__(self) -> Generator[Matcher, None, None]:
        if self.device_filter.actuators:
            yield EqualityMatcher('actuators', self.device_filter.actuators)
        if self.device_filter.size:
            yield SizeMatcher('size', self.device_filter.size)
        if self.device_filter.model:
            yield SubstringMatcher('model', self.device_filter.model)
        if self.device_filter.vendor:
            yield SubstringMatcher('vendor', self.device_filter.vendor)
        if self.device_filter.rotational is not None:
            val = '1' if self.device_filter.rotational else '0'
            yield EqualityMatcher('rotational', val)
        if self.device_filter.all:
            yield AllMatcher('all', str(self.device_filter.all))
