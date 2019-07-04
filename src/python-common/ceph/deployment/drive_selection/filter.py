# -*- coding: utf-8 -*-

import logging

from ceph.deployment.drive_group import DeviceSelection

try:
    from typing import Optional, Generator
except ImportError:
    pass

from .matchers import Matcher, SubstringMatcher, AllMatcher, SizeMatcher, EqualityMatcher

logger = logging.getLogger(__name__)


class Filter(object):
    """ Filter class to assign properties to bare filters.

    This is a utility class that tries to simplify working
    with information comming from a textfile (drive_group.yaml)

    """

    def __init__(self,
                 name,  # type: str
                 matcher,  # type: Optional[Matcher]
                 ):
        self.name = str(name)
        self.matcher = matcher
        logger.debug("Initializing {} filter <{}>".format(
            self.matcher.__class__.__name__, self.name))

    @property
    def is_matchable(self):
        # type: () -> bool
        """ A property to indicate if a Filter has a matcher

        Some filter i.e. 'limit' or 'osd_per_device' are valid filter
        attributes but cannot be applied to a disk set. In this case
        we return 'None'
        :return: If a matcher is present True/Flase
        :rtype: bool
        """
        return self.matcher is not None

    def __repr__(self):
        """ Visual representation of the filter
        """
        return 'Filter<{}>'.format(self.name)


class FilterGenerator(object):
    def __init__(self, device_filter):
        # type: (DeviceSelection) -> None
        self.device_filter = device_filter

    def __iter__(self):
        # type: () -> Generator[Filter, None, None]
        if self.device_filter.size:
            yield Filter('size', SizeMatcher('size', self.device_filter.size))
        if self.device_filter.model:
            yield Filter('model', SubstringMatcher('model', self.device_filter.model))
        if self.device_filter.vendor:
            yield Filter('vendor', SubstringMatcher('vendor', self.device_filter.vendor))
        if self.device_filter.rotational is not None:
            val = '1' if self.device_filter.rotational else '0'
            yield Filter('rotational', EqualityMatcher('rotational', val))
        if self.device_filter.all:
            yield Filter('all', AllMatcher('all', str(self.device_filter.all)))
