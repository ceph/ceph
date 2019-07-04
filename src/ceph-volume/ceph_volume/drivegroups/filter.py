# -*- coding: utf-8 -*-

import logging
from .matchers import SubstringMatcher, AllMatcher, SizeMatcher, EqualityMatcher
from ceph_volume import terminal

mlogger = terminal.MultiLogger(__name__)
logger = logging.getLogger(__name__)


class Filter(object):
    """ Filter class to assign properties to bare filters.

    This is a utility class that tries to simplify working
    with information comming from a textfile (drive_group.yaml)

    """

    def __init__(self, **kwargs):
        self.name: str = str(kwargs.get('name', None))
        self.matcher = kwargs.get('matcher', None)
        self.value: str = str(kwargs.get('value', None))
        self._assign_matchers()
        mlogger.debug("Initializing filter <{}> with value <{}>".format(
            self.name, self.value))

    @property
    def is_matchable(self) -> bool:
        """ A property to indicate if a Filter has a matcher

        Some filter i.e. 'limit' or 'osd_per_device' are valid filter
        attributes but cannot be applied to a disk set. In this case
        we return 'None'
        :return: If a matcher is present True/Flase
        :rtype: bool
        """
        return self.matcher is not None

    def _assign_matchers(self) -> None:
        """ Assign a matcher based on filter_name

        This method assigns an individual Matcher based
        on `self.name` and returns it.
        """
        if self.name == "size":
            self.matcher = SizeMatcher(self.name, self.value)
        elif self.name == "model":
            self.matcher = SubstringMatcher(self.name, self.value)
        elif self.name == "vendor":
            self.matcher = SubstringMatcher(self.name, self.value)
        elif self.name == "rotational":
            self.matcher = EqualityMatcher(self.name, self.value)
        elif self.name == "all":
            self.matcher = AllMatcher(self.name, self.value)
        else:
            logger.debug(f"No suitable matcher for <{self.name}> could be found.")

    def __repr__(self) -> str:
        """ Visual representation of the filter
        """
        return 'Filter<{}>'.format(self.name)


class FilterGenerator(object):
    def __init__(self, device_filter):
        self.device_filter = device_filter

    def __iter__(self):
        for name, val in self.device_filter.items():
            yield Filter(name=name, value=val)
