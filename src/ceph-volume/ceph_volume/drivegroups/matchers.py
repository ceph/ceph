# -*- coding: utf-8 -*-

from typing import Tuple
from .exceptions import UnitNotSupported
import re
import logging

logger = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods
class Matcher(object):
    """ The base class to all Matchers

    It holds utility methods such as _get_disk_key
    and handles the initialization.

    """

    def __init__(self, key: str, value: str) -> None:
        """ Initialization of Base class

        :param str key: Attribute like 'model, size or vendor'
        :param str value: Value of attribute like 'X123, 5G or samsung'
        """
        self.key: str = key
        self.value: str = value
        self.fallback_key: str = ''

    # pylint: disable=inconsistent-return-statements
    def _get_disk_key(self, disk: dict) -> str:
        """ Helper method to safely extract values form the disk dict

        There is a 'key' and a _optional_ 'fallback' key that can be used.
        The reason for this is that the output of ceph-volume is not always
        consistent (due to a bug currently, but you never know).
        There is also a safety measure for a disk_key not existing on
        virtual environments. ceph-volume apparently sources its information
        from udev which seems to not populate certain fields on VMs.

        :param dict disk: A disk representation
        :raises: A generic Exception when no disk_key could be found.
        :return: A disk value
        :rtype: str
        """
        # TODO: Actually you get a Disk() object back which can be accessed
        # using the . notation, but some keys are nested, and hidden behind
        # a different hierarchy, which makes it harder to access programatically
        # hence, make it a dict.
        disk = disk.json_report()

        def findkeys(node, key_val):
            """ Find keys in non-flat dict recursively """
            if isinstance(node, list):
                for i in node:
                    for key in findkeys(i, key_val):
                        yield key
            elif isinstance(node, dict):
                if key_val in node:
                    yield node[key_val]
                for j in node.values():
                    for key in findkeys(j, key_val):
                        yield key

        disk_value: str = list(findkeys(disk, self.key))
        if not disk_value and self.fallback_key:
            disk_value = list(findkeys(disk, self.fallback_key))

        if disk_value:
            return disk_value[0]
        else:
            raise Exception("No value found for {} or {}".format(
                self.key, self.fallback_key))

    def compare(self, disk: dict):
        """ Implements a valid comparison method for a SubMatcher
        This will get overwritten by the individual classes

        :param dict disk: A disk representation
        """
        pass


# pylint: disable=too-few-public-methods
class SubstringMatcher(Matcher):
    """ Substring matcher subclass
    """

    def __init__(self, key: str, value: str, fallback_key=None) -> None:
        Matcher.__init__(self, key, value)
        self.fallback_key = fallback_key

    def compare(self, disk: dict) -> bool:
        """ Overwritten method to match substrings

        This matcher does substring matching
        :param dict disk: A disk representation (see base for examples)
        :return: True/False if the match succeeded
        :rtype: bool
        """
        if not disk:
            return False
        disk_value: str = self._get_disk_key(disk)
        if str(self.value) in str(disk_value):
            return True
        return False


# pylint: disable=too-few-public-methods
class AllMatcher(Matcher):
    """ All matcher subclass
    """

    def __init__(self, key: str, value: str, fallback_key=None) -> None:
        Matcher.__init__(self, key, value)
        self.fallback_key = fallback_key

    def compare(self, disk: dict) -> bool:
        """ Overwritten method to match all

        A rather dumb matcher that just accepts all disks
        (regardless of the value)
        :param dict disk: A disk representation (see base for examples)
        :return: always True
        :rtype: bool
        """
        if not disk:
            return False
        return True


# pylint: disable=too-few-public-methods
class EqualityMatcher(Matcher):
    """ Equality matcher subclass
    """

    def __init__(self, key: str, value: str) -> None:
        Matcher.__init__(self, key, value)

    def compare(self, disk: dict) -> bool:
        """ Overwritten method to match equality

        This matcher does value comparison
        :param dict disk: A disk representation
        :return: True/False if the match succeeded
        :rtype: bool
        """
        if not disk:
            return False
        disk_value: str = self._get_disk_key(disk)
        if int(disk_value) == int(self.value):
            return True
        return False


class SizeMatcher(Matcher):
    """ Size matcher subclass
    """

    # pylint: disable=too-many-instance-attributes
    def __init__(self, key: str, value: str) -> None:
        # The 'key' value is overwritten here because
        # the user_defined attribute does not neccessarily
        # correspond to the desired attribute
        # requested from the inventory output
        Matcher.__init__(self, key, value)
        self.key: str = "human_readable_size"
        self.fallback_key: str = "size"
        self._high = None
        self._high_suffix = None
        self._low = None
        self._low_suffix = None
        self._exact = None
        self._exact_suffix = None
        self._parse_filter()

    @property
    def low(self) -> Tuple:
        """ Getter for 'low' matchers
        """
        return self._low, self._low_suffix

    @low.setter
    def low(self, low: Tuple) -> None:
        """ Setter for 'low' matchers
        """
        self._low, self._low_suffix = low

    @property
    def high(self) -> Tuple:
        """ Getter for 'high' matchers
        """
        return self._high, self._high_suffix

    @high.setter
    def high(self, high: Tuple) -> None:
        """ Setter for 'high' matchers
        """
        self._high, self._high_suffix = high

    @property
    def exact(self) -> Tuple:
        """ Getter for 'exact' matchers
        """
        return self._exact, self._exact_suffix

    @exact.setter
    def exact(self, exact: Tuple) -> None:
        """ Setter for 'exact' matchers
        """
        self._exact, self._exact_suffix = exact

    @property
    def supported_suffixes(self) -> list:
        """ Only power of 10 notation is supported
        """
        return ["MB", "GB", "TB", "M", "G", "T"]

    def _normalize_suffix(self, suffix: str) -> str:
        """ Normalize any supported suffix

        Since the Drive Groups are user facing, we simply
        can't make sure that all users type in the requested
        form. That's why we have to internally agree on one format.
        It also checks if any of the supported suffixes was used
        and raises an Exception otherwise.

        :param str suffix: A suffix ('G') or ('M')
        :return: A normalized output
        :rtype: str
        """
        if suffix not in self.supported_suffixes:
            raise UnitNotSupported("Unit '{}' not supported".format(suffix))
        if suffix == "G":
            return "GB"
        if suffix == "T":
            return "TB"
        if suffix == "M":
            return "MB"
        return suffix

    def _parse_suffix(self, obj: str) -> str:
        """ Wrapper method to find and normalize a prefix

        :param str obj: A size filtering string ('10G')
        :return: A normalized unit ('GB')
        :rtype: str
        """
        return self._normalize_suffix(re.findall(r"[a-zA-Z]+", obj)[0])

    def _get_k_v(self, data: str) -> Tuple:
        """ Helper method to extract data from a string

        It uses regex to extract all digits and calls _parse_suffix
        which also uses a regex to extract all letters and normalizes
        the resulting suffix.

        :param str data: A size filtering string ('10G')
        :return: A Tuple with normalized output (10, 'GB')
        :rtype: tuple
        """
        return (re.findall(r"\d+", data)[0], self._parse_suffix(data))

    def _parse_filter(self):
        """ Identifies which type of 'size' filter is applied

        There are four different filtering modes:

        1) 10G:50G (high-low)
           At least 10G but at max 50G of size

        2) :60G
           At max 60G of size

        3) 50G:
           At least 50G of size

        4) 20G
           Exactly 20G in size

        This method uses regex to identify and extract this information
        and raises if none could be found.
        """
        low_high = re.match(r"\d+[A-Z]{1,2}:\d+[A-Z]{1,2}", self.value)
        if low_high:
            low, high = low_high.group().split(":")
            self.low = self._get_k_v(low)
            self.high = self._get_k_v(high)

        low = re.match(r"\d+[A-Z]{1,2}:$", self.value)
        if low:
            self.low = self._get_k_v(low.group())

        high = re.match(r"^:\d+[A-Z]{1,2}", self.value)
        if high:
            self.high = self._get_k_v(high.group())

        exact = re.match(r"^\d+[A-Z]{1,2}$", self.value)
        if exact:
            self.exact = self._get_k_v(exact.group())

        if not self.low and not self.high and not self.exact:
            raise Exception("Couldn't parse {}".format(self.value))

    @staticmethod
    # pylint: disable=inconsistent-return-statements
    def to_byte(tpl: Tuple) -> float:
        """ Convert any supported unit to bytes

        :param tuple tpl: A tuple with ('10', 'GB')
        :return: The converted byte value
        :rtype: float
        """
        value = float(tpl[0])
        suffix = tpl[1]
        if suffix == "MB":
            return value * 1e+6
        elif suffix == "GB":
            return value * 1e+9
        elif suffix == "TB":
            return value * 1e+12
        # checkers force me to return something, although
        # it's not quite good to return something here.. ignore?
        return 0.00

    # pylint: disable=inconsistent-return-statements, too-many-return-statements
    def compare(self, disk: dict) -> bool:
        """ Convert MB/GB/TB down to bytes and compare

        1) Extracts information from the to-be-inspected disk.
        2) Depending on the mode, apply checks and return

        # This doesn't seem very solid and _may_
        be re-factored


        """
        if not disk:
            return False
        disk_value = self._get_disk_key(disk)
        # This doesn't neccessarily have to be a float.
        # The current output from ceph-volume gives a float..
        # This may change in the future..
        # todo: harden this paragraph
        if not disk_value:
            logger.warning("Could not retrieve value for disk")
            return False

        disk_size = float(re.findall(r"\d+\.\d+", disk_value)[0])
        disk_suffix = self._parse_suffix(disk_value)
        disk_size_in_byte = self.to_byte((disk_size, disk_suffix))

        if all(self.high) and all(self.low):
            if disk_size_in_byte <= self.to_byte(
                    self.high) and disk_size_in_byte >= self.to_byte(self.low):
                return True
            # is a else: return False neccessary here?
            # (and in all other branches)
            logger.debug("Disk didn't match for 'high/low' filter")

        elif all(self.low) and not all(self.high):
            if disk_size_in_byte >= self.to_byte(self.low):
                return True
            logger.debug("Disk didn't match for 'low' filter")

        elif all(self.high) and not all(self.low):
            if disk_size_in_byte <= self.to_byte(self.high):
                return True
            logger.debug("Disk didn't match for 'high' filter")

        elif all(self.exact):
            if disk_size_in_byte == self.to_byte(self.exact):
                return True
            logger.debug("Disk didn't match for 'exact' filter")
        else:
            logger.debug("Neither high, low, nor exact was given")
            raise Exception("No filters applied")
        return False
