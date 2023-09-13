# -*- coding: utf-8 -*-

from typing import Tuple, Optional, Any, Union, Iterator

from ceph.deployment.inventory import Device

import re
import logging

logger = logging.getLogger(__name__)


class _MatchInvalid(Exception):
    pass


# pylint: disable=too-few-public-methods
class Matcher(object):
    """ The base class to all Matchers

    It holds utility methods such as _get_disk_key
    and handles the initialization.

    """

    def __init__(self, key, value):
        # type: (str, Any) -> None
        """ Initialization of Base class

        :param str key: Attribute like 'model, size or vendor'
        :param str value: Value of attribute like 'X123, 5G or samsung'
        """
        self.key = key
        self.value = value
        self.fallback_key = ''  # type: Optional[str]

    def _get_disk_key(self, device):
        # type: (Device) -> Any
        """ Helper method to safely extract values form the disk dict

        There is a 'key' and a _optional_ 'fallback' key that can be used.
        The reason for this is that the output of ceph-volume is not always
        consistent (due to a bug currently, but you never know).
        There is also a safety measure for a disk_key not existing on
        virtual environments. ceph-volume apparently sources its information
        from udev which seems to not populate certain fields on VMs.

        :raises: A generic Exception when no disk_key could be found.
        :return: A disk value
        :rtype: str
        """
        # using the . notation, but some keys are nested, and hidden behind
        # a different hierarchy, which makes it harder to access programatically
        # hence, make it a dict.
        disk = device.to_json()

        def findkeys(node: Union[list, dict], key_val: str) -> Iterator[str]:
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

        disk_value = list(findkeys(disk, self.key))
        if not disk_value and self.fallback_key:
            disk_value = list(findkeys(disk, self.fallback_key))

        if disk_value:
            return disk_value[0]
        else:
            raise _MatchInvalid("No value found for {} or {}".format(
                self.key, self.fallback_key))

    def compare(self, disk):
        # type: (Device) -> bool
        """ Implements a valid comparison method for a SubMatcher
        This will get overwritten by the individual classes

        :param dict disk: A disk representation
        """
        raise NotImplementedError


# pylint: disable=too-few-public-methods
class SubstringMatcher(Matcher):
    """ Substring matcher subclass
    """

    def __init__(self, key, value, fallback_key=None):
        # type: (str, str, Optional[str]) -> None
        Matcher.__init__(self, key, value)
        self.fallback_key = fallback_key

    def compare(self, disk):
        # type: (Device) -> bool
        """ Overwritten method to match substrings

        This matcher does substring matching
        :param dict disk: A disk representation (see base for examples)
        :return: True/False if the match succeeded
        :rtype: bool
        """
        if not disk:
            return False
        disk_value = self._get_disk_key(disk)
        if str(self.value) in disk_value:
            return True
        return False


# pylint: disable=too-few-public-methods
class AllMatcher(Matcher):
    """ All matcher subclass
    """

    def __init__(self, key, value, fallback_key=None):
        # type: (str, Any, Optional[str]) -> None

        Matcher.__init__(self, key, value)
        self.fallback_key = fallback_key

    def compare(self, disk):
        # type: (Device) -> bool

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

    def __init__(self, key, value):
        # type: (str, Any) -> None

        Matcher.__init__(self, key, value)

    def compare(self, disk):
        # type: (Device) -> bool

        """ Overwritten method to match equality

        This matcher does value comparison
        :param dict disk: A disk representation
        :return: True/False if the match succeeded
        :rtype: bool
        """
        if not disk:
            return False
        disk_value = self._get_disk_key(disk)
        ret = disk_value == self.value
        if not ret:
            logger.debug('{} != {}'.format(disk_value, self.value))
        return ret


class SizeMatcher(Matcher):
    """ Size matcher subclass
    """

    SUFFIXES = (
        ["KB", "MB", "GB", "TB"],
        ["K", "M", "G", "T"],
        [1e+3, 1e+6, 1e+9, 1e+12]
    )

    supported_suffixes = SUFFIXES[0] + SUFFIXES[1]

    # pylint: disable=too-many-instance-attributes
    def __init__(self, key, value):
        # type: (str, str) -> None

        # The 'key' value is overwritten here because
        # the user_defined attribute does not necessarily
        # correspond to the desired attribute
        # requested from the inventory output
        Matcher.__init__(self, key, value)
        self.key = "human_readable_size"
        self.fallback_key = "size"
        self._high = None  # type: Optional[str]
        self._high_suffix = None  # type: Optional[str]
        self._low = None  # type: Optional[str]
        self._low_suffix = None  # type: Optional[str]
        self._exact = None  # type: Optional[str]
        self._exact_suffix = None  # type: Optional[str]
        self._parse_filter()

    @property
    def low(self):
        # type: () ->  Tuple[Optional[str], Optional[str]]
        """ Getter for 'low' matchers
        """
        return self._low, self._low_suffix

    @low.setter
    def low(self, low):
        # type: (Tuple[str, str]) ->  None
        """ Setter for 'low' matchers
        """
        self._low, self._low_suffix = low

    @property
    def high(self):
        # type: () ->  Tuple[Optional[str], Optional[str]]
        """ Getter for 'high' matchers
        """
        return self._high, self._high_suffix

    @high.setter
    def high(self, high):
        # type: (Tuple[str, str]) ->  None
        """ Setter for 'high' matchers
        """
        self._high, self._high_suffix = high

    @property
    def exact(self):
        # type: () -> Tuple[Optional[str], Optional[str]]
        """ Getter for 'exact' matchers
        """
        return self._exact, self._exact_suffix

    @exact.setter
    def exact(self, exact):
        # type: (Tuple[str, str]) ->  None
        """ Setter for 'exact' matchers
        """
        self._exact, self._exact_suffix = exact

    @classmethod
    def _normalize_suffix(cls, suffix):
        # type: (str) -> str
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
        suffix = suffix.upper()
        if suffix not in cls.supported_suffixes:
            raise _MatchInvalid("Unit '{}' not supported".format(suffix))
        return dict(zip(
            cls.SUFFIXES[1],
            cls.SUFFIXES[0],
        )).get(suffix, suffix)

    @classmethod
    def _parse_suffix(cls, obj):
        # type: (str) -> str
        """ Wrapper method to find and normalize a prefix

        :param str obj: A size filtering string ('10G')
        :return: A normalized unit ('GB')
        :rtype: str
        """
        return cls._normalize_suffix(re.findall(r"[a-zA-Z]+", obj)[0])

    @classmethod
    def _get_k_v(cls, data):
        # type: (str) -> Tuple[str, str]
        """ Helper method to extract data from a string

        It uses regex to extract all digits and calls _parse_suffix
        which also uses a regex to extract all letters and normalizes
        the resulting suffix.

        :param str data: A size filtering string ('10G')
        :return: A Tuple with normalized output (10, 'GB')
        :rtype: tuple
        """
        return re.findall(r"\d+\.?\d*", data)[0], cls._parse_suffix(data)

    def _parse_filter(self) -> None:
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
        if low_high is not None:
            lowpart, highpart = low_high.group().split(":")
            self.low = self._get_k_v(lowpart)
            self.high = self._get_k_v(highpart)

        low = re.match(r"\d+[A-Z]{1,2}:$", self.value)
        if low:
            self.low = self._get_k_v(low.group())

        high = re.match(r"^:\d+[A-Z]{1,2}", self.value)
        if high:
            self.high = self._get_k_v(high.group())

        exact = re.match(r"^\d+\.?\d*[A-Z]{1,2}$", self.value)
        if exact:
            self.exact = self._get_k_v(exact.group())

        if not self.low and not self.high and not self.exact:
            raise _MatchInvalid("Couldn't parse {}".format(self.value))

    @staticmethod
    # pylint: disable=inconsistent-return-statements
    def to_byte(tpl):
        # type: (Tuple[Optional[str], Optional[str]]) -> float

        """ Convert any supported unit to bytes

        :param tuple tpl: A tuple with ('10', 'GB')
        :return: The converted byte value
        :rtype: float
        """
        val_str, suffix = tpl
        value = float(val_str) if val_str is not None else 0.0
        return dict(zip(
            SizeMatcher.SUFFIXES[0],
            SizeMatcher.SUFFIXES[2],
        )).get(str(suffix), 0.00) * value

    @staticmethod
    def str_to_byte(input):
        # type: (str) -> float
        return SizeMatcher.to_byte(SizeMatcher._get_k_v(input))

    # pylint: disable=inconsistent-return-statements, too-many-return-statements
    def compare(self, disk):
        # type: (Device) -> bool
        """ Convert MB/GB/TB down to bytes and compare

        1) Extracts information from the to-be-inspected disk.
        2) Depending on the mode, apply checks and return

        # This doesn't seem very solid and _may_
        be re-factored


        """
        if not disk:
            return False
        disk_value = self._get_disk_key(disk)
        # This doesn't necessarily have to be a float.
        # The current output from ceph-volume gives a float..
        # This may change in the future..
        # todo: harden this paragraph
        if not disk_value:
            logger.warning("Could not retrieve value for disk")
            return False

        disk_size = re.findall(r"\d+\.\d+", disk_value)[0]
        disk_suffix = self._parse_suffix(disk_value)
        disk_size_in_byte = self.to_byte((disk_size, disk_suffix))

        if all(self.high) and all(self.low):
            if disk_size_in_byte <= self.to_byte(
                    self.high) and disk_size_in_byte >= self.to_byte(self.low):
                return True
            # is a else: return False necessary here?
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
            raise _MatchInvalid("No filters applied")
        return False
