# -*- coding: utf-8 -*-

from .exceptions import FilterNotSupported, FeatureNotSupported


class DriveGroupValidator(object):
    def __init__(self, spec_obj):
        self.spec = spec_obj

    @property
    def _supported_filters(self) -> list:
        """ List of supported filters
        """
        return [
            "size", "vendor", "model", "rotational", "limit",
            "osds_per_device", "all"
        ]

    @property
    def _supported_features(self) -> list:
        """ List of supported features """
        return [
            "encrypted", "block_wal_size", "osd_per_device", "format",
            "db_slots", "wal_slots", "block_db_size", "target"
        ]

    def _check_filter(self, attr: dict) -> None:
        """ Check if the used filters are supported

        :param dict attr: A dict of filters
        :raises: FilterNotSupported if not supported
        :return: None
        """
        for applied_filter in list(attr.keys()):
            if applied_filter not in self._supported_filters:
                raise FilterNotSupported("Filtering for <{}> is not supported".
                                         format(applied_filter))

    def _check_filter_support(self) -> None:
        """ Iterates over attrs to check support
        """
        for attr in [
                self.spec.data_device_attrs,
                self.spec.wal_device_attrs,
                self.spec.db_device_attrs,
        ]:
            self._check_filter(attr)

    def _check_feature_support(self) -> None:
        """ Iterates over attrs to check support

        Check every key of this dict that is no dict itself.
        This would indicate a section(like data_devices, etc. which can't have features)
        """
        dict_spec = self.spec.__dict__.get('specs', dict())
        for feature, value in dict_spec.items():
            if isinstance(value, dict):
                continue
            if feature not in self._supported_features:
                raise FeatureNotSupported(
                    "<{}> is not supported".format(feature))

    def validate(self):
        self._check_feature_support()
        self._check_filter_support()
