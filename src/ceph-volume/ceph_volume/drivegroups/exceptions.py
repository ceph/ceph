# -*- coding: utf-8 -*-


class FilterNotSupported(Exception):
    """ A critical error when the user specified filter is unsupported
    """
    pass


class FeatureNotSupported(Exception):
    """ A critical error when the user specified feature is unsupported
    """
    pass


class UnitNotSupported(Exception):
    """ A critical error which encouters when a unit is parsed which
    isn't supported.
    """
    pass


class ConfigError(Exception):
    """ A critical error which is encountered when a configuration is not supported
    or is invalid.
    """
    pass
