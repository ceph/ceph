import warnings

warning_msg = """
The 'ceph_rest_api' is deprecated and has been moved to a new module. Upgrade your imports to:

    import ceph_rest
"""
# Don't use ``DeprecationWarning`` as they are ignored by default in Python 2.7+
warnings.warn(warning_msg, Warning)

from ceph_rest import *
