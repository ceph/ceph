ceph-python-common
==================

This library is meant to be used to keep common data structures and
functions usable throughout the Ceph project.

Like for example:

- All different Cython bindings.
- MGR modules.
- ``ceph`` command line interface and other Ceph tools.
- Also external tools.

Usage
=====

From within the Ceph git, just import it:

.. code:: python

    from ceph.deployment_utils import DriveGroupSpec
    from ceph.exceptions import OSError
