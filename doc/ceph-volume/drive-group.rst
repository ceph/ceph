.. _ceph-volume-drive-group:

``drive-group``
===============
The drive-group subcommand allows for passing :ref:'drivegroups' specifications
straight to ceph-volume as json. ceph-volume will then attempt to deploy this
drive groups via the batch subcommand.

The specification can be passed via a file, string argument or on stdin.
See the subcommand help for further details::

    # ceph-volume drive-group --help
