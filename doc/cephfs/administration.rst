
CephFS Administrative commands
==============================

Filesystems
-----------

These commands operate on the CephFS filesystems in your Ceph cluster.
Note that by default only one filesystem is permitted: to enable
creation of multiple filesystems use ``ceph fs flag set enable_multiple true``.

::

    fs new <filesystem name> <metadata pool name> <data pool name>

::

    fs ls

::

    fs rm <filesystem name> [--yes-i-really-mean-it]

::

    fs reset <filesystem name>

::

    fs get <filesystem name>

::

    fs set <filesystem name> <var> <val>

::

    fs add_data_pool <filesystem name> <pool name/id>

::

    fs rm_data_pool <filesystem name> <pool name/id>


Daemons
-------

These commands act on specific mds daemons or ranks.  For convenience,
they accept arguments that 

::

    mds fail <gid/name/role

::

    mds deactivate <role>

::

    tell mds.<daemon name>

::

    mds metadata <gid/name/role>

::

    mds repaired <role>



Global settings
---------------

::

    fs dump

::

    fs flag set <flag name> <flag val>

    flag name must be one of ['enable_multiple']

Advanced
--------

These commands are not required in normal operation, and exist
for use in exceptional circumstances.  Incorrect use of these
commands may cause serious problems, such as an inaccessible
filesystem.

::

    mds compat rm_compat

::

    mds compat rm_incompat

::

    mds compat show

::

    mds getmap

::

    mds set_state

::

    mds rmfailed

Legacy
======

::

    mds stat
    mds dump  # replaced by "fs get"
    mds stop  # replaced by "mds deactivate"
    mds set_max_mds # replaced by "fs set max_mds"
    mds set # replaced by "fs set"
    mds cluster_down  # replaced by "fs set cluster_down"
    mds cluster_up  # replaced by "fs set cluster_down"
    mds newfs # replaced by "fs new"
    mds add_data_pool # replaced by "fs add_data_pool"
    mds remove_data_pool #replaced by "fs remove_data_pool"

