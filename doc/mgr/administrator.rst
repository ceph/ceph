
ceph-mgr administrator's guide
==============================

Manual setup
------------

Usually, you would set up a ceph-mgr daemon using a tool such
as ceph-ansible.  These instructions describe how to set up
a ceph-mgr daemon manually.

First, create an authentication key for your daemon::

    ceph auth get-or-create mgr.$name mon 'allow profile mgr' osd 'allow *' mds 'allow *'

Place that key into ``mgr data`` path, which for a cluster "ceph"
and mgr $name "foo" would be ``/var/lib/ceph/mgr/ceph-foo``.

Start the ceph-mgr daemon::

    ceph-mgr -i $name

Check that the mgr has come up by looking at the output
of ``ceph status``, which should now include a mgr status line::

    mgr active: $name

Client authentication
---------------------

The manager is a new daemon which requires new CephX capabilities. If you upgrade
a cluster from an old version of Ceph, or use the default install/deploy tools,
your admin client should get this capability automatically. If you use tooling from
elsewhere, you may get EACCES errors when invoking certain ceph cluster commands.
To fix that, add a "mgr allow \*" stanza to your client's cephx capabilities by
`Modifying User Capabilities`_.

High availability
-----------------

In general, you should set up a ceph-mgr on each of the hosts
running a ceph-mon daemon to achieve the same level of availability. 

By default, whichever ceph-mgr instance comes up first will be made
active by the monitors, and the others will be standbys.  There is
no requirement for quorum among the ceph-mgr daemons.

If the active daemon fails to send a beacon to the monitors for
more than ``mon mgr beacon grace`` (default 30s), then it will be replaced
by a standby.

If you want to pre-empt failover, you can explicitly mark a ceph-mgr
daemon as failed using ``ceph mgr fail <mgr name>``.

Calling module commands
-----------------------

Where a module implements command line hooks, using the Ceph CLI's
``tell`` command to call them like this::

    ceph tell mgr <command | help>

Note that it is not necessary to address a particular mgr instance,
simply ``mgr`` will pick the current active daemon.

Use the ``help`` command to get a list of available commands from all
modules.

Configuration
-------------

OPTION(mgr_module_path, OPT_STR, CEPH_PKGLIBDIR "/mgr") // where to load python modules from

``mgr module path``

:Description: Path to load modules from
:Type: String
:Default: ``"<library dir>/mgr"``

``mgr data``

:Description: Path to load daemon data (such as keyring)
:Type: String
:Default: ``"/var/lib/ceph/mgr/$cluster-$id"``

``mgr tick period``

:Description: How many seconds between mgr beacons to monitors, and other
              periodic checks.
:Type: Integer
:Default: ``5``

``mon mgr beacon grace``

:Description: How long after last beacon should a mgr be considered failed
:Type: Integer
:Default: ``30``

.. _Modifying User Capabilities: ../../rados/operations/user-management/#modify-user-capabilities
