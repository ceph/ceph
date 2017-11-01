.. _mgr-administrator-guide:

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

Using modules
-------------

Use the command ``ceph mgr module ls`` to see which modules are
available, and which are currently enabled.  Enable or disable modules
using the commands ``ceph mgr module enable <module>`` and
``ceph mgr module disable <module>`` respectively.

If a module is *enabled* then the active ceph-mgr daemon will load
and execute it.  In the case of modules that provide a service,
such as an HTTP server, the module may publish its address when it
is loaded.  To see the addresses of such modules, use the command 
``ceph mgr services``.

Some modules may also implement a special standby mode which runs on
standby ceph-mgr daemons as well as the active daemon.  This enables
modules that provide services to redirect their clients to the active
daemon, if the client tries to connect to a standby.

Consult the documentation pages for individual manager modules for more
information about what functionality each module provides.

Here is an example of enabling the ``dashboard`` module:

::

	$ ceph mgr module ls
	{
		"enabled_modules": [
			"restful",
			"status"
		],
		"disabled_modules": [
			"dashboard"
		]
	}

	$ ceph mgr module enable dashboard
	$ ceph mgr module ls
	{
		"enabled_modules": [
			"restful",
			"status",
			"dashboard"
		],
		"disabled_modules": [
		]
	}

	$ ceph mgr services
	{
		"dashboard": "http://myserver.com:7789/",
		"restful": "https://myserver.com:8789/"
	}


Calling module commands
-----------------------

Where a module implements command line hooks, the commands will
be accessible as ordinary Ceph commands::

    ceph <command | help>

If you would like to see the list of commands handled by the
manager (where normal ``ceph help`` would show all mon and mgr commands),
you can send a command directly to the manager daemon::

    ceph tell mgr help

Note that it is not necessary to address a particular mgr instance,
simply ``mgr`` will pick the current active daemon.

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
