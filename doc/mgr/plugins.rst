
ceph-mgr plugin author guide
============================

Creating a plugin
-----------------

In pybind/mgr/, create a python module.  Within your module, create a class
that inherits from ``MgrModule``.

The most important methods to override are:

* a ``serve`` member function for server-type modules.  This
  function should block forever.
* a ``notify`` member function if your module needs to
  take action when new cluster data is available.
* a ``handle_command`` member function if your module
  exposes CLI commands.

Installing a plugin
-------------------

Once your module is present in the location set by the
``mgr module path`` configuration setting, you can enable it
via the ``ceph mgr module enable`` command::

  ceph mgr module enable mymodule

Note that the MgrModule interface is not stable, so any modules maintained
outside of the Ceph tree are liable to break when run against any newer
or older versions of Ceph.

Logging
-------

``MgrModule`` instances have a ``log`` property which is a logger instance that
sends log messages into the Ceph logging layer where they will be recorded
in the mgr daemon's log file.

Use it the same way you would any other python logger.  The python
log levels debug, info, warn, err are mapped into the Ceph
severities 20, 4, 1 and 0 respectively.

Exposing commands
-----------------

Set the ``COMMANDS`` class attribute of your plugin to a list of dicts
like this::

    COMMANDS = [
        {
            "cmd": "foobar name=myarg,type=CephString",
            "desc": "Do something awesome",
            "perm": "rw"
        }
    ]

The ``cmd`` part of each entry is parsed in the same way as internal
Ceph mon and admin socket commands (see mon/MonCommands.h in
the Ceph source for examples)

Config settings
---------------

Modules have access to a simple key/value store (keys and values are
byte strings) for storing configuration.  Don't use this for
storing large amounts of data.

Config values are stored using the mon's config-key commands.

Hints for using these:

* Reads are fast: ceph-mgr keeps a local in-memory copy
* Don't set things by hand with "ceph config-key", the mgr doesn't update
  at runtime (only set things from within modules).
* Writes block until the value is persisted, but reads from another
  thread will see the new value immediately.

Any config settings you want to expose to users from your module will
need corresponding hooks in ``COMMANDS`` to expose a setter.

Accessing cluster data
----------------------

Modules have access to the in-memory copies of the Ceph cluster's
state that the mgr maintains.  Accessor functions as exposed
as members of MgrModule.

Calls that access the cluster or daemon state are generally going
from Python into native C++ routines.  There is some overhead to this,
but much less than for example calling into a REST API or calling into
an SQL database.

There are no consistency rules about access to cluster structures or
daemon metadata.  For example, an OSD might exist in OSDMap but
have no metadata, or vice versa.  On a healthy cluster these
will be very rare transient states, but plugins should be written
to cope with the possibility.

Note that these accessors must not be called in the modules ``__init__``
function. This will result in a circular locking exception.

.. py:currentmodule:: mgr_module
.. automethod:: MgrModule.get
.. automethod:: MgrModule.get_server
.. automethod:: MgrModule.list_servers
.. automethod:: MgrModule.get_metadata
.. automethod:: MgrModule.get_counter

What if the mons are down?
--------------------------

The manager daemon gets much of its state (such as the cluster maps)
from the monitor.  If the monitor cluster is inaccessible, whichever
manager was active will continue to run, with the latest state it saw
still in memory.

However, if you are creating a module that shows the cluster state
to the user then you may well not want to mislead them by showing
them that out of date state.

To check if the manager daemon currently has a connection to
the monitor cluster, use this function:

.. automethod:: MgrModule.have_mon_connection

Reporting if your module cannot run
-----------------------------------

If your module cannot be run for any reason (such as a missing dependency),
then you can report that by implementing the ``can_run`` function.

.. automethod:: MgrModule.can_run

Note that this will only work properly if your module can always be imported:
if you are importing a dependency that may be absent, then do it in a
try/except block so that your module can be loaded far enough to use
``can_run`` even if the dependency is absent.

Sending commands
----------------

A non-blocking facility is provided for sending monitor commands
to the cluster.

.. automethod:: MgrModule.send_command


Implementing standby mode
-------------------------

For some modules, it is useful to run on standby manager daemons as well
as on the active daemon.  For example, an HTTP server can usefully
serve HTTP redirect responses from the standby managers so that
the user can point his browser at any of the manager daemons without
having to worry about which one is active.

Standby manager daemons look for a subclass of ``StandbyModule``
in each module.  If the class is not found then the module is not
used at all on standby daemons.  If the class is found, then
its ``serve`` method is called.  Implementations of ``StandbyModule``
must inherit from ``mgr_module.MgrStandbyModule``.

The interface of ``MgrStandbyModule`` is much restricted compared to
``MgrModule`` -- none of the Ceph cluster state is available to
the module.  ``serve`` and ``shutdown`` methods are used in the same
way as a normal module class.  The ``get_active_uri`` method enables
the standby module to discover the address of its active peer in
order to make redirects.  See the ``MgrStandbyModule`` definition
in the Ceph source code for the full list of methods.

For an example of how to use this interface, look at the source code
of the ``dashboard`` module.

Logging
-------

Use your module's ``log`` attribute as your logger.  This is a logger
configured to output via the ceph logging framework, to the local ceph-mgr
log files.

Python log severities are mapped to ceph severities as follows:

* DEBUG is 20
* INFO is 4
* WARN is 1
* ERR is 0

Shutting down cleanly
---------------------

If a module implements the ``serve()`` method, it should also implement
the ``shutdown()`` method to shutdown cleanly: misbehaving modules
may otherwise prevent clean shutdown of ceph-mgr.

Is something missing?
---------------------

The ceph-mgr python interface is not set in stone.  If you have a need
that is not satisfied by the current interface, please bring it up
on the ceph-devel mailing list.  While it is desired to avoid bloating
the interface, it is not generally very hard to expose existing data
to the Python code when there is a good reason.

