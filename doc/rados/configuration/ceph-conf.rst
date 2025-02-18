.. _configuring-ceph:

==================
 Configuring Ceph
==================

When Ceph services start, the initialization process activates a set of
daemons that run in the background. A :term:`Ceph Storage Cluster` runs at
least three types of daemons:

- :term:`Ceph Monitor` (``ceph-mon``)
- :term:`Ceph Manager` (``ceph-mgr``)
- :term:`Ceph OSD Daemon` (``ceph-osd``)

Any Ceph Storage Cluster that supports the :term:`Ceph File System` also runs
at least one :term:`Ceph Metadata Server` (``ceph-mds``). Any Cluster that
supports :term:`Ceph Object Storage` runs Ceph RADOS Gateway daemons
(``radosgw``).

Each daemon has a number of configuration options, and each of those options
has a default value. Adjust the behavior of the system by changing these
configuration options. Make sure to understand the consequences before
overriding the default values, as it is possible to significantly degrade the
performance and stability of your cluster. Remember that default values
sometimes change between releases. For this reason, it is best to review the
version of this documentation that applies to your Ceph release.

Option names
============

Each of the Ceph configuration options has a unique name that consists of words
formed with lowercase characters and connected with underscore characters
(``_``).

When option names are specified on the command line, underscore (``_``) and
dash (``-``) characters can be used interchangeably (for example,
``--mon-host`` is equivalent to ``--mon_host``).

When option names appear in configuration files, spaces can also be used in
place of underscores or dashes. However, for the sake of clarity and
convenience, we suggest that you consistently use underscores, as we do
throughout this documentation.

Config sources
==============

Each Ceph daemon, process, and library pulls its configuration from one or more
of the several sources listed below. Sources that occur later in the list
override those that occur earlier in the list (when both are present).

- the compiled-in default value
- the monitor cluster's centralized configuration database
- a configuration file stored on the local host
- environment variables
- command-line arguments
- runtime overrides that are set by an administrator

One of the first things a Ceph process does on startup is parse the
configuration options provided via the command line, via the environment, and
via the local configuration file. Next, the process contacts the monitor
cluster to retrieve centrally-stored configuration for the entire cluster.
After a complete view of the configuration is available, the startup of the
daemon or process will commence.

.. _bootstrap-options:

Bootstrap options
-----------------

Bootstrap options are configuration options that affect the process's ability
to contact the monitors, to authenticate, and to retrieve the cluster-stored
configuration.  For this reason, these options might need to be stored locally
on the node, and set by means of a local configuration file. These options
include the following:

.. confval:: mon_host
.. confval:: mon_host_override

- :confval:`mon_dns_srv_name`
- :confval:`mon_data`, :confval:`osd_data`, :confval:`mds_data`,
  :confval:`mgr_data`, and similar options that define which local directory
  the daemon stores its data in.
- :confval:`keyring`, :confval:`keyfile`, and/or :confval:`key`, which can be
  used to specify the authentication credential to use to authenticate with the
  monitor. Note that in most cases the default keyring location is in the data
  directory specified above.

In most cases, there is no reason to modify the default values of these
options. However, there is one exception to this: the :confval:`mon_host`
option that identifies the addresses of the cluster's monitors. But when
:ref:`DNS is used to identify monitors<mon-dns-lookup>`, a local Ceph
configuration file can be avoided entirely.


Skipping monitor config
-----------------------

The option ``--no-mon-config`` can be passed in any command in order to skip
the step that retrieves configuration information from the cluster's monitors.
Skipping this retrieval step can be useful in cases where configuration is
managed entirely via configuration files, or when maintenance activity needs to
be done but the monitor cluster is down.

.. _ceph-conf-file:

Configuration sections
======================

Each of the configuration options associated with a single process or daemon
has a single value. However, the values for a configuration option can vary
across daemon types, and can vary even across different daemons of the same
type. Ceph options that are stored in the monitor configuration database or in
local configuration files are grouped into sections |---| so-called "configuration
sections" |---| to indicate which daemons or clients they apply to.


These sections include the following:

.. confsec:: global

   Settings under ``global`` affect all daemons and clients
   in a Ceph Storage Cluster.

   :example: ``log_file = /var/log/ceph/$cluster-$type.$id.log``

.. confsec:: mon

   Settings under ``mon`` affect all ``ceph-mon`` daemons in
   the Ceph Storage Cluster, and override the same setting in
   ``global``.

   :example: ``mon_cluster_log_to_syslog = true``

.. confsec:: mgr

   Settings in the ``mgr`` section affect all ``ceph-mgr`` daemons in
   the Ceph Storage Cluster, and override the same setting in
   ``global``.

   :example: ``mgr_stats_period = 10``

.. confsec:: osd

   Settings under ``osd`` affect all ``ceph-osd`` daemons in
   the Ceph Storage Cluster, and override the same setting in
   ``global``.

   :example: ``osd_op_queue = wpq``

.. confsec:: mds

   Settings in the ``mds`` section affect all ``ceph-mds`` daemons in
   the Ceph Storage Cluster, and override the same setting in
   ``global``.

   :example: ``mds_cache_memory_limit = 10G``

.. confsec:: client

   Settings under ``client`` affect all Ceph clients
   (for example, mounted Ceph File Systems, mounted Ceph Block Devices)
   as well as RADOS Gateway (RGW) daemons.

   :example: ``objecter_inflight_ops = 512``


Configuration sections can also specify an individual daemon or client name. For example,
``mon.foo``, ``osd.123``, and ``client.smith`` are all valid section names.


Any given daemon will draw its settings from the global section, the daemon- or
client-type section, and the section sharing its name. Settings in the
most-specific section take precedence so precedence: for example, if the same
option is specified in both :confsec:`global`, :confsec:`mon`, and ``mon.foo``
on the same source (i.e. that is, in the same configuration file), the
``mon.foo`` setting will be used.

If multiple values of the same configuration option are specified in the same
section, the last value specified takes precedence.

Note that values from the local configuration file always take precedence over
values from the monitor configuration database, regardless of the section in
which they appear.

.. _ceph-metavariables:

Metavariables
=============

Metavariables dramatically simplify Ceph storage cluster configuration. When a
metavariable is set in a configuration value, Ceph expands the metavariable at
the time the configuration value is used. In this way, Ceph metavariables
behave similarly to the way that variable expansion works in the Bash shell.

Ceph supports the following metavariables:

.. describe:: $cluster

   Expands to the Ceph Storage Cluster name. Useful when running
   multiple Ceph Storage Clusters on the same hardware.

   :example: ``/etc/ceph/$cluster.keyring``
   :default: ``ceph``

.. describe:: $type

   Expands to a daemon or process type (for example, ``mds``, ``osd``, or ``mon``)

   :example: ``/var/lib/ceph/$type``

.. describe:: $id

   Expands to the daemon or client identifier. For
   ``osd.0``, this would be ``0``; for ``mds.a``, it would
   be ``a``.

   :example: ``/var/lib/ceph/$type/$cluster-$id``

.. describe:: $host

   Expands to the host name where the process is running.

.. describe:: $name

   Expands to ``$type.$id``.

   :example: ``/var/run/ceph/$cluster-$name.asok``

.. describe:: $pid

   Expands to daemon pid.

   :example: ``/var/run/ceph/$cluster-$name-$pid.asok``


Ceph configuration file
=======================

On startup, Ceph processes search for a configuration file in the
following locations:

#. ``$CEPH_CONF`` (that is, the path following the ``$CEPH_CONF``
   environment variable)
#. ``-c path/path``  (that is, the ``-c`` command line argument)
#. ``/etc/ceph/$cluster.conf``
#. ``~/.ceph/$cluster.conf``
#. ``./$cluster.conf`` (that is, in the current working directory)
#. On FreeBSD systems only, ``/usr/local/etc/ceph/$cluster.conf``

Here ``$cluster`` is the cluster's name (default: ``ceph``).

The Ceph configuration file uses an ``ini`` style syntax. You can add "comment
text" after a pound sign (#) or a semi-colon semicolon (;). For example:

.. code-block:: ini

    # <--A number (#) sign number sign (#) precedes a comment.
    ; A comment may be anything.
    # Comments always follow a semi-colon semicolon (;) or a pound sign (#) on each line.
    # The end of the line terminates a comment.
    # We recommend that you provide comments in your configuration file(s).


.. _ceph-conf-settings:

Config file section names
-------------------------

The configuration file is divided into sections. Each section must begin with a
valid configuration section name (see `Configuration sections`_, above) that is
surrounded by square brackets. For example:

.. code-block:: ini

    [global]
    debug_ms = 0

    [osd]
    debug_ms = 1

    [osd.1]
    debug_ms = 10

    [osd.2]
    debug_ms = 10

Config file option values
-------------------------

The value of a configuration option is a string. If the string is too long to
fit on a single line, you can put a backslash (``\``) at the end of the line
and the backslash will act as a line continuation marker. In such a case, the
value of the option will be the string after ``=`` in the current line,
combined with the string in the next line. Here is an example::

  [global]
  foo = long long ago\
  long ago

In this example, the value of the "``foo``" option is "``long long ago long
ago``".

An option value typically ends with either a newline or a comment. For
example:

.. code-block:: ini

    [global]
    obscure_one = difficult to explain # I will try harder in next release
    simpler_one = nothing to explain

In this example, the value of the "``obscure one``" option is "``difficult to
explain``" and the value of the "``simpler one`` options is "``nothing to
explain``".

When an option value contains spaces, it can be enclosed within single quotes
or double quotes in order to make its scope clear and in order to make sure
that the first space in the value is not interpreted as the end of the value.
For example:

.. code-block:: ini

    [global]
    line = "to be, or not to be"

In option values, there are four characters that are treated as escape
characters: ``=``, ``#``, ``;`` and ``[``. They are permitted to occur in an
option value only if they are immediately preceded by the backslash character
(``\``). For example:

.. code-block:: ini

    [global]
    secret = "i love \# and \["

Each configuration option falls under one of the following types:

.. describe:: int

   64-bit signed integer. Some SI suffixes are supported, such as "K", "M",
   "G", "T", "P", and "E" (meaning, respectively, 10\ :sup:`3`, 10\ :sup:`6`,
   10\ :sup:`9`, etc.). "B" is the only supported unit string. Thus "1K", "1M",
   "128B" and "-1" are all valid option values. When a negative value is
   assigned to a threshold option, this can indicate that the option is
   "unlimited" -- that is, that there is no threshold or limit in effect.

   :example: ``42``, ``-1``

.. describe:: uint

   This differs from ``integer`` only in that negative values are not
   permitted.

   :example: ``256``, ``0``

.. describe:: str

   A string encoded in UTF-8. Certain characters are not permitted. Reference
   the above notes for the details.

   :example: ``"hello world"``, ``"i love \#"``, ``yet-another-name``

.. describe:: boolean

   Typically either of the two values ``true`` or ``false``. However, any
   integer is permitted: "0" implies ``false``, and any non-zero value implies
   ``true``.

   :example: ``true``, ``false``, ``1``, ``0``

.. describe:: addr

   A single address, optionally prefixed with ``v1``, ``v2`` or ``any`` for the
   messenger protocol. If no prefix is specified, the ``v2`` protocol is used.
   For more details, see :ref:`address_formats`.

   :example: ``v1:1.2.3.4:567``, ``v2:1.2.3.4:567``, ``1.2.3.4:567``, ``2409:8a1e:8fb6:aa20:1260:4bff:fe92:18f5::567``, ``[::1]:6789``

.. describe:: addrvec

   A set of addresses separated by ",". The addresses can be optionally quoted
   with ``[`` and ``]``.

   :example: ``[v1:1.2.3.4:567,v2:1.2.3.4:568]``, ``v1:1.2.3.4:567,v1:1.2.3.14:567``  ``[2409:8a1e:8fb6:aa20:1260:4bff:fe92:18f5::567], [2409:8a1e:8fb6:aa20:1260:4bff:fe92:18f5::568]``

.. describe:: uuid

   The string format of a uuid defined by `RFC4122
   <https://www.ietf.org/rfc/rfc4122.txt>`_. Certain variants are also
   supported: for more details, see `Boost document
   <https://www.boost.org/doc/libs/1_74_0/libs/uuid/doc/uuid.html#String%20Generator>`_.

   :example: ``f81d4fae-7dec-11d0-a765-00a0c91e6bf6``

.. describe:: size

   64-bit unsigned integer. Both SI prefixes and IEC prefixes are supported.
   "B" is the only supported unit string. Negative values are not permitted.

   :example: ``1Ki``, ``1K``, ``1KiB`` and ``1B``.

.. describe:: secs

   Denotes a duration of time. The default unit of time is the second.
   The following units of time are supported:

              * second: ``s``, ``sec``, ``second``, ``seconds``
              * minute: ``m``, ``min``, ``minute``, ``minutes``
              * hour: ``hs``, ``hr``, ``hour``, ``hours``
              * day: ``d``, ``day``, ``days``
              * week: ``w``, ``wk``, ``week``, ``weeks``
              * month: ``mo``, ``month``, ``months``
              * year: ``y``, ``yr``, ``year``, ``years``

   :example: ``1 m``, ``1m`` and ``1 week``

.. _ceph-conf-database:

Monitor configuration database
==============================

The monitor cluster manages a database of configuration options that can be
consumed by the entire cluster. This allows for streamlined central
configuration management of the entire system. For ease of administration and
transparency, the vast majority of configuration options can and should be
stored in this database.

Some settings might need to be stored in local configuration files because they
affect the ability of the process to connect to the monitors, to authenticate,
and to fetch configuration information. In most cases this applies only to the
``mon_host`` option. This issue can be avoided by using :ref:`DNS SRV
records<mon-dns-lookup>`.

Sections and masks
------------------

Configuration options stored by the monitor can be stored in a global section,
in a daemon-type section, or in a specific daemon section. In this, they are
no different from the options in a configuration file.

In addition, options may have a *mask* associated with them to further restrict
which daemons or clients the option applies to. Masks take two forms:

#. ``type:location`` where ``type`` is a CRUSH property like ``rack`` or
   ``host``, and ``location`` is a value for that property. For example,
   ``host:foo`` would limit the option only to daemons or clients
   running on a particular host.
#. ``class:device-class`` where ``device-class`` is the name of a CRUSH
   device class (for example, ``hdd`` or ``ssd``). For example,
   ``class:ssd`` would limit the option only to OSDs backed by SSDs.
   (This mask has no effect on non-OSD daemons or clients.)

In commands that specify a configuration option, the argument of the option (in
the following examples, this is the "who" string) may be a section name, a
mask, or a combination of both separated by a slash character (``/``). For
example, ``osd/rack:foo`` would refer to all OSD daemons in the ``foo`` rack.

When configuration options are shown, the section name and mask are presented
in separate fields or columns to make them more readable.

Commands
--------

The following CLI commands are used to configure the cluster:

* ``ceph config dump`` dumps the entire monitor configuration
  database for the cluster.

* ``ceph config get <who>`` dumps the configuration options stored in
  the monitor configuration database for a specific daemon or client
  (for example, ``mds.a``).

* ``ceph config get <who> <option>`` shows either a configuration value
  stored in the monitor configuration database for a specific daemon or client
  (for example, ``mds.a``), or, if that value is not present in the monitor
  configuration database, the compiled-in default value.

* ``ceph config set <who> <option> <value>`` specifies a configuration
  option in the monitor configuration database.

* ``ceph config show <who>`` shows the configuration for a running daemon.
  These settings might differ from those stored by the monitors if there are
  also local configuration files in use or if options have been overridden on
  the command line or at run time. The source of the values of the options is
  displayed in the output.

* ``ceph config assimilate-conf -i <input file> -o <output file>`` ingests a
  configuration file from *input file* and moves any valid options into the
  monitor configuration database. Any settings that are unrecognized, are
  invalid, or cannot be controlled by the monitor will be returned in an
  abbreviated configuration file stored in *output file*. This command is
  useful for transitioning from legacy configuration files to centralized
  monitor-based configuration.

Note that ``ceph config set <who> <option> <value>`` and ``ceph config get
<who> <option>`` will not necessarily return the same values. The latter
command will show compiled-in default values. In order to determine whether a
configuration option is present in the monitor configuration database, run
``ceph config dump``.

Help
====

To get help for a particular option, run the following command:

.. prompt:: bash $

   ceph config help <option>

For example:

.. prompt:: bash $

   ceph config help log_file

::

   log_file - path to log file
    (std::string, basic)
    Default (non-daemon):
    Default (daemon): /var/log/ceph/$cluster-$name.log
    Can update at runtime: false
    See also: [log_to_stderr,err_to_stderr,log_to_syslog,err_to_syslog]

or:

.. prompt:: bash $

   ceph config help log_file -f json-pretty

::

  {
      "name": "log_file",
      "type": "std::string",
      "level": "basic",
      "desc": "path to log file",
      "long_desc": "",
      "default": "",
      "daemon_default": "/var/log/ceph/$cluster-$name.log",
      "tags": [],
      "services": [],
      "see_also": [
          "log_to_stderr",
          "err_to_stderr",
          "log_to_syslog",
          "err_to_syslog"
      ],
      "enum_values": [],
      "min": "",
      "max": "",
      "can_update_at_runtime": false
  }

The ``level`` property can be ``basic``, ``advanced``, or ``dev``.  The `dev`
options are intended for use by developers, generally for testing purposes, and
are not recommended for use by operators.

.. note:: This command uses the configuration schema that is compiled into the
   running monitors. If you have a mixed-version cluster (as might exist, for
   example, during an upgrade), you might want to query the option schema from
   a specific running daemon by running a command of the following form:

.. prompt:: bash $

   ceph daemon <name> config help [option]

Runtime Changes
===============

In most cases, Ceph permits changes to the configuration of a daemon at
run time. This can be used for increasing or decreasing the amount of logging
output, for enabling or disabling debug settings, and for runtime optimization.

Use the ``ceph config set`` command to update configuration options. For
example, to enable the most verbose  debug log level on a specific OSD, run a
command of the following form:

.. prompt:: bash $

   ceph config set osd.123 debug_ms 20

.. note:: If an option has been customized in a local configuration file, the
   `central config
   <https://ceph.io/en/news/blog/2018/new-mimic-centralized-configuration-management/>`_
   setting will be ignored because it has a lower priority than the local
   configuration file.

.. note:: Log levels range from 0 to 20.

Override values
---------------

Options can be set temporarily by using the Ceph CLI ``tell`` or ``daemon``
interfaces on the Ceph CLI. These *override* values are ephemeral, which means
that they affect only the current instance of the daemon and revert to
persistently configured values when the daemon restarts.

Override values can be set in two ways:

#. From any host, send a message to a daemon with a command of the following
   form:

   .. prompt:: bash $

      ceph tell <name> config set <option> <value>

   For example:

   .. prompt:: bash $

      ceph tell osd.123 config set debug_osd 20

   The ``tell`` command can also accept a wildcard as the daemon identifier.
   For example, to adjust the debug level on all OSD daemons, run a command of
   the following form:

   .. prompt:: bash $

      ceph tell osd.* config set debug_osd 20

#. On the host where the daemon is running, connect to the daemon via a socket
   in ``/var/run/ceph`` by running a command of the following form:

   .. prompt:: bash $

      ceph daemon <name> config set <option> <value>

   For example:

   .. prompt:: bash $

      ceph daemon osd.4 config set debug_osd 20

.. note:: In the output of the ``ceph config show`` command, these temporary
   values are shown to have a source of ``override``.


Viewing runtime settings
========================

You can see the current settings specified for a running daemon with the ``ceph
config show`` command. For example, to see the (non-default) settings for the
daemon ``osd.0``, run the following command:

.. prompt:: bash $

   ceph config show osd.0

To see a specific setting, run the following command:

.. prompt:: bash $

   ceph config show osd.0 debug_osd

To see all settings (including those with default values), run the following
command:

.. prompt:: bash $

   ceph config show-with-defaults osd.0

You can see all settings for a daemon that is currently running by connecting
to it on the local host via the admin socket. For example, to dump all
current settings, run the following command:

.. prompt:: bash $

   ceph daemon osd.0 config show

To see non-default settings and to see where each value came from (for example,
a config file, the monitor, or an override), run the following command:

.. prompt:: bash $

   ceph daemon osd.0 config diff

To see the value of a single setting, run the following command:

.. prompt:: bash $

   ceph daemon osd.0 config get debug_osd


Changes introduced in Octopus
=============================

The Octopus release changed the way the configuration file is parsed.
These changes are as follows:

- Repeated configuration options are allowed, and no warnings will be
  displayed. This means that the setting that comes last in the file is the one
  that takes effect. Prior to this change, Ceph displayed warning messages when
  lines containing duplicate options were encountered, such as::

    warning line 42: 'foo' in section 'bar' redefined
- Prior to Octopus, options containing invalid UTF-8 characters were ignored
  with warning messages. But in Octopus, they are treated as fatal errors.
- The backslash character ``\`` is used as the line-continuation marker that
  combines the next line with the current one. Prior to Octopus, there was a
  requirement that any end-of-line backslash be followed by a non-empty line.
  But in Octopus, an empty line following a backslash is allowed.
- In the configuration file, each line specifies an individual configuration
  option. The option's name and its value are separated with ``=``, and the
  value may be enclosed within single or double quotes. If an invalid
  configuration is specified, we will treat it as an invalid configuration
  file::

    bad option ==== bad value
- Prior to Octopus, if no section name was specified in the configuration file,
  all options would be set as though they were within the :confsec:`global`
  section. This approach is discouraged. Since Octopus, any configuration
  file that has no section name must contain only a single option.

.. |---|   unicode:: U+2014 .. EM DASH :trim:
