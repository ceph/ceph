.. _configuring-ceph:

==================
 Configuring Ceph
==================

Every :term:`Ceph Storage Cluster` runs at
least three types of daemons:

- :term:`Ceph Monitor` (``ceph-mon``)
- :term:`Ceph Manager` (``ceph-mgr``)
- :term:`Ceph OSD Daemon` (``ceph-osd``)

A Ceph Storage Cluster that deploys the :term:`Ceph File System` also runs
at least one :term:`Ceph Metadata Server` (``ceph-mds``). A Cluster that
deploys :term:`Ceph Object Storage` runs Ceph RADOS Gateway daemons
(``radosgw``).

Each daemon has a number of configuration options, and each of those options
has a default value. Adjust the behavior of the system by changing these
configuration options. Make sure to understand the consequences before
overriding the default values, as it is possible to significantly degrade the
performance and stability of your cluster. Remember that default values
sometimes change between releases. For this reason, it is best to review the
version of this documentation that applies to your Ceph release.  When updating
to a new Ceph release, also review the release notes for important changes.

Option Names
============

Each Ceph configuration option has a unique name that consists of words
formed with lowercase characters and connected with underscore characters
(``_``).

When option names are specified on the command line or in persisted
configuration, underscore (``_``) and
dash (``-``) characters can be used interchangeably (for example,
``--mon-host`` is equivalent to ``--mon_host``).

When option names appear in configuration files, spaces can also be used in
place of underscores or dashes.

For the sake of clarity and
convenience, we suggest that you consistently use underscores, as we do
throughout this documentation.

Config Sources
==============

Each Ceph daemon and client pulls configuration option values from one or more
of the sources listed below. Option values found via sources later in the list
will override any found in sources ealier in the list.  In other words,
the last value wins.

- The compiled-in default value
- The central configuration database maintained by the Monitors
- A configuration file stored on the local host
- Environment variables
- Command-line arguments
- Runtime overrides set via the admin socket or injection

One of the first things a Ceph process does on startup is parse the
configuration options provided via the command line, via the environment, and
via the local configuration file. Next, the process contacts the monitor
cluster to retrieve centrally-stored configuration for the entire cluster.
After a complete set of configuration options is available, the startup of the
daemon or process will commence.

.. _bootstrap-options:

Bootstrap Options
-----------------

Bootstrap options enable each Ceph daemon
to contact the Monitors, to authenticate, and to retrieve central
configuration values.  For this reason, these options are ususally stored locally
on each node in a local configuration file. These options
include the following:

.. confval:: mon_host
.. confval:: mon_host_override

- :confval:`mon_dns_srv_name`
- :confval:`mon_data`, :confval:`osd_data`, :confval:`mds_data`,
  :confval:`mgr_data`, and similar options that define the local directory
  in which the daemon stores data.
- :confval:`keyring`, :confval:`keyfile`, and/or :confval:`key`, which can be
  used to specify the authentication credential to use to authenticate with the
  Monitors. Note that in most cases the default keyring location is in the data
  directory specified above.

There is usually no reason to modify the default values of these
options. However, there is one exception: the :confval:`mon_host`
option that identifies the addresses of the cluster's Monitors. When
:ref:`DNS is used to identify monitors<mon-dns-lookup>`, a local Ceph
configuration file can be avoided entirely.


Skipping Monitor Config
-----------------------

The option ``--no-mon-config`` can be passed to any command in order to skip
the step that retrieves configuration information from the cluster's Monitors.
Skipping this config option source can be useful in cases where configuration is
managed entirely via configuration files, or when maintenance is necessary
but the Monitor quorum is down.

.. _ceph-conf-file:

Configuration Sections
======================

Each configuration option associated with a single process or daemon
has a single value. The value for a configuration option may be
set for all daemon types or for only daemons of a given type.
Ceph options that stored in the Monitor configuration database or in
local configuration files are grouped into *configuration
sections* in order to indicate to which daemons or clients they apply.


These sections include the following:

.. confsec:: global

   Settings under ``global`` affect all daemons and clients
   in a Ceph Storage Cluster.  In some cases an option may
   need to be set in one or more sections, for one or more daemons,
   that is not obvious from its name. In such cases the description
   of that option may call this out, or in most cases one can
   select the ``global`` section (aka central config ``who``) to
   ensure that it is applied to all appropriate daemons or clients.

   :example: ``log_file = /var/log/ceph/$cluster-$type.$id.log``

.. confsec:: mon

   Settings under ``mon`` affect all ``ceph-mon`` Monitor daemons in
   the Ceph Storage Cluster, and override any value set in
   ``global``.

   :example: ``mon_cluster_log_to_syslog = true``

.. confsec:: mgr

   Settings in the ``mgr`` section affect all ``ceph-mgr`` Manager daemons in
   the Ceph Storage Cluster, and override any value set in
   ``global``.

   :example: ``mgr_stats_period = 10``

.. confsec:: osd

   Settings under ``osd`` affect all ``ceph-osd`` OSD daemons in
   the Ceph Storage Cluster, and override any value set in
   ``global``.

   :example: ``osd_op_queue = wpq``

.. confsec:: mds

   Settings in the ``mds`` section affect all CephFS ``ceph-mds`` daemons in
   the Ceph Storage Cluster, and override any value set in
   ``global``.

   :example: ``mds_cache_memory_limit = 10G``

.. confsec:: client

   Settings under ``client`` affect all Ceph clients
   (for example, mounted Ceph File Systems,  attached Ceph Block Devices)
   and daemons including the RADOS Gateway (RGW) and the NVMeoF Gateway.

   :example: ``objecter_inflight_ops = 512``


Configuration sections can also specify a specific daemon or client name. For example,
``mon.foo``, ``osd.123``, and ``client.smith`` are all valid section names.
This granularity is rarely needed: in most cases it is best to apply a setting
to all Monitors, all OSDs (potentially with a *mask* for a device class), etc.
so that as the cluster grows or the orchestrator changes daemon placement the
expected values remain in force.

Any given daemon will draw settings from the global section, the daemon- or
client-type section, and the section sharing its name. Settings in the
most-specific section take precedence so precedence: for example, if the same
option is specified in both :confsec:`global`, :confsec:`mon`, and ``mon.foo``
on the same source (i.e. that is, in the same configuration file), the
``mon.foo`` setting will be used.

If multiple values of the same configuration option are specified in the same
section, the last value specified takes precedence.

Note that values set in the local configuration file always take precedence over
values from the Monitor central configuration database, regardless of the section in
which they appear.

.. _ceph-metavariables:

Metavariables
=============

Metavariables dramatically simplify Ceph storage cluster configuration. When a
metavariable is set within a configuration value, Ceph expands the metavariable at
the time the configuration value is used. In this way, Ceph metavariables
behave similarly to the way that variable expansion works in the Bash shell.

Ceph supports the following metavariables:

.. describe:: $cluster

   Expands to the Ceph Storage Cluster name. Useful when running
   multiple Ceph Storage Clusters on the same hardware. Note that cluster
   *vanity names* are deprecated and may be removed entirely from future
   releases. We *strongly* urge that new clusters be provisioned only with
   the default name ``ceph`` and that existing clusters with vanity names
   be retrofitted to the default name.

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

   Expands to the hostname where the process is running.

.. describe:: $name

   Expands to ``$type.$id``.

   :example: ``/var/run/ceph/$cluster-$name.asok``

.. describe:: $pid

   Expands to the daemon's process id (``PID``).

   :example: ``/var/run/ceph/$cluster-$name-$pid.asok``


Ceph Configuration File
=======================

On startup, Ceph processes search for a configuration file in the
following locations:

#. ``$CEPH_CONF`` (that is, the value of the ``$CEPH_CONF``
   environment variable if set)
#. ``-c path/path``  (that is, the ``-c`` command line argument if supplied)
#. ``/etc/ceph/$cluster.conf``
#. ``~/.ceph/$cluster.conf``
#. ``./$cluster.conf`` (that is, in the current working directory)
#. On FreeBSD systems only, ``/usr/local/etc/ceph/$cluster.conf``

Here ``$cluster`` is the cluster's name (default: ``ceph``).

The Ceph configuration file uses an ``ini`` style syntax. One may add comment
text after a pound sign (#) or a semi-colon semicolon (;). For example:

.. code-block:: ini

    # <--A number (#) sign number sign (#) precedes a comment.
    ; A comment may be anything.
    # Comments always follow a semi-colon semicolon (;) or a pound sign (#) on each line.
    # The end of the line terminates a comment.
    # We recommend that you provide comments in your configuration file(s).


.. _ceph-conf-settings:

Config File Section Names
-------------------------

The configuration file is divided into sections. Each section must begin with a
valid configuration section name (see :ref:`ceph-conf-file`, above) within
square brackets. For example:

.. code-block:: ini

    [global]
    debug_ms = 0

    [osd]
    debug_ms = 1

    [osd.1]
    debug_ms = 10

    [osd.2]
    debug_ms = 10

Config File Option Values
-------------------------

The value of a configuration option is a string. If the string is too long to
fit on a single line, you may place a backslash (``\``) at the end of the line
and the backslash will act as a line continuation marker. In such a case, the
value of the option will be the string after ``=`` in the current line,
combined with the string in the next line. Here is an example:

.. code-block:: ini

    [global]
    foo = long long ago\
    long ago

In this example, the value of the "``foo``" option is "``long long ago long
ago``".  Be careful to not place a backslash at the end of the final line
of the multi-line string.

An option value setting in a local config file ends with a newline.
A comment prefixed with ``#`` may be added before the newline.

Examples:

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

There are four metacharacters that must be escaped with a backslash (``\``)
if they are meant to be part of the option's value: ``=``, ``#``, ``;`` and ``[``.

Example:

.. code-block:: ini

    [global]
    secret = "I l0ve \# and \["

Each configuration option specifies one of the following types for its value:

.. describe:: int

   A 64-bit signed integer. Some SI suffixes are supported, including ``K``, ``M``,
   ``G``, ``T``, ``P``, and ``E``.  These represent, respectively, 10\ :sup:`3`, 10\ :sup:`6`,
   10\ :sup:`9`, etc.). ``B`` (bytes)is the only supported unit string. Thus ``1K``, ``1M``,
   ``128B`` and ``-1`` are all valid option values. When a negative value is
   assigned to an option that defines a threshold or limit, this often indicates that the value is
   "unlimited" -- that is, no threshold or limit will be enforced. Options that
   allow such a value will usually indicate so in their individual description text.

   :example: ``42``, ``-1``

.. describe:: uint

   An unsigned integer, which differs from ``integer`` only in that negative values are not
   permitted.

   :example: ``256``, ``0``

.. describe:: str

   A string encoded in UTF-8. Certain characters are not permitted. Reference
   the above notes for details.

   :example: ``"hello world"``, ``"i love \#"``, ``yet-another-name``

.. describe:: boolean

   Typically either ``true`` or ``false``. However, any
   integer is permitted: "0" implies ``false``, and any non-zero value implies
   ``true``. We encourage the use of ``true`` or ``false`` for clarity.

   :example: ``true``, ``false``, ``1``, ``0``

.. describe:: addr

   A single address, optionally prefixed with ``v1``, ``v2`` or ``any`` for the
   messenger protocol. If no prefix is specified, the ``v2`` protocol is used.
   For more details, see :ref:`address_formats`.

   :example: ``v1:1.2.3.4:567``, ``v2:1.2.3.4:567``, ``1.2.3.4:567``, ``2409:8a1e:8fb6:aa20:1260:4bff:fe92:18f5::567``, ``[::1]:6789``

.. describe:: addrvec

   A set of IPv4 or IPv6 addresses separated by commas (``,``). The set of addresses can be optionally delimited
   with ``[`` and ``]``.

   :example: ``[v1:1.2.3.4:567,v2:1.2.3.4:568]``, ``v1:1.2.3.4:567,v1:1.2.3.14:567``, ``[2409:8a1e:8fb6:aa20:1260:4bff:fe92:18f5::567], [2409:8a1e:8fb6:aa20:1260:4bff:fe92:18f5::568]``

.. describe:: uuid

   A UUID string in the format defined by `RFC4122
   <https://www.ietf.org/rfc/rfc4122.txt>`_. Certain variants are also
   supported. For more details, see this `Boost document
   <https://www.boost.org/doc/libs/1_74_0/libs/uuid/doc/uuid.html#String%20Generator>`_.

   :example: ``f81d4fae-7dec-11d0-a765-00a0c91e6bf6``

.. describe:: size

   A 64-bit unsigned integer. Both SI prefixes and IEC prefixes are supported.
   ``B`` is the only supported unit string. Negative values are not permitted.

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

The Monitors manage a database of configuration options that can be
consumed by the entire cluster. This allows for streamlined central
configuration of the entire system. For ease of administration,
transparency, and to avoid inconsistencies, the vast majority of configuration options can and should be
set in this database instead of in ``ceph.conf`` files on daemon or
client nodes.

A few specific settings might need to be stored in local configuration files because they
affect the ability of the process to connect to the Monitors, to authenticate,
and to fetch additional configuration information. In most cases this applies only to the
``mon_host`` option. This issue can be avoided by using :ref:`DNS SRV
records<mon-dns-lookup>` if your DNS infrastructure is very robust and
under your control.

Sections and Masks
------------------

Configuration options stored by the Monitors can be stored in a global section,
in a daemon-type section, or in a specific daemon section. In this sense they are
no different than options set in a node-local configuration file, subject to the above
source precedence.

In addition, options may have a *mask* associated with them to further restrict to
which daemons or clients the option's value applies.. Masks take two forms:

#. ``type:location`` where ``type`` is a CRUSH bucket type, for example ``rack`` or
   ``host``, and ``location`` is a value for that property. For example,
   ``host:foo`` would limit the option only to daemons or clients
   running on a host named ``foo``. Recent Ceph releases provide functionality
   that obviates most situations that formerly required host-specific configuration
   values. Examples include OSD device classses, the ``osd_memory_target`` autotuner,
   and options with values that are specific to certain media. Examples
   of the latter include ``osd_recovery_sleep_ssd`` and ``osd_recovery_max_active_hdd``.

#. ``class:device-class`` where ``device-class`` is the name of a CRUSH
   device class (for example, ``hdd`` or ``ssd``). For example,
   ``class:ssd`` would limit the option only to OSDs built solely on
   SAS, SATA, and NVMe SSDs. This mask has no effect on non-OSD daemons or clients

In commands that specify a configuration option, the argument of the option (in
the following examples, this is the ``who`` string) may be a section name, a
mask, or a combination of both separated by a slash character (``/``). For
example, ``osd/rack:foo`` would refer to all OSD daemons under the ``foo`` CRUSH ``rack`` bucket.

When configuration options are displayed, the section name and any mask are presented
in separate fields or columns to make them more readable.

Commands
--------

The following CLI commands are used to configure the cluster:

* ``ceph config dump`` dumps the entire Monitor central configuration database.

* ``ceph config get <who>`` dumps the configuration options stored in
  the Monitor configuration database for a specific daemon or client
  (for example, ``mds.a``).

* ``ceph config get <who> <option>`` shows either a configuration value
  stored in the Monitor configuration database for a specific daemon or client
  (for example, ``mds.a``), or, if that value is not present in the Monitor
  configuration database, the compiled-in default value.

* ``ceph config set <who> <option> <value>`` sets a configuration
  option in the Monitor's configuration database. If a value for this
  option was previously set, it will be overwritten.  Take care to
  set values with appropriate ``who`` and optional mask attributes. If,
  for example, a value exists with the ``who`` scope of ``osd`` for
  the ``someoption`` option, then a command of the
  form ``ceph config set global someoption somevalue`` is executed,
  the central database will retain both.  This may be useful in
  certain situations, but it can lead to confusion and is often best
  avoided.

* ``ceph config show <who>`` shows configuration values for a running daemon.
  These settings might differ from those stored by the monitors if there are
  also local configuration files in use or if options have been overridden on
  the command line or at runtime via admin socket, ``ceph tell``, or ``ceph daemon``
  commands. The source of each option value is displayed.

* ``ceph config assimilate-conf -i <input_file> -o <output_file>`` ingests a
  configuration file from *input_file* and sets any valid options found into the
  Monitor configuration database. Any settings that are unrecognized, are
  invalid, or cannot be managed by the Monitors will be returned in an
  abbreviated configuration file stored in *output_file*. This command is
  useful when transitioning from legacy configuration files to centralized
  Monitor-based configuration.

Note that ``ceph config get <who> [<option>]`` and ``ceph config show
<who>`` will not necessarily return the same values. The former
command shows only compiled-in default values. In order to determine whether a
configuration option is present in the Monitor configuration database, run
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

The ``level`` property of each option is ``basic``, ``advanced``, or ``dev``. Options
tagged with the ``dev`` level are intended for use by developers for testing purposes, and
Ceph admins (operators) are urged to not change their values without expert-level understanding
or advice from expert support professionals.

.. note:: This command uses the configuration schema that is compiled into the
   running Monitors. If you have a mixed-version cluster (as might exist
   during an upgrade), you might want to query the option schema from
   a specific running daemon by running a command of the following form:

.. prompt:: bash $

   ceph daemon <name> config help [option]

.. note:: The Ceph release versions of *running* daemons may be reported by
   running the ``ceph versions`` commands.  If your cluster is not in the
   process of an upgrade, all daemons should show the same version. If multiple
   versions are reported outside of an upgrade, a prior upgrade may have failed
   or manual changes may have been executed, and the circumstances should be
   examined and an upgrade to harmonize versions should be considered.

Runtime Changes
===============

In most cases, runtime changes to the configuration of a daemon take effect
without requiring that the daemon be restarted. This might be used for
increasing or decreasing the amount of logging output, for temporarily
raising or lowering log subsystem debug levels, or for runtime optimization.

Use the ``ceph config set`` command to update configuration options. For
example, to enable the most verbose debug log level on a specific OSD, run a
command of the following form:

.. prompt:: bash #

   ceph config set osd.1701 debug_ms 20

.. note:: If an option has been customized in a local configuration file, the
   `central config
   <https://ceph.io/en/news/blog/2018/new-mimic-centralized-configuration-management/>`_
   setting will be ignored because it has a lower precedence than the local
   configuration file.

.. note:: Log subsystem levels range from 0 to 20.

Override Values
---------------

Runtime option values can be set temporarily by using the ``ceph tell``
or ``ceph daemon`` CLI commands.  This process is known as *injection*.
These *override* values are ephemeral, which means
that they affect only the current instance of the daemon and revert to
persistently configured values when the daemon restarts.  Thus they are
useful for careful testing of option value adjustments, but take care to
also persist permanent changes via ``ceph config set``.

Override values can be set in two ways:

#. From any host, send a message to a daemon with a command of the following
   form:

   .. prompt:: bash #

      ceph tell <name> config set <option> <value>

   For example:

   .. prompt:: bash #

      ceph tell osd.1701 config set debug_osd 20

   The ``tell`` command can also accept a wildcard as the daemon identifier.
   For example, to adjust the debug level on all OSD daemons, run a command of
   the following form:

   .. prompt:: bash #

      ceph tell osd.* config set debug_osd 20

#. On the host where a specific daemon is running, connect to the daemon via a socket
   in ``/var/run/ceph`` by running a command of the following form:

   .. prompt:: bash #

      ceph daemon <name> config set <option> <value>

   For example:

   .. prompt:: bash #

      ceph daemon osd.4 config set debug_osd 20

.. note:: In the output of the ``ceph config show`` command, these temporary
   values are shown to have a source of ``override``.


Viewing Runtime Settings
========================

You can see the current settings specified for a running daemon with the ``ceph
config show`` command. For example, to see the (non-default) settings for the
daemon ``osd.1701``, run the following command:

.. prompt:: bash #

   ceph config show osd.1701

To see only the value of a single option for a specific daemon, run a command of following form:

.. prompt:: bash #

   ceph config show osd.1701 debug_osd

To see all settings for a specific daemon (including the settings with default
values), run a command of the following form:

.. prompt:: bash #

   ceph config show-with-defaults osd.1701

You can show all settings for a daemon that is currently running by connecting
to the admin socket on the host where it runs. For example, to dump all
current settings for ``osd.1701``, run the following command on the host
where ``osd.1701`` runs. The host whre a daemon runs can be determined with
the ``ceph osd find`` command or ``ceph orch ps`` commands.

.. prompt:: bash #

   ceph daemon osd.1701 config show

To see non-default settings and to see the source of each value came (for example,
a config file, the central Monitor DB, or an override), run a command of the
following form:

.. prompt:: bash #

   ceph daemon osd.1701 config diff

To see the value of a single option, run a command of the following form:

.. prompt:: bash #

   ceph daemon osd.1701 config get debug_osd


Changes Introduced in Octopus
=============================

The Octopus release changed the way that the configuration file is parsed.
These changes are as follows:

- Repeated configuration options are allowed, and no warnings will be
  displayed. This means that the value that comes last in the file is the one
  that takes effect. Prior to this change, Ceph displayed warning messages
  of the following form when lines containing duplicate options were encountered::

    warning line 42: 'foo' in section 'bar' redefined
- Prior to Octopus, options containing invalid UTF-8 characters were ignored
  with warning messages. In Octopus and later releases they are treated as fatal errors.
- The backslash character ``\`` is interpreted as a line-continuation marker that
  combines the next line with the current one. Prior to Octopus, there was a
  requirement that any end-of-line backslash be followed by a non-empty line.
  In Octopus and later releases, an empty line following a backslash is allowed.
- In the configuration file, each line specifies an individual configuration
  option. The option's name and its value are separated with ``=``, and the
  value may be enclosed within single or double quotes. If an invalid
  configuration is specified, we will treat it as an invalid configuration
  file and log a message of the following form::

    bad option ==== bad value
- Prior to Octopus, if no section name was specified in the configuration file,
  all options would be set as though they were within the :confsec:`global`
  section. This approach is discouraged. Since Octopus, any configuration
  file that has no section name must contain only a single option.

.. |---|   unicode:: U+2014 .. EM DASH :trim:
