=================================
 Configuration Management System
=================================

The configuration management system exists to provide every daemon with the
proper configuration information. The configuration can be viewed as a set of
key-value pairs.

How can the configuration be set? Well, there are several sources:

 - the ceph configuration file, usually named ceph.conf
 - command line arguments::

    --debug-ms=1
    --debug-monc=10

   etc.
 - arguments injected at runtime using ``injectargs`` or ``config set``


The Configuration File
======================

Most configuration settings originate in the Ceph configuration file.

How do we find the configuration file? Well, in order, we check:

 - the default locations
 - the environment variable ``CEPH_CONF``
 - the command line argument ``-c``

Each stanza of the configuration file describes the key-value pairs that will be in
effect for a particular subset of the daemons. The "global" stanza applies to
everything. The "mon", "osd", and "mds" stanzas specify settings to take effect
for all monitors, all OSDs, and all mds servers, respectively.  A stanza of the
form ``mon.$name``, ``osd.$name``, or ``mds.$name`` gives settings for the monitor, OSD, or
MDS of that name, respectively. Configuration values that appear later in the
file win over earlier ones.

A sample configuration file can be found in src/sample.ceph.conf.


Metavariables
=============

The configuration system allows any configuration value to be
substituted into another value using the ``$varname`` syntax, similar
to how bash shell expansion works.

A few additional special metavariables are also defined:

 - $host: expands to the current hostname
 - $type: expands to one of "mds", "osd", "mon", or "client"
 - $id: expands to the daemon identifier. For ``osd.0``, this would be ``0``; for ``mds.a``, it would be ``a``; for ``client.admin``, it would be ``admin``.
 - $num: same as $id
 - $name: expands to $type.$id


Reading configuration values
====================================================

There are two ways for Ceph code to get configuration values. One way is to
read it directly from a variable named ``g_conf``, or equivalently,
``g_ceph_ctx->_conf``. The other is to register an observer that will be called
every time the relevant configuration values change. This observer will be
called soon after the initial configuration is read, and every time after that
when one of the relevant values changes. Each observer tracks a set of keys
and is invoked only when one of the relevant keys changes.

The interface to implement is found in ``common/config_obs.h``.

The observer method should be preferred in new code because

 - It is more flexible, allowing the code to do whatever reinitialization needs
   to be done to implement the new configuration value.
 - It is the only way to create a std::string configuration variable that can
   be changed by injectargs.
 - Even for int-valued configuration options, changing the values in one thread
   while another thread is reading them can lead to subtle and
   impossible-to-diagnose bugs.

For these reasons, reading directly from ``g_conf`` should be considered deprecated
and not done in new code.  Do not ever alter ``g_conf``.

Changing configuration values
====================================================

Configuration values can be changed by calling ``g_conf()->set_val``. After changing
the configuration, you should call ``g_conf()->apply_changes`` to re-run all the
affected configuration observers. For convenience, you can call
``g_conf()->set_val_or_die`` to make a configuration change which you think should
never fail.

``injectargs``, ``parse_argv``, and ``parse_env`` are three other functions which modify
the configuration. Just like with set_val, you should call apply_changes after
calling these functions to make sure your changes get applied.


.. _dev config defining options:

Defining config options
=======================

Config options are defined in ``common/options/*.yaml.in``. The options are categorized
by their consumers. If an option is only used by ceph-osd, it should go to
``osd.yaml.in``. All the ``.yaml.in`` files are translated into ``.cc`` and ``.h`` files
at build time by ``y2c.py``.

.. note::
   Ceph-mgr modules use the same configuration system as other Ceph components,
   but their configuration options are defined within each module's Python
   implementation. For details on defining mgr module configuration options,
   see :ref:`mgr module dev configuration options`.


Each option is represented using a YAML mapping (dictionary). A typical option looks like

.. code-block:: yaml

   - name: public_addr
     type: addr
     level: basic
     desc: public-facing address to bind to
     long_desc: The IP address for the public (front-side) network.
       Set for each daemon.
     services:
     - mon
     - mds
     - osd
     - mgr
     flags:
     - startup
     with_legacy: true

In which, following keys are allowed:

level
-----

The ``level`` property of an option is an indicator for the probability the
option is adjusted by an operator or a developer:

.. describe:: basic

   for basic config options that a normal operator is likely to adjust.

.. describe:: advanced

   for options that an operator *can* adjust, but should not touch unless they
   understand what they are doing. Adjusting advanced options poorly can lead to
   problems (performance or even data loss) if done incorrectly.

.. describe:: dev

   for options in place for use by developers only, either for testing purposes,
   or to describe constants that no user should adjust but we prefer not to compile
   into the code.

``desc``, ``long_desc`` and ``fmt_desc``
----------------------------------------

.. describe:: desc

   Short description of the option. Sentence fragment. e.g.

   .. code-block:: yaml

      desc: Default checksum algorithm to use

.. describe:: long_desc

   The long description is complete sentences, perhaps even multiple
   paragraphs, and may include other detailed information or notes. e.g.

   .. code-block:: yaml

      long_desc: crc32c, xxhash32, and xxhash64 are available.  The _16 and _8 variants use
        only a subset of the bits for more compact (but less reliable) checksumming.

.. describe:: fmt_desc

   The description formatted using reStructuredText. This property is
   only used by the ``confval`` directive to render an option in the
   document. e.g.:

   .. code-block:: yaml

      fmt_desc: The interval for "deep" scrubbing (fully reading all data). The
        ``osd_scrub_load_threshold`` does not affect this setting.

Default values
--------------

There is a default value for every config option. In some cases, there may
also be a *daemon default* that only applies to code that declares itself
as a daemon (in this case, the regular default only applies to non-daemons). Like:

.. code-block:: yaml

   default: crc32c

Some literal postfixes are allowed when options with type of ``float``, ``size``
and ``secs``, like:

.. code-block:: yaml

   - name: mon_scrub_interval
     type: secs
     default: 1_day
   - name: osd_journal_size
     type: size
     default: 5_K

For better readability, it is encouraged to use these literal postfixes when
adding or updating the default value for an option.

Service
-------

Service is a component name, like "common", "osd", "rgw", "mds", etc. It may
be a list of components, like:

.. code-block:: yaml

   services:
   - mon
   - mds
   - osd
   - mgr

For example, the rocksdb options affect both the osd and mon. If an option is put
into a service specific ``.yaml.in`` file, the corresponding service is added to
its ``services`` property automatically. For instance, ``osd_scrub_begin_hour``
option is located in ``osd.yaml.in``, even its ``services`` is not specified
explicitly in this file, this property still contains ``osd``.

Tags
----

Tags identify options across services that relate in some way. For example:

network
  options affecting network configuration
mkfs
  options that only matter at mkfs time

Like:

.. code-block:: yaml

   tags:
   - network

Enums
-----

For options with a defined set of allowed values:

.. code-block:: yaml

   enum_values:
   - none
   - crc32c
   - crc32c_16
   - crc32c_8
   - xxhash32
   - xxhash64

Flags
-----

.. describe:: runtime

   the value can be updated at runtime

.. describe:: no_mon_update

   Daemons/clients do not pull this value from the monitor config database.  We
   disallow setting this option via ``ceph config set ...``.  This option should
   be configured via ``ceph.conf`` or via the command line.

.. describe:: startup

   option takes effect only during daemon startup

.. describe:: cluster_create

   option only affects cluster creation

.. describe:: create

   option only affects daemon creation

Documentation of Configuration Values
=====================================

Ceph configuration options are documented on-demand using the ``:confval:``
directive rather than in a centralized location.

Documenting Configuration Options
---------------------------------

To document a configuration option, use the ``:confval:`` directive:

.. code-block:: rst

   The check interval can be customized by the ``check_interval`` option:

   .. confval:: mgr/inbox/check_interval

.. note::
   Ceph-mgr module options must include the ``mgr/<module>/``
   namespace prefix. In the example above, ``check_interval`` belongs to the
   ``inbox`` module, so it's documented as ``mgr/inbox/check_interval``.

Referencing Configuration Options
---------------------------------

Once documented, reference options using the ``:confval:`` role:

.. code-block:: rst

   With the :confval:`mgr/inbox/check_interval` setting, you can customize the
   check interval.

For regular Ceph options (non-mgr modules), the process is the same but without the module prefix:

.. code-block:: rst

   You can set the initial monitor members with :confval:`mon_initial_members`:

   .. confval:: mon_initial_members

Naming Conventions
------------------

* **Mgr module options**: Use ``mgr/<module>/<option_name>`` format
* **Regular options**: Use the option name directly (e.g., ``mon_initial_members``)

This approach ensures consistent cross-referencing throughout the documentation
while maintaining proper namespacing for different configuration contexts.
