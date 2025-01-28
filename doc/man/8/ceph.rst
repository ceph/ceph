:orphan:

==================================
 ceph -- ceph administration tool
==================================

.. program:: ceph

Synopsis
========

| **ceph** **auth** [ *add* \| *caps* \| *del* \| *export* \| *get* \| *get-key* \| *get-or-create* \| *get-or-create-key* \| *import* \| *list* \| *print-key* \| *print_key* ] ...

| **ceph** **compact**

| **ceph** **config** [ *dump* | *ls* | *help* | *get* | *show* | *show-with-defaults* | *set* | *rm* | *log* | *reset* | *assimilate-conf* | *generate-minimal-conf* ] ...

| **ceph** **config-key** [ *rm* | *exists* | *get* | *ls* | *dump* | *set* ] ...

| **ceph** **daemon** *<name>* \| *<path>* *<command>* ...

| **ceph** **daemonperf** *<name>* \| *<path>* [ *interval* [ *count* ] ]

| **ceph** **df** *{detail}*

| **ceph** **fs** [ *add_data_pool* \| *authorize* \| *dump* \| *feature ls* \| *flag set* \| *get* \| *ls* \| *lsflags* \| *new* \| *rename* \| *reset* \| *required_client_features add* \| *required_client_features rm* \| *rm* \| *rm_data_pool* \| *set* \| *swap* ] ...

| **ceph** **fsid**

| **ceph** **health** *{detail}*

| **ceph** **injectargs** *<injectedargs>* [ *<injectedargs>*... ]

| **ceph** **log** *<logtext>* [ *<logtext>*... ]

| **ceph** **mds** [ *compat* \| *fail* \| *rm* \| *rmfailed* \| *set_state* \| *stat* \| *repaired* ] ...

| **ceph** **mon** [ *add* \| *dump* \| *enable_stretch_mode* \| *getmap* \| *remove* \| *stat* ] ...

| **ceph** **osd** [ *blocklist* \| *blocked-by* \| *create* \| *new* \| *deep-scrub* \| *df* \| *down* \| *dump* \| *erasure-code-profile* \| *find* \| *getcrushmap* \| *getmap* \| *getmaxosd* \| *in* \| *ls* \| *lspools* \| *map* \| *metadata* \| *ok-to-stop* \| *out* \| *pause* \| *perf* \| *pg-temp* \| *force-create-pg* \| *primary-affinity* \| *primary-temp* \| *repair* \| *reweight* \| *reweight-by-pg* \| *rm* \| *destroy* \| *purge* \| *safe-to-destroy* \| *scrub* \| *set* \| *setcrushmap* \| *setmaxosd*  \| *stat* \| *tree* \| *unpause* \| *unset* ] ...

| **ceph** **osd** **crush** [ *add* \| *add-bucket* \| *create-or-move* \| *dump* \| *get-tunable* \| *link* \| *move* \| *remove* \| *rename-bucket* \| *reweight* \| *reweight-all* \| *reweight-subtree* \| *rm* \| *rule* \| *set* \| *set-tunable* \| *show-tunables* \| *tunables* \| *unlink* ] ...

| **ceph** **osd** **pool** [ *create* \| *delete* \| *get* \| *get-quota* \| *ls* \| *mksnap* \| *rename* \| *rmsnap* \| *set* \| *set-quota* \| *stats* ] ...

| **ceph** **osd** **pool** **application** [ *disable* \| *enable* \| *get* \| *rm* \| *set* ] ...

| **ceph** **osd** **tier** [ *add* \| *add-cache* \| *cache-mode* \| *remove* \| *remove-overlay* \| *set-overlay* ] ...

| **ceph** **pg** [ *debug* \| *deep-scrub* \| *dump* \| *dump_json* \| *dump_pools_json* \| *dump_stuck* \| *getmap* \| *ls* \| *ls-by-osd* \| *ls-by-pool* \| *ls-by-primary* \| *map* \| *repair* \| *scrub* \| *stat* ] ...

| **ceph** **quorum_status**

| **ceph** **report** { *<tags>* [ *<tags>...* ] }

| **ceph** **status**

| **ceph** **sync** **force** {--yes-i-really-mean-it} {--i-know-what-i-am-doing}

| **ceph** **tell** *<name (type.id)> <command> [options...]*

| **ceph** **version**

Description
===========

:program:`ceph` is a control utility which is used for manual deployment and maintenance
of a Ceph cluster. It provides a diverse set of commands that allows deployment of
monitors, OSDs, placement groups, MDS and overall maintenance, administration
of the cluster.

Commands
========

auth
----

Manage authentication keys. It is used for adding, removing, exporting
or updating of authentication keys for a particular  entity such as a monitor or
OSD. It uses some additional subcommands.

Subcommand ``add`` adds authentication info for a particular entity from input
file, or random key if no input is given and/or any caps specified in the command.

Usage::

	ceph auth add <entity> {<caps> [<caps>...]}

Subcommand ``caps`` updates caps for **name** from caps specified in the command.

Usage::

	ceph auth caps <entity> <caps> [<caps>...]

Subcommand ``del`` deletes all caps for ``name``.

Usage::

	ceph auth del <entity>

Subcommand ``export`` writes keyring for requested entity, or master keyring if
none given.

Usage::

	ceph auth export {<entity>}

Subcommand ``get`` writes keyring file with requested key.

Usage::

	ceph auth get <entity>

Subcommand ``get-key`` displays requested key.

Usage::

	ceph auth get-key <entity>

Subcommand ``get-or-create`` adds authentication info for a particular entity
from input file, or random key if no input given and/or any caps specified in the
command.

Usage::

	ceph auth get-or-create <entity> {<caps> [<caps>...]}

Subcommand ``get-or-create-key`` gets or adds key for ``name`` from system/caps
pairs specified in the command.  If key already exists, any given caps must match
the existing caps for that key.

Usage::

	ceph auth get-or-create-key <entity> {<caps> [<caps>...]}

Subcommand ``import`` reads keyring from input file.

Usage::

	ceph auth import

Subcommand ``ls`` lists authentication state.

Usage::

	ceph auth ls

Subcommand ``print-key`` displays requested key.

Usage::

	ceph auth print-key <entity>

Subcommand ``print_key`` displays requested key.

Usage::

	ceph auth print_key <entity>


compact
-------

Causes compaction of monitor's RocksDB storage.

Usage::

	ceph compact


config
------

Configure the cluster. By default, Ceph daemons and clients retrieve their
configuration options from monitor when they start, and are updated if any of
the tracked options is changed at run time. It uses following additional
subcommand.

Subcommand ``dump`` to dump all options for the cluster

Usage::

	ceph config dump

Subcommand ``ls`` to list all option names for the cluster

Usage::

	ceph config ls

Subcommand ``help`` to describe the specified configuration option

Usage::

    ceph config help <option>

Subcommand ``get`` to dump the option(s) for the specified entity.

Usage::

    ceph config get <who> {<option>}

Subcommand ``show`` to display the running configuration of the specified
entity. Please note, unlike ``get``, which only shows the options managed
by monitor, ``show`` displays all the configurations being actively used.
These options are pulled from several sources, for instance, the compiled-in
default value, the monitor's configuration database, ``ceph.conf`` file on
the host. The options can even be overridden at runtime. So, there is chance
that the configuration options in the output of ``show`` could be different
from those in the output of ``get``.

Usage::

	ceph config show {<who>}

Subcommand ``show-with-defaults`` to display the running configuration along with the compiled-in defaults of the specified entity

Usage::

	ceph config show {<who>}

Subcommand ``set`` to set an option for one or more specified entities

Usage::

    ceph config set <who> <option> <value> {--force}

Subcommand ``rm`` to clear an option for one or more entities

Usage::

    ceph config rm <who> <option>

Subcommand ``log`` to show recent history of config changes. If `count` option
is omitted it defaults to 10.

Usage::

    ceph config log {<count>}

Subcommand ``reset`` to revert configuration to the specified historical version

Usage::

    ceph config reset <version>


Subcommand ``assimilate-conf`` to assimilate options from stdin, and return a
new, minimal conf file

Usage::

    ceph config assimilate-conf -i <input-config-path> > <output-config-path>
    ceph config assimilate-conf < <input-config-path>

Subcommand ``generate-minimal-conf`` to generate a minimal ``ceph.conf`` file,
which can be used for bootstrapping a daemon or a client.

Usage::

    ceph config generate-minimal-conf > <minimal-config-path>


config-key
----------

Manage configuration key. Config-key is a general purpose key/value service
offered by the monitors. This service is mainly used by Ceph tools and daemons
for persisting various settings. Among which, ceph-mgr modules uses it for
storing their options. It uses some additional subcommands.

Subcommand ``rm`` deletes configuration key.

Usage::

	ceph config-key rm <key>

Subcommand ``exists`` checks for configuration keys existence.

Usage::

	ceph config-key exists <key>

Subcommand ``get`` gets the configuration key.

Usage::

	ceph config-key get <key>

Subcommand ``ls`` lists configuration keys.

Usage::

	ceph config-key ls

Subcommand ``dump`` dumps configuration keys and values.

Usage::

	ceph config-key dump

Subcommand ``set`` puts configuration key and value.

Usage::

	ceph config-key set <key> {<val>}


daemon
------

Submit admin-socket commands.

Usage::

	ceph daemon {daemon_name|socket_path} {command} ...

Example::

	ceph daemon osd.0 help


daemonperf
----------

Watch performance counters from a Ceph daemon.

Usage::

	ceph daemonperf {daemon_name|socket_path} [{interval} [{count}]]


df
--

Show cluster's free space status.

Usage::

	ceph df {detail}

.. _ceph features:

features
--------

Show the releases and features of all connected daemons and clients connected
to the cluster, along with the numbers of them in each bucket grouped by the
corresponding features/releases. Each release of Ceph supports a different set
of features, expressed by the features bitmask. New cluster features require
that clients support the feature, or else they are not allowed to connect to
these new features. As new features or capabilities are enabled after an
upgrade, older clients are prevented from connecting.

Usage::

    ceph features

fs
--

Manage cephfs file systems. It uses some additional subcommands.

Subcommand ``add_data_pool`` adds an new data pool to the FS. Ths pool can
be used for file layouts as an alternate location to store the file data.

Usage::

    ceph fs add_data_pool <fs-name> <pool name/id>

Subcommand ``authorize`` creates a new client (if the client doesn't exists
on the cluster) that will be authorized for the given path in ``<fs_name>``.
Pass ``/`` to authorize for the entire FS. ``<perms>`` below can be ``r``,
``rw`` or ``rwp``.

Running it for an existing client can grant the client a new capability
(capability for a different CephFS on the same cluster or for a different
path on the same CephFS). Or it can also change read/write permission in the
capability that client already holds.


Usage::

    ceph fs authorize <fs_name> client.<client_id> <path> <perms> [<path> <perms>...]

Subcommand ``dump`` displays the FSMap at the given epoch (default: current).
This includes all file system settings, MDS daemons and the ranks they hold
and list of standby MDS daemons.

Usage::

    ceph fs dump [epoch]

Subcommand ``feature ls`` lists all CephFS features supported by current
version of Ceph.

Usage::

    ceph fs feature ls

Subcommand ``flag set`` sets a global CephFS flag. Right now the only flag
is ``enable_multiple`` which allows multiple CephFSs on a Ceph cluster.

Usage::

    ceph fs flag set <flag-name> <flag-val> --yes-i-really-mean-it

Subcommand ``get`` displays the information about FS, including settings and
ranks. Information printed here in subset of same information from the
``fs dump`` command.

Usage::

    ceph fs get <fs-name>

Subcommand ``ls`` to list file systems

Usage::

	ceph fs ls

Subcommand ``lsflags`` displays all the flags set on the given FS.

Usage::

    ceph fs lsflags <fs-name>

Subcommand ``new`` to make a new file system using named pools <metadata> and <data>

Usage::

	ceph fs new <fs_name> <metadata> <data>

Subcommand ``rename`` assigns a new name to CephFS and also updates
application tags on the pools of this CephFS.

Usage::

    ceph fs rename <fs-name> <new-fs-name> {--yes-i-really-mean-it}

Subcommand ``required_client_features`` disables a client that doesn't
possess a certain feature from connecting. This subcommand has two
subcommands, one to add a requirement and other to remove the requirement.

Usage::

    ceph fs required_client_features <fs name> add <feature-name>
    ceph fs required_client_features <fs name> rm <feature-name>

Subcommand ``reset`` is used for disaster recovery only: reset to a single-MDS
map

Usage::

	ceph fs reset <fs_name> {--yes-i-really-mean-it}

Subcommand ``rm`` to disable the named file system

Usage::

	ceph fs rm <fs_name> {--yes-i-really-mean-it}

Subcommand ``rm_data_pool``  removes the specified pool from FS's list of
data pools. File data on this pool will become unavailable. Default data pool
cannot be removed.

Usage::

    ceph fs rm_data_pool <fs-name> <pool name/id>

Subcommand ``set`` sets or updates a FS setting value for given FS name.

Usage::

    ceph fs set <fs-name> <fs-setting> <value>

Subcommand ``swap`` swaps the names of two Ceph file system and updates
application tags on the pool of the file systems accordingly. Optionally,
FSIDs of the filesystems can also be swapped along with names by passing
``--swap-fscids``.

Usage::

    ceph fs swap <fs1-name> <fs1-id> <fs2-name> <fs2-id> [--swap-fscids] {--yes-i-really-meant-it}

fsid
----

Show cluster's FSID/UUID.

Usage::

	ceph fsid


health
------

Show cluster's health.

Usage::

	ceph health {detail}


heap
----

Show heap usage info (available only if compiled with tcmalloc)

Usage::

	ceph tell <name (type.id)> heap dump|start_profiler|stop_profiler|stats

Subcommand ``release`` to make TCMalloc to releases no-longer-used memory back to the kernel at once. 

Usage::

	ceph tell <name (type.id)> heap release

Subcommand ``(get|set)_release_rate`` get or set the TCMalloc memory release rate. TCMalloc releases 
no-longer-used memory back to the kernel gradually. the rate controls how quickly this happens. 
Increase this setting to make TCMalloc to return unused memory more frequently. 0 means never return
memory to system, 1 means wait for 1000 pages after releasing a page to system. It is ``1.0`` by default..

Usage::

	ceph tell <name (type.id)> heap get_release_rate|set_release_rate {<val>}

injectargs
----------

Inject configuration arguments into monitor.

Usage::

	ceph injectargs <injected_args> [<injected_args>...]


log
---

Log supplied text to the monitor log.

Usage::

	ceph log <logtext> [<logtext>...]


mds
---

Manage metadata server configuration and administration. It uses some
additional subcommands.

Subcommand ``compat`` manages compatible features. It uses some additional
subcommands.

Subcommand ``rm_compat`` removes compatible feature.

Usage::

	ceph mds compat rm_compat <int[0-]>

Subcommand ``rm_incompat`` removes incompatible feature.

Usage::

	ceph mds compat rm_incompat <int[0-]>

Subcommand ``show`` shows mds compatibility settings.

Usage::

	ceph mds compat show

Subcommand ``fail`` forces mds to status fail.

Usage::

	ceph mds fail <role|gid>

Subcommand ``rm`` removes inactive mds.

Usage::

	ceph mds rm <int[0-]> <name> (type.id)>

Subcommand ``rmfailed`` removes failed mds.

Usage::

	ceph mds rmfailed <int[0-]>

Subcommand ``set_state`` sets mds state of <gid> to <numeric-state>.

Usage::

	ceph mds set_state <int[0-]> <int[0-20]>

Subcommand ``stat`` shows MDS status.

Usage::

	ceph mds stat

Subcommand ``repaired`` mark a damaged MDS rank as no longer damaged.

Usage::

	ceph mds repaired <role>

mon
---

Manage monitor configuration and administration. It uses some additional
subcommands.

Subcommand ``add`` adds new monitor named <name> at <addr>.

Usage::

	ceph mon add <name> <IPaddr[:port]>

Subcommand ``dump`` dumps formatted monmap (optionally from epoch)

Usage::

	ceph mon dump {<int[0-]>}

Subcommand ``getmap`` gets monmap.

Usage::

	ceph mon getmap {<int[0-]>}

Subcommand ``enable_stretch_mode`` enables stretch mode, changing the peering
rules and failure handling on all pools. For a given PG to successfully peer
and be marked active, ``min_size`` replicas will now need to be active under all
(currently two) CRUSH buckets of type <dividing_bucket>.

<tiebreaker_mon> is the tiebreaker mon to use if a network split happens.

<dividing_bucket> is the bucket type across which to stretch.
This will typically be ``datacenter`` or other CRUSH hierarchy bucket type that
denotes physically or logically distant subdivisions.

<new_crush_rule> will be set as CRUSH rule for all pools.

Usage::

	ceph mon enable_stretch_mode <tiebreaker_mon> <new_crush_rule> <dividing_bucket>

Subcommand ``remove`` removes monitor named <name>.

Usage::

	ceph mon remove <name>

Subcommand ``stat`` summarizes monitor status.

Usage::

	ceph mon stat

mgr
---

Ceph manager daemon configuration and management.

Subcommand ``dump`` dumps the latest MgrMap, which describes the active
and standby manager daemons.

Usage::

  ceph mgr dump

Subcommand ``fail`` will mark a manager daemon as failed, removing it
from the manager map.  If it is the active manager daemon a standby
will take its place.

Usage::

  ceph mgr fail <name>

Subcommand ``module ls`` will list currently enabled manager modules (plugins).

Usage::

  ceph mgr module ls

Subcommand ``module enable`` will enable a manager module.  Available modules are included in MgrMap and visible via ``mgr dump``.

Usage::

  ceph mgr module enable <module>

Subcommand ``module disable`` will disable an active manager module.

Usage::

  ceph mgr module disable <module>

Subcommand ``metadata`` will report metadata about all manager daemons or, if the name is specified, a single manager daemon.

Usage::

  ceph mgr metadata [name]

Subcommand ``versions`` will report a count of running daemon versions.

Usage::

  ceph mgr versions

Subcommand ``count-metadata`` will report a count of any daemon metadata field.

Usage::

  ceph mgr count-metadata <field>

.. _ceph-admin-osd:

osd
---

Manage OSD configuration and administration. It uses some additional
subcommands.

Subcommand ``blocklist`` manage blocklisted clients. It uses some additional
subcommands.

Subcommand ``add`` add <addr> to blocklist (optionally until <expire> seconds
from now)

Usage::

	ceph osd blocklist add <EntityAddr> {<float[0.0-]>}

Subcommand ``ls`` show blocklisted clients

Usage::

	ceph osd blocklist ls

Subcommand ``rm`` remove <addr> from blocklist

Usage::

	ceph osd blocklist rm <EntityAddr>

Subcommand ``blocked-by`` prints a histogram of which OSDs are blocking their peers

Usage::

	ceph osd blocked-by

Subcommand ``create`` creates new osd (with optional UUID and ID).

This command is DEPRECATED as of the Luminous release, and will be removed in
a future release.

Subcommand ``new`` should instead be used.

Usage::

	ceph osd create {<uuid>} {<id>}

Subcommand ``new`` can be used to create a new OSD or to recreate a previously
destroyed OSD with a specific *id*. The new OSD will have the specified *uuid*,
and the command expects a JSON file containing the base64 cephx key for auth
entity *client.osd.<id>*, as well as optional base64 cepx key for dm-crypt
lockbox access and a dm-crypt key. Specifying a dm-crypt requires specifying
the accompanying lockbox cephx key.

Usage::

    ceph osd new {<uuid>} {<id>} -i {<params.json>}

The parameters JSON file is optional but if provided, is expected to maintain
a form of the following format::

    {
        "cephx_secret": "AQBWtwhZdBO5ExAAIDyjK2Bh16ZXylmzgYYEjg==",
	"crush_device_class": "myclass"
    }

Or::

    {
        "cephx_secret": "AQBWtwhZdBO5ExAAIDyjK2Bh16ZXylmzgYYEjg==",
        "cephx_lockbox_secret": "AQDNCglZuaeVCRAAYr76PzR1Anh7A0jswkODIQ==",
        "dmcrypt_key": "<dm-crypt key>",
	"crush_device_class": "myclass"
    }

Or::

    {
	"crush_device_class": "myclass"
    }

The "crush_device_class" property is optional. If specified, it will set the
initial CRUSH device class for the new OSD.


Subcommand ``crush`` is used for CRUSH management. It uses some additional
subcommands.

Subcommand ``add`` adds or updates crushmap position and weight for <name> with
<weight> and location <args>.

Usage::

	ceph osd crush add <osdname (id|osd.id)> <float[0.0-]> <args> [<args>...]

Subcommand ``add-bucket`` adds no-parent (probably root) crush bucket <name> of
type <type>.

Usage::

	ceph osd crush add-bucket <name> <type>

Subcommand ``create-or-move`` creates entry or moves existing entry for <name>
<weight> at/to location <args>.

Usage::

	ceph osd crush create-or-move <osdname (id|osd.id)> <float[0.0-]> <args>
	[<args>...]

Subcommand ``dump`` dumps crush map.

Usage::

	ceph osd crush dump

Subcommand ``get-tunable`` get crush tunable straw_calc_version

Usage::

	ceph osd crush get-tunable straw_calc_version

Subcommand ``link`` links existing entry for <name> under location <args>.

Usage::

	ceph osd crush link <name> <args> [<args>...]

Subcommand ``move`` moves existing entry for <name> to location <args>.

Usage::

	ceph osd crush move <name> <args> [<args>...]

Subcommand ``remove`` removes <name> from crush map (everywhere, or just at
<ancestor>).

Usage::

	ceph osd crush remove <name> {<ancestor>}

Subcommand ``rename-bucket`` renames bucket <srcname> to <dstname>

Usage::

	ceph osd crush rename-bucket <srcname> <dstname>

Subcommand ``reweight`` change <name>'s weight to <weight> in crush map.

Usage::

	ceph osd crush reweight <name> <float[0.0-]>

Subcommand ``reweight-all`` recalculate the weights for the tree to
ensure they sum correctly

Usage::

	ceph osd crush reweight-all

Subcommand ``reweight-subtree`` changes all leaf items beneath <name>
to <weight> in crush map

Usage::

	ceph osd crush reweight-subtree <name> <weight>

Subcommand ``rm`` removes <name> from crush map (everywhere, or just at
<ancestor>).

Usage::

	ceph osd crush rm <name> {<ancestor>}

Subcommand ``rule`` is used for creating crush rules. It uses some additional
subcommands.

Subcommand ``create-erasure`` creates crush rule <name> for erasure coded pool
created with <profile> (default default).

Usage::

	ceph osd crush rule create-erasure <name> {<profile>}

Subcommand ``create-simple`` creates crush rule <name> to start from <root>,
replicate across buckets of type <type>, using a choose mode of <firstn|indep>
(default firstn; indep best for erasure pools).

Usage::

	ceph osd crush rule create-simple <name> <root> <type> {firstn|indep}

Subcommand ``dump`` dumps crush rule <name> (default all).

Usage::

	ceph osd crush rule dump {<name>}

Subcommand ``ls`` lists crush rules.

Usage::

	ceph osd crush rule ls

Subcommand ``rm`` removes crush rule <name>.

Usage::

	ceph osd crush rule rm <name>

Subcommand ``set`` used alone, sets crush map from input file.

Usage::

	ceph osd crush set

Subcommand ``set`` with osdname/osd.id update crushmap position and weight
for <name> to <weight> with location <args>.

Usage::

	ceph osd crush set <osdname (id|osd.id)> <float[0.0-]> <args> [<args>...]

Subcommand ``set-tunable`` set crush tunable <tunable> to <value>.  The only
tunable that can be set is straw_calc_version.

Usage::

	ceph osd crush set-tunable straw_calc_version <value>

Subcommand ``show-tunables`` shows current crush tunables.

Usage::

	ceph osd crush show-tunables

Subcommand ``tree`` shows the crush buckets and items in a tree view.

Usage::

	ceph osd crush tree

Subcommand ``tunables`` sets crush tunables values to <profile>.

Usage::

	ceph osd crush tunables legacy|argonaut|bobtail|firefly|hammer|optimal|default

Subcommand ``unlink`` unlinks <name> from crush map (everywhere, or just at
<ancestor>).

Usage::

	ceph osd crush unlink <name> {<ancestor>}

Subcommand ``df`` shows OSD utilization

Usage::

	ceph osd df {plain|tree}

Subcommand ``deep-scrub`` initiates deep scrub on specified osd.

Usage::

	ceph osd deep-scrub <who>

Subcommand ``down`` sets osd(s) <id> [<id>...] down.

Usage::

	ceph osd down <ids> [<ids>...]

Subcommand ``dump`` prints summary of OSD map.

Usage::

	ceph osd dump {<int[0-]>}

Subcommand ``erasure-code-profile`` is used for managing the erasure code
profiles. It uses some additional subcommands.

Subcommand ``get`` gets erasure code profile <name>.

Usage::

	ceph osd erasure-code-profile get <name>

Subcommand ``ls`` lists all erasure code profiles.

Usage::

	ceph osd erasure-code-profile ls

Subcommand ``rm`` removes erasure code profile <name>.

Usage::

	ceph osd erasure-code-profile rm <name>

Subcommand ``set`` creates erasure code profile <name> with [<key[=value]> ...]
pairs. Add a --force at the end to override an existing profile (IT IS RISKY).

Usage::

	ceph osd erasure-code-profile set <name> {<profile> [<profile>...]}

Subcommand ``find`` find osd <id> in the CRUSH map and shows its location.

Usage::

	ceph osd find <int[0-]>

Subcommand ``getcrushmap`` gets CRUSH map.

Usage::

	ceph osd getcrushmap {<int[0-]>}

Subcommand ``getmap`` gets OSD map.

Usage::

	ceph osd getmap {<int[0-]>}

Subcommand ``getmaxosd`` shows largest OSD id.

Usage::

	ceph osd getmaxosd

Subcommand ``in`` sets osd(s) <id> [<id>...] in.

Usage::

	ceph osd in <ids> [<ids>...]

Subcommand ``lost`` marks osd as permanently lost. THIS DESTROYS DATA IF NO
MORE REPLICAS EXIST, BE CAREFUL.

Usage::

	ceph osd lost <int[0-]> {--yes-i-really-mean-it}

Subcommand ``ls`` shows all OSD ids.

Usage::

	ceph osd ls {<int[0-]>}

Subcommand ``lspools`` lists pools.

Usage::

	ceph osd lspools {<int>}

Subcommand ``map`` finds pg for <object> in <pool>.

Usage::

	ceph osd map <poolname> <objectname>

Subcommand ``metadata`` fetches metadata for osd <id>.

Usage::

	ceph osd metadata {int[0-]} (default all)

Subcommand ``out`` sets osd(s) <id> [<id>...] out.

Usage::

	ceph osd out <ids> [<ids>...]

Subcommand ``ok-to-stop`` checks whether the list of OSD(s) can be
stopped without immediately making data unavailable.  That is, all
data should remain readable and writeable, although data redundancy
may be reduced as some PGs may end up in a degraded (but active)
state.  It will return a success code if it is okay to stop the
OSD(s), or an error code and informative message if it is not or if no
conclusion can be drawn at the current time.  When ``--max <num>`` is
provided, up to <num> OSDs IDs will return (including the provided
OSDs) that can all be stopped simultaneously.  This allows larger sets
of stoppable OSDs to be generated easily by providing a single
starting OSD and a max.  Additional OSDs are drawn from adjacent locations
in the CRUSH hierarchy.

Usage::

  ceph osd ok-to-stop <id> [<ids>...] [--max <num>]

Subcommand ``pause`` pauses osd.

Usage::

	ceph osd pause

Subcommand ``perf`` prints dump of OSD perf summary stats.

Usage::

	ceph osd perf

Subcommand ``pg-temp`` set pg_temp mapping pgid:[<id> [<id>...]] (developers
only).

Usage::

	ceph osd pg-temp <pgid> {<id> [<id>...]}

Subcommand ``force-create-pg`` forces creation of pg <pgid>.

Usage::

	ceph osd force-create-pg <pgid>


Subcommand ``pool`` is used for managing data pools. It uses some additional
subcommands.

Subcommand ``create`` creates pool.

Usage::

	ceph osd pool create <poolname> {<int[0-]>} {<int[0-]>} {replicated|erasure}
	{<erasure_code_profile>} {<rule>} {<int>} {--autoscale-mode=<on,off,warn>}

Subcommand ``delete`` deletes pool.

Usage::

	ceph osd pool delete <poolname> {<poolname>} {--yes-i-really-really-mean-it}

Subcommand ``get`` gets pool parameter <var>.

Usage::

	ceph osd pool get <poolname> size|min_size|pg_num|pgp_num|crush_rule|write_fadvise_dontneed

Only for tiered pools::

	ceph osd pool get <poolname> hit_set_type|hit_set_period|hit_set_count|hit_set_fpp|
	target_max_objects|target_max_bytes|cache_target_dirty_ratio|cache_target_dirty_high_ratio|
	cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|
	min_read_recency_for_promote|hit_set_grade_decay_rate|hit_set_search_last_n

Only for erasure coded pools::

	ceph osd pool get <poolname> erasure_code_profile

Use ``all`` to get all pool parameters that apply to the pool's type::

	ceph osd pool get <poolname> all

Subcommand ``get-quota`` obtains object or byte limits for pool.

Usage::

	ceph osd pool get-quota <poolname>

Subcommand ``ls`` list pools

Usage::

	ceph osd pool ls {detail}

Subcommand ``mksnap`` makes snapshot <snap> in <pool>.

Usage::

	ceph osd pool mksnap <poolname> <snap>

Subcommand ``rename`` renames <srcpool> to <destpool>.

Usage::

	ceph osd pool rename <poolname> <poolname>

Subcommand ``rmsnap`` removes snapshot <snap> from <pool>.

Usage::

	ceph osd pool rmsnap <poolname> <snap>

Subcommand ``set`` sets pool parameter <var> to <val>.

Usage::

	ceph osd pool set <poolname> size|min_size|pg_num|
	pgp_num|crush_rule|hashpspool|nodelete|nopgchange|nosizechange|
	hit_set_type|hit_set_period|hit_set_count|hit_set_fpp|debug_fake_ec_pool|
	target_max_bytes|target_max_objects|cache_target_dirty_ratio|
	cache_target_dirty_high_ratio|
	cache_target_full_ratio|cache_min_flush_age|cache_min_evict_age|
	min_read_recency_for_promote|write_fadvise_dontneed|hit_set_grade_decay_rate|
	hit_set_search_last_n
	<val> {--yes-i-really-mean-it}

Subcommand ``set-quota`` sets object or byte limit on pool.

Usage::

	ceph osd pool set-quota <poolname> max_objects|max_bytes <val>

Subcommand ``stats`` obtain stats from all pools, or from specified pool.

Usage::

	ceph osd pool stats {<name>}

Subcommand ``application`` is used for adding an annotation to the given
pool. By default, the possible applications are object, block, and file
storage (corresponding app-names are "rgw", "rbd", and "cephfs"). However,
there might be other applications as well. Based on the application, there
may or may not be some processing conducted.

Subcommand ``disable`` disables the given application on the given pool.

Usage::

        ceph osd pool application disable <pool-name> <app> {--yes-i-really-mean-it}

Subcommand ``enable`` adds an annotation to the given pool for the mentioned
application.

Usage::

        ceph osd pool application enable <pool-name> <app> {--yes-i-really-mean-it}

Subcommand ``get`` displays the value for the given key that is associated
with the given application of the given pool. Not passing the optional
arguments would display all key-value pairs for all applications for all
pools.

Usage::

        ceph osd pool application get {<pool-name>} {<app>} {<key>}

Subcommand ``rm`` removes the key-value pair for the given key in the given
application of the given pool.

Usage::

        ceph osd pool application rm <pool-name> <app> <key>

Subcommand ``set`` associates or updates, if it already exists, a key-value
pair with the given application for the given pool.

Usage::

        ceph osd pool application set <pool-name> <app> <key> <value>

Subcommand ``primary-affinity`` adjust osd primary-affinity from 0.0 <=<weight>
<= 1.0

Usage::

	ceph osd primary-affinity <osdname (id|osd.id)> <float[0.0-1.0]>

Subcommand ``primary-temp`` sets primary_temp mapping pgid:<id>|-1 (developers
only).

Usage::

	ceph osd primary-temp <pgid> <id>

Subcommand ``repair`` initiates repair on a specified osd.

Usage::

	ceph osd repair <who>

Subcommand ``reweight`` reweights osd to 0.0 < <weight> < 1.0.

Usage::

	osd reweight <int[0-]> <float[0.0-1.0]>

Subcommand ``reweight-by-pg`` reweight OSDs by PG distribution
[overload-percentage-for-consideration, default 120].

Usage::

	ceph osd reweight-by-pg {<int[100-]>} {<poolname> [<poolname...]}
	{--no-increasing}

Subcommand ``reweight-by-utilization`` reweights OSDs by utilization.  It only reweights
outlier OSDs whose utilization exceeds the average, eg. the default 120%
limits reweight to those OSDs that are more than 20% over the average.
[overload-threshold, default 120 [max_weight_change, default 0.05 [max_osds_to_adjust, default 4]]] 

Usage::

	ceph osd reweight-by-utilization {<int[100-]> {<float[0.0-]> {<int[0-]>}}}
	{--no-increasing}

Subcommand ``rm`` removes osd(s) <id> [<id>...] from the OSD map.


Usage::

	ceph osd rm <ids> [<ids>...]

Subcommand ``destroy`` marks OSD *id* as *destroyed*, removing its cephx
entity's keys and all of its dm-crypt and daemon-private config key
entries.

This command will not remove the OSD from crush, nor will it remove the
OSD from the OSD map. Instead, once the command successfully completes,
the OSD will show marked as *destroyed*.

In order to mark an OSD as destroyed, the OSD must first be marked as
**lost**.

Usage::

    ceph osd destroy <id> {--yes-i-really-mean-it}


Subcommand ``purge`` performs a combination of ``osd destroy``,
``osd rm`` and ``osd crush remove``.

Usage::

    ceph osd purge <id> {--yes-i-really-mean-it}

Subcommand ``safe-to-destroy`` checks whether it is safe to remove or
destroy an OSD without reducing overall data redundancy or durability.
It will return a success code if it is definitely safe, or an error
code and informative message if it is not or if no conclusion can be
drawn at the current time.

Usage::

  ceph osd safe-to-destroy <id> [<ids>...]

Subcommand ``scrub`` initiates scrub on specified osd.

Usage::

	ceph osd scrub <who>

Subcommand ``set`` sets cluster-wide <flag> by updating OSD map.
The ``full`` flag is not honored anymore since the Mimic release, and
``ceph osd set full`` is not supported in the Octopus release.

Usage::

	ceph osd set pause|noup|nodown|noout|noin|nobackfill|
	norebalance|norecover|noscrub|nodeep-scrub|notieragent

Subcommand ``setcrushmap`` sets crush map from input file.

Usage::

	ceph osd setcrushmap

Subcommand ``setmaxosd`` sets new maximum osd value.

Usage::

	ceph osd setmaxosd <int[0-]>

Subcommand ``set-require-min-compat-client`` enforces the cluster to be backward
compatible with the specified client version. This subcommand prevents you from
making any changes (e.g., crush tunables, or using new features) that
would violate the current setting. Please note, This subcommand will fail if
any connected daemon or client is not compatible with the features offered by
the given <version>. To see the features and releases of all clients connected
to cluster, please see `ceph features`_.

Usage::

    ceph osd set-require-min-compat-client <version>

Subcommand ``stat`` prints summary of OSD map.

Usage::

	ceph osd stat

Subcommand ``tier`` is used for managing tiers. It uses some additional
subcommands.

Subcommand ``add`` adds the tier <tierpool> (the second one) to base pool <pool>
(the first one).

Usage::

	ceph osd tier add <poolname> <poolname> {--force-nonempty}

Subcommand ``add-cache`` adds a cache <tierpool> (the second one) of size <size>
to existing pool <pool> (the first one).

Usage::

	ceph osd tier add-cache <poolname> <poolname> <int[0-]>

Subcommand ``cache-mode`` specifies the caching mode for cache tier <pool>.

Usage::

	ceph osd tier cache-mode <poolname> writeback|proxy|readproxy|readonly|none

Subcommand ``remove`` removes the tier <tierpool> (the second one) from base pool
<pool> (the first one).

Usage::

	ceph osd tier remove <poolname> <poolname>

Subcommand ``remove-overlay`` removes the overlay pool for base pool <pool>.

Usage::

	ceph osd tier remove-overlay <poolname>

Subcommand ``set-overlay`` set the overlay pool for base pool <pool> to be
<overlaypool>.

Usage::

	ceph osd tier set-overlay <poolname> <poolname>

Subcommand ``tree`` prints OSD tree.

Usage::

	ceph osd tree {<int[0-]>}

Subcommand ``unpause`` unpauses osd.

Usage::

	ceph osd unpause

Subcommand ``unset`` unsets cluster-wide <flag> by updating OSD map.

Usage::

	ceph osd unset pause|noup|nodown|noout|noin|nobackfill|
	norebalance|norecover|noscrub|nodeep-scrub|notieragent


pg
--

It is used for managing the placement groups in OSDs. It uses some
additional subcommands.

Subcommand ``debug`` shows debug info about pgs.

Usage::

	ceph pg debug unfound_objects_exist|degraded_pgs_exist

Subcommand ``deep-scrub`` starts deep-scrub on <pgid>.

Usage::

	ceph pg deep-scrub <pgid>

Subcommand ``dump`` shows human-readable versions of pg map (only 'all' valid
with plain).

Usage::

	ceph pg dump {all|summary|sum|delta|pools|osds|pgs|pgs_brief} [{all|summary|sum|delta|pools|osds|pgs|pgs_brief...]}

Subcommand ``dump_json`` shows human-readable version of pg map in json only.

Usage::

	ceph pg dump_json {all|summary|sum|delta|pools|osds|pgs|pgs_brief} [{all|summary|sum|delta|pools|osds|pgs|pgs_brief...]}

Subcommand ``dump_pools_json`` shows pg pools info in json only.

Usage::

	ceph pg dump_pools_json

Subcommand ``dump_stuck`` shows information about stuck pgs.

Usage::

	ceph pg dump_stuck {inactive|unclean|stale|undersized|degraded [inactive|unclean|stale|undersized|degraded...]}
	{<int>}

Subcommand ``getmap`` gets binary pg map to -o/stdout.

Usage::

	ceph pg getmap

Subcommand ``ls`` lists pg with specific pool, osd, state

Usage::

	ceph pg ls {<int>} {<pg-state> [<pg-state>...]}

Subcommand ``ls-by-osd`` lists pg on osd [osd]

Usage::

	ceph pg ls-by-osd <osdname (id|osd.id)> {<int>}
	{<pg-state> [<pg-state>...]}

Subcommand ``ls-by-pool`` lists pg with pool = [poolname]

Usage::

	ceph pg ls-by-pool <poolstr> {<int>} {<pg-state> [<pg-state>...]}

Subcommand ``ls-by-primary`` lists pg with primary = [osd]

Usage::

	ceph pg ls-by-primary <osdname (id|osd.id)> {<int>}
	{<pg-state> [<pg-state>...]}

Subcommand ``map`` shows mapping of pg to osds.

Usage::

	ceph pg map <pgid>

Subcommand ``repair`` starts repair on <pgid>.

Usage::

	ceph pg repair <pgid>

Subcommand ``scrub`` starts scrub on <pgid>.

Usage::

	ceph pg scrub <pgid>

Subcommand ``stat`` shows placement group status.

Usage::

	ceph pg stat


quorum
------

Cause a specific MON to enter or exit quorum.

Usage::

	ceph tell mon.<id> quorum enter|exit

quorum_status
-------------

Reports status of monitor quorum.

Usage::

	ceph quorum_status


report
------

Reports full status of cluster, optional title tag strings.

Usage::

	ceph report {<tags> [<tags>...]}


status
------

Shows cluster status.

Usage::

	ceph status


tell
----

Sends a command to a specific daemon.

Usage::

	ceph tell <name (type.id)> <command> [options...]


List all available commands.

Usage::

 	ceph tell <name (type.id)> help

version
-------

Show mon daemon version

Usage::

	ceph version

Options
=======

.. option:: -i infile, --in-file=infile

   will specify an input file to be passed along as a payload with the
   command to the monitor cluster. This is only used for specific
   monitor commands.

.. option:: -o outfile, --out-file=outfile

   will write any payload returned by the monitor cluster with its
   reply to outfile.  Only specific monitor commands (e.g. osd getmap)
   return a payload.

.. option:: --setuser user

   will apply the appropriate user ownership to the file specified by
   the option '-o'.

.. option:: --setgroup group

   will apply the appropriate group ownership to the file specified by
   the option '-o'.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use ceph.conf configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during startup.

.. option:: --id CLIENT_ID, --user CLIENT_ID

   Client id for authentication.

.. option:: --name CLIENT_NAME, -n CLIENT_NAME

	Client name for authentication.

.. option:: --cluster CLUSTER

	Name of the Ceph cluster.

.. option:: --admin-daemon ADMIN_SOCKET, daemon DAEMON_NAME

	Submit admin-socket commands via admin sockets in /var/run/ceph.

.. option:: --admin-socket ADMIN_SOCKET_NOPE

	You probably mean --admin-daemon

.. option:: -s, --status

	Show cluster status.

.. option:: -w, --watch

	Watch live cluster changes on the default 'cluster' channel

.. option:: -W, --watch-channel

	Watch live cluster changes on any channel (cluster, audit, cephadm, or * for all)

.. option:: --watch-debug

	Watch debug events.

.. option:: --watch-info

	Watch info events.

.. option:: --watch-sec

	Watch security events.

.. option:: --watch-warn

	Watch warning events.

.. option:: --watch-error

	Watch error events.

.. option:: --version, -v

	Display version.

.. option:: --verbose

	Make verbose.

.. option:: --concise

	Make less verbose.

.. option:: -f {json,json-pretty,xml,xml-pretty,plain,yaml}, --format

	Format of output.

    Note: yaml is only valid for orch commands.

.. option:: --daemon-output-file OUTPUT_FILE

    When using --format=json|json-pretty, you may specify a file name on the
    host running the daemon to stream output to. Be mindful this is probably
    not the same machine running the ceph command. So to analyze the output, it
    will be necessary to fetch the file once the command completes.

    OUTPUT_FILE may also be ``:tmp:``, indicating that the daemon should create
    a temporary file (subject to configurations tmp_dir and tmp_file_template).

    The ``tell`` command will output json with the path to the output file
    written to, the size of the file, the result code of the command, and any
    output produced by the command.

    Note: this option is only used for ``ceph tell`` commands.

.. option:: --connect-timeout CLUSTER_TIMEOUT

	Set a timeout for connecting to the cluster.

.. option:: --no-increasing

	 ``--no-increasing`` is off by default. So increasing the osd weight is allowed
         using the ``reweight-by-utilization`` or ``test-reweight-by-utilization`` commands.
         If this option is used with these commands, it will help not to increase osd weight
         even the osd is under utilized.

.. option:: --block

	 block until completion (scrub and deep-scrub only)

Availability
============

:program:`ceph` is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at https://docs.ceph.com for more information.


See also
========

:doc:`ceph-mon <ceph-mon>`\(8),
:doc:`ceph-osd <ceph-osd>`\(8),
:doc:`ceph-mds <ceph-mds>`\(8)
