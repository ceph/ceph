.. _snap-schedule:

==========================
Snapshot Scheduling Module
==========================
This module implements scheduled snapshots for CephFS.
It provides a user interface to add, query and remove snapshots schedules and
retention policies, as well as a scheduler that takes the snapshots and prunes
existing snapshots accordingly.


How to enable
=============

The *snap_schedule* module is enabled with::

  ceph mgr module enable snap_schedule

Usage
=====

This module uses :doc:`/dev/cephfs-snapshots`, please consider this documentation
as well.

This module's subcommands live under the `ceph fs snap-schedule` namespace.
Arguments can either be supplied as positional arguments or as keyword
arguments. Once a keyword argument was encountered, all following arguments are
assumed to be keyword arguments too.

Snapshot schedules are identified by path, their repeat interval and their start
time. The
repeat interval defines the time between two subsequent snapshots. It is
specified by a number and a period multiplier, one of `h(our)`, `d(ay)`,
`w(eek)`, `M(onth)` and `y(ear)`. E.g. a repeat interval of `12h` specifies one
snapshot every 12 hours.
The start time is specified as a time string (more details about passing times
below). By default
the start time is last midnight. So when a snapshot schedule with repeat
interval `1h` is added at 13:50
with the default start time, the first snapshot will be taken at 14:00.
The time zone is assumed to be UTC if none is explicitly included in the string.
An explicit time zone will be mapped to UTC at execution.
The start time must be in ISO8601 format. Examples below:

UTC: 2022-08-08T05:30:00 i.e. 5:30 AM UTC, without explicit time zone offset
IDT: 2022-08-08T09:00:00+03:00 i.e. 6:00 AM UTC
EDT: 2022-08-08T05:30:00-04:00 i.e. 9:30 AM UTC

Retention specifications are identified by path and the retention spec itself. A
retention spec consists of either a number and a time period separated by a
space or concatenated pairs of `<number><time period>`.
The semantics are that a spec will ensure `<number>` snapshots are kept that are
at least `<time period>` apart. For Example `7d` means the user wants to keep 7
snapshots that are at least one day (but potentially longer) apart from each other.
The following time periods are recognized: `h(our)`, `d(ay)`, `w(eek)`, `M(onth)`, 
`y(ear)` and `n`. The latter is a special modifier where e.g. `10n` means keep
the last 10 snapshots regardless of timing,

All subcommands take optional `fs` argument to specify paths in
multi-fs setups and :doc:`/cephfs/fs-volumes` managed setups. If not
passed `fs` defaults to the first file system listed in the fs_map.
When using :doc:`/cephfs/fs-volumes` the argument `fs` is equivalent to a
`volume`.

When a timestamp is passed (the `start` argument in the `add`, `remove`,
`activate` and `deactivate` subcommands) the ISO format `%Y-%m-%dT%H:%M:%S` will
always be accepted. When either python3.7 or newer is used or
https://github.com/movermeyer/backports.datetime_fromisoformat is installed, any
valid ISO timestamp that is parsed by python's `datetime.fromisoformat` is valid.

When no subcommand is supplied a synopsis is printed::

  #> ceph fs snap-schedule
  no valid command found; 8 closest matches:
  fs snap-schedule status [<path>] [<fs>] [<format>]
  fs snap-schedule list <path> [--recursive] [<fs>] [<format>]
  fs snap-schedule add <path> <snap_schedule> [<start>] [<fs>]
  fs snap-schedule remove <path> [<repeat>] [<start>] [<fs>]
  fs snap-schedule retention add <path> <retention_spec_or_period> [<retention_count>] [<fs>]
  fs snap-schedule retention remove <path> <retention_spec_or_period> [<retention_count>] [<fs>]
  fs snap-schedule activate <path> [<repeat>] [<start>] [<fs>]
  fs snap-schedule deactivate <path> [<repeat>] [<start>] [<fs>]
  Error EINVAL: invalid command

Note:
^^^^^
A `subvolume` argument is no longer accepted by the commands.


Inspect snapshot schedules
--------------------------

The module offers two subcommands to inspect existing schedules: `list` and
`status`. Bother offer plain and json output via the optional `format` argument.
The default is plain.
The `list` sub-command will list all schedules on a path in a short single line
format. It offers a `recursive` argument to list all schedules in the specified
directory and all contained directories.
The `status` subcommand prints all available schedules and retention specs for a
path.

Examples::

  ceph fs snap-schedule status /
  ceph fs snap-schedule status /foo/bar --format=json
  ceph fs snap-schedule list /
  ceph fs snap-schedule list / --recursive=true # list all schedules in the tree


Add and remove schedules
------------------------
The `add` and `remove` subcommands add and remove snapshots schedules
respectively. Both require at least a `path` argument, `add` additionally
requires a `schedule` argument as described in the USAGE section.

Multiple different schedules can be added to a path. Two schedules are considered
different from each other if they differ in their repeat interval and their
start time.

If multiple schedules have been set on a path, `remove` can remove individual
schedules on a path by specifying the exact repeat interval and start time, or
the subcommand can remove all schedules on a path when just a `path` is
specified.

Examples::

  ceph fs snap-schedule add / 1h
  ceph fs snap-schedule add / 1h 11:55
  ceph fs snap-schedule add / 2h 11:55
  ceph fs snap-schedule remove / 1h 11:55 # removes one single schedule
  ceph fs snap-schedule remove / 1h # removes all schedules with --repeat=1h
  ceph fs snap-schedule remove / # removes all schedules on path /

Add and remove retention policies
---------------------------------
The `retention add` and `retention remove` subcommands allow to manage
retention policies. One path has exactly one retention policy. A policy can
however contain multiple count-time period pairs in order to specify complex
retention policies.
Retention policies can be added and removed individually or in bulk via the
forms `ceph fs snap-schedule retention add <path> <time period> <count>` and
`ceph fs snap-schedule retention add <path> <countTime period>[countTime period]`

Examples::

  ceph fs snap-schedule retention add / h 24 # keep 24 snapshots at least an hour apart
  ceph fs snap-schedule retention add / d 7 # and 7 snapshots at least a day apart
  ceph fs snap-schedule retention remove / h 24 # remove retention for 24 hourlies
  ceph fs snap-schedule retention add / 24h4w # add 24 hourly and 4 weekly to retention
  ceph fs snap-schedule retention remove / 7d4w # remove 7 daily and 4 weekly, leaves 24 hourly

.. note: When adding a path to snap-schedule, remember to strip off the mount
   point path prefix. Paths to snap-schedule should start at the appropriate
   CephFS file system root and not at the host file system root.
   e.g. if the Ceph File System is mounted at ``/mnt`` and the path under which
   snapshots need to be taken is ``/mnt/some/path`` then the acutal path required
   by snap-schedule is only ``/some/path``.

.. note: It should be noted that the "created" field in the snap-schedule status
   command output is the timestamp at which the schedule was created. The "created"
   timestamp has nothing to do with the creation of actual snapshots. The actual
   snapshot creation is accounted for in the "created_count" field, which is a
   cumulative count of the total number of snapshots created so far.

.. note: The maximum number of snapshots to retain per directory is limited by the
   config tunable `mds_max_snaps_per_dir`. This tunable defaults to 100.
   To ensure a new snapshot can be created, one snapshot less than this will be
   retained. So by default, a maximum of 99 snapshots will be retained.

.. note: The --fs argument is now required if there is more than one file system.

Active and inactive schedules
-----------------------------
Snapshot schedules can be added for a path that doesn't exist yet in the
directory tree. Similarly a path can be removed without affecting any snapshot
schedules on that path.
If a directory is not present when a snapshot is scheduled to be taken, the
schedule will be set to inactive and will be excluded from scheduling until
it is activated again.
A schedule can manually be set to inactive to pause the creating of scheduled
snapshots.
The module provides the `activate` and `deactivate` subcommands for this
purpose.

Examples::

  ceph fs snap-schedule activate / # activate all schedules on the root directory
  ceph fs snap-schedule deactivate / 1d # deactivates daily snapshots on the root directory

Limitations
-----------
Snapshots are scheduled using python Timers. Under normal circumstances
specifying 1h as the schedule will result in snapshots 1 hour apart fairly
precisely. If the mgr daemon is under heavy load however, the Timer threads
might not get scheduled right away, resulting in a slightly delayed snapshot. If
this happens, the next snapshot will be schedule as if the previous one was not
delayed, i.e. one or more delayed snapshots will not cause drift in the overall
schedule.

In order to somewhat limit the overall number of snapshots in a file system, the
module will only keep a maximum of 50 snapshots per directory. If the retention
policy results in more then 50 retained snapshots, the retention list will be
shortened to the newest 50 snapshots.

Data storage
------------
The snapshot schedule data is stored in a rados object in the cephfs metadata
pool. At runtime all data lives in a sqlite database that is serialized and
stored as a rados object.
