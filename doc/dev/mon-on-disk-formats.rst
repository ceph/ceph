##############
ON-DISK FORMAT
##############


************
UPGRADE PATH
************

On-disk formats, or even data structure formats, may be changed during an
upgrade. Services wishing to do so, may so do it via the
`PaxosService::upgrade_format()` call path. There is no formalized, unified
format versioning; the `PaxosService` class keeps track of its
`format_version` through a key in the store, assumed an `unsigned int`,  but
it will be the service's responsibility to give meaning to those versions.

AUTH MONITOR
============

versions
--------

versions are represented with a single `unsigned int`. By default, the value
zero represents the absence of a formal upgraded format. The first format
version was introduced in Dumpling; clusters upgrading to Dumpling saw their
format version being increased from zero to one.::

  0 to 1 - introduced in v0.65, dev release for v0.67 dumpling
  1 to 2 - introduced in v12.0.2, dev release for luminous
  2 to 3 - introduced in mimic

  0 - all clusters pre-dumpling
  1 - all clusters dumpling+ and pre-luminous
  2 - all clusters luminous+ and pre-mimic
  3 - all clusters mimic+

  version 1: introduces new-style monitor caps (i.e., profiles)
  version 2: introduces mgr caps and bootstrap-mgr key
  version 3: creates all bootstrap and admin keys if they don't yet exist

callstack
---------

format_version set on `PaxosService::refresh()`:::

  - initially called from Monitor::refresh_from_paxos
    - initially called from Monitor::init_paxos()
      - initially called from Monitor::preinit()

AuthMonitor::upgrade_format() called by `PaxosService::_active()`:::

  - called from C_Committed callback, from PaxosService::propose_pending()
  - called from C_Active callback, from PaxosService::_active()
  - called from PaxosService::election_finished()

  - on a freshly deployed cluster, upgrade_format() will be first called
    *after* create_initial().
  - on an existing cluster, upgrade_format() will be called after the first
    election.

  - upgrade_format() is irrelevant on a freshly deployed cluster, as there is
    no format to upgrade at this point.

boil down
---------

* if `format_version >= current_version` then format is uptodate, return.
* if `features doesn't contain LUMINOUS` then `current_version = 1`
* else if `features doesn't contain MIMIC` then `current_version = 2`
* else `current_version = 3`

if `format_version == 0`:::

  - upgrade to format version 1
    - move to new-style monitor caps (i.e., profiles):
      - set daemon profiles for existing entities
      - set profile for existing bootstrap keys

if `format_version == 1`:::

  - upgrade to format version 2
    - for existing entities:
      - add new cap for mgr
    - for existing 'mgr' entities, fix 'mon' caps due to bug from kraken
      setting 'allow \*', and set 'allow profile mgr' instead.
    - add bootstrap-mgr key.

if `format_version == 2`:::

  - upgrade to format version 3
    - create all bootstrap keys if they don't currently exist
