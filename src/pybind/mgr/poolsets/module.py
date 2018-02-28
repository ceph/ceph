import errno
import threading
import json
import uuid
from collections import defaultdict

from mgr_module import MgrModule

"""
Some terminology is made up for the purposes of this module:

 - "raw pgs": pg count after applying replication, i.e. the real resource
              consumption of a pool.
 - "grow/shrink" - increase/decrease the pg_num in a pool
 - "crush subtree" - non-overlapping domains in crush hierarchy: used as
                     units of resource management.
"""


INTERVAL = 5

MIN_PG_NUM = 8


def nearest_power_of_two(n):
    v = int(n)

    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16

    # High bound power of two
    v += 1

    # Low bound power of tow
    x = v >> 1

    return x if (v - n) > (n - x) else v


def maybe_raise(r, outs, msg):
    """
    Helper for handling Ceph CLI output, raising RuntimeError on anything
    that doesn't succeed.  This is appropriate for calls that are expected
    to always succeed -- if you have expected errors that you want to handle,
    don't use this.
    """
    if r != 0:
        raise RuntimeError(msg + ": " + outs)


def parse_friendly_bytes(input_str):
    """
    Convert a human-friendly size string ("10MB", "100gb", etc)
    into a raw number of bytes.  Raises ValueError on invalid input.

    :param friendly_str: case insensitive input
    :return: integer number of bytes
    """
    # Trivial case first -- a unit-less byte count
    try:
        result = int(input_str)
    except ValueError:
        pass
    else:
        return result

    units = {
        'p': 10E15,
        't': 10E12,
        'g': 10E9,
        'm': 10E6
    }

    friendly_str = input_str.lower()

    for unit, factor in units.items():
        if friendly_str.endswith(unit):
            numeric_part = friendly_str[0:-1]
        elif friendly_str.endswith(unit + "b"):
            numeric_part = friendly_str[0:-2]
        else:
            continue

        # This will raise ValueError for us if the non-unit
        # part of the string isn't a valid integer
        return int(numeric_part) * factor

    raise ValueError("Invalid size value {0}".format(input_str))


class AdjustmentIntent(object):
    """
    During auto-adjustment, this class represents changes we would *like*
    to make to pg_num.  Used as input to selection for which changes
    we should really make.
    """

    def __init__(self, rs, ps, pool, raw_used_rate, pg_count, usf):
        self.pool_set = ps
        self.new_pg_num_target = pg_count

        self.raw_used_rate = raw_used_rate

        self.resource_status = rs

        self.pool = pool

        # How far is the current pg_num_target beneath the
        # desired pg_num_target?  For example if we're half
        # what we should be, this would be 2.
        self._undersize_fraction = usf

    @property
    def pool_name(self):
        return self.pool['pool_name']

    @property
    def current_pg_num(self):
        return self.pool['pg_num_target']

    def is_growth(self):
        return self.new_pg_num_target > self.current_pg_num

    @property
    def pg_delta(self):
        return abs(self.current_pg_num - self.new_pg_num_target)

    @property
    def raw_pg_delta(self):
        return self.pg_delta * self.raw_used_rate


class AdjustmentAborted(Exception):
    pass


class AdjustmentInProgress(object):
    STATE_WAIT_PGS = 'wait_pgs'
    STATE_WAIT_OSDMAP = 'wait_osdmap'

    def __init__(self, pool_name, old_pg_num_target, new_pg_num_target):
        self.pool_name = pool_name
        self.old_pg_num_target = old_pg_num_target
        self.new_pg_num_target = new_pg_num_target

        assert self.old_pg_num_target != self.new_pg_num_target

        self.uuid = str(uuid.uuid4())

    def _get_pool(self, osdmap):
        """
        Helper to get the pool of interest from the osdmap,
        handling the corner case where it might not exist
        """
        pool = osdmap.get_pool(self.pool_name)

        if pool is None:
            raise AdjustmentAborted("Pool {0} no longer exists!".format(
                self.pool_name))

        return pool

    @property
    def message(self):
        # FIXME: the whole point of this magic is that the user
        # doesn't need to know about PGs any more.  How can I rephrase
        # this message?
        return "Adjusting pool {0} placement groups from {1} to {2}".format(
                self.pool_name, self.old_pg_num_target, self.new_pg_num_target)

    def get_progress(self, osdmap, pg_summary):
        """
        Return float between 0.0 and 1.0
        """
        pool = self._get_pool(osdmap)

        # While what we're setting is pg_num_target, what we want to track
        # the progress/completion of is the actual pg_num adjustment that
        # happens automatically in response to our setting pg_num_target
        pg_num = pool['pg_num']

        return float(abs(pg_num - self.old_pg_num_target)) \
             / float(abs(self.new_pg_num_target - self.old_pg_num_target))

    def advance(self, osdmap, pg_summary, module):
        """
        The actual adjustment of pg_num/pgp_num is done in the
        native C++ code (based on pool's pg_target).  This function
        is responsible for observing that progress and updating the status
        of this adjustment based on the change in pg_num seen.
        """
        pool = self._get_pool(osdmap)
        pg_num_target = pool['pg_num_target']

        try:
            pg_states = pg_summary['by_pool'][pool['pool']]
            pgmap_pgs_total = sum(pg_states.values())
        except KeyError:
            # Newly created pool?  No stats for its PGs yet.
            pg_states = {}
            pgmap_pgs_total = 0

        module.log.info("pg_states: {0}".format(json.dumps(pg_states)))

        # Abort if any PGs are in unhealthy states
        abort_states = ['repair', 'recovery_toofull']
        for state in pg_states.keys():
            if state in abort_states:
                raise AdjustmentAborted("Pool {0} is unhealthy".format(
                    self.pool_name
                ))

        if pgmap_pgs_total != pg_num_target:
            # Waiting for pg map to update
            return False

        pgmap_pgs_creating_or_unknown = sum(v for k, v in pg_states.items()
                                            if
                                            'unknown' in k or 'creating' in k)

        if pgmap_pgs_creating_or_unknown > 0:
            # Waiting for creations, merges, or states to be
            # determined
            return False

        return True


class PoolProperties(object):
    """
    Info that we store about each individual pool in a poolset.
    """

    def __init__(self):
        # User hint for how much capacity they expect the set to use
        # This is not a limit!  Only used for improving PG count selection.
        self.target_size = None

        # User hint for what fraction of the overall cluster they
        # expect the set to use.  This is not a limit!  Only used
        # improving PG count selection.
        self.target_ratio = None


class PoolIntent(object):
    def __init__(self, application=None, suffix=None,
                       metadata=None, weight=None):
        # Populate these fields from user input
        self.application = application
        self.suffix = suffix
        self.metadata = metadata
        self.weight = weight
        self.target_size = None
        self.target_ratio = None

        # Populate these fields from policy
        self.initial_pg_num = None
        self.crush_rule = None
        self.name = None
        self.replicas = None
        self.erasure_coding = False
        self.erasure_code_profile = None

        # Generated based on replicas/erasure coding
        self.shard_count = None

        # Populate these fields after creation
        self.pool_id = None

    def encode(self):
        return {
            'name': self.name,
            'initial_pg_num': self.initial_pg_num,
            'crush_rule': self.crush_rule,
            'application': self.application,
            'erasure_coding': self.erasure_coding,
            'erasure_code_profile': self.erasure_code_profile,
            'replicas': self.replicas
        }

    def decode(self, data):
        self.name = data['name']
        self.initial_pg_num = data['initial_pg_num']
        self.crush_rule = data['crush_rule']
        self.application = data['application']
        self.erasure_coding = data['erasure_coding']
        self.erasure_code_profile = data['erasure_code_profile']
        self.replicas = data['replicas']


class PoolSet(object):
    """
    A set of pools with a shared purpose, such as a CephFS filesystem
    or an RGW zone.
    """

    POLICY_SILENT = 'silent'  # Do no pg count management for this pool.
    POLICY_WARN = 'warn'  # Emit warning if the pg num is too low
    POLICY_AUTOSCALE = 'autoscale'  # Automatically adjust pg num up and down.

    POLICIES = [
        POLICY_SILENT, POLICY_WARN, POLICY_AUTOSCALE
    ]


    # Simple struct versioning, imitating our C++ ::encode conventions
    ENC_VERSION = 1
    ENC_COMPAT_VERSION = 1

    def __init__(self):
        # Map of ID to extra policy per-pool
        self.pool_properties = {}

        self.policy = PoolSet.POLICY_SILENT
        self._name = None
        self._application = {}

        # During creation, we populate this with the pools we will create,
        # before later updating self.pool_properties with the resulting
        # pools.  If this field has entries then the poolset creation
        # is incomplete
        self.intents = []

    @property
    def pools(self):
        return self.pool_properties.keys()

    @property
    def name(self):
        return self._name

    def from_json(self, data):
        assert data['compat_version'] <= self.ENC_VERSION

        self._name = data['name']
        self.policy = data['policy']
        self._application = data['application']

        for pool_id, pp_data in data['pool_properties'].items():
            pp = PoolProperties()
            pp.target_size = pp_data['target_size']
            pp.target_ratio = pp_data['target_ratio']
            self.pool_properties[int(pool_id)] = pp

        for pi_data in data['intents']:
            pi = PoolIntent()
            pi.decode(pi_data)
            self.intents.append(pi)

    def to_json(self):
        pool_properties = {}
        for pool_id, pp in self.pool_properties.items():
            pool_properties[pool_id] = {
                'target_size': pp.target_size,
                'target_ratio': pp.target_ratio
            }
        data = {
            'version': self.ENC_VERSION,
            'compat_version': self.ENC_COMPAT_VERSION,
            'policy': self.policy,
            'pool_properties': pool_properties,
            'name': self._name,
            'application': self._application,
            'intents': [pi.encode() for pi in self.intents]
        }

        return data


class Adjustment(object):
    """
    Describe an ongoing change to a pool
    """

    def __init__(self, pool_id, from_pg_num, to_pg_num):
        self.pool_id = pool_id
        self.from_pg_num = from_pg_num
        self.to_pg_num = to_pg_num


class Module(MgrModule):
    """
    High level pool management.  Rather than creating individual pools,
    the user requests poolsets, which are groups of pools managed as
    a unit to fulfil a particular application requirement, such as
    an RGW zone or a CephFS filesystem.
    """

    # TODO: This should be added to always_on_modules once we're happy with it

    COMMANDS = [
        # TODO: optionally flag for all-ssd or all-hdd.  Default is to
        # use SSDs if available (+ sufficient space) for metadata/index pools
        # (important if user wants SSD pools but hasn't started their OSDs
        #  yet, such that we wouldn't automatically use SSDs)

        # TODO: flags for selecting crush rule (or perhaps not... if they
        #       are this much of a power user maybe they should be creating
        #       pools by hand)

        # TODO: some interface for extending a poolset with an additional
        #       (data) pool as we might do for CephFS -- we can pick this
        #       up automatically by watching the FSMap, but that's kind of
        #       a janky series of commands for the user: they should be able
        #       to "say what they mean" and extend their poolgroup in
        #       one command.

        # TODO: option for passing in non-default CRUSH rules, probably
        #       separately for data/metadata pools.

        # TODO: make sensible use of expected_num_objects in pool creation

        {
            "cmd": "poolset create name=app,type=CephChoices,"
                   "strings=rados|rbd|rgw|cephfs "
                   "name=psname,type=CephString "
                   "name=capacity,type=CephString,req=false "
                   "name=replicas,type=CephInt,req=false "
                   "name=erasure_coding,type=CephBool,req=false "
                   "name=erasure_code_profile,type=CephString,req=false ",
            "desc": "Configure pool group for a workload",
            "perm": "rw"
        },
        {
            "cmd": "poolset set "
                   "name=poolset,type=CephString "
                   "name=key,type=CephChoices,strings=policy "
                   "name=value,type=CephString",
            "desc": "Config poolset parameters",
            "perm": "rw"
        },
        {
            "cmd": "poolset delete name=psname,type=CephString",
            "desc": "Delete all pools in a poolset",
            "perm": "rw"
        },
        {
            "cmd": "poolset ls",
            "desc": "Show all poolsets",
            "perm": "r"
        },
        {
            "cmd": "poolset resource status",
            "desc": "Report on current placement group utilization",
            "perm": "r"
        }

    ]

    ENC_VERSION = 1
    ENC_COMPAT_VERSION = 1

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)

        # Map poolset name to PoolSet instance
        self._poolsets = {}

        self._shutdown = threading.Event()

        # List of AdjustmentInProgress instances which are not complete
        self._active_adjustments = []

        # So much of what we do peeks at the osdmap that it's easiest
        # to just keep a copy of the pythonized version.
        self._osd_map = None

        # Whether a save() is needed.  Call _mark_dirty whenever
        # modifying the poolset state.
        self._dirty = None

        # Populated at start of serve (can't access cluster data during init)
        self.target_pgs_per_osd = None
        self.max_pgs_per_osd = None

    @property
    def am_creating(self):
        """
        Return True if some pools are currently being created (i.e. now
        is not a good time to try doing any adjustments)
        """
        return any([len(ps.intents) > 0 for ps in self._poolsets.values()])

    def serve(self):
        self._load()

        # Load settings
        c = self.get("config")
        self.target_pgs_per_osd = int(c['mon_target_pg_per_osd'])
        self.max_pgs_per_osd = int(c['mon_max_pg_per_osd'])

        # Peek at latest FSMap and OSDMap to get up to date
        self._on_fs_map()
        self._on_osd_map()

        # Recover any incomplete poolset creations.  Note that any
        # exceptions from creation will surface to user in health status
        # as module failures from within serve().
        for ps_name, ps in self._poolsets.items():
            if ps.intents:
                self.log.warn(
                        "Recovering incomplete creation of poolset {0}"\
                                .format(ps_name))

                self._do_create_poolset(ps_name, ps)

        while True:
            self._shutdown.wait(timeout=INTERVAL)
            if self._shutdown.is_set():
                return

            if not self.am_creating:
                self._maybe_adjust()

            self._save()

    def shutdown(self):
        self._shutdown.set()

    def handle_command(self, inbuf, cmd):
        if cmd['prefix'] == "poolset create":
            retval = self._command_poolset_create(cmd)
        elif cmd['prefix'] == "poolset set":
            retval = self._command_poolset_set(cmd)
        elif cmd['prefix'] == "poolset delete":
            retval = self._command_poolset_delete(cmd)
        elif cmd['prefix'] == "poolset ls":
            retval = self._command_poolset_ls(cmd)
        elif cmd['prefix'] == "poolset resource status":
            retval = self._command_poolset_resource_status(cmd)
        else:
            assert False  # ceph-mgr should never pass us unknown cmds

        self._save()

        return retval

    def _command_poolset_ls(self, cmd):
        data = [ps.to_json() for ps in self._poolsets.values()]
        return 0, json.dumps(data, indent=2), ""

    def _command_poolset_resource_status(self, cmd):
        osdmap = self.get_osdmap()
        crush_map = osdmap.get_crush()

        resource_status = self.get_subtree_resource_status(osdmap, crush_map)

        pools_by_rule = defaultdict(list)
        for pool_id, pool in osdmap.get_pools().items():
            rule_name = crush_map.get_rule_by_id(pool['crush_rule'])['rule_name']
            pools_by_rule[rule_name].append(pool)

        subtree_results = {}
        for rule_name, status in resource_status.items():
            subtree_results[rule_name] = {
                'osd_count': status.osd_count,
                'pg_target': status.pg_target,
                'pg_current': status.pg_current,
                'capacity': status.capacity,
                'pool_count': len(pools_by_rule[rule_name])
            }

        adjustments = self._get_desired_adjustments(osdmap)
        adj_results = []
        for a in adjustments:
            adj_results.append({
                'pool_name': a.pool_name,
                'from': a.current_pg_num,
                'to': a.new_pg_num_target
            })

        result = {
            'subtrees': subtree_results,
            'adjustments': adj_results
        }

        return 0, json.dumps(result, indent=2), ""

    def _save(self):
        if not self._dirty:
            return

        self.log.debug("Saving ({0} poolsets)".format(
            len(self._poolsets)))

        data = {
            'poolsets': [
                ps.to_json() for ps in self._poolsets.values()
                ],
            'version': self.ENC_VERSION,
            'compat_version': self.ENC_COMPAT_VERSION
        }

        self.set_store('state', json.dumps(data))
        self._dirty = False

    def _mark_dirty(self):
        self._dirty = True

    def _load(self):
        data_json = self.get_store('state', None)
        data = json.loads(data_json) if data_json else None

        if data is None:
            # First run
            return

        assert data['compat_version'] <= self.ENC_COMPAT_VERSION

        for ps in data['poolsets']:
            ps_instance = PoolSet()
            ps_instance.from_json(ps)
            self._poolsets[ps_instance.name] = ps_instance

    def notify(self, notify_type, notify_id):
        if notify_type == "fs_map":
            self._on_fs_map()
        elif notify_type == "osd_map":
            if not self.am_creating:
                self._on_osd_map()

            adjustments = self._active_adjustments
            for a in adjustments:
                self._advance_adjustment(a)
        elif notify_type == "pg_summary":
            adjustments = self._active_adjustments
            for a in adjustments:
                self._advance_adjustment(a)

        self._save()

    def _on_osd_map(self):
        self._osd_map = self.get('osd_map')

        osdmap = self.get_osdmap()
        pools = osdmap.get_pools()

        # RGW pools get special handling
        rgw_pools = [pool for pool in pools.values() if
                     "rgw" in pool['application_metadata']]

        # Everything but cephfs and rgw (cephfs handled later in _on_fs_map)
        generic_pools = [pool for pool in pools.values() if
                         set(pool['application_metadata'].keys()) & {"rgw",
                                                                     "cephfs"}]

        # TODO: compose mapping of zone to poolset, then for any rgw-looking
        # pools not in a poolset, assign them.
        poolsets_by_zone = {}

        # If any pool is gone from OSDMap, make sure it's also
        # gone from poolset record.
        remove = []  # 2-tuple of poolset, pool_id
        for ps_name, ps in self._poolsets.items():
            for pool_id, pool_properties in ps.pool_properties.items():
                if pool_id is not None and pool_id not in pools:
                    self.log.info(
                        "Pool {0} gone from osdmap, removing".format(pool_id))

                    remove.append((ps, pool_id))

        for r_ps, r_pool in remove:
            del r_ps.pool_properties[r_pool]
            if len(r_ps.pool_properties) == 0 and len(r_ps.intents) == 0:
                self.log.info("Removing empty poolset '{0}'".format(r_ps.name))
                del self._poolsets[r_ps._name]

        if len(remove):
            self._mark_dirty()

        for pool in self._osd_map['pools']:
            existing_poolset = self._find_poolset_by_pool(pool['pool'])
            if existing_poolset is None:
                if 'cephfs' in pool['application_metadata']:
                    # Ignore it, _on_fs_map will handle poolset creation
                    pass
                elif 'rgw' in pool['application_metadata']:
                    # TODO: compose poolsets by zone
                    pass
                else:
                    # Simple rbd/rados pool, construct a poolset with
                    # this single pool in it.
                    poolset_name = self._unique_poolset_name(pool['pool_name'])
                    ps = PoolSet()
                    ps.pool_properties = {
                        pool['pool']: PoolProperties()
                    }
                    ps.policy = PoolSet.POLICY_WARN
                    ps._name = poolset_name
                    for k in pool['application_metadata'].keys():
                        ps._application[k] = {}
                    self._poolsets[poolset_name] = ps
                    self._mark_dirty()

                    self.log.info("Auto-created poolset {0}".format(
                        poolset_name
                    ))

    def _find_poolset_by_pool(self, pool_id):
        """

        :param pool_id:
        :return: PoolSet instance or None
        """
        for ps_name, ps in self._poolsets.items():
            if pool_id in ps.pools:
                return ps

        return None

    def _find_poolset_by_application(self, app, app_key, app_value):
        """
        Look up a pool by a particular value in its application metadata

        :param app:
        :param app_key:
        :param app_value:
        :return: PoolSet instance or None
        """
        for ps_name, ps in self._poolsets.items():
            app_data = ps._application.get(app, None)
            if app_data:
                v = app_data.get(app_key, None)
                if v == app_value:
                    return ps

        return None

    def _unique_name(self, root, lookup):
        return root

        # TODO reinstate unique-ness in a way that retains
        # idempotency, e.g. consider a name un-unused if the pool
        # exists but with zero objects?  or if it exists and is
        # already tagged with our poolset in the application metadata.
        i = 1
        candidate = root
        while lookup(candidate):
            i += 1
            candidate = "{0}_{1}".format(root, i)

        return candidate

    def _unique_pool_name(self, root):
        """
        Helper for creating a pool that we'd like to call `root`, but
        handling the case where it already exists by adding a suffix.
        """
        return self._unique_name(root, lambda x: x in [p['pool_name']
                                                       for p in
                                                       self._osd_map['pools']])

    def _unique_poolset_name(self, root):
        """
        Helper for creating a poolset that we'd like to call `root`, but
        handling the case where it already exists by adding a suffix.
        """
        return self._unique_name(root, lambda x: x in self._poolsets)

    def _get_default_ec_shard_count(self):
        """
        When creating EC pools targeting the default EC profile,
        we need to inspect the cluster config to learn the shard
        count (k+m) that the pool will have
        """
        def_profile = self.get_option("osd_pool_default_erasure_code_profile")
        params = {}
        for token in def_profile.split():
            try:
                k, v = token.split("=")
            except ValueError:
                pass

            params[k] = v

        if 'k' in params and 'm' in params:
            return int(params['k']) + int(params['m'])
        else:
            # Something is weird with the config option, use a value
            # based on a typical k=2 m=1 configuration
            self.log.warn("Could not parse k+m from default EC profile")
            return 3

    def _on_fs_map(self):
        fs_map = self.get('fs_map')
        for fs in fs_map['filesystems']:
            mdp_id = fs['mdsmap']['metadata_pool']
            poolset = self._find_poolset_by_pool(mdp_id)

            if poolset is None:
                poolset = PoolSet()

                # FIXME: handle case where someone has already
                # created a poolset with the same name as the filesystem
                # and is using it for something else
                poolset._name = fs['mdsmap']['fs_name']
                poolset.pool_properties = {
                    mdp_id: PoolProperties()
                }
                # We are inheriting a pre-existing set of pools, so be
                # gentle and do not enable invasive pg_num adjustment
                # by default.
                poolset.policy = PoolSet.POLICY_WARN

                poolset._application = {"cephfs": {}}

                self._poolsets[poolset._name] = poolset
                self._mark_dirty()

                self.log.info("Auto-created poolset for filesystem {0}".format(
                    poolset._name
                ))

            # We only incorporate the *first* data pool into the
            # filesystem's poolset.  Putting all the data pools for a
            # filesystem into the poolset is not logical, because
            # if someone is using multiple pools then they probably have
            # some desire for distinctive configuration of those pools,
            # so we don't want to handle them as a unit.
            dp_id = fs['mdsmap']['data_pools'][0]
            if dp_id not in poolset.pools:
                existing_dp_poolset = self._find_poolset_by_pool(dp_id)
                if existing_dp_poolset is None:
                    poolset.pool_properties[dp_id] = PoolProperties()
                    self._mark_dirty()
                else:
                    if len(existing_dp_poolset.pools) == 1:
                        # If the data pool's poolset was just containing
                        # that single pool, integrate it with the filesystem's
                        # poolset
                        poolset.target_ratio = existing_dp_poolset.target_ratio
                        poolset.target_size = existing_dp_poolset.target_size
                        poolset.policy = PoolSet.POLICY_WARN
                        del self._poolsets[existing_dp_poolset._name]
                        self._mark_dirty()
                    else:
                        # The data pool was already in a multi-pool poolset,
                        # don't interfere with this existing configuration.
                        pass

    def get_subtree_resource_status(self, osdmap, crush_map, rules=None):
        """
        For each CRUSH subtree of interest (i.e. the roots under which
        we have pools), calculate the current resource usages and targets,
        such as how many PGs there are, vs. how many PGs we would
        like there to be.
        """
        result = {}

        if rules is None:
            rules = set()
            for ps_name, ps in self._poolsets.items():
                for pool_id, pool_properties in ps.pool_properties.items():
                    p = osdmap.get_pool_by_id(pool_id)
                    if p is None:
                        self.log.warning("Pool {0} missing in osdmap!".format(
                            pool_id
                        ))
                        continue
                    crush_rule_id = p['crush_rule']

                    self.log.info("Crush rules: {0}".format(
                        json.dumps(crush_map.dump()['rules'])
                    ))
                    cr = crush_map.get_rule_by_id(crush_rule_id)
                    rules.add(cr['rule_name'])

        class CrushSubtreeResourceStatus(object):
            def __init__(self):
                self.root = None
                self.osds = None
                self.osd_count = None  # Number of OSDs
                self.pg_target = None  # Ideal full-capacity PG count?
                self.pg_current = None  # How many PGs already?
                self.capacity = None  # Total capacity of OSDs in subtree

        # Find the root node and set of OSDs under it for each rule
        for rule_name in rules:
            root = crush_map.get_rule_root(rule_name)
            self.log.debug("root of {0} is {1}".format(rule_name, root))
            if root is None:
                # Special case, if we are targeting a non existent rule
                # (expecting it to be auto created), must just assume that
                # it's covering all OSDs (default root)
                root = 'default'

            result[rule_name] = CrushSubtreeResourceStatus()
            result[rule_name].root = root
            result[rule_name].osds = crush_map.get_osds_under(root)

        # FIXME: avoid dumping out the whole PG map to python objects
        pg_map = self.get('pg_dump')

        def count_pgs_on_osds(osd_list):
            """
            We are counting the actual PGs, but for auto-adjustment
            purposes we're really more interested in the target number
            of PGs.  We can synthesize something close to this by
            applying the ratio pg_num_target/pg_num
            """

            target_factors = {}
            for pool_id, pool in osdmap.get_pools().items():
                target_factors[pool_id] = float(pool['pg_num_target']) / pool['pg_num']

            count = 0.0
            for pg in pg_map['pg_stats']:
                pool_id = int(pg['pgid'].split(".")[0])
                for osd in pg['acting']:
                    if osd in osd_list:
                        count += target_factors[pool_id]
                        # Intentionally do not break out of loop on match,
                        # because we can match multiple OSDs here and
                        # want to count the PG once for each replica (returning
                        # a raw PG count)

            return int(count)

        # TODO: because the logic here assumes non-overlapping hierarchies,
        # we should have a check that that's really true.  If it isn't,
        # we should fall back to not considering multiple sub-trees at all,
        # and treat everything as one big pot.

        # Count the number of PGs under each root.  Do this exhaustively
        # in pg map because it avoids the need to do calculations
        # based on pools higher up in the hierarchy
        for rule_name, resource_status in result.items():
            pg_count = count_pgs_on_osds(resource_status.osds)
            resource_status.pg_current = pg_count
            osd_count = len(resource_status.osds)
            resource_status.osd_count = osd_count
            resource_status.pg_target = osd_count * self.target_pgs_per_osd

            capacity = 0.0
            for osd_stats in pg_map['osd_stats']:
                if osd_stats['osd'] in resource_status.osds:
                    # Intentionally do not apply the OSD's reweight to
                    # this, because we want to calculate PG counts based
                    # on the physical storage available, not how it is
                    # reweighted right now.
                    capacity += osd_stats['kb'] * 1024

            resource_status.capacity = capacity

        return result

    def _update_health(self, adjustments):
        # Anyone in POLICY_WARN, set the health message for them and then
        # drop them from consideration.
        health_checks = []
        for adj in adjustments:
            if adj.is_growth() and adj.pool_set.policy == PoolSet.POLICY_WARN:
                health_checks.append(adj.pool_name)

        if len(health_checks) == 0:
            self.set_health_checks({})
        else:
            self.log.info(
                "Pools requiring growth: {0}".format(" ".join(health_checks)))
            if len(health_checks) == 1:
                summary = "Pool {0} has too few placement groups".format(
                    health_checks[0])
            else:
                summary = "{0} pools have too few placement groups".format(
                    len(health_checks))
            self.set_health_checks({
                "MGR_POOLSETS_TOO_FEW_PGS": {
                    'severity': 'warning',
                    'summary': summary,
                    'detail': health_checks
                }
            })

    def _maybe_grow(self, adjustments):
        # Now we've done warnings etc, cut down to only considering the
        # growths that are actually enabled for autoscaling
        growth_intents = [adj for adj in adjustments
                          if adj.pool_set.policy == PoolSet.POLICY_AUTOSCALE
                          and adj.is_growth()]

        if len(growth_intents) == 0:
            self.log.info("No growth intents")
            return

        # Prioritize growth intents by the severity of their undersize factor
        growth_intents = sorted(growth_intents,
                                key=lambda gi: gi._undersize_fraction)
        attempt_gi = growth_intents[-1]

        # Is there enough slack to do the growth we would like to do?
        available_pgs = attempt_gi.resource_status.pg_target - attempt_gi.resource_status.pg_current

        create_pgs = (attempt_gi.new_pg_num_target - attempt_gi.pool['pg_num_target']) * \
                     attempt_gi.raw_used_rate
        if available_pgs >= create_pgs:
            # Great!  There is PG allowance available
            self._start_adjustment(attempt_gi)
        else:
            self.log.warning("Insufficient resources to grow pool {0}, "
                             "looking for other pools to shrink".format(
                attempt_gi._pool_name
            ))
            # We can't do the growth we want to do, so let's try to shrink
            # something.
            self._maybe_shrink(create_pgs - available_pgs)

    def _maybe_adjust(self):
        osdmap = self.get_osdmap()
        adjustments = self._get_desired_adjustments(osdmap)

        self._update_health(adjustments)

        if len(self._active_adjustments) > 0:
            # Because _maybe_adjust is for opportunistic adjustments,
            # we don't do it if there's already something going on
            return

        by_rule = defaultdict(list)
        for adj in adjustments:
            by_rule[adj.resource_status.root].append(adj)

        for rule_root, adjustments in by_rule.items():
            # TODO: consider whether the rule root is subject to
            # any health conditions

            self._maybe_grow(adjustments)

    def _find_rule_by_root_name(self, crush_map, root_name):
        """
        Find the first CRUSH rule that uses the named root (item name)

        :return: Rule name, or None
        """
        rules = crush_map.dump()['rules']
        for r in rules:
            if len(r['steps']) and r['steps'][0]['item_name'] == root_name:
                return r['rule_name']

        # Fall through
        return None

    def _command_poolset_create(self, cmd):
        pool_set_name = cmd['psname']
        application = cmd['app']

        capacity_ratio = None
        capacity_bytes = None
        capacity_str = cmd.get('capacity', "0%")
        if capacity_str.endswith("%"):
            capacity_ratio = int(capacity_str[0:-1]) / 100.0
        else:
            try:
                capacity_bytes = parse_friendly_bytes(capacity_str)
            except ValueError as e:
                return -errno.EINVAL, "", str(e)

        if pool_set_name in self._poolsets:
            if application in self._poolsets[pool_set_name]._application:
                # It's kind of awkward to try and fully check whether
                # the existing poolset is the same as what we would have
                # created, so be pretty liberal in what we accept for
                # the idempotent case.
                return 0, "", "Poolset '%s' already exists" % pool_set_name
            else:
                return -errno.EEXIST, "", "Poolset '%s' already exists" % pool_set_name

        # Construct PoolIntent objects to describe the pools we'd like to create
        if application == "rados" or application == "rbd":
            # Single data pool
            pool_intents = [PoolIntent(application, "", False, 1.0)]
        elif application == "cephfs":
            pool_intents = [PoolIntent(application, "meta", True, 0.1),
                            PoolIntent(application, "data", False, 1.0)]
        elif application == "rgw":
            # Do not create .rgw.root pool within this poolset, it is created
            # standalone if necessary a little later.
            pool_intents = [
                PoolIntent(application, "rgw.control", True, 0.001),
                PoolIntent(application, "rgw.log", True, 0.001),
                PoolIntent(application, "rgw.meta", True, 0.001),
                PoolIntent(application, "rgw.buckets.data", False, 1.0)
            ]
        else:
            return -errno.EINVAL, '', "Invalid application '%s'" % application

        if 'replicas' in cmd:
            replicas = int(cmd['replicas'])
        else:
            replicas = int(self.get_option("osd_pool_default_size"))

        if replicas < 1:
            return (-errno.EINVAL, '',
                    'Invalid replica count {0}'.format(replicas))

        osdmap = self.get_osdmap()
        crush_map = osdmap.get_crush()

        erasure_coding = cmd.get('erasure_coding', False)
        erasure_code_profile = cmd.get('erasure_code_profile', None)

        # For working out whether there are enough OSDs of a device class
        # to create a pool, we work out the number of shards needed
        metadata_shard_count = 0
        data_shard_count = 0

        # Apply replication/EC settings to PoolIntents depending on whether
        # they're metadata
        for pi in pool_intents:
            if pi.metadata:
                # Metadata pools are always replicated
                pi.replicas = replicas
                pi.shard_count = replicas
                metadata_shard_count = max(metadata_shard_count,
                                           pi.shard_count)
            else:
                # Data pools might be EC
                if erasure_coding or erasure_code_profile:
                    pi.erasure_coding = True
                    pi.erasure_code_profile = erasure_code_profile

                    if pi.erasure_code_profile:
                        ecp = osdmap.get_ec_profile(erasure_code_profile)
                        if ecp is None:
                            return (-errno.ENOENT, '',
                                    "Erasure code profile '{0}' not found".\
                                            format(erasure_code_profile))
                        pi.shard_count = int(ecp['k']) + int(ecp['m'])
                    else:
                        pi.shard_count = self._get_default_ec_shard_count()
                else:
                    pi.replicas = replicas
                    pi.shard_count = replicas

                data_shard_count = max(metadata_shard_count,
                                       pi.shard_count)


        any_metadata_pools = any([pi.metadata for pi in pool_intents])

        # FIXME: data rule selection will currently result in targeting
        # a replicated-style CRUSH rule.  THe monitor will automatically
        # create an EC-style crush rule, but it will target the 'default'
        # root instead of the device-specific one we selected here.
        data_rule = self._select_rule(crush_map,
                                      data_shard_count, ['hdd', 'ssd'])
        if any_metadata_pools:
            metadata_rule = self._select_rule(crush_map, metadata_shard_count,
                                              ['ssd', 'hdd'])

        # Refresh, in case rule creation modified these
        osdmap = self.get_osdmap()
        crush_map = osdmap.get_crush()

        # Apply the selected rules to the PoolIntents
        for pool_intent in pool_intents:
            pool_intent.crush_rule = metadata_rule if pool_intent.metadata \
                                                   else data_rule

        # Now that we have crush rules etc set up, handle the special situation
        # where .rgw.root doesn't exist, we create it outside of this poolset
        # (the pool may be shared between multiple RGW poolsets)
        if application == "rgw":
            if '.rgw.root' not in [p['pool_name'] for p in
                                   self._osd_map['pools']]:
                # Special case: if '.rgw.root' doesn't exist yet,
                # create it as a separate poolset (because we don't want
                # the root pool deleted if someone later deletes this
                # zone's poolset)
                rgw_root = PoolIntent("rgw", ".rgw.root", True, 1.0)
                rgw_root.name = ".rgw.root"
                rgw_root.initial_pg_num = MIN_PG_NUM
                rgw_root.crush_rule = metadata_rule
                rgw_root.replicas = replicas
                self._do_create(".rgw.root", [rgw_root])

        # Clamp total weight per crush rule to 1.0.
        all_rules = list(set([pi.crush_rule for pi in pool_intents]))
        for crush_rule in all_rules:
            total_weight = sum([pi.weight for pi in pool_intents
                                if pi.crush_rule == crush_rule])

            if total_weight > 1.0:
                for pi in pool_intents:
                    if pi.crush_rule == crush_rule:
                        pi.weight /= total_weight

        rule_resource_status = self.get_subtree_resource_status(
            osdmap, crush_map, all_rules)

        # Calculate the initial_pg_num fields
        for pool_intent in pool_intents:
            # If the user didn't give projected % of cluster, then take their
            # projected GB figure, and use it to calculate the % of the capacity
            # in the targeted crush root.
            if capacity_bytes is not None:
                # Apply the pool intent's weight factor to decide how much
                # of capacity_bytes to consume for this pool.
                adjusted = capacity_bytes * pool_intent.weight
                pool_intent.target_size = adjusted

                capacity = rule_resource_status[
                    pool_intent.crush_rule].capacity
                if capacity == 0:
                    # No OSDs in our targeted CRUSH tree yet?
                    ratio = 0
                else:
                    ratio = min(adjusted / capacity, 1.0)
            else:
                ratio = capacity_ratio * pool_intent.weight
                pool_intent.target_ratio = ratio
                # FIXME: the ratio handling is a bit weird for metadata pools,
                # for example if we have a 100% ratio for data then
                # it hardly follows that the metadata pool's projected
                # size should also be 100% of the SSD OSDs.

            # Then take that projected % of the cluster and apply it to the
            # overall pg allowance in the crush tree we're targeting.
            self.log.info("Shard count of {0} is {1}".format(
                pool_intent.suffix, pool_intent.shard_count))
            initial_pg_num = pool_intent.initial_pg_num = \
                (ratio * rule_resource_status[
                    pool_intent.crush_rule].pg_target) / pool_intent.shard_count

            # Power-of-two-ize
            pool_intent.initial_pg_num = nearest_power_of_two(initial_pg_num)

            pool_intent.initial_pg_num = max(pool_intent.initial_pg_num,
                                             MIN_PG_NUM)

            self.log.info(
                "Picked pg_num {0} for pool '{1}' because it has ratio {2} "
                "of pg target {3}".format(
                    pool_intent.initial_pg_num,
                    pool_intent.suffix,
                    ratio,
                    rule_resource_status[pool_intent.crush_rule].pg_target))

        # OK, now we're all set to create the pools, but we haven't actually
        # checked whether creating these pools would cause an excessive number
        # of PGs to exist.
        for crush_rule, resource_status in rule_resource_status.items():
            pg_target = resource_status.pg_target
            pg_current = resource_status.pg_current

            rule_intents = [pi for pi in pool_intents if
                            pi.crush_rule == crush_rule]

            new_pg_count = pg_current + \
                           sum([ri.initial_pg_num * ri.shard_count for ri in
                                rule_intents])

            self.log.info("Checking proposed target pg count {0} against "
                     "ideal target for crush tree {1}".format(
                new_pg_count, pg_target
            ))

            if new_pg_count > pg_target:
                # Oh dear!  Someone before us has created too many PGs, and
                # we will exceed the target if we don't shrink someone before
                # creating our new pools.

                # We will shrink the existing pools to make room for
                # the number of PGs that we would like to have created
                adjustments = self._get_desired_adjustments(
                    osdmap, crush_rule_filter=[crush_rule])

                want_capacity = new_pg_count - pg_target
                got_capacity = self._make_room_for(adjustments, want_capacity)

                if new_pg_count - got_capacity > (
                    pg_target * (float(self.max_pgs_per_osd) / self.target_pgs_per_osd)):
                    # We have exceeded the max_pgs_per_osd threshold, pool
                    # creation would fail if we tried this, so we need to
                    # scale back the PG count on the newly created pool
                    # TODO: something smarter than blasting them all back
                    # to the minimum.
                    for pi in rule_intents:
                        pi.initial_pg_num = MIN_PG_NUM
                elif new_pg_count - got_capacity > pg_target:
                    # We couldn't free up enough PG capacity to make the
                    # target, but we're still within the hard max_pgs_per_osd
                    # limit so we're good to go.
                    pass
                else:
                    # We can successfully creat the pool within
                    # target_pgs_per_osd, great
                    pass

        # Generate pool names
        for pool_intent in pool_intents:
            if pool_intent.suffix:
                # We use "." to join names, because it happens to
                # conveniently match the RGW convention for zone pools
                pool_intent.name = self._unique_pool_name(
                    "{0}.{1}".format(pool_set_name, pool_intent.suffix))
            else:
                pool_intent.name = self._unique_pool_name(pool_set_name)

        # Now we know everything we need to know to create pools.  Create
        # a PoolSet and populate its .intents member
        pool_set = PoolSet()
        pool_set._name = pool_set_name
        pool_set.policy = PoolSet.POLICY_AUTOSCALE
        pool_set._application = {application: {}}
        pool_set.intents = pool_intents
        self._poolsets[pool_set._name] = pool_set

        # Persist the poolset (including the intents), so that we can
        # recover if anything goes wrong during the creation operations
        self._mark_dirty()
        self._save()

        # The actual pool creation, and final save of the poolset
        # in its final non-dirty state.
        self._do_create_poolset(pool_set_name, pool_set)

        # TODO: emit progress events for the actual creation of PGs
        # (i.e. after the pool is created but before the PGs have
        #  all gone active+clean)

        return 0, '', 'Created poolset {0}'.format(pool_set_name)

    def _get_desired_adjustments(
            self,
            osdmap,
            threshold=2.0,
            crush_rule_filter=None
    ):
        """
        Iterate over pools, and generate AdjustmentIntent objects
        for any pools that are `threshold` times too big or small.

        Whatever the threshold, the resulting adjustments will
        always be a halving or doubling (jcsp: no fundamental reason, this is
        just to make life simple)

        :param crush_rule_filter: optional, only consider adjusting pools
                                  in this list of crush rule names.
        """
        crush_map = osdmap.get_crush()

        # Calculate which CRUSH rules we're interested in
        rule_resource_status = self.get_subtree_resource_status(
            osdmap, crush_map)

        df = self.get('df')
        pool_stats = dict([(p['id'], p['stats']) for p in df['pools']])

        intents = []

        # First, iterate over all poolsets to determine whether their pools would
        # *like* to grow:
        # - are they smaller than (their used capacity as a fraction of the total capacity of
        #   the crush root) * the intended pg count for the crush root
        for ps_name, ps in self._poolsets.items():
            self.log.info("Checking poolset '{0}'".format(ps_name))
            for pool_id, pool_properties in ps.pool_properties.items():
                p = osdmap.get_pool_by_id(pool_id)
                if p is None:
                    self.log.warn("Pool missing from osdmap: {0}".format(
                        pool_id
                    ))
                    continue

                cr_name = crush_map.get_rule_by_id(p['crush_rule'])[
                    'rule_name']

                if crush_rule_filter is not None \
                        and cr_name not in crush_rule_filter:
                    self.log.debug("_get_desired_adjustments: ignoring pool {0}"
                                   "because it doesn't match crush rule filter {1}".format(
                        p['pool_name'],
                        crush_rule_filter
                    ))
                    continue

                raw_used_rate = osdmap.pool_raw_used_rate(pool_id)

                pool_logical_used = pool_stats[pool_id]['bytes_used']
                pool_raw_used = pool_logical_used * raw_used_rate

                capacity = rule_resource_status[cr_name].capacity

                # What proportion of space are we using?
                capacity_ratio = float(pool_raw_used) / capacity

                # So what proportion of pg allowance should we be using?
                pool_pg_target = (capacity_ratio * rule_resource_status[
                    cr_name].pg_target) / raw_used_rate

                pool_pg_target = max(MIN_PG_NUM, pool_pg_target)

                self.log.info("Pool '{0}' using {1} of space, "
                              "pg target {2} (current {3})".format(
                    p['pool_name'], capacity_ratio, pool_pg_target, p['pg_num_target']
                ))

                if pool_pg_target > p['pg_num_target'] * threshold:
                    intents.append(
                        AdjustmentIntent(rule_resource_status[cr_name], ps, p,
                                         raw_used_rate,
                                         p['pg_num_target'] * 2,
                                         pool_pg_target / p['pg_num_target']))
                elif pool_pg_target <= p['pg_num_target'] / threshold:
                    intents.append(
                        AdjustmentIntent(rule_resource_status[cr_name], ps, p,
                                         raw_used_rate,
                                         p['pg_num_target'] / 2,
                                         pool_pg_target / p['pg_num_target']))

        for i in intents:
            self.log.debug("Intent {0} {1}->{2} (autoscale={3})".format(
                i.pool_name, i.current_pg_num, i.new_pg_num_target,
                i.pool_set.policy == PoolSet.POLICY_AUTOSCALE
            ))

        return intents

    def _make_room_for(self, adjustments, new_pgs):
        """
        Call this function if one or more pools wants to grow, but there
        isn't enough PG allowance free.

        Because PG merging is expensive, we will try to do the least work:
        rather than shrinking the pool with the most extraneous PGs,
        we will shrink the smallest pool we can.

        :param adjustments: some available pool adjustments affecting
                            the crush subtree where we would like more PG
                            capacity
        :param: new_pgs: how many new PGs we would like capacity to create.

        :return: PG capacity we managed to free up: usually this should be
                 >= new_pgs.
        """

        shrink_intents = [adj for adj in adjustments
                          if adj.pool_set.policy == PoolSet.POLICY_AUTOSCALE
                          and not adj.is_growth()]

        if len(shrink_intents) == 0:
            self.log.warning("No shrink adjustments available!")
            return 0

        # Sort by PG yield
        shrink_intents = sorted(shrink_intents,
                                key=lambda x: x.raw_pg_delta)

        selected_intents = []

        self.log.info("Attempting to select pool shrinks to free {0} "
                      "raw pg capacity".format(new_pgs))
        freed_pgs = 0
        while freed_pgs < new_pgs and shrink_intents:
            intent = None
            for si in shrink_intents:
                if si.raw_pg_delta >= new_pgs:
                    intent = si

            if intent is None:
                intent = shrink_intents[-1]
                
            selected_intents.append(intent)
            shrink_intents.remove(intent)
            freed_pgs += intent.raw_pg_delta

        for intent in selected_intents:
            self.log.info("Shrinking pool {0}".format(intent.pool_name))
            self._start_adjustment(intent)

        return freed_pgs

    def _command_poolset_delete(self, cmd):
        ps_name = cmd['psname']
        self.log.info("Deleting poolset {0}".format(ps_name))

        # TODO: tombstone in Poolset so that recovery path
        # from creation doesn't try re-creating the missing
        # pools we deleted.

        # TODO: emit warning message about number of objects still
        #       in pool being deleted?
        # TODO: carry forward --yes-i-really-mean-it checks?
        #       or maybe now is the time to implement a trash-like
        #       feature.
        try:
            ps = self._poolsets[ps_name]
        except KeyError:
            self.log.warning("poolset delete on non-existent '{0}'".format(
                ps_name
            ))
            return 0, "", "Poolset '{0}' already does not exist".format(
                ps_name
            )

        # Recovery on startup should have dealt with this field
        assert len(ps.intents) == 0

        osd_map = self.get('osd_map')
        pool_id_to_name = {}
        for pool in osd_map['pools']:
            pool_id_to_name[pool['pool']] = pool['pool_name']

        for pool_id in ps.pools:
            pool_name = pool_id_to_name[pool_id]
            self.log.info("Deleting pool {0}".format(
                pool_name
            ))

            r, out, err = self.mon_command({
                'prefix': 'osd pool rm',
                'pool': pool_name,
                'pool2': pool_name,
                'yes_i_really_really_mean_it': True
            })
            if r != 0:
                return r, out, err

        del self._poolsets[ps_name]
        self._mark_dirty()

        return 0, "", ""

    def _command_poolset_set(self, cmd):
        try:
            poolset = self._poolsets[cmd['poolset']]
        except KeyError:
            return (-errno.ENOENT, '',
                   "Poolset '{0}' not found".format(cmd['poolset']))

        if cmd['key'] == 'policy':
            if cmd['value'] not in PoolSet.POLICIES:
                return (-errno.EINVAL, '',
                       "Unknown policy '{0}', options are {1}".format(
                               cmd['key'], ",".join(PoolSet.POLICIES)))

            poolset.policy = cmd['value']
            self._dirty = True
            self._save()
        else:
            return -errno.EINVAL, '', "Unknown key '{0}'".format(cmd['key'])

        return 0, '', ''

    def _select_rule(self, crush_map, min_osds, preferred_devices):
        """
        For a pool that requires at least ``min_osds`` OSDs, select
        a CRUSH rule to use, based on a preference order of
        device classes.

        This will create a CRUSH rule targeting the relevant
        device class if one doesn't already exist.
        """

        device_counts = crush_map.device_class_counts()

        for device_class in preferred_devices:
            osd_count = device_counts.get(device_class, 0)

            if osd_count >= min_osds:
                return self._get_or_create_replicated_rule(crush_map,
                        'replicated_rule_{0}'.format(device_class),
                        device_class)

        # Fall-through: can't auto select a hardward class, so target
        # the default CRUSH rule (will use whatever devices are added
        # to the system when they eventually show up).
        return self._find_rule_by_root_name(crush_map, 'default')

    def _get_or_create_replicated_rule(self, crush_map,
                                       default_name, device_class):
        """
        Find a CRUSH rule whose root targets devices of class
        ``device_class``.  If none exists, create one with
        ``default_name``.
        """
        rule_name = self._find_rule_by_root_name(crush_map,
            "default~{0}".format(device_class))

        if rule_name is None:
            r, outb, outs = self.mon_command({
                "prefix": "osd crush rule create-replicated",
                "name": default_name,
                "root": "default",
                "type": "osd",
                "class": device_class
            })
            maybe_raise(r, outs, "Unhandled error creating CRUSH rule")

            return default_name
        else:
            return rule_name

    def _do_create_poolset(self, poolset_name, poolset):
        """
        Given a poolset with some entries in .intents, create
        the pools and populate the poolset's pool_properties
        with the resulting IDs.
        """
        self._do_create_pools(poolset_name, poolset.intents)

        # do_create populates pool IDs
        for pi in poolset.intents:
            pp = PoolProperties()
            pp.target_size = pi.target_size
            pp.target_ratio = pi.target_ratio
            assert pi.pool_id is not None
            poolset.pool_properties[pi.pool_id] = pp
        poolset.intents = []

        self.log.debug("Re-saving with Pool IDs")
        self._mark_dirty()
        self._save()

    def _do_create_pools(self, poolset_name, pool_intents):
        """
        Once all resource management logic is done, issue pool creation
        commands and decorate pool intents with resulting IDs.

        :poolset_name: Poolset name, tagged onto pools as application metadata.
        """

        for pool_intent in pool_intents:
            create_args = {
                'prefix': 'osd pool create',
                'pool': pool_intent.name,
                'pg_num': pool_intent.initial_pg_num,
                'pgp_num': pool_intent.initial_pg_num,
                'rule': pool_intent.crush_rule
            }
            if pool_intent.erasure_coding:
                # Hack: We don't create CRUSH rules for EC pools because
                # the mon auto-creates them
                del create_args['rule']

                create_args['pool_type'] = 'erasure'
                if pool_intent.erasure_code_profile:
                    create_args['erasure_code_profile'] = \
                            pool_intent.erasure_code_profile
            else:
                assert pool_intent.replicas is not None
                create_args['pool_type'] = 'replicated'
                create_args['size'] = pool_intent.replicas

            r, outb, outs = self.mon_command(create_args)
            maybe_raise(r, outs, "Unhandled error creating pool {0}".format(
                pool_intent.name))

            r, outb, outs = self.mon_command({
                'prefix': 'osd pool application enable',
                'pool': pool_intent.name,
                'app': pool_intent.application
            })
            maybe_raise(r, outs,
                        "Unhandled error enabling application on {0}".format(
                            pool_intent.name))

            if poolset_name:
                r, outb, outs = self.mon_command({
                    'prefix': 'osd pool application set',
                    'pool': pool_intent.name,
                    'app': pool_intent.application,
                    'key': 'poolset',
                    'value': poolset_name
                })
                maybe_raise(r, outs,
                            "Unhandled error setting metadata on {0}".format(
                                pool_intent.name))

            osd_map = self.get_osdmap()
            pools = osd_map.dump()['pools']
            for p in pools:
                if p['pool_name'] == pool_intent.name:
                    pool_intent.pool_id = p['pool']

            if pool_intent.pool_id is None:
                raise RuntimeError()

            assert pool_intent.pool_id is not None

    def _start_adjustment(self, adj_intent):
        """
        Once all resource management logic is done, actually adjust
        pg_num_target on a pool and starting tracking progress
        of the resulting pg_num adjustment.
        """

        # Note that setting pg_num actually sets pg_num_target (see
        # OSDMonitor.cc)
        r = self.mon_command({
            'prefix': 'osd pool set',
            'pool': adj_intent.pool_name,
            'var': 'pg_num',
            'val': str(adj_intent.new_pg_num_target)
        })

        if r != 0:
            # FIXME: this is a serious and unexpected thing, we should
            # expose it as a cluster log error once the hook for doing
            # that from ceph-mgr modules is in.
            self.log.error("pg_num_target adjustment on {0} to {1} failed: {2}"
                .format(adj_intent.pool_name, adj_intent.new_pg_num_target, r))
            return

        adj = AdjustmentInProgress(
            adj_intent.pool_name,
            adj_intent.pool['pg_num_target'],
            adj_intent.new_pg_num_target
        )
        self._active_adjustments.append(adj)
        self._advance_adjustment(adj)

    def _update_progress(self, adjustment, osdmap, pg_summary):
        """
        Update the progress module about the current adjustment
        """

        self.remote("progress", "update", adjustment.uuid,
                adjustment.message, adjustment.get_progress(osdmap, pg_summary))

    def _stop_adjustment(self, adjustment):
        """
        Helper for paths (successful or not) that clear out
        the currently active adjustment
        """
        self.remote("progress", "complete", adjustment.uuid)
        self._active_adjustments.remove(adjustment)

    def _advance_adjustment(self, adjustment):
        pg_summary = self.get("pg_summary")
        osdmap = self.get_osdmap()

        try:
            complete = adjustment.advance(
                osdmap,
                pg_summary,
                self
            )
        except AdjustmentAborted as e:
            self.log.error("Adjustment aborted: {0}".format(e))
            self._stop_adjustment(adjustment)
        else:
            if complete:
                self.log.info("Complete adjustment!")
                self._stop_adjustment(adjustment)
            else:
                self.log.debug("Adjustment still in progress ({0}, {1})".format(
                    adjustment.message,
                    adjustment.get_progress(osdmap, pg_summary)))

                self._update_progress(adjustment, osdmap, pg_summary)

