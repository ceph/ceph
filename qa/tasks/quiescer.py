"""
Thrash mds by randomly quiescing the fs root
"""
import logging
import contextlib

from teuthology import misc

from tasks.cephfs.filesystem import MDSCluster, Filesystem
from tasks.thrasher import ThrasherGreenlet

import random
import math
import errno
import json
import time

from io import StringIO

log = logging.getLogger(__name__)

class Quiescer(ThrasherGreenlet):
    """
    The Quiescer does periodic quiescing of the configured paths, by default - the root '/'.

    quiesce_timeout: [1..)                      default: 90
     :: maximum time in seconds to wait for the quiesce to succeed
    quiesce_factor: [0.005..0.5]                default: 0.35
     :: the fraction of the total runtime we want the system quiesced
    min_quiesce: [1..)                          default: 10
     :: the minimum pause time in seconds
    max_quiesce: [1..)                          default: 60
     :: the maximum pause time in seconds
    initial_delay: [0..)                        default: 15
     :: the time in seconds before the first quiesce
    seed:                                       default: None
     :: an optional seed to a pseudorandom sequence of quiesce durations
    roots: List[String]                         default: ["/"]
     :: the roots to quiesce
    cancelations_cap: [-1..)                    default: 10
     :: the number of times we ignore canceled quiesce sets
    split_if_longer: int                        default: mean(min_quiesce, max_quiesce)
     :: if the duration is longer than this,
        it will be split into two back-to-back half durations
    ops_dump_interval: [1..quiesce_timeout)     default: 0.5*quiesce_timeout
     :: during the quiesce phase, the quiescer will dump current ops from all
        ranks until the quiesce terminates. values outside the allowed
        range (1 <= x < quiesce_timeout) disable the dump
    """

    MAX_QUIESCE_FACTOR      = 0.5    # 50%
    MIN_QUIESCE_FACTOR      = 0.005  # 0.5%
    QDB_CMD_TIMEOUT_GUARD   = 15     # sec (will be added to the configured quiesce_timeout)

    def __init__(self, ctx, fscid,
                 cluster_name='ceph',
                 quiesce_timeout=90,
                 quiesce_factor=0.35,
                 min_quiesce=10,
                 max_quiesce=60,
                 initial_delay=15,
                 cancelations_cap=10,
                 seed=None,
                 roots=None,
                 split_if_longer=None,
                 ops_dump_interval=None,
                 **other_config):
        super(Quiescer, self).__init__()

        fs = Filesystem(ctx, fscid=fscid, cluster_name=cluster_name)
        self.run_dir = fs.get_config("run_dir")
        self.logger = log.getChild('fs.[{f}]'.format(f=fs.name))
        self.name = 'quiescer.fs.[{f}]'.format(f=fs.name)
        self.archive_path = ctx.archive.strip("/")
        self.fs = fs
        try:
            self.cluster_fsid = ctx.ceph[cluster_name].fsid
        except Exception as e:
            self.logger.error(f"Couldn't get cluster fsid with exception: {e}")
            self.cluster_fsid = ''

        if seed is None:
            # try to inherit the teuthology seed,
            # otherwise, 1M seems sufficient and avoids possible huge numbers
            seed = ctx.config.get('seed', random.randint(0, 999999))
        self.logger.info(f"Initializing Quiescer with seed {seed}")
        self.rnd = random.Random(seed)

        self.quiesce_timeout = quiesce_timeout

        if (quiesce_factor > self.MAX_QUIESCE_FACTOR):
            self.logger.warn("Capping the quiesce factor at %f (requested: %f)" % (self.MAX_QUIESCE_FACTOR, quiesce_factor))
            quiesce_factor = self.MAX_QUIESCE_FACTOR

        if quiesce_factor < self.MIN_QUIESCE_FACTOR:
            self.logger.warn("Setting the quiesce factor to %f (requested: %f)" % (self.MIN_QUIESCE_FACTOR, quiesce_factor))
            quiesce_factor = self.MIN_QUIESCE_FACTOR

        self.quiesce_factor = quiesce_factor
        self.min_quiesce = max(1, min_quiesce)
        self.max_quiesce = max(1, max_quiesce)
        self.initial_delay = max(0, initial_delay)
        self.roots = roots or ["/"]
        self.cancelations_cap = cancelations_cap

        if ops_dump_interval is None:
            ops_dump_interval = 0.5 * self.quiesce_timeout

        if ops_dump_interval < 1 or ops_dump_interval >= self.quiesce_timeout:
            self.logger.warn(f"ops_dump_interval ({ops_dump_interval}) is outside the valid range [1..{self.quiesce_timeout}), disabling the dump")
            self.ops_dump_interval = None
        else:
            self.ops_dump_interval = ops_dump_interval

        # this can be used to exercise repeated quiesces with minimal delay between them 
        self.split_if_longer = split_if_longer if split_if_longer is not None else (self.min_quiesce + self.max_quiesce) / 2

    def next_quiesce_duration(self):
        """Generate the next quiesce duration

        This function is using a gauss distribution on self.rnd around the
        midpoint of the requested quiesce duration range [min_quiesce..max_quiesce]
        For that, the mu is set to mean(min_quiesce, max_quiesce) and the sigma
        is chosen so as to increase the chance of getting close to the edges of the range.
        Empirically, 3 * √(max-min), gave good results. Feel free to update this math.

        Note: self.rnd is seeded, so as to allow for repeatable sequence of durations
        Note: the duration returned by this funciton may be further split into two half-time
              quiesces, subject to the self.split_if_longer logic"""
        mu = (self.min_quiesce + self.max_quiesce) / 2
        sigma = 3 * math.sqrt(self.max_quiesce - self.min_quiesce)
        duration = round(self.rnd.gauss(mu, sigma), 1)
        duration = max(duration, self.min_quiesce)
        duration = min(duration, self.max_quiesce)
        return duration

    def tell_quiesce_leader(self, *args):
        leader = None
        rc = None
        stdout = None

        while leader is None and not self.is_stopped:
            leader = self.fs.get_var('qdb_leader')
            if leader is None:
                self.logger.warn("Couldn't get quiesce db leader from the mds map")
                self.sleep_unless_stopped(5)

        while leader is not None and not self.is_stopped:
            command = ['tell', f"mds.{leader}", 'quiesce', 'db']
            command.extend(args)
            self.logger.debug("Running ceph command: '%s'" % " ".join(command))
            result = self.fs.run_ceph_cmd(args=command, check_status=False, stdout=StringIO(),
                                          # (quiesce_timeout + guard) is a sensible cmd timeout
                                          # for both `--quiesce --await` and `--release --await`
                                          # It is an overkill for a query,
                                          # but since it's just a safety net, we use it unconditionally
                                          timeoutcmd=self.quiesce_timeout+self.QDB_CMD_TIMEOUT_GUARD)
            rc, stdout = result.exitstatus, result.stdout.getvalue()
            if rc == errno.ENOTTY:
                try:
                    resp = json.loads(stdout)
                    leader = int(resp['leader'])
                    self.logger.info("Retrying a quiesce db command with leader %d" % leader)
                except Exception as e:
                    self.logger.error("Couldn't parse ENOTTY response from an mds with error: %s\n%s" % (str(e), stdout))
                    self.sleep_unless_stopped(5)
            else:
                break

        return (rc, stdout)

    def dump_ops_all_ranks(self, dump_tag):
        remote_dumps = []

        # begin by executing dump on all ranks
        for info in self.fs.get_ranks():
            name = info['name']
            rank = info['rank']

            dump_file = f"ops-{dump_tag}-mds.{name}.json"
            daemon_path = f"{self.run_dir}/{dump_file}"
            # This gets ugly due to the current state of cephadm support
            remote_path = daemon_path
            if self.fs.mon_manager.cephadm:
                remote_path = f"{self.run_dir}/{self.cluster_fsid}/{dump_file}"

            self.logger.debug(f"Dumping ops on rank {rank} ({name}) to a remote file {remote_path}")
            try:
                args = ['tell', f'mds.{self.fs.id}:{rank}', 'ops', '--flags=locks', f'--path={daemon_path}']
                p = self.fs.run_ceph_cmd(args=args, wait=False, stdout=StringIO())
                remote_dumps.append((info, remote_path, p))
            except Exception as e:
                self.logger.error(f"Couldn't execute ops dump on rank {rank}, error: {e}")

        # now get the ops from the files
        for info, remote_path, p in remote_dumps:
            name = info['name']
            rank = info['rank']
            mds_remote = self.fs.mon_manager.find_remote('mds', name)
            try:
                p.wait()
                blob = misc.get_file(mds_remote, remote_path, sudo=True).decode('utf-8')
                self.logger.debug(f"read {len(blob)}B of ops from '{remote_path}' on mds.{rank} ({name})")
                ops_dump = json.loads(blob)
                out_name = f"{self.archive_path}/ops-{dump_tag}-mds.{name}.json"
                with open(out_name, "wt") as out:
                    out.write("{\n")
                    out.write(f'\n"info":\n{json.dumps(info, indent=2)},\n\n"ops":[\n')
                    first_op = True
                    for op in ops_dump['ops']:
                        type_data = op['type_data']
                        flag_point = type_data['flag_point']
                        if 'quiesce complete' not in flag_point:
                            self.logger.debug(f"Outstanding op at rank {rank} ({name}) for {dump_tag}: '{op['description']}'")
                        if not first_op:
                            out.write(",\n")
                        first_op = False
                        json.dump(op, fp=out, indent=2)
                    out.write("\n]}")
                self.logger.info(f"Pulled {len(ops_dump['ops'])} ops from rank {rank} ({name}) into {out_name}")
            except Exception as e:
                self.logger.error(f"Couldn't pull ops dump at '{remote_path}' on rank {info['rank']} ({info['name']}), error: {e}")
            finally:
                misc.delete_file(mds_remote, remote_path, sudo=True, check=False)

    def get_set_state_name(self, response, set_id = None):
        if isinstance(response, (str, bytes, bytearray)):
            response = json.loads(response)

        sets = response['sets']
        if len(sets) == 0:
            raise ValueError("response has no sets")

        if set_id is None:
            if len(sets) > 1:
                raise ValueError("set_id must be provided for a multiset response")
            else:
                set_id = next(iter(sets.keys()))

        return response['sets'][set_id]['state']['name']

    def check_canceled(self, response, set_id = None):
        if 'CANCELED' == self.get_set_state_name(response, set_id):
            if self.cancelations_cap == 0:
                raise RuntimeError("Reached the cap of canceled quiesces")
            else:
                self.logger.warn(f"Quiesce set got cancelled (cap = {self.cancelations_cap})."
                                "Won't raise an error since this could be a failover, "
                                "will wait for the next quiesce attempt")

            if self.cancelations_cap > 0:
                self.cancelations_cap -= 1

            return True
        return False
            
    
    def do_quiesce(self, duration):
        
        start_time = time.time()
        self.logger.debug(f"Going to quiesce for duration: {duration}")

        if self.ops_dump_interval is None:
           await_args = ["--await"]
        else:
           await_args = ["--await-for", str(self.ops_dump_interval)]

        set_id = None
        iteration = 0

        def rcinfo(rc):
            return f"{rc} ({errno.errorcode.get(rc, 'Unknown')})"

        while True:
            iteration += 1
            if set_id is None:
                # quiesce the root
                rc, stdout = self.tell_quiesce_leader(
                    *self.roots,
                    "--timeout", str(self.quiesce_timeout),
                    "--expiration", str(duration + 120), # give us 2 minutes (!) to run the release command
                    *await_args
                )
            else:
                # await the set
                rc, stdout = self.tell_quiesce_leader(
                    "--set-id", set_id,
                    *await_args
                )

            self.proceed_unless_stopped()

            try:
                response = json.loads(stdout)
                set_id = next(iter(response["sets"].keys()))
            except Exception as e:
                self.logger.error(f"Couldn't parse response with error {e}; rc: {rcinfo(rc)}; stdout:\n{stdout}")
                raise RuntimeError(f"Error parsing quiesce response: {e}")

            elapsed = round(time.time() - start_time, 1)

            if rc == errno.EINPROGRESS:
                self.logger.warn(f"Set '{set_id}' hasn't quiesced after {elapsed} seconds (timeout: {self.quiesce_timeout}). Dumping ops with locks from all ranks.")
                self.dump_ops_all_ranks(f'{set_id}-{iteration}')
            else:
                break

        if self.check_canceled(response):
            return

        if rc != 0:
            self.logger.error(f"Couldn't quiesce root with rc: {rcinfo(rc)}, stdout:\n{stdout}")
            raise RuntimeError(f"Error quiescing set '{set_id}': {rcinfo(rc)}")

        elapsed = round(time.time() - start_time, 1)
        self.logger.info(f"Successfully quiesced set '{set_id}', quiesce took {elapsed} seconds. Will release after: {duration - elapsed}")
        self.sleep_unless_stopped(duration - elapsed)

        # release the root
        rc, stdout = self.tell_quiesce_leader(
            "--set-id", set_id,
            "--release",
            "--await"
        )

        self.proceed_unless_stopped()
        
        if rc != 0:
            if self.check_canceled(stdout, set_id):
                return

            self.logger.error(f"Couldn't release set '{set_id}' with rc: {rcinfo(rc)}, stdout:\n{stdout}")
            raise RuntimeError(f"Error releasing set '{set_id}': {rcinfo(rc)}")
        else:
            elapsed = round(time.time() - start_time, 1)
            self.logger.info(f"Successfully released set '{set_id}', total seconds elapsed: {elapsed}")


    def _run(self):
        try:
            self.fs.wait_for_daemons()
            log.info(f'Ready to start quiesce thrashing; initial delay: {self.initial_delay} sec')

            self.sleep_unless_stopped(self.initial_delay)

            while not self.is_stopped:
                duration = self.next_quiesce_duration()

                if duration > self.split_if_longer:
                    self.logger.info(f"Total duration ({duration}) is longer than `split_if_longer` ({self.split_if_longer}), "
                                     "will split into two consecutive quiesces")
                    durations = [duration/2, duration/2]
                else:
                    durations = [duration]

                for d in durations:
                    self.do_quiesce(d)

                # now we sleep to maintain the quiesce factor
                self.sleep_unless_stopped((duration/self.quiesce_factor) - duration)

        except Exception as e:
            if not isinstance(e, self.Stopped):
                self.set_thrasher_exception(e)
                self.logger.exception("exception:")
            # allow successful completion so gevent doesn't see an exception...

    def stop(self):
        log.warn('The quiescer is requested to stop, running cancel all')
        self.tell_quiesce_leader( "--cancel", "--all" )
        super(Quiescer, self).stop()


def stop_all_quiescers(thrashers):
    for thrasher in thrashers:
        if not isinstance(thrasher, Quiescer):
            continue
        thrasher.stop()
        thrasher.join()
        if thrasher.exception is not None:
            raise RuntimeError(f"error during quiesce thrashing: {thrasher.exception}")


@contextlib.contextmanager
def task(ctx, config):
    """
    Stress test the mds by randomly quiescing the whole FS while another task/workunit
    is running.
    Example config (see Quiescer initializer for all available options):

    - quiescer:
        quiesce_factor: 0.2
        max_quiesce: 30
        quiesce_timeout: 10
    """

    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'quiescer task only accepts a dict for configuration'
    mdslist = list(misc.all_roles_of_type(ctx.cluster, 'mds'))
    assert len(mdslist) > 0, \
        'quiescer task requires at least 1 metadata server'

    cluster_name = config.get('cluster', 'ceph')
    # the manager should be there
    manager = ctx.managers[cluster_name]
    manager.wait_for_clean()
    assert manager.is_clean()

    mds_cluster = MDSCluster(ctx)
    for fs in mds_cluster.status().get_filesystems():
        quiescer = Quiescer(ctx=ctx, fscid=fs['id'], cluster_name=cluster_name, **config)
        quiescer.start()
        ctx.ceph[cluster_name].thrashers.append(quiescer)

    try:
        log.debug('Yielding')
        yield
    finally:
        log.info('joining Quiescers')
        stop_all_quiescers(ctx.ceph[cluster_name].thrashers)
        log.info('done joining Quiescers')
