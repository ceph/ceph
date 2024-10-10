from .exception import MDSPartException
from .util import get_max_mds, WorkQueue, set_bal_rank_mask
from .analyzer import SubdirState
from .workload_table import MoveType
from enum import Enum, unique
from prettytable import PrettyTable
import queue
import threading
import logging
import pandas as pd
import errno
import json
import time

log = logging.getLogger(__name__)

@unique
class MoverCtxState(Enum):
    INITIALIZED = 0
    READY       = 1
    RUNNING     = 2
    DONE        = 3

class MoverContext:
    def __init__(self, fs_name, func, policy_mgr):
        self.fs_name = fs_name
        self.func = func
        self.job_queue: queue.Queue = queue.Queue()
        self.policy_mgr = policy_mgr
        self.policy_name = ''
        self.total_jobs = 0
        self.processed_jobs = 0
        self.stopping = threading.Event()
        self.terminated = threading.Event()
        self.lock = threading.Lock()
        self.set_state(MoverCtxState.INITIALIZED)

    def add_jobs(self, job_list):
        with self.lock:
            for job in job_list:
                self.job_queue.put(job)
            self.total_jobs += len(job_list)

    def set_policy(self, policy_name):
        with self.lock:
            self.policy_name = policy_name

    def set_state(self, state):
        with self.lock:
            self.state = state

    def run(self):
        self.set_state(MoverCtxState.RUNNING)

        while not self.job_queue.empty():
            if self.stopping.is_set():
                break

            job = self.job_queue.get()
            log.debug(f'MoverContext: {job} ')
            (fs_name, subtree_path, target_rank, rank_mask, move_type) = job
            # main function
            self.func(fs_name, subtree_path, target_rank, rank_mask, move_type)

            # update subtree state
            self.policy_mgr.update_subtree_state(self.policy_name, subtree_path, SubdirState.DONE.name)

            with self.lock:
                self.processed_jobs += 1

        self.terminated.set()

        self.set_state(MoverCtxState.DONE)
        self.policy_mgr.create_or_update_history(self.policy_name, 'mover')
        self.policy_mgr.freeze_history()

    def status(self, output_format='dict'):
        with self.lock:
            dict_data = {
                            'policy_name': self.policy_name,
                            'waiting_jobs': self.job_queue.qsize(),
                            'processed_jobs': self.processed_jobs,
                            'total_jobs': self.total_jobs,
                            'status': self.state.name,
                       }
            if output_format == 'dict':
                return dict_data
            elif output_format == 'json':
                return json.dumps(dict_data, indent=4)
            else:
                table = PrettyTable()
                table.field_names = ['key', 'value']
                for k, v in dict_data.items():
                    table.add_row([k, v])
                return table.get_string(header=False)

    def stop(self):
        log.debug(f'stop() state {self.state.name}')
        with self.lock:
            if self.state == MoverCtxState.READY or self.state == MoverCtxState.RUNNING:
                self.stopping.set()
                log.debug('wating until mover thread is terminated')
                self.terminated.wait()
            else:
                log.debug('mover has already stopped.')

class Mover:
    def __init__(self, mgr, vxattr, subtree_ctx, fs_name, policy_mgr):
        log.debug("Init Mover module.")
        self.fs_name = fs_name
        self.mgr = mgr
        self.fs_map = self.mgr.get('fs_map')
        self.vxattr = vxattr
        self.subtree_ctx = subtree_ctx
        self.wq = WorkQueue(f'mover')
        self.policy_mgr = policy_mgr
        self.context = MoverContext(fs_name, self._move_subtree, policy_mgr)
        self.lock = threading.Lock()

    def status(self, output_format='dict'):
        with self.lock:
            context = self.context
            if context:
                return context.status(output_format)
            return {}

    def stop(self):
        with self.lock:
            context = self.context
            if context:
                context.stop()

    def start(self, policy_name):
        with self.lock:
            self.context.set_policy(policy_name)
            status = self.context.status()
            if status['status'] != MoverCtxState.INITIALIZED.name and status['status'] != MoverCtxState.DONE.name:
                msg = f'mover has been already started status {status["status"]}'
                return False, msg

            subtree_df = self.policy_mgr.get_df(policy_name)
            if isinstance(subtree_df, type(None)):
                return False, 'no subtress to move'

            subtree_df = subtree_df[(subtree_df['state'] == SubdirState.NEED_TO_MOVE.name)]
            log.debug(subtree_df)
            job_list = []
            for key, row in subtree_df.iterrows():
                job = (self.fs_name, row['subtree'], row['rank'], row['rank_mask'], row['move_type'])
                job_list.append(job)

            log.debug(f'make mover jobs {len(job_list)}')

            if len(job_list):
                self.context.add_jobs(job_list)
                self.wq.enqueue(self.context)
                self.context.set_state(MoverCtxState.READY)
                return True, 'mover is invoked'

            return False, 'no subtress to move'

    def _set_and_verify_vxattr(self, fs_name, subtree_path, key, val, check_wait=5, timeout=600):
        self.vxattr.set_vxattr(fs_name, subtree_path, key, val)
        for _ in range(timeout//check_wait):
            cur_val = self.vxattr.get_vxattr(fs_name, subtree_path, key)
            if str(cur_val) == str(val):
                return True
            time.sleep(check_wait)

        log.debug(f'Failed to set {key} to {val} for {subtree_path}')
        return False

    def _wait_subtree(self, fs_name, subtree_path, target_rank, check_wait=5, timeout=600, check_pin=True):
            mds_slow_request = False
            for _ in range(timeout//check_wait):
                if check_pin:
                    res = self._check_subtree_pinned(fs_name, subtree_path)
                else:
                    res = self._check_subtree_auth(fs_name, subtree_path, target_rank)

                if res:
                    log.debug(f"subtree {subtree_path} is moved to {target_rank}.")
                    return

                time.sleep(check_wait)

                if self._check_mds_slow_ops():
                    raise MDSPartException(-errno.ETIMEDOUT, \
                            f'MDS_SLOW_REQUEST occured during moving {subtree_path} to {target_rank}')

            raise MDSPartException(-errno.ETIMEDOUT, \
                    f'{subtree_path} is not moved to {target_rank} during {timeout}')

    def _move_subtree(self, fs_name, subtree_path, target_rank, rank_mask, move_type, check_wait=5, timeout=600, health_check=False):
        if health_check:
            self._wait_health_ok()

        res = self._set_and_verify_vxattr(fs_name, subtree_path, 'ceph.dir.pin', str(target_rank))
        if res == False:
            return

        if move_type == MoveType.PIN.name:
            self._wait_subtree(fs_name, subtree_path, check_wait, timeout, check_pin=True)
        elif move_type == MoveType.BAL_RANK_MASK.name:
            log.debug(f'set bal_rank_mask {rank_mask} for {subtree_path}')
            set_bal_rank_mask(self.mgr, fs_name, rank_mask)
            time.sleep(check_wait)
        elif move_type == MoveType.CEPH_DIR_BAL_MASK.name:
            log.debug(f'set ceph.dir.bal.mask to {rank_mask} for {subtree_path}')
            res = self._set_and_verify_vxattr(fs_name, subtree_path, 'ceph.dir.bal.mask', str(rank_mask))
            if res == False:
                return
            self._wait_subtree(fs_name, subtree_path, check_wait, timeout, check_pin=False)

    def _wait_health_ok(self, timeout=300, timewait=10):
        while True:
            health = json.loads(self.mgr.get('health')['json'])
            log.debug(health)
            health_status = health['status']
            if health_status == 'HEALTH_OK':
                return

            time.sleep(timewait)
            timeout -= timewait
            if timeout <= 0:
                raise MDSPartException(-errno.ETIMEDOUT, \
                    f'timedout due to {health_status}')
        assert False

    def _check_mds_slow_ops(self):
        health = json.loads(self.mgr.get('health')['json'])
        if 'MDS_SLOW_REQUEST' in health['checks']:
            log.debug(health['checks']['MDS_SLOW_REQUEST'])
            return True
        return False

    def _check_subtree_auth(self, fs_name, subtree_path, rank):
        subtree_infos = self.subtree_ctx.get_subtrees(str(rank))
        for subtree_info in subtree_infos:
            if subtree_path == subtree_info['dir']['path'] and \
                int(subtree_info['auth_first']) == rank:
                    return True
        return False

    def _check_subtree_pinned(self, fs_name, subtree_path, verbose=False):
        max_mds = get_max_mds(self.fs_map, fs_name)
        subtree_match_count = 0
        # for debug
        pin_count = 0
        mismatch_count = 0

        for rank in range(max_mds):
            subtree_infos = self.subtree_ctx.get_subtrees(str(rank))
            for subtree_info in subtree_infos:
                if subtree_path in subtree_info['dir']['path']:
                    subtree_match_count += 1
                    if int(subtree_info['export_pin']) > -1:
                        pin_count += 1
                    if int(subtree_info['auth_first']) != subtree_info['export_pin']:
                        mismatch_count += 1

        log.debug(f'auth mismatch count {mismatch_count} subtree_match_count {subtree_match_count} pin_count {pin_count}')

        if subtree_match_count > max_mds:
            return False
        return True
