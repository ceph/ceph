from mgr_module import NotifyType, HandleCommandResult
from mgr_util import CephfsClient
from prettytable import PrettyTable
from .util import norm_path, set_bal_rank_mask, dict_to_prettytable
from .perf_metric import PerfMetric
from .analyzer import Analyzer, AnalyzerState
from .workload_policy_mgr import WorkloadPolicyMgr
from .scheduler import Scheduler
from .mover import Mover
from .subtree import Subtree
from .vxattr import Vxattr
import pandas as pd
import json
import logging
import threading
import rados
import errno

log = logging.getLogger(__name__)


class PartContext:
    def __init__(self, fs_name: str):
        self.fs_name = fs_name
        self.enable = False
        self.analyzer = None
        self.mover = None
        self.policy_mgr = None
        self.scheduler = None

    def __str__(self):
        return f'fs_name: {self.fs_name}, enable: {self.enable}'

    def __repr__(self):
        return f'fs_name: {self.fs_name}, enable: {self.enable}'

    def status(self, output_format="dict"):
        dict_data = {
                        'fs_name': self.fs_name,
                        'enable': self.enable,
                    }

        if output_format == "dict":
            return dict_data
        elif output_format == "json":
            return json.dumps(dict_data, indent=4)
        else:
            return dict_to_prettytable(dict_data)


class FSMDSPartition:
    def __init__(self, mgr):
        log.debug("Init FSMDSPartition module.")
        self.mgr = mgr
        self.rados = mgr.rados
        self.fs_map = self.mgr.get('fs_map')
        self.subtree_ctx = Subtree(self.mgr)
        self.fsc = CephfsClient(mgr)
        self.vxattr = Vxattr(self.mgr, self.fsc)
        self.fsmetric = PerfMetric(self.mgr)
        self.context_map = {}
        self.lock = threading.Lock()

    def notify(self, notify_type: NotifyType):
        self.mgr.log.debug(f'got notify type {notify_type}')
        if notify_type == NotifyType.fs_map:
            with self.lock:
                self.fs_map = self.mgr.get('fs_map')

    def get_part_context(self, fs_name:str):
        if fs_name in self.context_map:
            return self.context_map[fs_name]
        return None

    def check_part_context(self, fs_name:str):
        if self.get_part_context(fs_name):
            return (0, f'mds_partitioner is not enabled for {fs_name}')
        return (-errno.EINVAL, f'mds_partitioner is not enabled for {fs_name}')

    def partitioner_enable(self, fs_name: str):
        with self.lock:
            context = self.get_part_context(fs_name)
            if context == None:
                self.context_map[fs_name] = PartContext(fs_name)
                context = self.context_map[fs_name]

            if context.enable == True:
                return HandleCommandResult(retval=0, stdout=f'mds_partitioner already enabled for {fs_name}')

            context.enable = True
            context.policy_mgr = WorkloadPolicyMgr(self.mgr, fs_name, self.vxattr, self.subtree_ctx)
            context.analyzer = Analyzer(self.mgr, context.policy_mgr, self.fsmetric, fs_name)
            context.mover = Mover(self.mgr, self.vxattr, self.subtree_ctx, fs_name, context.policy_mgr)
            context.scheduler = Scheduler(context.analyzer, context.mover, context.policy_mgr)

            # disable dynamic balancer
            set_bal_rank_mask(self.mgr, fs_name, '0x0')

            return HandleCommandResult(retval=0, stdout=f'mds_partitioner enabled for {fs_name}')

    def partitioner_disable(self, fs_name: str):
        with self.lock:
            context = self.get_part_context(fs_name)
            if not context:
                return HandleCommandResult(stdout=f'mds_partitioner already disabled for {fs_name}')

            context.analyzer.stop()
            context.mover.stop()
            context.policy_mgr.stop()
            del self.context_map[fs_name]
            return HandleCommandResult(stdout=f'mds_partitioner disabled for {fs_name}')

    def partitioner_status(self, fs_name: str, output_format: str):
        log.debug('partitioner_status')
        with self.lock:
            context = self.get_part_context(fs_name)
            output = context.status(output_format) if context else json.dumps({})
            return HandleCommandResult(stdout=output)

    def valid_output_format(self, output_format):
        if output_format.lower() in ['json', 'plain', 'prettytable']:
            return True
        return False

    def partitioner_list(self, output_format: str):
        if not self.valid_output_format(output_format):
            return HandleCommandResult(retval=-errno.EINVAL, stderr=f'Invalid output_format {output_format}')

        with self.lock:
            partitioner_dict = {}
            for fs_name in self.context_map.keys():
                partitioner_dict[fs_name] = self.context_map[fs_name].status()

            if output_format == "json":
                ret_data = json.dumps(partitioner_dict, indent=4)
            else:
                table = PrettyTable()
                table.field_names = ['fs_name', 'enable']
                for _, dict_data in partitioner_dict.items():
                    table.add_row([dict_data['fs_name'], dict_data['enable']])
                ret_data = table.get_string()

            return HandleCommandResult(stdout=ret_data)

    def workload_policy_create(self, fs_name: str, policy_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            policy_mgr = self.context_map[fs_name].policy_mgr
            res, msg = policy_mgr.create(policy_name)
            if res == True:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def workload_policy_show(self, fs_name: str, policy_name: str, output_format: str):
        if not self.valid_output_format(output_format):
            return HandleCommandResult(retval=-errno.EINVAL, stderr=f'Invalid output_format {output_format}')

        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.show(policy_name, output_format=output_format)
            if res:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def workload_policy_save(self, fs_name: str, policy_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.save(policy_name)
            if res:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def workload_policy_activate(self, fs_name: str, policy_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.activate(policy_name)
            if res == True:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def workload_policy_deactivate(self, fs_name: str, policy_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg  = self.context_map[fs_name].policy_mgr.deactivate(policy_name)
            if res == True:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stdout=msg)

    def workload_policy_list(self, fs_name: str, output_format: str):
        if not self.valid_output_format(output_format):
            return HandleCommandResult(retval=-errno.EINVAL, stderr=f'Invalid output_format {output_format}')

        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res = self.context_map[fs_name].policy_mgr.list(output_format)
            return HandleCommandResult(stdout=res)

    def workload_policy_remove(self, fs_name: str, policy_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            # check policy is used in mover or analyzer
            res, msg = self.context_map[fs_name].policy_mgr.remove(policy_name)
            if res == True:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stdout=msg)

    def workload_policy_remove_all(self, fs_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            removed_policy_names, unremoved_policy_names = self.context_map[fs_name].policy_mgr.remove_all()
            return HandleCommandResult(stdout=json.dumps({
                        'removed_policies': removed_policy_names,
                        'unremoved_policies': unremoved_policy_names,
                    }, indent=4))

    def workload_policy_history_list(self, fs_name: str, output_format: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.list_history(output_format)
            if res == True:
                output = json.dumps(msg, indent=4) if output_format == 'json' else msg
                return HandleCommandResult(stdout=output)
            else:
                return HandleCommandResult(retval=-errno.EINVAL)

    def workload_policy_history_show(self, fs_name: str, history_id: str, output_format: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.show_history(history_id, output_format)
            if res == True:
                output = json.dumps(msg, indent=4) if output_format == 'json' else msg
                log.debug(output)
                return HandleCommandResult(stdout=output)
            else:
                return HandleCommandResult(retval=-errno.EINVAL)

    def workload_policy_history_delete(self, fs_name: str, history_id: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.delete_history(history_id)
            if res == True:
                return HandleCommandResult(stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL)

    def workload_policy_history_delete_all(self, fs_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.delete_all_history()
            if res == True:
                return HandleCommandResult(stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL)

    def workload_policy_history_freeze(self, fs_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.freeze_history()
            if res == True:
                return HandleCommandResult(stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL)

    def workload_policy_set_max_mds(self, fs_name: str, policy_name: str, max_mds: int):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.set_max_mds(policy_name, max_mds)
            if res == True:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def workload_policy_dirpath_add(self, fs_name: str, policy_name: str, dir_path: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            dir_path = norm_path(dir_path)
            res, msg = self.context_map[fs_name].policy_mgr.add_subtree(policy_name, dir_path)
            if res:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def workload_policy_dirpath_list(self, fs_name: str, policy_name: str, output_format: str):
        if not self.valid_output_format(output_format):
            return HandleCommandResult(retval=-errno.EINVAL, stderr=f'Invalid output_format {output_format}')

        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.list_subtree(policy_name, output_format)
            if res:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def workload_policy_dirpath_rm(self, fs_name: str, policy_name: str, dir_path: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            res, msg = self.context_map[fs_name].policy_mgr.remove_subtree(policy_name, dir_path)
            if res:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def workload_policy_set_partition_mode(self, fs_name: str, policy_name: str, partition_mode_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            policy_mgr = self.context_map[fs_name].policy_mgr
            res, msg = policy_mgr.set_partition_mode(policy_name, partition_mode_name)
            if res == True:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def analyzer_start(self, fs_name: str, policy_name: str, duration_: str, interval_: str):
        with self.lock:
            try:
                duration = int(duration_)
            except ValueError:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'Invalid duration value {duration}')

            try:
                interval = int(interval_)
            except ValueError:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'Invalid interval value {interval}')

            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            policy_mgr = self.context_map[fs_name].policy_mgr
            if policy_mgr.is_active(policy_name) == False:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'anlayzer cannot be started because policy {policy_name} is not activated')

            cur_owner = policy_mgr.get_owner(policy_name)
            if cur_owner in ['mover', 'scheduler']:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'policy owner is {cur_owner}')

            res, subtree_list = policy_mgr.list_subtree(policy_name, output_format='dict')
            assert res == True
            if len(subtree_list) == 0:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'Subtree path must be added before starting analyzer')

            policy_mgr.set_owner(policy_name, 'analyzer')

            analyzer = self.context_map[fs_name].analyzer
            res, msg = analyzer.start(policy_name, duration, interval)
            if res == True:
                return HandleCommandResult(retval=0, stdout=msg)
            else:
                return HandleCommandResult(retval=-errno.EINVAL, stdout=msg)

    def analyzer_status(self, fs_name: str, output_format: str):
        with self.lock:
            analyzer = self.context_map[fs_name].analyzer
            res = analyzer.status(output_format)
            return HandleCommandResult(stdout=res)

    def analyzer_stop(self, fs_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            analyzer = self.context_map[fs_name].analyzer
            analyzer_status = analyzer.status()
            if analyzer_status['state'] == AnalyzerState.STOPPED:
                return HandleCommandResult(stdout='analyzer has already been stopped')

            policy_name = analyzer_status['policy_name']
            policy_mgr = self.context_map[fs_name].policy_mgr
            if policy_mgr.is_active(policy_name) == False:
                return HandleCommandResult(stdout=f'policy {policy_name} is not activated')

            cur_owner = policy_mgr.get_owner(policy_name)
            if cur_owner != 'analyzer':
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'policy owner is {cur_owner}')

            analyzer.stop()

            policy_mgr = self.context_map[fs_name].policy_mgr
            policy_mgr.release_owner(policy_name)

            result_dict = analyzer.status()
            return HandleCommandResult(stdout=json.dumps(result_dict, indent=4))


    def mover_start(self, fs_name: str, policy_name: str):
        with self.lock:
            log.debug(f'partition start {policy_name}')
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            policy_mgr = self.context_map[fs_name].policy_mgr
            if policy_mgr.is_active(policy_name) == False:
                return HandleCommandResult(stdout=f'policy {policy_name} is not activated')

            cur_owner = policy_mgr.get_owner(policy_name)
            if cur_owner in ['analyzer', 'scheduler']:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'policy owner is {cur_owner}')

            policy_mgr.set_owner(policy_name, 'mover')

            mover = self.context_map[fs_name].mover
            res, msg = mover.start(policy_name)
            if res:
                return HandleCommandResult(retval=0, stdout=msg)

            return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def mover_status(self, fs_name: str, output_format: str):
        with self.lock:
            log.debug(f'mover_status')
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            mover = self.context_map[fs_name].mover
            status = mover.status(output_format)
            return HandleCommandResult(stdout=status)

    def mover_stop(self, fs_name: str):
        log.debug(f'mover_stop')
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            mover = self.context_map[fs_name].mover
            mover_status = mover.status()
            log.debug(f'mover_status {mover_status}')
            policy_name = mover_status['policy_name']
            assert len(policy_name)
            policy_mgr = self.context_map[fs_name].policy_mgr
            cur_owner = policy_mgr.get_owner(policy_name)
            if cur_owner != 'mover':
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'policy owner is {cur_owner}')

            mover.stop()

            policy_mgr.release_owner(policy_name)

            return HandleCommandResult(stdout=f'partition stop {fs_name}')

    def scheduler_start(self, fs_name: str, policy_name: str, analyzer_period: int, scheduler_period: int):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            policy_mgr = self.context_map[fs_name].policy_mgr
            if policy_mgr.is_active(policy_name) == False:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'scheduler cannot be started because policy {policy_name} is not activated')

            cur_owner = policy_mgr.get_owner(policy_name)
            if cur_owner in ['mover', 'analyzer']:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'policy owner is {cur_owner}')

            policy_mgr.set_owner(policy_name, 'scheduler')

            scheduler = self.context_map[fs_name].scheduler
            res, msg = scheduler.start(policy_name, analyzer_period, scheduler_period)

        if res:
            return HandleCommandResult(retval=0, stdout=msg)
        return HandleCommandResult(retval=-errno.EINVAL, stderr=msg)

    def scheduler_stop(self, fs_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            scheduler = self.context_map[fs_name].scheduler
            status = scheduler.status(output_format='dict')
            policy_name = status['policy_name']

            policy_mgr = self.context_map[fs_name].policy_mgr
            if policy_mgr.is_active(policy_name) == False:
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'scheduler cannot be stopped because policy {policy_name} is not activated')

            cur_owner = policy_mgr.get_owner(policy_name)
            if cur_owner != 'scheduler':
                return HandleCommandResult(retval=-errno.EINVAL, stderr=f'policy owner is {cur_owner}')

            scheduler.stop()
            policy_mgr.release_owner(policy_name)
        return HandleCommandResult(stdout=f'scheduler stop {fs_name}')

    def scheduler_status(self, fs_name: str):
        with self.lock:
            res = self.check_part_context(fs_name)
            if res[0] != 0:
                return HandleCommandResult(retval=res[0], stderr=res[1])

            scheduler = self.context_map[fs_name].scheduler
            status = scheduler.status()
        return HandleCommandResult(stdout=status)
