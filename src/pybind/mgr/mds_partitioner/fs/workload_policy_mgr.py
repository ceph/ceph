from .workload_table import WorkloadTable, SubdirState, WorkloadType, PartitionMode
from prettytable import PrettyTable
import logging
import threading
import json
import os
import datetime

log = logging.getLogger(__name__)


class WorkloadPolicy:
    def __init__(self, mgr, fs_name, vxattr, subtree_ctx, policy_name, owner='', subtree_set=[], max_mds=-1, workload_table={}):
        self.mgr = mgr
        self.vxattr = vxattr
        self.subtree_ctx = subtree_ctx
        self.activate = False
        self.subtree_set_scan = set()

        # on-kv structures
        self.fs_name = fs_name
        self.policy_name = policy_name
        self.owner = ''
        self.max_mds = max_mds
        self.subtree_set = set(subtree_set)
        self.workload_table = WorkloadTable(mgr, fs_name, vxattr, subtree_ctx, workload_table)

    def get_dict_data(self):
        workload_table_dict = self.workload_table.get_on_kv_structures()
        policy_dict = {
                          'owner': self.owner,
                          'policy_name': self.policy_name,
                          'max_mds': self.max_mds,
                          'fs_name': self.fs_name,
                          'subtree_set': list(self.subtree_set),
                          'workload_table': workload_table_dict,
                      }
        return policy_dict

    def get_pretty_table(self):
        output_str = f"policy_name: {self.policy_name}\n"
        output_str += f"activate: {self.activate}\n"
        output_str += f"owner: {self.owner}\n"
        output_str += f"max_mds: {self.max_mds}\n"
        wt = self.workload_table
        output_str += f"target_workload: {wt.get_target_moderate_workload_per_rank()}\n"
        output_str += f"target_delta: {wt.get_target_delta()}\n"
        output_str += f"max_delta_ratio: {wt.get_max_delta_ratio()}\n"
        output_str += f"current workload_stddev: {wt.get_old_workload_stddev()}\n"
        output_str += f"target workload_stddev: {wt.get_new_workload_stddev()}\n"
        output_str += f"workload_table_ready: {wt.check_df_ready()}\n"
        output_str += f"workload_table\n{wt.get_pretty_table()}\n"
        output_str += f"rank_table\n{wt.get_pretty_table(group_by=True, include_index=True)}\n"
        return output_str


class History:
    def __init__(self, mgr, fs_name, store_kv, get_kv):
        self.mgr = mgr
        self.policy_list = {}
        self.owner = ""
        self.history_id = ""
        self.history_max = self.mgr.get_module_option('history_max')
        self.store_kv = store_kv
        self.get_kv = get_kv
        self.fs_name = fs_name
        self.key = f'history_{fs_name}'

        self.recovery()

    def recovery(self):
        log.debug('recovery')
        self.policy_list = self.get_kv(self.key)
        self.adjust_history()

    def remove_last_entry(self):
        if len(self.policy_list.keys()):
            old = sorted(self.policy_list.keys())[0]
            log.debug(f"remove {old} history")
            del self.policy_list[old]

    def adjust_history(self):
        log.debug("adjust history")
        history_size = len(self.policy_list)
        if history_size > self.mgr.get_module_option('history_max'):
            log.debug(f"history_max old: {history_size}, new: {self.mgr.get_module_option('history_max')}")
            diff = history_size - self.mgr.get_module_option('history_max')
            log.debug(f"diff {diff}")
            for _ in range(diff):
                self.remove_last_entry()

        self.history_max = self.mgr.get_module_option('history_max')

    def create_or_update(self, fs_name, policy, owner):
        assert owner in ['analyzer', 'mover']

        log.debug("create_or_update")
        self.adjust_history()
        if len(self.history_id) == 0:
            now_str = datetime.datetime.now().strftime("%Y-%m-%d-%H_%M_%S")
            self.history_id = f"{now_str}_{fs_name}_{policy.policy_name}"

            log.debug(f"created history id {self.history_id}")
            if len(self.policy_list.keys()) >= self.history_max:
                self.remove_last_entry()

        self.owner = owner
        history_data = {
                            'history_id': self.history_id,
                            'fs_name': fs_name,
                            'policy': policy.get_dict_data(),
                        }
        self.policy_list[self.history_id] = history_data
        self.store_kv(self.key, self.policy_list)

    def freeze(self):
        log.debug("freeze")
        history_id = self.history_id
        self.history_id = ""
        self.owner = ""

        return True, f"history_id {history_id} has been freezed"

    def delete(self, history_id):
        log.debug("delete")
        if history_id not in self.policy_list:
            return False, f"{history_id} is not available" 

        del self.policy_list[history_id]

        self.adjust_history()

        if len(self.policy_list):
            self.store_kv(self.key, self.policy_list)
        else:
            self.store_kv(self.key, None)
        return True, f"history_id {history_id} has been removed"

    def delete_all(self):
        log.debug("delete_all")
        self.policy_list = {}
        self.adjust_history()
        self.store_kv(self.key, None)
        return True, f"histories have been removed"

    def list(self, output_format="json"):
        log.debug(f'history list {self.policy_list.keys()}')
        self.adjust_history()
        ids = sorted(self.policy_list.keys())
        ids = list(map(lambda id: id + "*" if self.history_id == id else id, ids))
        if output_format == "json":
            return True, ids 
        else:
            table = PrettyTable()
            table.field_names = ['history_id']
            for id in ids:
                table.add_row([id])
            log.debug(table.get_string())
            return True, table.get_string()

    def show(self, history_id, output_format="json"):
        log.debug("show")
        if history_id not in self.policy_list:
            return False, f"{history_id} is not available" 
        if output_format == "json":
            return True, self.policy_list[history_id]
        else:
            history_data = self.policy_list[history_id]
            output_string = f"history_id: {history_data['history_id']}\n"
            output_string += f"fs_name: {history_data['fs_name']}\n"
            policy = history_data['policy']

            output_string += f"owner: {policy['owner']}\n"
            output_string += f"policy_name: {policy['policy_name']}\n"
            output_string += f"max_mds: {policy['max_mds']}\n"

            workload_table = policy['workload_table']
            partition_mode  = PartitionMode(workload_table['partition_mode']).name
            output_string += f"partition_mode: {partition_mode}\n"
            workload_type = WorkloadType(workload_table['workload_type']).name
            output_string += f"workload_type: {workload_type}\n"
            df =  WorkloadTable.dict_to_df(workload_table['data'])
            prettytable_str = WorkloadTable.make_pretty_table(df).get_string()
            output_string += f"{prettytable_str}\n"
            return True, output_string

class WorkloadPolicyMgr:
    def __init__(self, mgr, fs_name, vxattr, subtree_ctx):
        self.mgr = mgr
        self.fs_name = fs_name
        self.vxattr = vxattr
        self.subtree_ctx = subtree_ctx
        self.workload_policies = {}
        self.lock = threading.Lock()
        self.history = History(mgr, fs_name, self.store_kv, self.get_kv)

        # recover policies from the kv store
        self.recover(fs_name)

    def create_or_update_history(self, policy_name, owner):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return
            self.history.create_or_update(self.fs_name, policy, owner)

    def freeze_history(self):
        with self.lock:
            return self.history.freeze()

    def delete_history(self, history_id):
        with self.lock:
            return self.history.delete(history_id)

    def delete_all_history(self):
        with self.lock:
            return self.history.delete_all()

    def list_history(self, output_format):
        with self.lock:
            return self.history.list(output_format)

    def show_history(self, history_id, output_format):
        with self.lock:
            return self.history.show(history_id, output_format)

    def recover(self, fs_name):
        with self.lock:
            data = self.get_kv_prefix(fs_name)
            for k, v in data.items():
                v = json.loads(v)
                if 'policy_name' in v:
                    log.debug(f'recovered workload_table {v["workload_table"]}')
                    self._create_policy(v['policy_name'], v['owner'], v['subtree_set'], v['max_mds'], v['workload_table'])
                else:
                    log.debug(f'skip recovery with {k}:{v}')

    def load(self, policy_name):
        with self.lock:
            if policy_name not in self.workload_policies:
                return False

            key = f'{self.fs_name}.{policy_name}'
            data = self.get_kv(key)
            log.debug(data)
            return True

    def _save(self, fs_name, policy_name, policy):
        policy_dict = policy.get_dict_data()
        log.debug(policy_dict)
        key = f'{self.fs_name}.{policy_name}'
        self.store_kv(key, policy_dict)

    def save(self, policy_name):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return False, f"{policy_name} is not available" 

            self._save(self.fs_name, policy_name, policy)
            return True, f"{policy_name} has been saved in the kv store"

    def _create_policy(self, policy_name, owner='', subtree_set=[], max_mds=-1, workload_table={}):
        policy = self.workload_policies.get(policy_name, None)
        if policy:
            return False, f"{policy_name} has already been created"

        self.workload_policies[policy_name] = \
                WorkloadPolicy(self.mgr, self.fs_name, self.vxattr, self.subtree_ctx, policy_name,
                owner, subtree_set, max_mds, workload_table)
        policy = self.workload_policies[policy_name]
        output = self._policy_show(policy, output_format='dict')
        log.debug(f'recovered policy data {output}')
        self._scan_all_subdirs(policy)
        self._save(self.fs_name, policy_name, policy)
        return True, '{policy_name} has been created'

    def create(self, policy_name):
        with self.lock:
            return self._create_policy(policy_name)

    def _policy_show(self, policy, output_format='json'):
        output_dict = {
                    'policy_name': policy.policy_name,
                    'activate': policy.activate,
                    'owner': policy.owner,
                    'subtree_list': list(policy.subtree_set),
                    'subtree_list_scan': list(policy.subtree_set_scan),
                    'max_mds': policy.max_mds,
                    'workload_table': policy.workload_table.get_dict_data(),
                    'rank_table': policy.workload_table.get_dict_data(group_by=True),
                    'workload_table_ready': str(policy.workload_table.check_df_ready()),
                    'target_workload': policy.workload_table.get_target_moderate_workload_per_rank(),
                    'target_delta': policy.workload_table.get_target_delta(),
                    'max_delta_ratio': policy.workload_table.get_max_delta_ratio(),
                    'curr_workload_stddev': policy.workload_table.get_old_workload_stddev(),
                    'new_workload_stddev': policy.workload_table.get_new_workload_stddev(),
                }
        if output_format == 'json':
            return json.dumps(output_dict, indent=4)
        elif output_format == 'dict':
            return output_dict
        else:
            return policy.get_pretty_table()

    def show(self, policy_name, output_format='json'):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return False, f"{policy_name} is not available"

            return True, self._policy_show(policy, output_format=output_format)

    def update_workload_table(self, policy_name, subtree_perf_metric):
        log.info('update_workload_table')
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return

            self._scan_all_subdirs(policy)
            policy.workload_table.update_workload_table(policy.subtree_set_scan, subtree_perf_metric)
            # save policy
            self._save(self.fs_name, policy_name, policy)
            # save history
            self.history.create_or_update(self.fs_name, policy, 'analyzer')

    def update_subtree_state(self, policy_name, subtree_path, state):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return
            policy.workload_table.update_subtree_state(subtree_path, state)
            self._save(self.fs_name, policy_name, policy)

    def activate(self, policy_name):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return False, f"{policy_name} is not available"
            if policy.activate:
                return False, f"{policy_name} has already been activated"
            # activate policy
            policy.activate = True
            self._save(self.fs_name, policy_name, policy)
            return True, f"policy {policy_name} has been activated"

    def deactivate(self, policy_name):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return False, f"{policy_name} is not available"
            # deactivate policy
            policy.activate = False
            self._save(self.fs_name, policy_name, policy)
            return True, f"policy {policy_name} has been deactivated"

    def set_max_mds(self, policy_name, max_mds):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return False, f"{policy_name} is not available"

            policy.max_mds = max_mds
            self._save(self.fs_name, policy_name, policy)
            return True, f"set max_mds {policy_name} to {max_mds}"

    def list(self, output_format='json'):
        with self.lock:
            res = {}
            for policy_name, policy in self.workload_policies.items():
                res[policy_name] = self._policy_show(policy, output_format='dict')

            if output_format == 'json':
                return json.dumps(res, indent=4)
            else:
                table = PrettyTable()
                table.field_names = ['policy_name', 'activate', 'owner', 'max_mds', 'workload_table_ready', 'target_workload', 'target_delta', 'max_delta_ratio']
                for _, d in res.items():
                    table.add_row([d['policy_name'], d['activate'], d['owner'], d['max_mds'], d['workload_table_ready'], d['target_workload'], d['target_delta'], d['max_delta_ratio']])
                return table.get_string()

    def _remove(self, policy_name):
        policy = self.workload_policies.get(policy_name, None)
        if not policy:
            msg = f"policy_name {policy_name} doesn't exist."
            return False, msg

        if policy.activate:
            msg = f"{policy_name} cannot be removed because of activated."
            return False, msg

        del self.workload_policies[policy_name]
        key = f'{self.fs_name}.{policy_name}'
        self.remove_kv(key)
        return True, ""

    def remove(self, policy_name):
        with self.lock:
            return self._remove(policy_name)

    def remove_all(self):
        with self.lock:
            removed = []
            unremoved = []
            for policy_name in list(self.workload_policies.keys()):
                res, msg = self._remove(policy_name)
                if res:
                    removed.append({'policy_name': policy_name, 
                                    'msg': msg})
                else:
                    unremoved.append({'policy_name': policy_name,
                                    'msg': msg})
            return removed, unremoved 

    def add_subtree(self, policy_name, subtree_path):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return False, f"{policy_name} is not available" 

            if self._get_owner(policy_name) == 'mover':
                return False, f'dirpath {subtree_path} cannot be added because mover is activated'

            policy.subtree_set.add(subtree_path)
            self._scan_subdir(policy, subtree_path)
            self._save(self.fs_name, policy_name, policy)

            return True, f'add dirpath {subtree_path} to {policy_name}'

    def _scan_subdir(self, policy, subtree_path):
        log.debug('_scan_subdir')
        if subtree_path[-1] == '*':
            subdir_list = self.vxattr.scan_subdir(self.fs_name, subtree_path[:-1])
            new_subtree_list = subdir_list
        else:
            new_subtree_list = [subtree_path]

        policy.subtree_set_scan.update(new_subtree_list)

        log.debug(f'scan subdir {new_subtree_list}')

    def _scan_all_subdirs(self, policy):
        log.debug('_scan_all_subdir')
        policy.subtree_set_scan = set()
        for subtree_path in policy.subtree_set:
            self._scan_subdir(policy, subtree_path)

    def _remove_scan_subdirs(self, policy, subtree_path):
        log.debug('_remove_scan_subdirs')
        if subtree_path[-1] == '*':
            subtree_path = subtree_path[:-1]
        subtree_path = os.path.normpath(subtree_path)
        to_remove = {s for s in policy.subtree_set_scan if s.startswith(subtree_path)}
        for s in to_remove:
            policy.subtree_set_scan.remove(s)

    def list_subtree(self, policy_name, output_format='dict', use_subtree_set=True):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return False, f"{policy_name} is not available" 

            if use_subtree_set:
                subtree_set = policy.subtree_set
            else:
                subtree_set = policy.subtree_set_scan

            if output_format == 'dict':
                return True, list(subtree_set)
            elif output_format == 'json':
                return True,  json.dumps(list(subtree_set), indent=4)
            else:
                table = PrettyTable()
                table.field_names = ['dirpath']
                for dirpath in subtree_set:
                    table.add_row([dirpath])
                return True, table.get_string()

    def remove_subtree(self, policy_name, subtree_path):
        with self.lock:
            policy = self.workload_policies.get(policy_name, None)
            if not policy:
                return False, f"{policy_name} is not available" 

            if self._get_owner(policy_name) == 'mover':
                return False, f'dirpath {subtree_path} cannot be removed because mover is activated'

            if subtree_path not in policy.subtree_set:
                return False, f'No such dirpath {subtree_path}'

            policy.subtree_set.remove(subtree_path)
            self._remove_scan_subdirs(policy, subtree_path)
            self._save(self.fs_name, policy_name, policy)
            return True, f"dirpath {subtree_path} has been removed"

    def get_df(self, policy_name):
        with self.lock:
            policy = self.workload_policies[policy_name]
            return policy.workload_table.get_df()

    def set_partition_mode(self, policy_name, partition_mode_name):
        with self.lock:
            policy = self.workload_policies[policy_name]
            return policy.workload_table.set_partition_mode(partition_mode_name)

    def is_active(self, policy_name):
        with self.lock:
            if policy_name in self.workload_policies:
                return self.workload_policies[policy_name].activate
            else:
                return False

    def _get_owner(self, policy_name):
        if policy_name in self.workload_policies:
            return self.workload_policies[policy_name].owner
        return ''

    def get_owner(self, policy_name):
        with self.lock:
            return self._get_owner(policy_name)

    def set_owner(self, policy_name, owner):
        assert owner in ['analyzer', 'mover', 'scheduler']
        with self.lock:
            if policy_name in self.workload_policies:
                policy = self.workload_policies[policy_name]
                policy.owner = owner
                self._save(self.fs_name, policy_name, policy)

    def release_owner(self, policy_name):
        with self.lock:
            if policy_name in self.workload_policies:
                policy = self.workload_policies[policy_name]
                policy.owner = ''
                self._save(self.fs_name, policy_name, policy)

    def stop(self):
        with self.lock:
            self._save_all()

    def _save_all(self):
        for policy_name, policy in self.workload_policies.items():
            self._save(self.fs_name, policy_name, policy)

    def store_kv(self, key, value):
        log.debug(f'set_store {key}')
        if value:
            value = json.dumps(value)
        self.mgr.set_store(key, value)

    def remove_kv(self, key):
        log.debug(f'set_store remove {key}')
        self.mgr.set_store(key, None)

    def get_kv_prefix(self, prefix):
        log.debug(f'get_store_prefix {prefix}')
        return self.mgr.get_store_prefix(prefix)

    def get_kv(self, key):
        log.debug(f'get_store {key}')
        value = self.mgr.get_store(key)
        if value == None:
            return {}

        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
