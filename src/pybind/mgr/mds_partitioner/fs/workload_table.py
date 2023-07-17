from .util import get_max_mds, get_bal_rank_mask
from .perf_metric import MetricType
from enum import Enum, unique
from prettytable import PrettyTable
import datetime
import math 
import pandas as pd
import logging

log = logging.getLogger(__name__)

MAX_MDS = 256

@unique
class PartitionMode(Enum):
    # subtrees are distributed with ceph.dir.pin
    PIN               = 0
    # subtrees with moderate workload are distrbuted by ceph.dir.pin,
    # while other subtrees with heavy workload are managed by bal_rank_mask
    PIN_BAL_RANK_MASK = 1
    # subtrees with moderate workload are distrbuted by ceph.dir.pin,
    # while other subtrees with heavy workload are managed by ceph.dir.bal.mask
    PIN_DIR_BAL_MASK  = 2

@unique
class MoveType(Enum):
    PIN               = 0
    BAL_RANK_MASK     = 1
    CEPH_DIR_BAL_MASK = 2

@unique
class WorkloadType(Enum):
    HYBRID          = 0
    PERFORMANCE     = 1
    WORKINGSET      = 2

@unique
class SubdirState(Enum):
    READY          = 0
    NEED_TO_MOVE   = 1
    MOVING         = 2
    ABORTED        = 3
    SUSPEND        = 4
    DONE           = 5


PARTITION_MODE_DEFAULT = PartitionMode.PIN_BAL_RANK_MASK
WORKLOAD_TYPE_DEFAULT = WorkloadType.HYBRID

pd.set_option('display.max_columns', None)

class RankAllocator:
    def __init__(self, max_mds):
        self.mdss = list(range(max_mds))
        self.moderate_ranks = set(range(max_mds))
        # per subtree rank mask
        self.bal_rank_masks = {}
        # per subtree old rank mask
        self.old_bal_rank_masks = {}

    def get_aggr_ranks(self, old=True):
        aggr_heavy_ranks: set = set()
        mask_dict = self.old_bal_rank_masks if old else self.bal_rank_masks
        for rank_mask_hex in mask_dict.values():
            rank_mask_decimal = WorkloadTable._hex_str_to_decimal(rank_mask_hex)
            heavy_ranks = WorkloadTable.rank_mask_to_list(rank_mask_decimal)
            aggr_heavy_ranks = aggr_heavy_ranks | set(heavy_ranks)
        return aggr_heavy_ranks

class WorkloadTable:
    def __init__(self, mgr, fs_name, vxattr, subtree_ctx, workload_table={}):
        self.mgr = mgr
        self.fs_name = fs_name
        self.vxattr = vxattr
        self.subtree_ctx = subtree_ctx
        self.fs_map = self.mgr.get('fs_map')
        self.refresh_module_option()

        # on-kv structures
        if workload_table:
            self.partition_mode = PartitionMode(workload_table['partition_mode'])
            self.workload_type = WorkloadType(workload_table['workload_type'])
            self.df = WorkloadTable.dict_to_df(workload_table['data'])
            self.df_ready = workload_table['ready']
        else:
            self.partition_mode = PARTITION_MODE_DEFAULT
            self.workload_type = WORKLOAD_TYPE_DEFAULT
            self.df = None
            self.df_ready = False

        self.update_time = None
        self.dir_bal_mask_cache = {}
        self.target_moderate_workload_per_rank = 0.0
        self.max_delta_ratio = 0.0
        self.old_workload_stddev = 0.0
        self.new_workload_stddev = 0.0

    def refresh_module_option(self):
        self.key_metric = self.mgr.get_module_option('key_metric')
        self.key_metric_delta = f'{self.key_metric}_delta'
        self.heavy_rentries_threshold = self.mgr.get_module_option('heavy_rentries_threshold')
        self.key_metric_delta_threshold = self.mgr.get_module_option('key_metric_delta_threshold')
        log.debug(f'key_metric {self.key_metric}')
        log.debug(f'heavy_rentries_threshold {self.heavy_rentries_threshold}')
        log.debug(f'key_metric_delta_threshold {self.key_metric_delta_threshold}')

    def set_partition_mode(self, partition_mode_name):
        partition_mode_name = partition_mode_name.upper()
        success = True
        if partition_mode_name == PartitionMode.PIN.name:
            self.partition_mode = PartitionMode.PIN
        elif partition_mode_name == PartitionMode.PIN_BAL_RANK_MASK.name:
            self.partition_mode = PartitionMode.PIN_BAL_RANK_MASK
        elif partition_mode_name == PartitionMode.PIN_DIR_BAL_MASK.name:
            self.partition_mode = PartitionMode.PIN_DIR_BAL_MASK
        else:
            success = False
            msg = f'set_partition_mode with wrong value {partition_mode_name}'

        if success:
            msg = f'set_partition_mode with value {partition_mode_name}'
        return success, msg

    def get_on_kv_structures(self):
        return {
                   'partition_mode': self.partition_mode.value,
                   'workload_type': self.workload_type.value,
                   'data': self.get_dict_data(),
                   'ready': self.df_ready
               }

    def get_target_moderate_workload_per_rank(self):
        return self.target_moderate_workload_per_rank

    # target_delta 모호
    def get_target_delta(self):
        return self.key_metric_delta_threshold

    def get_max_delta_ratio(self):
        return self.max_delta_ratio

    def get_old_workload_stddev(self):
        return self.old_workload_stddev

    def get_new_workload_stddev(self):
        return self.new_workload_stddev

    def update_workload_table(self, subtree_set_scan, subtree_perf_metric):
        # refresh configuration
        self.refresh_module_option()
        # refresh fs_map
        self.fs_map = self.mgr.get('fs_map')
        # update workload df
        self.update_workload_df(subtree_set_scan, subtree_perf_metric)

    def update_workload_df(self, subtree_list, subtree_perf_metric):
        log.debug('update workload table')

        self.update_time = datetime.datetime.now()
        # per subtree workload metric calculation
        subtree_xattr = self.vxattr.get_vxattr_values(self.fs_name, subtree_list)
        subtree_auth_map = self.subtree_ctx.get_subtree_auth_map(self.fs_name, subtree_list)
        self.df = self._make_workload_table_df(subtree_list, subtree_perf_metric, subtree_xattr, subtree_auth_map)
        self._calculate_workload_metric()

        self._set_heavy_entry()
        self.moderate_df = self.df[self.df['heavy']==False]
        self.heavy_df = self.df[self.df['heavy']==True]
        rank_allocator = self._make_rank_allocator()

        heavy_workload_exist = True if not self.heavy_df.empty else False
        if heavy_workload_exist and self.partition_mode != PartitionMode.PIN:
            self._prep_heavy_workload_strategy(self.heavy_df, rank_allocator)

        # prepare analyze workload
        res = self._prep_moderate_workload_strategy(self.moderate_df, rank_allocator)
        if res[0]:
            # try match
            self._try_match(self.moderate_df, res[1], res[2], res[3])
            self.moderate_df['rank_mask'] = self.moderate_df['rank'].apply(lambda x: hex(1 << x))

        if heavy_workload_exist or res[0]:
            self.df_ready = True
            self.rank_allocator = rank_allocator
            self.df = pd.concat([self.heavy_df, self.moderate_df])
            log.debug(self.df)

            # change state
            self.change_subtree_state_in_workload_df()
            # change column order
            self.change_column_order_in_workload_df()

            # for debug
            table = self.get_pretty_table()
            log.debug(f'\n{table}')
            return

        self.df_ready = False
        self.rank_allocator = None
        self.df = None
        return

    def _set_heavy_entry(self):
        df = self.df
        if self.partition_mode != PartitionMode.PIN:
            df.loc[df['rentries'] > self.heavy_rentries_threshold, 'heavy'] = True
            df.loc[df['rentries'] <= self.heavy_rentries_threshold, 'heavy'] = False
        else:
            df['heavy'] = False

        log.debug(f'\n{self.df}')

    def _rank_set_to_hex_str(self, rank_set):
        decimal = 0
        for rank in rank_set:
            decimal += self._rank_to_hex(rank)
        return hex(decimal)

    @staticmethod
    def _rank_to_hex(rank):
        return 1 << rank

    @staticmethod
    def _hex_str_to_decimal(hex_str):
        return int(hex_str, 16)

    @staticmethod
    def rank_mask_to_list(rank_mask_decimal):
        ranks = []
        for rank in range(MAX_MDS):
            if rank_mask_decimal & WorkloadTable._rank_to_hex(rank):
                ranks.append(rank)
        return ranks

    def _assign_heavy_ranks(self, subtree_data, rank_allocator):
        subtree = subtree_data['subtree']
        workload = subtree_data[self.key_metric]
        log.debug(f'_map_heavy_ranks {subtree} {workload}')

        rank_mask_hex = self.vxattr.get_vxattr(self.fs_name, subtree, 'ceph.dir.bal.mask')
        rank_mask_hex = '0x0' if rank_mask_hex == '' else rank_mask_hex
        rank_allocator.old_bal_rank_masks[subtree] = rank_mask_hex
        rank_mask_decimal = self._hex_str_to_decimal(rank_mask_hex)
        heavy_ranks = self.rank_mask_to_list(rank_mask_decimal)
        need_mdss = math.floor(workload / self.per_rank_workload) - len(heavy_ranks)

        if len(heavy_ranks) + need_mdss == 0:
            need_mdss += 1

        log.debug(f'{subtree} needs {need_mdss} mdss to cover workload {workload}')

        if need_mdss > 0:
            assert need_mdss <= len(rank_allocator.moderate_ranks)
            selected_mdss = list(rank_allocator.moderate_ranks)[:need_mdss]
            log.debug(f'a set of mdss {selected_mdss} is used for bal_rank_mask')
            heavy_ranks += selected_mdss
        elif need_mdss < 0:
            assert -need_mdss <= len(heavy_ranks)
            log.debug(f'a set of mdss {heavy_ranks[:need_mdss]} will be removed from bal_rank_mask')
            heavy_ranks = heavy_ranks[:need_mdss]

        rank_allocator.bal_rank_masks[subtree] = self._rank_set_to_hex_str(heavy_ranks)
        rank_allocator.moderate_ranks = rank_allocator.moderate_ranks - set(heavy_ranks)
        log.debug(f'heavy ranks {heavy_ranks} moderate_ranks {rank_allocator.moderate_ranks}')

    def _make_rank_allocator(self):
        log.debug('distinguish heavy workload mds')

        self.fs_map = self.mgr.get('fs_map')
        max_mds = get_max_mds(self.fs_map, self.fs_name)

        rank_allocator = RankAllocator(max_mds)
        if self.partition_mode == PartitionMode.PIN:
            return rank_allocator

        df = self.df
        total_workload_sum = df[self.key_metric].sum().astype(int)
        self.per_rank_workload = total_workload_sum / max_mds
        heavy_workload_sum = df[df['heavy'] == True][self.key_metric].sum().astype(int)
        heavy_count = df[df['heavy'] == True]['heavy'].count()
        log.debug(f'heavy_count: {heavy_count}, heavy_workload_sum: {heavy_workload_sum}, total_workload_sum: {total_workload_sum}')
        if heavy_count == 0 or heavy_workload_sum == 0 or total_workload_sum == 0:
            return rank_allocator

        for _, subtree_data in df[df['heavy'] == True].iterrows():
            self._assign_heavy_ranks(subtree_data, rank_allocator)

        # In case of PIN_BAL_MASK_MASK mode, subtrees of heavy workload are adjusted by the bal_rank_mask option of mdsmap.
        if self.partition_mode == PartitionMode.PIN_BAL_RANK_MASK:
            rank_allocator.old_bal_rank_masks = {subtree: self._rank_set_to_hex_str(rank_allocator.get_aggr_ranks(old=True)) for subtree in rank_allocator.old_bal_rank_masks.keys() }
            rank_allocator.bal_rank_masks = {subtree: self._rank_set_to_hex_str(rank_allocator.get_aggr_ranks(old=False)) for subtree in rank_allocator.bal_rank_masks.keys() }

        moderate_count = df[df['heavy'] == False]['heavy'].count()
        assert (moderate_count and len(rank_allocator.moderate_ranks)) or moderate_count == 0

        return rank_allocator

    def _calc_workload_delta_with_reweight(self, per_rank_df, rank_reweight):
        dict_data = WorkloadTable.df_to_dict(per_rank_df)

        for rank, val in rank_reweight.items():
            if rank not in dict_data:
                dict_data[rank] = {
                    'rentries': 0,
                    'avg_lat': 0,
                    'workload': 0,
                }

            dict_data[rank]['reweight'] = val

        per_rank_df = WorkloadTable.dict_to_df(dict_data)

        dict_data = WorkloadTable.df_to_dict(per_rank_df)
        for rank, data in dict_data.items():
            assert rank >= 0
            if rank_reweight[rank] == 100:
                data[self.key_metric_delta] = self.target_moderate_workload_per_rank - data[self.key_metric]
            else:
                data[self.key_metric_delta] = data[self.key_metric] * -1

            log.debug(f"{rank}, {data}")

        per_rank_df = WorkloadTable.dict_to_df(dict_data)
        per_rank_df = per_rank_df.sort_index()
        log.debug(f"\nPer Rank Workload Stat\n{per_rank_df}")

        return per_rank_df

    def _select_max_importer(self, importers):
        max_rank = -1
        max_howmuch = 0
        selected = -1

        rank = 0
        for rank, value in enumerate(importers): 
            if max_rank == -1:
                max_rank = value['rank']
                max_howmuch = value['howmuch']
                selected = rank 
            elif value['howmuch'] > max_howmuch:
                max_rank = value['rank']
                max_howmuch = value['howmuch']
                selected = rank 

        return selected


    def _try_match(self, subtree_df, export_subtrees, exporters, importers, key_metric='workload'):
        if not importers or len(importers) == 0:
            log.debug('skipping _try_match() due to no importers')
            return

        if subtree_df[subtree_df['rank'] != subtree_df['old_rank']].empty == False:
            log.debug('skipping _try_match() due to subtree empty')
            return

        subtree_df['old_rank'] = subtree_df['rank']

        for subtree_info in export_subtrees:
            sub_df = subtree_df.loc[subtree_df['subtree'] == subtree_info['subtree']]
            assert not sub_df.empty and len(sub_df) == 1
            subtree_index = sub_df.index.tolist()[0]
            importer = self._select_max_importer(importers)
            if importer == -1 or importers[importer]['howmuch'] < 0:
                break
            importers[importer]['howmuch'] -= int(sub_df[self.key_metric])
            rank = importers[importer]['rank']
            subtree_df.at[subtree_index, 'rank'] = rank

        log.debug(subtree_df)
        return

    def _gather_export_subtrees(self, rank_subtree_df, ex_amount, key_metric):
        export_subtrees = []
        can_export_amount = 0
        for idx, row in rank_subtree_df.iterrows():
            ex_amount -= row[key_metric]
            can_export_amount += row[key_metric]
            export_subtrees.append({
                                    'subtree': row['subtree'],
                                    'workload': row[key_metric],
                                   })
            if ex_amount <= 0:
                break

        log.debug(f'gathered export_subtrees: {len(export_subtrees)} export_amount: {can_export_amount}')

        return export_subtrees, can_export_amount

    def _get_default_rank_reweight(self, rank_allocator):
        log.debug('get_default_mds_reweight')
        rank_reweight = {}
        for rank in rank_allocator.mdss:
            log.debug(f'rank {rank}')
            # currently 0 and 100 are only allowed
            if rank in rank_allocator.moderate_ranks:
                rank_reweight[rank] = 100
            else:
                rank_reweight[rank] = 0

        log.debug(f"MDS Rank Reweight\n{rank_reweight}")
        return rank_reweight

    def _prep_heavy_workload_strategy(self, subtree_df, rank_allocator):
        log.debug('_prep_heavy_workload_strategy')

        # unpin and then move by ceph.dir.bal.mask (or mdsmap's bal_rank_mask)
        subtree_df['rank'] = -1

        for subvol, bal_rank_mask in rank_allocator.bal_rank_masks.items():
            subtree_df.loc[subtree_df['subtree']==subvol, 'rank_mask'] = bal_rank_mask
            subtree_df.loc[subtree_df['subtree']==subvol, 'move_type'] = MoveType.BAL_RANK_MASK.name

    def _get_delta_ratio(self, delta, target_workload):
        return abs(delta) / target_workload 

    def _prep_moderate_workload_strategy(self, subtree_df, rank_allocator):
        log.debug('_prep_moderate_workload_strategy')

        rank_reweight = self._get_default_rank_reweight(rank_allocator)
        perf_cols = ['rank', 'rentries', 'avg_lat']
        if self.key_metric not in perf_cols:
            perf_cols.append(self.key_metric)

        subtree_df['move_type'] = MoveType.PIN.name

        # group by rank
        per_rank_df = subtree_df[perf_cols].groupby(['rank']).sum()
        log.debug(f"Per Rank Stat\n {per_rank_df}")

        num_moderate_mdss = len(rank_allocator.moderate_ranks)
        total_workload = subtree_df[self.key_metric].sum().astype(int)
        log.debug(f'num_moderate_mdss: {num_moderate_mdss}, total_workload: {total_workload}')

        if num_moderate_mdss == 0 or total_workload == 0:
            return (False, None, None, None)

        self.target_moderate_workload_per_rank = float(total_workload/num_moderate_mdss)
        log.debug(f'target_workload per mds for moderate workload: {self.target_moderate_workload_per_rank}')

        per_rank_df = self._calc_workload_delta_with_reweight(per_rank_df, rank_reweight)

        export_subtrees = []
        importers = []
        exporters = []
        self.max_delta_ratio = 0.0
        for rank, rank_data in per_rank_df.iterrows():
            rank_workload_delta = rank_data[self.key_metric_delta]

            # If delta_ratio is 0.5, it would be moved by 0.5 of the target workload.
            delta_ratio = abs(rank_workload_delta) / self.target_moderate_workload_per_rank
            delta_ratio = self._get_delta_ratio(rank_workload_delta, self.target_moderate_workload_per_rank)
            self.max_delta_ratio = max(delta_ratio, self.max_delta_ratio)
            per_rank_df[rank, 'delta_ratio'] = delta_ratio 

            log.debug(f'rank {rank} target_workload {self.target_moderate_workload_per_rank} delta {rank_workload_delta} delta_ratio {delta_ratio}')
            if delta_ratio <= self.key_metric_delta_threshold:
                log.debug(f'rank {rank} has been skipped because delta_ratio {delta_ratio} is lower than threshold {self.key_metric_delta_threshold}')
                continue

            if  rank_workload_delta < 0:
                log.debug(f"rank {rank} will export {rank_workload_delta} {self.key_metric_delta}")

                rank_subtree_df = subtree_df.loc[subtree_df['rank'] == rank]
                _export_subtrees, _export_amount = self._gather_export_subtrees(rank_subtree_df, -rank_workload_delta, self.key_metric)
                export_subtrees.extend(_export_subtrees)
                exporters.append({ 'rank': rank,
                                    'howmuch': _export_amount })
            else:
                if int(rank_workload_delta) == 0:
                    continue

                log.debug(f"rank {rank} will import {rank_workload_delta} {self.key_metric_delta}")
                importers.append({ 'rank': rank,
                                    'howmuch': int(rank_workload_delta) })

        log.debug(f"exporters stats\n{exporters}")
        log.debug(f"importers stats\n{importers}")
        log.debug(f"export subtrees\n{export_subtrees}")

        need_to_move = True if len(importers) and len(exporters) else False
        return (need_to_move, export_subtrees, exporters, importers)

    def _calculate_workload_metric(self):
        log.debug('_calculate_workload')
        subtree_df = self.df
        rentries_total = subtree_df['rentries'].sum()
        lat_total = subtree_df['avg_lat'].sum()

        if self.workload_type == WorkloadType.HYBRID:
            subtree_df['workload'] = ( subtree_df['rentries'] / rentries_total * 1 + \
                                       subtree_df['avg_lat'] / lat_total * 1  \
                                     ) * 1000
        elif self.workload_type == WorkloadType.PERFORMANCE:
            subtree_df['workload'] = ( subtree_df['avg_lat'] / lat_total * 1  \
                                     ) * 1000
        else:
            subtree_df['workload'] = ( subtree_df['rentries'] / rentries_total * 1  \
                                     ) * 1000
        log.debug(subtree_df)

    def _make_workload_table_df(self, subtree_list, subtree_perf_metric, subtree_xattr, subtree_auth_map):
        log.debug('make_workload_table')
        data_dict: dict = {}
        for subtree in subtree_list:
            rank = int(subtree_auth_map[subtree].get('rank', -1))
            pin = int(subtree_xattr[subtree].get('ceph.dir.pin', -1))
            old_rank_mask = ''
            if pin == -1:
                old_rank_mask = self.vxattr.get_vxattr(self.fs_name, subtree, 'ceph.dir.bal.mask')
                if old_rank_mask == '':
                    old_rank_mask = get_bal_rank_mask(self.fs_map, self.fs_name)
            
            if old_rank_mask in ['', '0x0']:
                old_rank_mask = self._rank_set_to_hex_str([rank])

            rentries = int(subtree_xattr[subtree].get('ceph.dir.rentries', 0))
            log.debug(subtree_perf_metric)
            avg_lat = subtree_perf_metric[subtree]['avg']

            # TODO: wait untile pin == rank?
            if pin >= 0 and pin != rank:
                log.error(f'{subtree}: pin {pin} is not equal to rank {rank}!')

            data = {
                'old_rank': rank, # auth
                'rank': rank, # auth
                'old_rank_mask': old_rank_mask,
                'rank_mask': old_rank_mask,
                'pin': pin, # ceph.dir.pin
                'rentries': rentries,
                'avg_lat': avg_lat ,
                # TODO: need to implement
                'wss': int(0),
                'state': SubdirState.READY.name,
                'workload': int(0),
                'heavy': None,
                'move_type': None,
            }

            data_dict[subtree] = data

        subtree_df = pd.DataFrame.from_dict(data_dict, orient='index')
        subtree_df.index.name = "subtree"
        subtree_df = subtree_df.reset_index()
        log.debug(f'make_default_table {subtree_df}')

        return subtree_df

    def update_subtree_state(self, subtree, state):
        self.df.loc[self.df['subtree'].isin([subtree]) , 'state'] = state

    def get_pretty_table(self, group_by=False, include_index=False):
        if self.df_ready:
            if group_by == False:
                df = self.df
            else:
                df = self.get_group_by_df()
            return self.make_pretty_table(df, include_index)
        return "No table"

    def get_group_by_df(self):
        columns = ['rentries', 'avg_lat', 'workload']
        df = self.df
        heavy_df = df[df['heavy'] == True].copy()
        moderate_df = df[df['heavy'] == False].copy()

        old_rank_df = moderate_df.groupby(['old_rank']).sum().copy()
        old_rank_df = old_rank_df[columns]
        old_rank_df.rename(columns={'old_rank': 'rank'}, inplace=True)
        log.debug(f'old_rank_df\n{old_rank_df}')

        cur_rank_df = moderate_df.groupby(['rank']).sum().copy()
        cur_rank_df = cur_rank_df[columns]
        cur_rank_df.rename(columns={'rentries': 'n_rentries', 'avg_lat': 'n_avg_lat', 'workload': 'n_workload'}, inplace=True)
        log.debug(f'cur_rank_df\n{cur_rank_df}')

        new_df = pd.concat([old_rank_df, cur_rank_df], axis=1)
        new_df.index.set_names(['rank'], inplace=True)
        new_df.columns = pd.Index(['old rentries','old avg_lat','old workload','new rentries','new avg_lat','new workload'])
        log.debug(f'new_df\n{new_df}')
        new_df = new_df.fillna(0)

        heavy_adjust_map = {}
        for index, row in heavy_df.iterrows():
            subtree = row['subtree']
            heavy_adjust_map[subtree] = {
                                 'old_ranks': None,
                                 'new_ranks': None,
                                 'rentries': row['rentries'],
                                 'avg_lat': row['avg_lat'],
                                 'workload': row['workload']
                                }

            rank_mask_decimal = self._hex_str_to_decimal(row['old_rank_mask'])
            heavy_adjust_map[subtree]['old_ranks'] = self.rank_mask_to_list(rank_mask_decimal)

            rank_mask_decimal = self._hex_str_to_decimal(row['rank_mask'])
            heavy_adjust_map[subtree]['new_ranks'] = self.rank_mask_to_list(rank_mask_decimal)

        def fill_empty_rank_data(df, rank):
            keys = df.columns
            dict_data = WorkloadTable.df_to_dict(df)
            dict_data[rank] = {k: 0.0 for k in keys}

            df = WorkloadTable.dict_to_df(dict_data)
            df = df.fillna(0.0)
            log.debug(f'fill_empty_rank_data rank\n{df}')
            return df

        for subtree, row in heavy_adjust_map.items():
            for old_rank in row['old_ranks']:
                if old_rank not in new_df.index:
                    new_df = fill_empty_rank_data(new_df, old_rank)
                new_df.at[old_rank, 'old rentries'] += row['rentries'] / len(row['old_ranks'])
                new_df.at[old_rank, 'old avg_lat'] += row['avg_lat'] / len(row['old_ranks'])
                new_df.at[old_rank, 'old workload'] += row['workload'] / len(row['old_ranks'])

            for new_rank in row['new_ranks']:
                if new_rank not in new_df.index:
                    new_df = fill_empty_rank_data(new_df, new_rank)
                new_df.at[new_rank, 'new rentries'] += row['rentries'] / len(row['old_ranks'])
                new_df.at[new_rank, 'new avg_lat'] += row['avg_lat'] / len(row['old_ranks'])
                new_df.at[new_rank, 'new workload'] += row['workload'] / len(row['old_ranks'])
        new_df['delta_ratio'] = 0.0
        new_df['role'] = '-' 

        for rank, row in new_df.iterrows():
            if self.target_moderate_workload_per_rank:
                delta = self.target_moderate_workload_per_rank - row['old workload']
                delta_ratio = self._get_delta_ratio(delta, self.target_moderate_workload_per_rank)
            else:
                delta_ratio = 0.0

            new_df.at[rank, 'delta_raio'] = delta_ratio 
            new_df.at[rank, 'role'] = 'exporter' if row['old workload'] > row['new workload'] else \
                    ('importer' if row['old workload'] < row['new workload'] else '')

            log.debug(f'rank {rank} target workload {self.target_moderate_workload_per_rank} old workload {row["old workload"]} delta_ratio {delta_ratio}')

        self.old_workload_stddev = new_df['old workload'].std()
        self.new_workload_stddev = new_df['new workload'].std()
        log.debug(f'old_workload_stddev {self.old_workload_stddev}')
        log.debug(f'new_workload_stddev {self.new_workload_stddev}')
        return new_df

    def get_dict_data(self, group_by=False):
        if self.df_ready:
            if group_by == False:
                df = self.df
            else:
                df = self.get_group_by_df()
            return WorkloadTable.df_to_dict(df)

        return {}

    def get_df(self):
        if self.df_ready:
            return self.df
        return None

    @staticmethod
    def df_to_dict(df):
        df = df.T
        dict_data = df.to_dict()
        return dict_data

    @staticmethod
    def dict_to_df(dict_data):
        return pd.DataFrame.from_dict(dict_data, orient='index')

    def change_subtree_state_in_workload_df(self):
        log.debug('change_subtree_state_in_workload_df')
        if not self.df_ready:
            return 

        df = self.df
        df.loc[(df['rank'] != df['old_rank']) & (df['heavy'] == False), 'state'] = SubdirState.NEED_TO_MOVE.name
        df.loc[(df['rank'] == df['old_rank']) & (df['heavy'] == False), 'state'] = SubdirState.DONE.name

        if self.partition_mode != PartitionMode.PIN:
            for subvol, bal_rank_mask in self.rank_allocator.bal_rank_masks.items():
                old_bal_rank_mask = self.rank_allocator.old_bal_rank_masks[subvol]

                if bal_rank_mask != old_bal_rank_mask:
                    df.loc[df['subtree'] == subvol, 'state'] = SubdirState.NEED_TO_MOVE.name
                else:
                    df.loc[df['subtree'] == subvol, 'state'] = SubdirState.DONE.name

    def change_column_order_in_workload_df(self):
        log.debug('change_column_order')
        if not self.df_ready:
            return

        df = self.df
        df = df.sort_values(by=[self.key_metric], ascending=False)
        column_order = ['subtree', 'rentries', 'wss', 'avg_lat', 'workload', 'pin', 'old_rank', 'rank', 'old_rank_mask', 'rank_mask', 'state', 'heavy', 'move_type']
        df = df.reindex(columns=column_order)

    def check_df_ready(self):
        if self.df_ready and 'workload' in self.df:
            return (self.df['workload'] > 0.0).all()
        else:
            return False

    @staticmethod
    def make_pretty_table(subtree_df, include_index=False):
        table = PrettyTable()
        columns = subtree_df.columns.tolist()
        if include_index:
            columns.insert(0, subtree_df.index.name)

        table.field_names = columns
        for index, row in subtree_df.iterrows():
            cell_list = row.tolist()
            if include_index:
                cell_list.insert(0, index)
            table.add_row(cell_list)
        return table
