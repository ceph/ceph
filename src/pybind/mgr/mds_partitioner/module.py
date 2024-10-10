"""
MDS Partitioner for CephFS
"""
from mgr_module import CLIReadCommand, CLIWriteCommand, HandleCommandResult, MgrModule, Option, NotifyType
from threading import Event
from typing import Any, List, Optional
from .fs.mds_partition import FSMDSPartition

class Module(MgrModule):
    MODULE_OPTIONS: List[Option] = [
            Option(name='heavy_rentries_threshold',
               type='uint',
               default=10000000,
               min=1,
               max=100000000,
               desc='heavy workload rentries threshold',
               long_desc=('This option is used to classify a workload as heavy'
                          'when the number of rentries in a subvolume exceeds a certain threashold.'),
               runtime=False),
            Option(name='key_metric',
               type='str',
               enum_allowed=['workload', 'rentries', 'avg_lat', 'wss'],
               default='workload',
               desc='Type of key metric to calculate workload for subtree',
               long_desc=('workload: Metric combining rentries, avg_lat, and wss'
                          'rentries: total rentries for subtree'
                          'avg_lat: average latency for subtree'
                          'wss: metadata working set size for subtree'
                          ),
               runtime=False),
            Option(name='key_metric_delta_threshold',
               type='float',
               default=0.1,
               min=0.1,
               max=100.0,
               desc='workload delta threshold to distribute',
               long_desc=('Ratio of difference between target workload and current workload value'
                          ' for distribution of rank workload'),
               runtime=False),
            Option(name='history_max',
               type='uint',
               default=10,
               min=1,
               max=20,
               desc='number of partition results',
               runtime=True),
            ]
    NOTIFY_TYPES = [NotifyType.fs_map]

    def __init__(self, *args: Any, **kwargs):
        super().__init__(*args, **kwargs)
        self.mds_partition = FSMDSPartition(self)

        self.run = True
        self.event = Event()

    @CLIWriteCommand('mds_partitioner enable')
    def mds_partitioner_enable(self, fs_name: str) -> HandleCommandResult:
        """Enable mds partitioner for a file system"""
        return self.mds_partition.partitioner_enable(fs_name)

    @CLIWriteCommand('mds_partitioner disable')
    def mds_partitioner_disable(self, fs_name: str) -> HandleCommandResult:
        """Disable mds partitioner for a file system"""
        return self.mds_partition.partitioner_disable(fs_name)

    @CLIReadCommand('mds_partitioner status')
    def mds_partitioner_status(self, fs_name: str, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """Show mds partitioner for a file system"""
        return self.mds_partition.partitioner_status(fs_name, str(output_format))

    @CLIReadCommand('mds_partitioner list')
    def mds_partitioner_list(self, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """list mds partitioner"""
        return self.mds_partition.partitioner_list(str(output_format))

    @CLIWriteCommand('mds_partitioner workload_policy create')
    def mds_partitioner_workload_policy_create(self, fs_name: str, policy_name: str) -> HandleCommandResult:
        """create workload policy for a file system"""
        return self.mds_partition.workload_policy_create(fs_name, policy_name)

    @CLIReadCommand('mds_partitioner workload_policy show')
    def mds_partitioner_workload_policy_show(self, fs_name: str, policy_name: str, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """show workload policy for a file system"""
        return self.mds_partition.workload_policy_show(fs_name, policy_name, str(output_format))

    @CLIWriteCommand('mds_partitioner workload_policy save')
    def mds_partitioner_workload_policy_save(self, fs_name: str, policy_name: str) -> HandleCommandResult:
        """save workload policy for a file system in kv store permanently"""
        return self.mds_partition.workload_policy_save(fs_name, policy_name)

    @CLIWriteCommand('mds_partitioner workload_policy activate')
    def mds_partitioner_workload_policy_activate(self, fs_name: str, policy_name: str) -> HandleCommandResult:
        """activate workload policy for a file system"""
        return self.mds_partition.workload_policy_activate(fs_name, policy_name)

    @CLIWriteCommand('mds_partitioner workload_policy deactivate')
    def mds_partitioner_workload_policy_deactivate(self, fs_name: str, policy_name: str) -> HandleCommandResult:
        """deactivate workload policy for a file system"""
        return self.mds_partition.workload_policy_deactivate(fs_name, policy_name)

    @CLIReadCommand('mds_partitioner workload_policy list')
    def mds_partitioner_workload_policy_list(self, fs_name: str, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """list workload polies for a file system"""
        return self.mds_partition.workload_policy_list(fs_name, str(output_format))

    @CLIWriteCommand('mds_partitioner workload_policy remove')
    def mds_partitioner_workload_policy_remove(self, fs_name: str, policy_name: str) -> HandleCommandResult:
        """remove workload policy for a file system"""
        return self.mds_partition.workload_policy_remove(fs_name, policy_name)

    @CLIWriteCommand('mds_partitioner workload_policy remove-all')
    def mds_partitioner_workload_policy_remove_all(self, fs_name: str) -> HandleCommandResult:
        """remove workload policies for a file system"""
        return self.mds_partition.workload_policy_remove_all(fs_name)

    @CLIWriteCommand('mds_partitioner workload_policy set max_mds')
    def mds_partitioner_workload_policy_set_max_mds(self, fs_name: str, policy_name: str, max_mds: int) -> HandleCommandResult:
        """set target max_mds for a file system"""
        return self.mds_partition.workload_policy_set_max_mds(fs_name, policy_name, max_mds)

    @CLIWriteCommand('mds_partitioner workload_policy dir_path add')
    def mds_partitioner_dirpath_add(self, fs_name: str, policy_name: str, dir_path: str) -> HandleCommandResult:
        """Add dir_path for a file system"""
        return self.mds_partition.workload_policy_dirpath_add(fs_name, policy_name, dir_path)

    @CLIReadCommand('mds_partitioner workload_policy dir_path list')
    def mds_partitioner_dirpath_list(self, fs_name: str, policy_name: str, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """list dir_path for a file system"""
        return self.mds_partition.workload_policy_dirpath_list(fs_name, policy_name, str(output_format))

    @CLIWriteCommand('mds_partitioner workload_policy dir_path rm')
    def mds_partitioner_dirpath_rm(self, fs_name: str, policy_name: str, dir_path: str) -> HandleCommandResult:
        """Remove dir_path for a file system"""
        return self.mds_partition.workload_policy_dirpath_rm(fs_name, policy_name, dir_path)

    @CLIWriteCommand('mds_partitioner workload_policy set partition_mode')
    def mds_partitioner_set_partition_mode(self, fs_name: str, policy_name: str, partition_mode_name: str) -> HandleCommandResult:
        """set partition mode for a file system"""
        return self.mds_partition.workload_policy_set_partition_mode(fs_name, policy_name, partition_mode_name)

    @CLIReadCommand('mds_partitioner workload_policy history list')
    def mds_partitioner_history_list(self, fs_name: str, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """list history"""
        return self.mds_partition.workload_policy_history_list(fs_name, str(output_format))

    @CLIReadCommand('mds_partitioner workload_policy history show')
    def mds_partitioner_history_show(self, fs_name: str, history_id: str, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """show history"""
        return self.mds_partition.workload_policy_history_show(fs_name, history_id, str(output_format))

    @CLIWriteCommand('mds_partitioner workload_policy history rm')
    def mds_partitioner_history_rm(self, fs_name: str, history_id: str) -> HandleCommandResult:
        """remove history"""
        return self.mds_partition.workload_policy_history_delete(fs_name, history_id)

    # rm-all, remove-all 통일하기
    @CLIWriteCommand('mds_partitioner workload_policy history rm-all')
    def mds_partitioner_history_rm_all(self, fs_name: str) -> HandleCommandResult:
        """remove all history"""
        return self.mds_partition.workload_policy_history_delete_all(fs_name)

    @CLIWriteCommand('mds_partitioner workload_policy history freeze')
    def mds_partitioner_history_freeze(self, fs_name: str) -> HandleCommandResult:
        """freeze history"""
        return self.mds_partition.workload_policy_history_freeze(fs_name)

    @CLIWriteCommand('mds_partitioner analyzer start')
    def mds_partitioner_analyzer_start(self, fs_name: str, policy_name: str, duration: str, interval: str) -> HandleCommandResult:
        """Analyze workload for a file system"""
        return self.mds_partition.analyzer_start(fs_name, policy_name, duration, interval)

    @CLIReadCommand('mds_partitioner analyzer status')
    def mds_partitioner_analyzer_status(self, fs_name: str, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """Show analysis status for a file system"""
        return self.mds_partition.analyzer_status(fs_name, str(output_format))

    @CLIWriteCommand('mds_partitioner analyzer stop')
    def mds_partitioner_analyzer_stop(self, fs_name: str) -> HandleCommandResult:
        """Stop analyzer for a file system"""
        return self.mds_partition.analyzer_stop(fs_name)

    @CLIWriteCommand('mds_partitioner mover start')
    def mds_partitioner_partition_start(self, fs_name: str, policy_name: str) -> HandleCommandResult:
        """Start partitioning for a file system"""
        return self.mds_partition.mover_start(fs_name, policy_name)

    @CLIReadCommand('mds_partitioner mover status')
    def mds_partitioner_partition_status(self, fs_name: str, output_format: Optional[str] = 'json') -> HandleCommandResult:
        """Show partitioning status for a file system"""
        return self.mds_partition.mover_status(fs_name, str(output_format))

    @CLIWriteCommand('mds_partitioner mover stop')
    def mds_partitioner_partition_stop(self, fs_name: str) -> HandleCommandResult:
        """Stop partitioning for a file system"""
        return self.mds_partition.mover_stop(fs_name)

    @CLIWriteCommand('mds_partitioner scheduler start')
    def mds_partitioner_scheduler_start(self, fs_name: str, policy_name: str,
            analyzer_period: Optional[int] = 3600, scheduler_period: Optional[int] = 3600) -> HandleCommandResult:
        """Start scheduler for a file system"""
        return self.mds_partition.scheduler_start(fs_name, policy_name, analyzer_period, scheduler_period)

    @CLIWriteCommand('mds_partitioner scheduler stop')
    def mds_partitioner_scheduler_stop(self, fs_name: str) -> HandleCommandResult:
        """Stop scheduler for a file system"""
        return self.mds_partition.scheduler_stop(fs_name)

    @CLIReadCommand('mds_partitioner scheduler status')
    def mds_partitioner_scheduler_status(self, fs_name: str) -> HandleCommandResult:
        """status scheduler for a file system"""
        return self.mds_partition.scheduler_status(fs_name)

    def serve(self) -> None:
        self.log.info("Starting")
        while self.run:
            sleep_interval = 5
            self.event.wait(sleep_interval)
            self.event.clear()

    def shutdown(self) -> None:
        self.log.info('Stopping')
        self.run = False
        self.event.set()

    def notify(self, notify_type: NotifyType, notify_id):
        self.mds_partition.notify(notify_type)
