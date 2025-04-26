from .framework import registered_collectors, load_all_collectors
from .interface import Collector
from ..models.base import MCMAgentBase
import threading
from queue import Queue
from ..models.config import MCMAgentConfig
from ..models.registry import ObjectRegistry

class CollectorManager():
    def __init__(self, config: MCMAgentConfig, data_queue: Queue, entity_registry: ObjectRegistry):
        self.collector_to_data_type = {}
        self.data_type_to_collector = {}
        self.config = config
        self.data_queue = data_queue
        self._entity_registry = entity_registry
        load_all_collectors()
        print("collectors are: ", registered_collectors, "\n")
        for cls in registered_collectors:
            collector_results_type = getattr(cls, '_collector_data_type', None)
            self.insert_into_collector_and_data_maps(cls, collector_results_type)

    def run_collector(self, cls: Collector, data_type: MCMAgentBase):
        name = cls.__name__
        instance = cls(config=self.config, entity=self._entity_registry.get(data_type.__name__))
        result = instance.collect()
        print("result fetched from collector in the manager: ", result.__dict__, "\n")
        self.delete_from_collector_and_data_maps(cls, data_type)
        if len(self.data_type_to_collector[data_type]) == 0:
            print("sending data to queue : ", result.__dict__, "\n")
            self.data_queue.put((data_type, result))

    def run(self) -> list[threading.Thread]:
        collector_threads = []
        for curr_collector in self.collector_to_data_type:
            collector_results_type = getattr(curr_collector, '_collector_data_type', None)
            t = threading.Thread(target=self.run_collector, args=(curr_collector, collector_results_type))
            collector_threads.append(t)
        return collector_threads
 
    def insert_into_collector_and_data_maps(self, collector: Collector, collector_results_type: MCMAgentBase):
        if collector not in self.collector_to_data_type:
            self.collector_to_data_type[collector] = {}
        self.collector_to_data_type[collector][collector_results_type] = None

        if collector_results_type not in self.data_type_to_collector:
            self.data_type_to_collector[collector_results_type] = {}
        self.data_type_to_collector[collector_results_type][collector] = None

    def delete_from_collector_and_data_maps(self, collector: Collector, collector_results_type: MCMAgentBase):
        if collector in self.collector_to_data_type:
            if collector_results_type in self.collector_to_data_type[collector]:
                del self.collector_to_data_type[collector][collector_results_type]

        if collector_results_type in self.data_type_to_collector:
            if collector in self.data_type_to_collector[collector_results_type]:
                del self.data_type_to_collector[collector_results_type][collector]
