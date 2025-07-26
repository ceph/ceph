import threading
import queue
import time
from ..models.config import RESTConfig
from .interface import Despatcher
from ..models.base import MCMAgentBase
from .framework import load_all_despatchers, registered_despatchers, clear_registry

class DespatcherManager():
    def __init__(self, data_queue: queue.Queue, config: RESTConfig):
        self._config = config
        self._data_queue = data_queue
        clear_registry()
        load_all_despatchers()

    def run_dispatcher(self, cls: Despatcher, data: MCMAgentBase):
        try:
            instance = cls(config=self._config)
            print("data in despatcher is: ", data, "\n")
            instance.despatch(data)
        except Exception as e:
            print(f"[ERROR] Dispatcher {cls.__name__} failed: {e}")

    def run(self):
        dispatcher_threads = []
        while True:
            try:
                data_type, data = self._data_queue.get(timeout=120)
                if data is None:
                    break
                despatcher = registered_despatchers[data_type]
                self.run_dispatcher(despatcher, data)
                self._data_queue.task_done()
            except Exception as e:
                print("[ERROR] Dispatcher failed: ", e, "\n")
