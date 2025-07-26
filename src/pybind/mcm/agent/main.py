# Register cluster
# Mute local alert manager
# Verify connections & authN to prometheus and ceph cluster
# Initialise schedule and start data collection threads
# As the threads finish collecting data, Hash out the data and validate if there's any change
# If there's any change since last pushed data, push the data to MCM API
# Validate response, store new hash and retry push if required.

import argparse
import signal
import threading
import time
from queue import Queue
from mcm.agent.models.config import MCMAgentConfig
from mcm.agent.collectors.manager import CollectorManager
from mcm.agent.despatchers.manager import DespatcherManager
from mcm.agent.models.registry import registry as entity_registry

class MCMAgent:
    def __init__(self):
        self.shutdown_event = threading.Event()

    def parse_args(self):
        parser = argparse.ArgumentParser(description="Agent Configuration")
        parser.add_argument("--prometheus-api-base-path", type=str, default="localhost")
        parser.add_argument("--dashboard-api-base-path", type=str, default="localhost")
        parser.add_argument("--http-retries", type=int, default=10)
        parser.add_argument("--max-queue-size", type=int, default=100)
        parser.add_argument("--mcm-api-base-path", type=str, default="localhost")
        return parser.parse_args()

    def start(self):
        args = self.parse_args()
        config = MCMAgentConfig(
            args.prometheus_api_base_path,
            args.dashboard_api_base_path,
            args.http_retries,
            args.mcm_api_base_path,
        )

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        while not self.shutdown_event.is_set():
            data_queue = Queue(maxsize=args.max_queue_size)
            despatcher = DespatcherManager(data_queue, config.mcm_api_config)
            despatcher_thread = threading.Thread(target=despatcher.run)
            despatcher_thread.start()

            collectors = CollectorManager(config, data_queue, entity_registry).run()
            for collector_thread in collectors:
                collector_thread.start()

            for collector_thread in collectors:
                collector_thread.join()

            data_queue.put((None, None))
            despatcher_thread.join()

            if not self.shutdown_event.is_set():
                time.sleep(20)

    def shutdown(self, signum, frame):
        print(f"[SIGNAL] Received {signum}. Exiting...")
        self.shutdown_event.set()

if __name__ == '__main__':
    MCMAgent().start()
