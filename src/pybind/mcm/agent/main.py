# Register cluster
# Mute local alert manager
# Verify connections & authN to prometheus and ceph cluster
# Initialise schedule and start data collection threads
# As the threads finish collecting data, Hash out the data and validate if there's any change
# If there's any change since last pushed data, push the data to MCM API
# Validate response, store new hash and retry push if required.
import argparse
from mcm.agent.models.config import MCMAgentConfig
from queue import Queue
from mcm.agent.collectors.manager import CollectorManager
from mcm.agent.despatchers.manager import DespatcherManager
import threading
import time
from mcm.agent.models.registry import registry as entity_registry

class MCMAgent:
    def parse_args(self):
        parser = argparse.ArgumentParser(description="Agent Configuration")
        parser.add_argument("--prometheus-api-base-path", type=str, default="localhost", help="Prometheus API Base path")
        parser.add_argument("--dashboard-api-base-path", type=str, default="localhost", help="Dashboard API Base path")
        parser.add_argument("--http-retries", type=int, default=10, help="Number of retries for HTTP calls")
        parser.add_argument("--max-queue-size", type=int, default=100, help="Max queue size for interaction between the collectors and despatchers")
        parser.add_argument("--mcm-api-base-path", type=str, default="localhost", help="MCM API Base path")
        return parser.parse_args()

    def start(self):
        args = self.parse_args()
        config = MCMAgentConfig (
            args.prometheus_api_base_path,
            args.dashboard_api_base_path,
            args.http_retries,
            args.mcm_api_base_path,
        )
        while True:
            data_queue = Queue(maxsize=args.max_queue_size)  # bounded queue for backpressure
            despatcher = DespatcherManager(data_queue, config.mcm_api_config)
            t = threading.Thread(target=despatcher.run)
            t.start()
            collectors = CollectorManager(config, data_queue, entity_registry).run()
            for curr_collector in collectors:
                curr_collector.start()
            for curr_collector in collectors:
                curr_collector.join()
            t.join()
            data_queue.put((None, None))
            time.sleep(120)
    
    def shutdown(self):
        pass

if __name__ == '__main__':
    MCMAgent().start()