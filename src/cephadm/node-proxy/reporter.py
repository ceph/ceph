from threading import Thread
import requests
import time

class Reporter:
    def __init__(self, system, observer_url):
        self.system = system
        self.observer_url = observer_url
        self.finish = False

    def stop(self):
        self.finish = True
        self.thread.join()

    def run(self):
        self.thread = Thread(target=self.loop)
        self.thread.start()

    def loop(self):
        while not self.finish:
            # Any logic to avoid sending the all the system
            # information every loop can go here. In a real
            # scenario probably we should just send the sub-parts
            # that have changed to minimize the traffic in
            # dense clusters
            d = self.system.get_system()
            requests.post(self.observer_url, json=d)
            time.sleep(10)
