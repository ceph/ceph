from ..tools import partial_dict
from . import APIDoc, APIRouter, BaseController, Endpoint, EndpointDoc, RESTController
import numpy as np
from scipy.optimize import curve_fit
import threading
from .. import mgr
from datetime import timedelta
from typing import List, Dict
import time
from ._version import APIVersion

def linear(x, a, b):
    return a + b*x

def lq(x, a, b, c):
    return (a*x) + (b*(x**2)) + c

def exp(x, a,b, c):
    return np.exp(x*c-b+a)

def log(x, a):
    return np.log(x+a) 

class Job(threading.Thread):
    def __init__(self, interval, execute, *args, **kwargs):
        threading.Thread.__init__(self)
        self.stopped = threading.Event()
        self.interval = interval
        self.execute = execute
        self.args = args
        self.kwargs = kwargs
        
    def stop(self):
                self.stopped.set()
                self.join()
    def run(self):
            while not self.stopped.wait(self.interval.total_seconds()):
                self.execute(*self.args, **self.kwargs)


class CapacityStore:
    class Point:
        def __init__(self, value: float, time: float):
            self.value = value
            self.time = time

        def __str__(self):
            return f'{self.value} in {self.time}'

    def __init__(self, name: str, saturation_point: float, warn_point: float,
                 persist_days: int = 365, interval_seconds: int = 24 * 60 * 60):
        """
        Arguments:
            name: name of the store 
            persist_days: Value current capacity can't exceed.
            saturation_point: Value current capacity can't exceed.
            warn_point: Value current capacity can't exceed.
            interval_seconds: Minimum time in seconds before adding a value
        """
        self.name = name
        self.data: List[CapacityStore.Point] = []
        self.persist_days = persist_days
        self.saturation_point = saturation_point
        self.warn_point = warn_point
        self.interval_seconds = interval_seconds

    def last_time(self):
        if not self.data:
            return 0
        return self.data[-1].time

    def can_add(self):
        now = time.time() * 1000
        return int((now - self.last_time()) > self.interval_seconds)

    def raw_add(self, value: float, time: float) -> None:
        """testing"""
        self.data.append(self.Point(value, time))

    def add(self, value: float) -> None:
        now = time.time() * 1000
        to_days = (24 * 60 * 60)
        if self.can_add():
            self.data.append(self.Point(value, now))
            remove_index = 0
            for point in self.data:
               if (int((now - point.time) / to_days) > self.persist_days):
                   remove_index += 1
               else:
                   break
            self.data = self.data[remove_index:]

    def prediction(self, func, p0=None):
        x = []
        y = []
        for point in self.data:
            x.append(point.time)
            y.append(point.value)
        if len(self.data) < 10:
            return x, y, [], []
        popt, _ = curve_fit(func, x, y, p0=p0, maxfev=100000)
        # define new input values
        x_new, y_new = self._next_days(5, func, popt)
        return x, y, x_new, y_new

    def _next_days(self, days, func, popt):
        x_new = []
        y_new = []
        for point in self.data:
            x_new.append(point.time)
            y_new.append(func(point.time, *popt))
            
        next_days = self.last_time()
        for i in range(days):
            next_days += (1000 * 60 * 60 * 24)
            x_new.append(next_days)
            y_new.append(func(next_days, *popt))
        return x_new, y_new
               
    def __str__(self):
        st = ''
        for i in self.data:
            st += str(i) + '\n'
        return st.strip()
        

@APIRouter('/capacity')
@APIDoc("Display Detailed Cluster health Status", "Health")
class Capacity(RESTController):
    def __init__(self):
        super().__init__()
        # FIXME:  capacity store or the capacity store gather job must persist the data.
        self.osd_store = CapacityStore('osd_capacity', saturation_point=100, warn_point=80, interval_seconds=0)
        # self.osd_store_job = Job(interval=timedelta(seconds=5), execute=self.df_gather)
        # self.osd_store_job.start()
        # fake data
        # data = [1, 2, 4, 8, 10, 12, 24, 26, 27, 28, 32, 40, 40, 40, 40, 40, 41]
        data = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]
        t = time.time() * 1000
        for i in data:
            t += (24 * 60 * 60 * 1000)
            self.osd_store.raw_add(i, t)

    def df_gather(self):
        df = self.df()
        total = df['stats']['total_bytes']
        raw = df['stats']['total_used_raw_bytes']
        self.osd_store.saturation_point = total / 1024 / 1024 / 1024
        self.osd_store.add(raw / 1024 / 1024 / 1024)

    def df(self):
        df = mgr.get('df')

        df = dict(stats=partial_dict(
            df['stats'],
            ['total_avail_bytes', 'total_bytes',
                'total_used_raw_bytes']
        ))
        return df

    @RESTController.MethodMap(version=APIVersion(1, 0))  # type: ignore
    def list(self):
        x, y, x_new, y_new = self.osd_store.prediction(lq)
        _, _, linearx, lineary = self.osd_store.prediction(linear)
        _, _, expx, expy = self.osd_store.prediction(exp, p0=(1, 1, 1e-10))
        return {'actual': [x, y], 'quadratic': [x_new, y_new], 'linear': [linearx, lineary],
                'exp': [expx, expy]}
