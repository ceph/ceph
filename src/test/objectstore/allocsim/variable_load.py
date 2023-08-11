#!/bin/python
import rados
import time
import math
import threading
import argparse
import logging
import random

logger = logging.getLogger()

"""
Workloads are defined based on sinusiodal waves.

The base is a definition of workload density 
  D(x) = 1 + sin(x)
where x defines a point in time.
The period of function D is 2pi.

The amount of IOs to execute in workload W until time t is:
  W(t) = Int(0, t){x + cos(x)}

The algorithm keeps calculating W(t) and schedules the additional IOs.

Actual useful form includes amplitiude(A), frequency(F) and shift(S)
  D(x) = A * (1 + sin(F * x + S))
Which translates into:
  Int(D) = A * (x + cos(F * x + S)/F)
  W(t) = Int(0,t){D} = D(t) - D(0)

To get workloads that go sometimes completely mute, one can shift
values of workload density by constant value M.
  D(x) = A * (1 + sin(F*x+S) - M)

At M<0    : D(x)>0, can be used to produce constant shift to workload density
At M=0    : D(x)>=0, basic sinusoidal workload that never stops
M in (0,1): D(x) gets both + and - values; for large t, W(t)->+inf, ideal to model sporadic bursts of load
M in <1,2>: W(t) can be possitive for small t, but for large t, W(t)->-inf; it is not useful
At M>=2   : D(x)<=0, produces no workload

  Int(D) = A * (x + cos(F*x+S)/F - M*x) =
         = A * ((1-M)*x + cos(F*x+S)/F)
This variant of D(x) can achieve negative values, and as result W(t) decreases.
The algorithm stops scheduling new IOs when W(t) decreases until W(t) rises above
previous maximum value.

M is replaced with coefficient P that models support for W(t), that is, area where workload W(t) is doing work.
It has to work like examples:
P=0   (M=1)   : W(t) no workload, useless
P=0.3 (M=0.7) : W(t) sporadic 10% time workload occurs
P=1   (M=0)   : W(t) nicely sinusoidal workload
P=1.2 (M=-0.2): W(t) amplified workload that never reaches less then 0.2*A
The relation is: M=(1-P) P=(1-M)

  Int(D) = A * (P*x + cos(F*x+S)/F)
  
Workloads W(t) are sum of set of sinusoidal components
  W(t) = Int(0,t){D0 + D1 + .. +Dn} =
       = Val(0,t)( A0*(P*x+cos(F0*x+S0)/F0) + ... + An*(P*x+cos(Fn*x+Sn)/Fn) ) =
       = Val(0,t)( Sum(Ai*P)*x + Sum(Ai*cos(Fi*x+Si)/Fi) )
The coefficent P is defined only once per set.
"""

class workload_sine:
  class AFS:
    def __init__(self, A: float, F:float, S:float) -> None:
      assert(F > 0)
      self.A = A
      self.F = F
      self.S = S
      self.AdivF = A / F

  def __init__(self) -> None:
    self.P = 0.0
    self.afs = list()
    self.sum_Ai_P = 0.0

  def due(self, x: float) -> float:
    s = self.sum_Ai_P * x
    for i in self.afs:
      s += i.AdivF * math.cos(i.F * x + i.S)
    return s - due0

class workload_custom:
  pass

class workload:
#  def __init__(self, name: str, ready_time: int, freq: int, history_size: int) -> None:
  def __init__(self, name: str) -> None:
    self.name = name
    self.start_time = time.time()
    self.object_name = 'test_object_'
    self.object_cnt = 1000
    self.object_rr = 0
    self.iodepth = 150
    self.ops_max_backlog = 2000 # how many ops we are allowed to fall behind before dropping some
    self.ops_started = 0 # how many ops were scheduled for execution
    self.ops_done = 0 # how many ops were finished already
    self.ops_skipped = 0 # ops that should have been started, but we decided to skip them
                         # we do it when the ops backlog is already too large
    self.stopped = False
    self.reporting_frequency = 1.0
    self.reporting_next = self.start_time

  #returns ops/s 
  def work_density(self, x: float):
    return 1 + math.cos(x*0.5)
  def work_needed_until(self, x: float):
    return 1000 * ((0.1)*x + 2*math.sin(x*0.5))

  def next_op(self):
    pass
  def on_complete(self, object_name):
    def action(result):
      self.ops_done += 1
      #print(str(object_name) + ":" + str(result.get_return_value()) + \
      #      ":" + str(self.ops_started - self.ops_done))
      if not self.stopped:
        self.schedule_ops()
      return
    return action
  #returns time.time() when next op should be scheduled
  def get_next_op_at(self) -> float:
    return next_op_at

  def schedule_op(self):
    self.ops_started += 1
    self.object_rr = (self.object_rr + 1 ) % self.object_cnt
    object_name = 'test_object_' + str(self.object_rr)
    #data = b'Hello, Ceph!'
    data = b'c'*4096
    completion_context = self.on_complete(object_name)
    self.ioctx.aio_write(object_name, data, oncomplete=completion_context)
    
  #returns time.time() when next op should be scheduled
  def schedule_ops(self) -> float:
    now = time.time()
    cumulative_todo = self.work_needed_until(now - self.start_time)
    if now > self.reporting_next:
      print("time="+str(now - self.start_time)+\
            " due="+str(cumulative_todo)+\
            " started="+str(self.ops_started)+\
            " done="+str(self.ops_done)+\
            " skipped="+str(self.ops_skipped) )
      self.reporting_next = now + self.reporting_frequency
    ops_to_schedule = int(cumulative_todo) - (self.ops_started + self.ops_skipped)
    # check if we have capacity to schedule more ops
    # maybe we are lagging behind
    #print("sum="+str(cumulative_todo)+" to_schd="+str(ops_to_schedule))
    if ops_to_schedule > self.ops_max_backlog:
      ops_to_skip = ops_to_schedule - self.ops_max_backlog
      self.ops_skipped += ops_to_skip
    #reduce if we would go above iodepth
    if ops_to_schedule + (self.ops_started - self.ops_done) > self.iodepth:
      ops_to_schedule = self.iodepth - (self.ops_started - self.ops_done)
    if ops_to_schedule >= 1:
      for i in range(math.floor(ops_to_schedule)):
        self.schedule_op()
    #print("ops_in_flight="+str(self.ops_started - self.ops_done))
    if self.ops_started - self.ops_done == 0:
      #signal main thread we need sleep
      self.next_op_at = time.time() + 1
      wup = threading.Timer(0.01, self.schedule_ops)
      wup.start()
    return 0

  def start(self, ioctx: rados.Ioctx):
    self.ioctx = ioctx
    self.schedule_ops()

#end workload class



def run(workloads):
  #wakeup = threading.Semaphore
  cluster = rados.Rados(conffile='ceph.conf')
  cluster.connect()
  pool_name = 'test'
  ioctx = cluster.open_ioctx(pool_name)

  for w in workloads:
    w.start(ioctx)

  #  w = workload(ioctx, "workload-1")

  #w.schedule_ops()
  #w1.work_needed_until = lambda x : 1000 *(3*x + 2*math.sin(x*0.8))
  #w1.schedule_ops()
  time.sleep(50);
  ioctx.close()




def main():
  parser = argparse.ArgumentParser(
      prog='OSD operations parser')
  parser.add_argument('--debug_level', type=str, default='1')
  parser.add_argument('--runtime', required=False, type=int, default=60)
  parser.add_argument('--workload', action='append', required=False, type=str, #nargs='*', 
                      help='Comma separated list of osd names to parse. Default: "0,1,2"')
  parser.add_argument('--out', required=False, help="filename to write output to. If none is provided it will be written to stdout")
  args = parser.parse_args()
  print(args)
  #Env.setup_env(args)

  log_levels = {
      '1': logging.CRITICAL,
      '2': logging.ERROR,
      '3': logging.WARNING,
      '4': logging.INFO,
      '5': logging.DEBUG,
      '6': logging.NOTSET
  }

  logger.setLevel(log_levels[args.debug_level.upper()])
  #logger.debug(str(Env.args()))
  #logger.debug(str(osd_ls()))
  print(args.workload)
  
  for i in args.workload:
    wparams = i.split(", ")
    wdict = dict()
    for j in wparams:
      k = j.split('=')
      wdict[k[0]] = k[1]
    print(wdict)

  workloads = list()
  workloads.append(workload("w-1"))
  #return
  run(workloads)


if __name__ == '__main__':
  main()
