============================================
BlueStore Bufferbloat Mitigation Using CoDel
============================================


Introduction
------------
Bufferbloat happens when a frontend buffers too much data to a backend.
This can introduce latency spikes to the backend and compromises the
request schedulability of the frontend.

BlueStore has the bufferbloat problem due to its large queue. All
write requests are submitted immediately to BlueStore to achieve high
performance. However, this can compromise request schedulability within an OSD daemon.
As a solution, the CoDel algorithm is implemented in BlueStore as
an admission control system to control the number of transactions
submitted to BlueStore. This mechanism will negatively impact the
throughput of BlueStore. However, a tradeoff parameter has been introduced
to control BlueStore throughput loss versus BlueStore latency decrease.

Configurations
--------------
CoDel can be enabled using ``bluestore_codel`` option. The other important
option that needs to be set is ``bluestore_codel_throughput_latency_tradeoff``.
This option adjusts the tradeoff between BlueStore throughput loss and
BlueStore latency decrease. This parameter defines the amount of throughput
loss in MB/s for one ms decrease in BlueStore latency. For example, a value
of 5 means that we are willing to lose maximum of 5 MB/s of throughput for
every 1 ms decrease in BlueStore latency.

Experiments
-----------
To determine the impact of BlueStore CoDel on BlueStore, we measured throughput and
transaction latency within BlueStore. We compared these measurements with those
collected from unmodified BlueStore.
These experiments shows that:

1. BlueStore CoDel can decrease BlueStore latency with a small and controllable
impact on throughput.
2. BlueStore CoDel can react to workload changes to maintain the desired tradeoff
between latency and throughput.