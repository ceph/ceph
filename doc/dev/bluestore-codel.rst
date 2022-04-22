============================================
BlueStore Bufferbloat Mitigation Using CoDel
============================================


Introduction
------------
Bufferbloat happens when a frontend buffer too much data to a backend.
This can introduce latency spikes to the backend and compromise the
request schedulability of the frontend.

BlueStore has the bufferbloat problem due to its large queue. All
write requests are submitted immediately to BlueStore to achieve high
performance. However, this can compromise request schedulability in OSD.
As a solution, the CoDel algorithm is implemented in the BlueStore as
an admission control system to control the amount of transaction
submitted to BlueStore. This mechanism will negatively impact the
throughput of BlueStore. However, a tradeoff parameter has been introduced
to control BlueStore throughput loss versus BlueStore latency decrease.

Configurations
--------------
CoDel can be enabled using "*bluestore_codel*" config. The other important
config that needs to be set is "*bluestore_codel_throughput_latency_tradeoff*".
This config adjust the tradeoff between BlueStore throughput loss and
BlueStore latency decrease. This parameter defines the amount of throughput
loss in MB/s for one ms decrease in BlueStore latency. For example, a value
of 5 means that we are willing to lose maximum of 5 MB/s of throughput for
every 1 ms decrease in BlueStore latency.

Experiments
-----------
For measuring the impact of BlueStore CoDel on BlueStore, we measured the
transaction latency inside the BlueStore (BlueStore latency) and BlueStore
throughput. We compared this measurements with measurements from Vanilla BlueStore.
These experiments shows that:

1. The BlueStore CoDel can decrease the BlueStore latency by small and controllable
impact on throughput.
2. The BlueStore CoDel can react to workload changes to keep the desired tradeoff
between latency and throughput.
