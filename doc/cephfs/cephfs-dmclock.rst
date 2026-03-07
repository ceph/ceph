.. _cephfs-dmclock:

=========================
CephFS DMClock Scheduler
=========================

MDS assigns QoS to throttle client metadata requests (e.g., create, mkdir, lookup, and so on) to subvolumes,
where each subvolume QoS is shared among multiple client sessions at that time. For instance, there are four
client sessions running on /volumes/_nogroup/subvolume configured with 100 QoS. The aggregated performance
of the four clients will satisfy 100 metadata IOPS to an MDS.
