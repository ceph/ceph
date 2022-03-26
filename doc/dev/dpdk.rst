=========================
Ceph messenger DPDKStack
=========================

Compiling DPDKStack
===================

Ceph dpdkstack is not compiled by default. Therefore, you need to recompile and
enable the DPDKstack component.
Optionally install ``dpdk-devel`` or ``dpdk-dev`` on distros with precompiled DPDK packages, and compile

.. prompt:: bash $

   do_cmake.sh -DWITH_DPDK=ON


Setting the DPDK Network Adapter
================================

Most mainstream NICs support SR-IOV and can be virtualized into multiple VF NICs.
Each OSD uses some dedicated NICs through DPDK. The mon, mgr and client use the PF NICs
through the POSIX protocol stack.

Load the driver on which DPDK depends:

.. prompt:: bash #

   modprobe vfio
   modprobe vfio_pci

Configure Hugepage by editing ``/etc/sysctl.conf`` ::

  vm.nr_hugepages = xxx

Configure the number of VFs based on the number of OSDs:

.. prompt:: bash #

   echo $numvfs > /sys/class/net/$port/device/sriov_numvfs

Binding NICs to DPDK Applications:

.. prompt:: bash #

   dpdk-devbind.py -b vfio-pci 0000:xx:yy.z


Configuring OSD DPDKStack
==========================

By default, the DPDK RTE initialization process requires the root privileges
for accessing various resources in system. To grant the root access to
the ``ceph`` user:

.. prompt:: bash #

   usermod -G root ceph

The OSD selects the NICs using ``ms_dpdk_devs_allowlist``:

#. Configure a single NIC.

   .. code-block:: ini

      ms_dpdk_devs_allowlist=-a 0000:7d:010

   or

   .. code-block:: ini

      ms_dpdk_devs_allowlist=--allow=0000:7d:010

#. Configure the Bond Network Adapter

   .. code-block:: ini

      ms_dpdk_devs_allowlist=--allow=0000:7d:01.0 --allow=0000:7d:02.6 --vdev=net_bonding0,mode=2,slave=0000:7d:01.0,slave=0000:7d:02.6

DPDK-related configuration items are as follows:

.. code-block:: ini

   [osd]
   ms_type=async+dpdk
   ms_async_op_threads=1

   ms_dpdk_port_id=0
   ms_dpdk_gateway_ipv4_addr=172.19.36.1
   ms_dpdk_netmask_ipv4_addr=255.255.255.0
   ms_dpdk_hugepages=/dev/hugepages
   ms_dpdk_hw_flow_control=false
   ms_dpdk_lro=false
   ms_dpdk_enable_tso=false
   ms_dpdk_hw_queue_weight=1
   ms_dpdk_memory_channel=2
   ms_dpdk_debug_allow_loopback = true

   [osd.x]
   ms_dpdk_coremask=0xf0
   ms_dpdk_host_ipv4_addr=172.19.36.51
   public_addr=172.19.36.51
   cluster_addr=172.19.36.51
   ms_dpdk_devs_allowlist=--allow=0000:7d:01.1

Debug and Optimization
======================

Locate faults based on logs and adjust logs to a proper level:

.. code-block:: ini

   debug_dpdk=xx
   debug_ms=xx

if the log contains a large number of retransmit messages,reduce the value of ms_dpdk_tcp_wmem.

Run the perf dump command to view DPDKStack statistics:

.. prompt:: bash $

   ceph daemon osd.$i perf dump | grep dpdk


if the ``dpdk_device_receive_nombuf_errors`` keeps increasing, check whether the
throttling exceeds the limit:

.. prompt:: bash $

   ceph daemon osd.$i perf dump | grep throttle-osd_client -A 7 | grep "get_or_fail_fail"
   ceph daemon osd.$i perf dump | grep throttle-msgr_dispatch_throttler -A 7 | grep "get_or_fail_fail"

if the throttling exceeds the threshold, increase the throttling threshold or
disable the throttling.

Check whether the network adapter is faulty or abnormal.Run the following 
command to obtain the network adapter status and statistics:

.. prompt:: bash $

   ceph daemon osd.$i show_pmd_stats
   ceph daemon osd.$i show_pmd_xstats

Some DPDK versions (eg. dpdk-20.11-3.e18.aarch64) or NIC TSOs are abnormal,
try disabling tso:

.. code-block:: ini

   ms_dpdk_enable_tso=false

if VF NICs support multiple queues, more NIC queues can be allocated to a
single core to improve performance:

.. code-block:: ini

   ms_dpdk_hw_queues_per_qp=4


Status and Future Work
======================

Compared with POSIX Stack, in the multi-concurrency test, DPDKStack has the same
4K random write performance, 8K random write performance is improved by 28%, and
1 MB packets are unstable. In the single-latency test,the 4K and 8K random write
latency is reduced by 15% (the lower the latency is, the better).

At a high level, our future work plan is:

  OSD multiple network support (public network and cluster network)
  The public and cluster network adapters can be configured.When connecting or
  listening,the public or cluster network adapters can be selected based on the
  IP address.During msgr-work initialization,initialize both the public and cluster
  network adapters and create two DPDKQueuePairs.
