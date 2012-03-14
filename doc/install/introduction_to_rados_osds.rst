==========================
Introduction to RADOS OSDs
==========================
RADOS OSD clusters are the foundation of Ceph. RADOS revolutionizes OSDs by utilizing the CPU, 
memory and network interface of the storage hosts to communicate with each other, replicate data, and 
redistribute data dynamically so that system administrators do not have to plan and coordinate
these tasks manually. By utilizing each host's computing resources, RADOS increases scalability while
simultaneously eliminating both a performance bottleneck and a single point of failure common
to systems that manage clusters centrally. Each OSD maintains a copy of the cluster map.

Ceph provides a light-weight monitor process to address faults in the OSD clusters as they
arise. System administrators must expect hardware failure in petabyte-to-exabyte scale systems 
with thousands of OSD hosts. Ceph's monitors increase the reliability of the OSD clusters by 
maintaining a master copy of the cluster map, and using the Paxos algorithm to resolve disparities 
among versions of the cluster map maintained by a plurality of monitors.

Ceph Metadata Servers (MDSs) are only required for Ceph FS. You can use RADOS block devices or the 
RADOS Gateway without MDSs. The MDS dynamically adapt their behavior to the current workload. 
As the size and popularity of parts of the file system hierarchy change over time, the 
that hierarchy the MDSs dynamically redistribute the file system hierarchy among the available
MDSs to balance the load to use server resources effectively.

<image>