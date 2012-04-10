=================================
Introduction to Clustered Storage
=================================

Storage clusters are the foundation of Ceph. The move to cloud computing presages a requirement 
to support storing many petabytes of data with the ability to store exabytes of data in the near future.	

A number of factors make it challenging to build large storage systems. Three of them include:

- **Capital Expenditure**: Proprietary systems are expensive. So building scalable systems requires
using less expensive commodity hardware and a "scale out" approach to reduce build-out expenses.

- **Ongoing Operating Expenses**: Supporting thousands of storage hosts can impose significant personnel
expenses, particularly as hardware and networking infrastructure must be installed, maintained and replaced
ongoingly. 

- **Loss of Data or Access to Data**: Mission-critical enterprise applications cannot suffer significant
amounts of downtime, including loss of data *or access to data*. Yet, in systems with thousands of storage hosts, 
hardware failure is an expectation, not an exception. 

Because of the foregoing factors and other factors, building massive storage systems requires new thinking.

Ceph uses a revolutionary approach to storage that utilizes "intelligent daemons." A major advantage of *n*-tiered
architectures is their ability to separate concerns--e.g., presentation layers, logic layers, storage layers, etc.
Tiered architectures simplify system design, but they also tend to underutilize resources such as CPU, RAM, and network bandwidth.
Ceph takes advantage of these resources to create a unified storage system with extraordinary scalability.

At the core of Ceph storage is a service entitled the Reliable Autonomic Distributed Object Store (RADOS). 
RADOS revolutionizes Object Storage Devices (OSD)s by utilizing the CPU, memory and network interface of 
the storage hosts to communicate with each other, replicate data, and redistribute data dynamically. RADOS 
implements an algorithm that performs Controlled Replication Under Scalable Hashing, which we refer we refer to as CRUSH.
CRUSH enables RADOS to plan and distribute the data automatically so that system administrators do not have to 
do it manually. By utilizing each host's computing resources, RADOS increases scalability while simultaneously 
eliminating both a performance bottleneck and a single point of failure common to systems that manage clusters centrally.
 
Each OSD maintains a map of all the hosts in the cluster. However, system administrators must expect hardware failure 
in petabyte-to-exabyte scale systems with thousands of OSD hosts. Ceph's monitors increase the reliability of the OSD 
clusters by maintaining a master copy of the cluster map. For example, storage hosts may be turned on and not be "in
the cluster for the purposes of providing data storage services; not connected via a network; powered off; or, suffering from 
a malfunction. 

Ceph provides a light-weight monitor process to address faults in the OSD clusters as they arise. Like OSDs, monitors 
should be replicated in large-scale systems so that if one monitor crashes, another monitor can serve in its place. 
When the Ceph storage cluster employs multiple monitors, the monitors may get out of sync and have different versions 
of the cluster map. Ceph utilizes an algorithm to resolve disparities among versions of the cluster map.

Ceph Metadata Servers (MDSs) are only required for Ceph FS. You can use RADOS block devices or the 
RADOS Gateway without MDSs. The MDSs dynamically adapt their behavior to the current workload. 
As the size and popularity of parts of the file system hierarchy change over time, the MDSs 
dynamically redistribute the file system hierarchy among the available
MDSs to balance the load to use server resources effectively.

<image>
