=============
Why use Ceph?
=============
Ceph provides an economic and technical foundation for massive scalability. Ceph is free and open source, 
which means it does not require expensive license fees or expensive updates. Ceph can run on economical 
commodity hardware, which reduces another economic barrier to scalability. Ceph is easy to install and administer, 
so it reduces expenses related to administration. Ceph supports popular and widely accepted interfaces in a 
unified storage system (e.g., Amazon S3, Swift, FUSE, block devices, POSIX-compliant shells, etc.), so you don't 
need to build out a different storage system for each storage interface you support.

Technical and personnel constraints also limit scalability. The performance profile of highly scaled systems 
can vary substantially. Ceph relieves system administrators of the complex burden of manual performance optimization
by utilizing the storage system's computing resources to balance loads intelligently and rebalance the file system dynamically.
Ceph replicates data automatically so that hardware failures do not result in data loss or cascading load spikes.
Ceph is fault tolerant, so complex fail-over scenarios are unnecessary. Ceph administrators can simply replace a failed host 
with new hardware. 

With POSIX semantics for Unix/Linux-based operating systems, popular interfaces like Amazon S3 or Swift, block devices
and advanced features like directory-level snapshots, you can deploy enterprise applications on Ceph while 
providing them with a long-term economical solution for scalable storage. While Ceph is open source, commercial 
support is available too! So Ceph provides a compelling solution for building petabyte-to-exabyte scale storage systems.

Reasons to use Ceph include:

- Extraordinary scalability

 - Terabytes to exabytes
 - Tens of thousands of client nodes

- Standards compliant

 - Virtual file system (vfs)
 - Shell (bash)
 - FUSE
 - Swift-compliant interface
 - Amazon S3-compliant interface

- Reliable and fault-tolerant

 - Strong data consistency and safety semantics
 - Intelligent load balancing and dynamic re-balancing
 - Semi-autonomous data replication
 - Node monitoring and failure detection 
 - Hot swappable hardware

- Economical (Ceph is free!)

 - Open source and free
 - Uses heterogeneous commodity hardware
 - Easy to setup and maintain
 - Commercial support is available (if needed)