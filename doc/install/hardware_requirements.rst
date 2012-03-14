=====================
Hardware Requirements
=====================
Ceph OSDs run on commodity hardware and a Linux operating system over a TCP/IP network. OSD hosts
should have ample data storage in the form of one or more hard drives or a Redundant Array of
Independent Devices (RAIDs).

<Need More Info>

Discussing the hardware requirements for each daemon, 
the tradeoffs of doing one ceph-osd per machine versus one per disk, 
and hardware-related configuration options like journaling locations.

+--------------+----------------+------------------------------------+
|  Process     | Criteria       | Minimum Requirement                |
+==============+================+====================================+
| ``ceph-osd`` | Processor      |  64-bit x86; x-cores; 2MB Ln Cache |
|              +----------------+------------------------------------+
|              | RAM            |  12 GB                             |
|              +----------------+------------------------------------+
|              | Disk Space     |  30 GB                             |
|              +----------------+------------------------------------+
|              | Volume Storage |  2-4TB SATA Drives                 |
|              +----------------+------------------------------------+
|              | Network        |  2-1GB Ethernet NICs               |
+--------------+----------------+------------------------------------+
| ``ceph-mon`` | Processor      |  64-bit x86; x-cores; 2MB Ln Cache |
|              +----------------+------------------------------------+
|              | RAM            |  12 GB                             |
|              +----------------+------------------------------------+
|              | Disk Space     |  30 GB                             |
|              +----------------+------------------------------------+
|              | Volume Storage |  2-4TB SATA Drives                 |
|              +----------------+------------------------------------+
|              | Network        |  2-1GB Ethernet NICs               | 
+--------------+----------------+------------------------------------+
| ``ceph-mds`` | Processor      |  64-bit x86; x-cores; 2MB Ln Cache |
|              +----------------+------------------------------------+
|              | RAM            |  12 GB                             |
|              +----------------+------------------------------------+
|              | Disk Space     |  30 GB                             |
|              +----------------+------------------------------------+
|              | Volume Storage |  2-4TB SATA Drives                 |
|              +----------------+------------------------------------+
|              | Network        |  2-1GB Ethernet NICs               |
+--------------+----------------+------------------------------------+