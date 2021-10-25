# Accelerating Ceph Cloud Storage with D4N
##### **Team members:** Daniel Paganelli, Khai Evdaev, Mandy Yao, Samarah Uriarte
##### **Mentors:** Ali Maredia, Amin Mosayyebzadeh, Mark Kogan, Matt Benjamin, Emine Ugur Kaynar
##### **Demo 1 Video Link:** https://drive.google.com/file/d/1gHP5dZ80w4Xn2DuMSbVdR6eSDQE3XQXG/view?usp=sharing
##### **Demo 2 Video Link:** https://drive.google.com/file/d/1zKDpXfYASGvzWEACVdQuIBRUEwI-7-e-/view?usp=sharing

---
### Project Overview
With data volumes growing exponentially, a highly scalable storage that preserves and records digital content for ongoing or future company operations is a paramount solution to any successful business. Red Hat Ceph, an open-source software that facilitates distributed object, block, and file storage, emerged as a massively scalable storage solution for modern data pipelines to store and streamline important digital information.

To access stored data in Ceph, one can achieve this in three ways: **radosgw (RGW)**, librados, and RADOS block device (RBD). Our project focuses on Ceph's object storage, so we will only be working with radosgw. With RGW, data can be accessed using an HTTP server with the Ceph Object Gateway daemon, which provides interfaces compatible with Amazon S3 and OpenStack Swift.

The current version of Ceph is paired with Datacenter-Data-Delivery Network (D3N), a multi-layer cache infrastructure for data centers. Its goal is to speed up performance of big data analytics by caching commonly accessed data on computing clusters connected to a larger data lake.

This project intends to build a functioning prototype of Ceph with an improved caching system: D4N. Compared to D3N, D4N introduces a Write Back cache and a Distributed Directory that allows the program to have an accurate and efficient method of locating cached data on both a cluster’s local cache or on nearby caches based on locality. D4N functions on an old research branch of Ceph, and this project is an attempt to create a Ceph Cluster with the beneficial functionality of D4N incorporated into the modern code base. The team will upstream blocks of D4N code, primarily the directory functionality, into D3N and modify the functions, classes, and pathways to properly implement the directory.

---
### Goals
The goal of this project is to incorporate D4N, an upgrade to the D3N caching architecture, in Ceph’s RADOS gateway (RGW), integrate it into the upstream open source repositories and make it available to the greater Ceph community. More specifically, this project aims to expand D4N to a global stage and synchronize D4N agents or servers into a single entity which will make the hybrid model accessible. Through a collaboration between researchers at BU, NEU and Red Hat, D4N with modified RGW servers can be distributed around the datacenter, allowing data to be cached in solid state storage (SSDs) near computer clusters to reduce load on the data center network and improve performance. This project attempts but not limited to the following:

1. Make D4N start up in vstart.sh, which is also the orchestration system in the developer workflow that is being followed.
2. Work with Red Hat and the research team to select components of D4N and rebase them on master.
3. Developing a set of unit tests for each of those components. 
4. Develop documentation and run reviews for newly introduced APIs.
5. Performance testing for different synchronization mechanisms.
6. Develop testing methodology to raise race conditions of complex distributed systems, e.g., write back while the object is being deleted - develop new interfaces to make race conditions deterministic.
7. Integrate testing into Teuthology.

---
### Requirements
Virtual Machines will be used rather than containers because the latter require more overhead. Additionally, utilizing containers creates unnecessary complications in comparison to the more optimal environment that VMs offer for the purpose of this project. Finally, deploying this system on an OCT is possible but difficult to set up. Working on a VM is analogous to working on a regular host, resulting in this final environment setup.

This project requires three separate Virtual Machines (VMs). These machines have the following specifications:

- VM One serves as a gateway and is less powerful than its fellow computers. It will run with 4 cores, 8 gigabytes of RAM, and a 64 gigabyte disk.

- VMs Two and Three act as the Ceph cluster that the team will be modifying. Each VM will have 16 cores, 32 gigabytes of RAM, and 250 gigabyte disk, along with their own floating IP addresses. All three machines will be accessed through an OpenStack Terminal.

<p align="center">
	<img src="./images/LAPTOP.png" width="35%" />
</p>
<p align="center">
	<strong>Figure 1.</strong> Three VMs should be set up in total, with one acting as a gateway and the other two running the Ceph cluster.
</p>

SSDs are not necessary because system performance and development is not being tested. Instead, the local hard disk will be used to emulate the SSD and play with the network. More simply, a large file is mounted like a block device so the OSD emulates it like a hard drive and the process is automatic after the Ceph build repository is cloned and the Ceph cluster is created.

Furthermore, the team will use the current Ceph source code as a target to modify the D4N code. The D4N source code is vital to the project and has its own repository. The team’s repository for modifications, improvements, and general work is available here.

---
### Business Value
This project is intended to be used by the existing Ceph community, which consists of start up companies, research institutes, and large entities such as those that already have used the storage system. Examples of previous users are the University of Kentucky, The Minnesota Supercomputing Institute, and the United States Airforce.

Users will access Ceph through the client portion of the program and benefit from the caching upgrades of D4N through accessing metadata functions and file I/O.
	
The current cloud computing trend is an increase in the use of cache storage. Implementation of D4N will allow for larger caches that place less pressure on oversubscribed cloud networks. Furthermore, D4N is intended to improve the positioning of data in caches closer to the physical access point, saving on network bandwidth. For a program such as Ceph that is designed to scale nearly infinitely, it is key that the growing distance between clients and servers is addressed.

---
### High-Level Design
Both D4N and D3N implementations in Ceph make heavy use of SSD or VRAM-based caching. The key limitation of D3N that this project addresses is the inability to access caches that are not part of the local computing cluster.

D4N solves this problem by introducing a directory (a Redis key store) that uses consistent value hashing to place important data in both a network cluster’s local cache and other neighboring caches. Upon a request for a data object’s location from a client, the RGW will access the directory for the data’s key and metadata, searching first through the local cache and then any nearby caches for the data. If these are all misses, then the program will access the primary data lake.

In our project, the four students will be split into two groups of two students. One team will focus on the implementation of the directory in our D3N Ceph cluster. The other group will work in parallel on the I/O side to ensure that cluster RGW’s can properly interact with nonlocal caches, the directory, and the data lake.

As noted above, D4N is already functioning on a non-upstream variant of Ceph. Since the project’s initial creation, Ceph has seen significant refactoring of classes, abstractions, and pathways. It will be up to each team to retool either the introduced D4N code or the base Ceph code to address these issues.

---
### Acceptance Criteria
The project's base goal is to implement the directory functionality from the D4N research code into our upstream Ceph cluster without signifigantly altering the existing upstream abstractions. We consider our most absolute basic goal to be implenting a 'get' function in D3N that utilizes the directory to find data stored across our Ceph network. Upon getting a get request from the client, the RGW should be able to first search its own local cache, and then query the directory in order to find if the object in remote caches, before finally searching the backend data storage.

Accomplishing this goal will lead into the next set of objectives for the team, which is to implement read and write functionality using the D4N style directory. Implementing these two additional features with the get function is what we consider to be full completion of the project. Overall, the limited scope of our project is due to our intended goal of producing a foundation for later teams to fully integrate D4N into the upstream code. Producing solid, testable code with good practices in mind is more important than implementing as many portions of D4N as possible.

---
### Resources
1. Batra, Aman. “D4N S3 Select Caching and the Addition of Arrow Columnar Format.” YouTube, YouTube, 7 Aug. 2021, https://www.youtube.com/watch?v=X4-s978FCtM.
2. CS6620-S21. “CS6620-S21/D4N-s3select-Caching.” GitHub, https://github.com/CS6620-S21/D4N-S3Select-Caching.
3. “Current Projects.” Current Projects - MOC Documentation, https://docs.massopen.cloud/en/latest/home.html#openstack.
4. DeCandia, Giuseppe, et al. “Dynamo: Amazon’s Highly Available Key-Value Store.” All Things Distributed, Association for Computing Machinery, 14 Oct. 2007, https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf.
5. Howard, John H., et al. “Scale and Performance in a Distributed File System .” Carnegie Mellon University School of Computer Science, ACM Transactions on Computer Systems, Feb. 1988, https://www.cs.cmu.edu/~satya/docdir/howard-tocs-afs-1988.pdf. 
6. E. U. Kaynar et al., "D3N: A multi-layer cache for the rest of us," 2019 IEEE International Conference on Big Data (Big Data), 2019, pp. 327-338, doi: 10.1109/BigData47090.2019.9006396.
7. Mosayyebzadeh, Amin, et al. “D4N: Directory Based D3N.” https://docs.google.com/presentation/d/1FiEtu3biXWdSehOekR1VGr6B6AWvRtauWu0j6KL5u-I/edit#slide=id.p. 
8. “OpenStack Tutorial Index.” OpenStack Tutorial Index - MOC Documentation, https://docs.massopen.cloud/en/latest/openstack/OpenStack-Tutorial-Index.html.
9. Platz, Carol. “Ceph Storage [A Complete Explanation].” Lightbits, 3 Sept. 2021, https://www.lightbitslabs.com/blog/ceph-storage/.
10. Request a Project.” Mass Open Cloud, https://massopen.cloud/request-an-account/.
11. Weil, Sage A., et al. “Ceph: A Scalable, High-Performance Distributed File System.” Storage Systems Research Center, USENIX Association, Nov. 2006, https://www.ssrc.ucsc.edu/media/pubs/6ebbf2736ae06c66f1293b5e431082410f41f83f.pdf.
