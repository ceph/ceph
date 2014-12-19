Notes on the Sepia community test lab
=====================================

The Ceph community maintains a test lab that is open to active
contributors to the Ceph project.

The lab is currently located in DreamHost's Irvine, CA data center.  There are
about 15 racks of gear.

Hardware loans or donations are gladly accepted and will be put to
good use running regression and other automated testing.


E-mail and IRC
--------------

Acitivity within the lab is coordinated in two places:

* `sepia@ceph.com`_ email discussion list.

* #sepia on irc.oftc.net

.. _sepia@ceph.com: http://lists.ceph.com/listinfo.cgi/ceph-qa-ceph.com/


Hardware overview
-----------------

* **96 plana**: 1u Dell R410. 4 core/8 thread Intel E5620 2.4GHz.  8G RAM.  4x 500GB SATA.  1gig and 10gig network.

* **64 burnupi**: 2u Dell R515.  16G RAM.  6 core.  8x1T SAS disk.  1gig and 10gig network.  Crummy SAS expanders.

* **122 mira**: 2u Supermicro. 4 core/8 thread.  16G RAM.  8x1T SATA disks.  1gig network.

* 8 vercoi: 2u Dell C6100 (2 nodes/chassis); 12 core/24 thread; 72G RAM; 4x500GB RAID; VM hosts

* 4 senta: 1u Supermicro; 12 core/24 thread; 72G RAM;  4x500GB MDADM; VM hosts

* 44 saya: Calxeda armv7l nodes (for testing); 36x Highbank (4 core/4GB RAM), 12x Midway (4 core/8GB ram)

* 24 tala: Calxeda armv7l nodes (gitbuilder); 24x Highbank (4 core/4GB RAM)

* ~200 vps: VMs running on mira hardware (25 nodes)

* 4 rex: 1u Supermicro; 12 core/24 thread; 64G RAM; 256G SSD; 1TB HD

* 4 rhoda: 4u Supermicro (FatTwin, 4 nodes/chassis); 4 core/8 thread; 32G RAM; 10x4TB; 2x250G

* 2 apama: 4u HP SL4540 (2 nodes/chassis); 8 core; 48G RAM; 2x500G (raid/boot); 25x3TB

* 4 Intel 910 series PCI-E SSDs (2x800G, 2x400G)

* 6 Force10 S4810 switches (2x AGG, 4x EDGE) (10G network)

* 4 Cisco 2960G switches (1G network)

* 5 Cisco 3560G switches (1G network)

* 8 Cisco 2960 switches (100M OOB network/IPMI)

* 8 Mellanox 40G NICs

* 1 Mellanox 40G Switch


Access
------

We use openvpn to grant access to the lab network.  Public ssh keys are used to
grant access to individual machines.


Locking machines
----------------

* All tests pull their builds from gitbuilder.ceph.com.

* Anybody can lock machines with ``teuthology-lock --lock-many NUM
  --machine-type TYPE``.

* Machines are locked as ``whoami''@``hostname -s``.  --owner to
  choose otherwise.

* Automated tests current run on the ``plana``; please avoid locking
  these for personal use.

* To unlock, please use ``teuthology-nuke -t list.yaml -r -u``, which
  will reboot and clean up any leftover test state before unlocking
  (or fail to unlock).  It looks for a ``targets::`` section in the
  yaml, so the regular job yaml will work.  You can get a list of all
  locked machines with ``teuthology-lock --list-targets``.

* ``teuthology-lock -a --brief`` or ``teuthology-lock --summary`` to
  see what is locked and by whom.

* Be conscientious about scheduling entire qa runs.  Coordinate
  utilization on IRC.  Make sure you are running the latest version
  ceph-qa-suite.git and teuthology.git.

* Results for scheduled runs appear in /a/$jobname on the teuthology
  machine.
