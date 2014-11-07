Notes on the Sepia community test lab
=====================================

The Ceph community maintains a test lab that is open to active
contributors to the Ceph project.

The lab is currently located in DreamHost's Irvine, CA data center.  There are
about 8 racks of gear.

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

* **96 plana**: 1u Dell R410. 8 core Intel E5620 2.4GHz.  4x 500GB SATA.  1gig and 10gig network.

* **64 burnupi**: 2u Dell R515.  6 core.  8 disk.  1gig and 10gig network.  Crummy SAS expanders.

* **120 mira**: 8 core.  4-8 1TB disks.  1gig network.

* 8 vercoi: 24 core, RAID; VM hosts

* 4 senta: 24 core: VM hosts

* 36 saya: Calxeda armv7l nodes (for testing)

* 24 tala: Calxeda armv7l nodes (gitbuilder)

* ~200 vps: VMs running on mira hardware

* 4 rex

* 4 rhoda

* force 10 switches


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
