:orphan:

===============================================
 ceph-syn -- ceph synthetic workload generator
===============================================

.. program:: ceph-syn

Synopsis
========

| **ceph-syn** [ -m *monaddr*:*port* ] --syn *command* *...*


Description
===========

**ceph-syn** is a simple synthetic workload generator for the Ceph
distributed file system. It uses the userspace client library to
generate simple workloads against a currently running file system. The
file system need not be mounted via ceph-fuse(8) or the kernel client.

One or more ``--syn`` command arguments specify the particular
workload, as documented below.


Options
=======

.. option:: -d

   Detach from console and daemonize after startup.

.. option:: -c ceph.conf, --conf=ceph.conf

   Use *ceph.conf* configuration file instead of the default
   ``/etc/ceph/ceph.conf`` to determine monitor addresses during
   startup.

.. option:: -m monaddress[:port]

   Connect to specified monitor (instead of looking through
   ``ceph.conf``).

.. option:: --num_client num

   Run num different clients, each in a separate thread.

.. option:: --syn workloadspec

   Run the given workload. May be specified as many times as
   needed. Workloads will normally run sequentially.


Workloads
=========

Each workload should be preceded by ``--syn`` on the command
line. This is not a complete list.

:command:`mknap` *path* *snapname*
  Create a snapshot called *snapname* on *path*.

:command:`rmsnap` *path* *snapname*
  Delete snapshot called *snapname* on *path*.

:command:`rmfile` *path*
  Delete/unlink *path*.

:command:`writefile` *sizeinmb* *blocksize*
  Create a file, named after our client id, that is *sizeinmb* MB by
  writing *blocksize* chunks.

:command:`readfile` *sizeinmb* *blocksize*
  Read file, named after our client id, that is *sizeinmb* MB by
  writing *blocksize* chunks.

:command:`rw` *sizeinmb* *blocksize*
  Write file, then read it back, as above.

:command:`makedirs` *numsubdirs* *numfiles* *depth*
  Create a hierarchy of directories that is *depth* levels deep. Give
  each directory *numsubdirs* subdirectories and *numfiles* files.

:command:`walk`
  Recursively walk the file system (like find).


Availability
============

**ceph-syn** is part of Ceph, a massively scalable, open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.

See also
========

:doc:`ceph <ceph>`\(8),
:doc:`ceph-fuse <ceph-fuse>`\(8)
