=======================
Basic Ceph Client Setup
=======================
Client machines need some basic configuration in order to interact with
a cluster. This document describes how to configure a client machine
for cluster interaction.

.. note:: Most client machines only need the `ceph-common` package and
          its dependencies installed. That will supply the basic `ceph`
          and `rados` commands, as well as other commands like
          `mount.ceph` and `rbd`.

Config File Setup
=================
Client machines can generally get away with a smaller config file than
a full-fledged cluster member. To generate a minimal config file, log
into a host that is already configured as a client or running a cluster
daemon, and then run::

    ceph config generate-minimal-conf

This will generate a minimal config file that will tell the client how to
reach the Ceph Monitors. The contents of this file should typically be
installed in `/etc/ceph/ceph.conf`.

Keyring Setup
=============
Most Ceph clusters are run with authentication enabled, and the client will
need keys in order to communicate with cluster machines. To generate a
keyring file with credentials for `client.fs`, log into an extant cluster
member and run::

    ceph auth get-or-create client.fs

The resulting output should be put into a keyring file, typically
`/etc/ceph/ceph.keyring`.
