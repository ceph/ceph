=======================
Developing with cephadm
=======================

There are several ways to develop with cephadm.  Which you use depends
on what you're trying to accomplish.

vstart --cephadm
================

- Start a cluster with vstart, with cephadm configured
- Manage any additional daemons with cephadm

In this case, the mon and manager at a minimum are running in the usual
vstart way, not managed by cephadm.  But cephadm is enabled and the local
host is added, so you can deploy additional daemons or add additional hosts.

This works well for developing cephadm itself, because any mgr/cephadm
or cephadm/cephadm code changes can be applied by kicking ceph-mgr
with ``ceph mgr fail x``.  (When the mgr (re)starts, it loads the
cephadm/cephadm script into memory.)

::

   MON=1 MGR=1 OSD=0 MDS=0 ../src/vstart.sh -d -n -x --cephadm

- ``~/.ssh/id_dsa[.pub]`` is used as the cluster key.  It is assumed that
  this key is authorized to ssh to root@`hostname`.
- No service spec is defined for mon or mgr, which means that cephadm
  does not try to manage them.
- You'll see health warnings from cephadm about stray daemons--that's because
  the vstart-launched mon and mgr aren't controlled by cephadm.
- The default image is ``quay.io/ceph-ci/ceph:master``, but you can change
  this by passing ``-o container_image=...`` or ``ceph config set global container_image ...``.


cstart and cpatch
=================

The ``cstart.sh`` script will launch a cluster using cephadm and put the
conf and keyring in your build dir, so that the ``bin/ceph ...`` CLI works
(just like with vstart).  The ``ckill.sh`` script will tear it down.

- A unique but stable fsid is stored in ``fsid`` (in the build dir).
- The mon port is random, just like with vstart.
- The container image is ``quay.io/ceph-ci/ceph:$tag`` where $tag is
  the first 8 chars of the fsid.
- If the container image doesn't exist yet when you run cstart for the
  first time, it is built with cpatch.

There are a few advantages here:

- The cluster is a "normal" cephadm cluster that looks and behaves
  just like a user's cluster would.  In contract, vstart and teuthology
  clusters tend to be special in subtle (and not-so-subtle) ways.

To start a test cluster::

  sudo ../src/cstart.sh

The last line of this will be a line you can cut+paste to update the
container image.  For instance::

  sudo ../src/script/cpatch -t quay.io/ceph-ci/ceph:8f509f4e

By default, cpatch will patch everything it can think of from the local
build dir into the container image.  If you are working on a specific
part of the system, though, can you get away with smaller changes so that
cpatch runs faster.  For instance::

  sudo ../src/script/cpatch -t quay.io/ceph-ci/ceph:8f509f4e --py

will update the mgr modules (minus the dashboard).  Or::

  sudo ../src/script/cpatch -t quay.io/ceph-ci/ceph:8f509f4e --core

will do most binaries and libraries.  Pass ``-h`` to cpatch for all options.

Once the container is updated, you can refresh/restart daemons by bouncing
them with::

  sudo systemctl restart ceph-`cat fsid`.target

When you're done, you can tear down the cluster with::

  sudo ../src/ckill.sh   # or,
  sudo ../src/cephadm/cephadm rm-cluster --force --fsid `cat fsid`

Note regarding network calls from CLI handlers
==============================================

Executing any cephadm CLI commands like ``ceph orch ls`` will block
the mon command handler thread within the MGR, thus preventing any
concurrent CLI calls. Note that pressing ``^C`` will not resolve this
situation, as *only* the client will be aborted, but not exceution
itself. This means, cephadm will be completely unresonsive, until the
execution of the CLI handler is fully completed. Note that even
``ceph orch ps`` will not respond, while another handler is executed.

This means, we should only do very few calls to remote hosts synchronously. 
As a guideline, cephadm should do at most ``O(1)`` network calls in CLI handlers. 
Everything else should be done asynchronously in other threads, like ``serve()``.
