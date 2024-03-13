=======================
Developing with cephadm
=======================

There are several ways to develop with cephadm.  Which you use depends
on what you're trying to accomplish.

vstart --cephadm
================

- Start a cluster with vstart, with cephadm configured
- Manage any additional daemons with cephadm
- Requires compiled ceph binaries

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
  this key is authorized to ssh with no passphrase to root@`hostname`.
- cephadm does not try to manage any daemons started by vstart.sh (any
  nonzero number in the environment variables).  No service spec is defined
  for mon or mgr.
- You'll see health warnings from cephadm about stray daemons--that's because
  the vstart-launched daemons aren't controlled by cephadm.
- The default image is ``quay.io/ceph-ci/ceph:main``, but you can change
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
  just like a user's cluster would.  In contrast, vstart and teuthology
  clusters tend to be special in subtle (and not-so-subtle) ways (e.g.
  having the ``lockdep`` turned on).

To start a test cluster::

  sudo ../src/cstart.sh

The last line of the output will be a line you can cut+paste to update
the container image.  For instance::

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

cephadm bootstrap --shared_ceph_folder
======================================

Cephadm can also be used directly without compiled ceph binaries.

Run cephadm like so::

  sudo ./cephadm bootstrap --mon-ip 127.0.0.1 \
    --ssh-private-key /home/<user>/.ssh/id_rsa \
    --skip-mon-network \
    --skip-monitoring-stack --single-host-defaults \
    --skip-dashboard \
    --shared_ceph_folder /home/<user>/path/to/ceph/

- ``~/.ssh/id_rsa`` is used as the cluster key.  It is assumed that
  this key is authorized to ssh with no passphrase to root@`hostname`.

Source code changes made in the ``pybind/mgr/`` directory then
require a daemon restart to take effect.

Kcli: a virtualization management tool to make easy orchestrators development
=============================================================================
`Kcli <https://github.com/karmab/kcli>`_ is meant to interact with existing
virtualization providers (libvirt, KubeVirt, oVirt, OpenStack, VMware vSphere,
GCP and AWS) and to easily deploy and customize VMs from cloud images.

It allows you to setup an environment with several vms with your preferred
configuration (memory, cpus, disks) and OS flavor.

main advantages:
----------------
  - Fast. Typically you can have a completely new Ceph cluster ready to debug
    and develop orchestrator features in less than 5 minutes.
  - "Close to production" lab. The resulting lab is close to "real" clusters
    in QE labs or even production. It makes it easy to test "real things" in
    an almost "real" environment.
  - Safe and isolated. Does not depend of the things you have installed in
    your machine. And the vms are isolated from your environment.
  - Easy to work "dev" environment. For "not compilated" software pieces,
    for example any mgr module. It is an environment that allow you to test your
    changes interactively.

Installation:
-------------
Complete documentation in `kcli installation <https://kcli.readthedocs.io/en/latest/#installation>`_
but we suggest to use the container image approach.

So things to do:
  - 1. Review `requeriments <https://kcli.readthedocs.io/en/latest/#libvirt-hypervisor-requisites>`_
    and install/configure whatever is needed to meet them.
  - 2. get the kcli image and create one alias for executing the kcli command
    ::

        # podman pull quay.io/karmab/kcli
        # alias kcli='podman run --net host -it --rm --security-opt label=disable -v $HOME/.ssh:/root/.ssh -v $HOME/.kcli:/root/.kcli -v /var/lib/libvirt/images:/var/lib/libvirt/images -v /var/run/libvirt:/var/run/libvirt -v $PWD:/workdir -v /var/tmp:/ignitiondir quay.io/karmab/kcli'

.. note:: This assumes that /var/lib/libvirt/images is your default libvirt pool.... Adjust if using a different path

.. note:: Once you have used your kcli tool to create and use different labs, we
   suggest you stick to a given container tag and update your kcli alias.
   Why? kcli uses a rolling release model and sticking to a specific
   container tag will improve overall stability.
   what we want is overall stability.

Test your kcli installation:
----------------------------
See the kcli `basic usage workflow <https://kcli.readthedocs.io/en/latest/#basic-workflow>`_

Create a Ceph lab cluster
-------------------------
In order to make this task simple, we are going to use a "plan".

A "plan" is a file where you can define a set of vms with different settings.
You can define hardware parameters (cpu, memory, disks ..), operating system and
it also allows you to automate the installation and configuration of any
software you want to have.

There is a `repository <https://github.com/karmab/kcli-plans>`_ with a collection of
plans that can be used for different purposes. And we have predefined plans to
install Ceph clusters using Ceph ansible or cephadm, so let's create our first Ceph
cluster using cephadm::

# kcli create plan -u https://github.com/karmab/kcli-plans/blob/master/ceph/ceph_cluster.yml

This will create a set of three vms using the plan file pointed by the url.
After a few minutes, let's check the cluster:

* Take a look to the vms created::

  # kcli list vms

* Enter in the bootstrap node::

  # kcli ssh ceph-node-00

* Take a look to the ceph cluster installed::

  [centos@ceph-node-00 ~]$ sudo -i
  [root@ceph-node-00 ~]# cephadm version
  [root@ceph-node-00 ~]# cephadm shell
  [ceph: root@ceph-node-00 /]# ceph orch host ls

Create a Ceph cluster to make easy developing in mgr modules (Orchestrators and Dashboard)
------------------------------------------------------------------------------------------
The cephadm kcli plan (and cephadm) are prepared to do that.

The idea behind this method is to replace several python mgr folders in each of
the ceph daemons with the source code folders in your host machine.
This "trick" will allow you to make changes in any orchestrator or dashboard
module and test them intermediately. (only needed to disable/enable the mgr module)

So in order to create a ceph cluster for development purposes you must use the
same cephadm plan but with a new parameter pointing to your Ceph source code folder::

  # kcli create plan -u https://github.com/karmab/kcli-plans/blob/master/ceph/ceph_cluster.yml -P ceph_dev_folder=/home/mycodefolder/ceph

Ceph Dashboard development
--------------------------
Ceph dashboard module is not going to be loaded if previously you have not
generated the frontend bundle.

For now, in order load properly the Ceph Dashboardmodule and to apply frontend
changes you have to run "ng build" on your laptop::

  # Start local frontend build with watcher (in background):
  sudo dnf install -y nodejs
  cd <path-to-your-ceph-repo>
  cd src/pybind/mgr/dashboard/frontend
  sudo chown -R <your-user>:root dist node_modules
  NG_CLI_ANALYTICS=false npm ci
  npm run build -- --deleteOutputPath=false --watch &

After saving your changes, the frontend bundle will be built again.
When completed, you'll see::

  "Localized bundle generation complete."

Then you can reload your Dashboard browser tab.

Cephadm DiD (Docker in Docker) box development environment
==========================================================

As kcli has a long startup time, we created an alternative which is faster using
Docker inside Docker. This approach has its downsides too as we have to
simulate the creation of osds and addition of devices with loopback devices.

Cephadm's DiD environment is a command which requires little to setup. The setup
requires you to get the required docker images for what we call boxes and ceph.
A box is the first layer of docker containers which can be either a seed or a
host. A seed is the main box which holds cephadm and where you bootstrap the
cluster. On the other hand, you have hosts with an ssh server setup so you can
add those hosts to the cluster. The second layer, managed by cephadm, inside the
seed box, requires the ceph image.

.. warning:: This development environment is still experimental and can have unexpected
             behaviour. Please take a look at the road map and the known issues section
             to see what the development progress.

Requirements
------------

* `docker-compose <https://docs.docker.com/compose/install/>`_
* lvm

Setup
-----

In order to setup Cephadm's box run::

  cd src/cephadm/box
  sudo ln -sf "$PWD"/box.py /usr/bin/box
  sudo box -v cluster setup

.. note:: It is recommended to run box with verbose (-v).

After getting all needed images we can create a simple cluster without osds and hosts with::

  sudo box -v cluster start

If you want to deploy the cluster with more osds and hosts::
  # 3 osds and 3 hosts by default
  sudo box -v cluster start --extended
  # explicitly change number of hosts and osds
  sudo box -v cluster start --extended --osds 5 --hosts 5

Without the extended option, explicitly adding either more hosts or osds won't change the state
of the cluster.

.. note:: Cluster start will try to setup even if cluster setup was not called.
.. note:: Osds are created with loopback devices and hence, sudo is needed to
   create loopback devices capable of holding osds.
.. note::  Each osd will require 5GiB of space.

After bootstraping the cluster you can go inside the seed box in which you'll be
able to run cehpadm commands::

  box -v cluster sh
  [root@8d52a7860245] cephadm --help
  ...


If you want to navigate to the dashboard you can find the ip address after running::
  docker ps
  docker inspect <container-id> | grep IPAddress

The address will be https://$IPADDRESS:8443

You can also find the hostname and ip of each box container with::

  sudo box cluster list

and you'll see something like::

  IP               Name            Hostname
  172.30.0.2       box_hosts_1     6283b7b51d91
  172.30.0.3       box_hosts_3     3dcf7f1b25a4
  172.30.0.4       box_seed_1      8d52a7860245
  172.30.0.5       box_hosts_2     c3c7b3273bf1

To remove the cluster and clean up run::

  box cluster down
 
If you just want to clean up the last cluster created run::

  box cluster cleanup

To check all available commands run::

  box --help


Known issues
------------

* If you get permission issues with cephadm because it cannot infer the keyring
  and configuration, please run cephadm like this example::

    cephadm shell --config /etc/ceph/ceph.conf --keyring /etc/ceph/ceph.kerying

* Docker containers run with the --privileged flag enabled which has been seen
  to make some computers log out.

* Sometimes when starting a cluster the osds won't get deployed because cephadm
  takes a while to update the state. If this happens wait and call::

    box -v osd deploy --vg vg1

Road map
------------

* Run containers without --privileged 
* Enable ceph-volume to mark loopback devices as a valid block device in
  the inventory.
* Make DiD ready to run dashboard CI tests (including cluster expansion).

Note regarding network calls from CLI handlers
==============================================

Executing any cephadm CLI commands like ``ceph orch ls`` will block the
mon command handler thread within the MGR, thus preventing any concurrent
CLI calls. Note that pressing ``^C`` will not resolve this situation,
as *only* the client will be aborted, but not execution of the command
within the orchestrator manager module itself. This means, cephadm will
be completely unresponsive until the execution of the CLI handler is
fully completed. Note that even ``ceph orch ps`` will not respond while
another handler is executing.

This means we should do very few synchronous calls to remote hosts.
As a guideline, cephadm should do at most ``O(1)`` network calls in CLI handlers.
Everything else should be done asynchronously in other threads, like ``serve()``.

Note regarding different variables used in the code
===================================================

* a ``service_type`` is something like mon, mgr, alertmanager etc defined
  in ``ServiceSpec``
* a ``service_id`` is the name of the service. Some services don't have
  names.
* a ``service_name`` is ``<service_type>.<service_id>``
* a ``daemon_type`` is the same as the service_type, except for ingress,
  which has the haproxy and keepalived daemon types.
* a ``daemon_id`` is typically ``<service_id>.<hostname>.<random-string>``.
  (Not the case for e.g. OSDs. OSDs are always called OSD.N)
* a ``daemon_name`` is ``<daemon_type>.<daemon_id>``

.. _compiling-cephadm:

Compiling cephadm
=================
Recent versions of cephadm are based on `Python Zip Application`_ support, and
are "compiled" from Python source code files in the ceph tree. To create your
own copy of the cephadm "binary" use the script located at
``src/cephadm/build.py`` in the Ceph tree.  The command should take the form
``./src/cephadm/build.py [output]``.

.. _Python Zip Application: https://peps.python.org/pep-0441/

You can pass a limited set of version metadata values to be stored in the
compiled cepadm. These options can be passed to the build script with
the ``--set-version-var`` or ``-S`` option. The values should take the form
``KEY=VALUE`` and valid keys include:
* ``CEPH_GIT_VER``
* ``CEPH_GIT_NICE_VER``
* ``CEPH_RELEASE``
* ``CEPH_RELEASE_NAME``
* ``CEPH_RELEASE_TYPE``

Example: ``./src/cephadm/build.py -SCEPH_GIT_VER=$(git rev-parse HEAD) -SCEPH_GIT_NICE_VER=$(git describe) /tmp/cephadm``

Typically these values will be passed to build.py by other, higher level, build
tools - such as cmake.

The compiled version of the binary may include a curated set of dependencies
within the zipapp. The tool used to fetch the bundled dependencies can be
Python's ``pip``, locally installed RPMs, or bundled dependencies can be
disabled. To select the mode for bundled dependencies use the
``--bundled-dependencies`` or ``-B`` option with a value of ``pip``, ``rpm``,
or ``none``.

The compiled cephadm zipapp file retains metadata about how it was built. This
can be displayed by running ``cephadm version --verbose``.  The command will
emit a JSON formatted object showing version metadata (if available), a list of
the bundled dependencies generated by the build script (if bundled dependencies
were enabled), and a summary of the top-level contents of the zipapp. Example::

  $ ./cephadm version --verbose
  {
    "name": "cephadm",
    "ceph_git_nice_ver": "18.0.0-6867-g6a1df2d0b01",
    "ceph_git_ver": "6a1df2d0b01da581bfef3357940e1e88d5ce70ce",
    "ceph_release_name": "reef",
    "ceph_release_type": "dev",
    "bundled_packages": [
      {
        "name": "Jinja2",
        "version": "3.1.2",
        "package_source": "pip",
        "requirements_entry": "Jinja2 == 3.1.2"
      },
      {
        "name": "MarkupSafe",
        "version": "2.1.3",
        "package_source": "pip",
        "requirements_entry": "MarkupSafe == 2.1.3"
      }
    ],
    "zip_root_entries": [
      "Jinja2-3.1.2-py3.9.egg-info",
      "MarkupSafe-2.1.3-py3.9.egg-info",
      "__main__.py",
      "__main__.pyc",
      "_cephadmmeta",
      "cephadmlib",
      "jinja2",
      "markupsafe"
    ]
  }
