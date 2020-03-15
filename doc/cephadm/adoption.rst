Converting an existing cluster to cephadm
=========================================

Cephadm allows you to (pretty) easily convert an existing Ceph cluster that
has been deployed with ceph-deploy, ceph-ansible, DeepSea, or similar tools.

Limitations
-----------

* Cephadm only works with BlueStore OSDs.  If there are FileStore OSDs
  in your cluster you cannot manage them.

Adoption Process
----------------

#. Get the ``cephadm`` command line too on each host.  You can do this with curl or by installing the package.  The simplest approach is::

     [each host] # curl --silent --remote-name --location https://github.com/ceph/ceph/raw/master/src/cephadm/cephadm
     [each host] # chmod +x cephadm

#. Prepare each host for use by ``cephadm``::

     [each host] # ./cephadm prepare-host

#. List all Ceph daemons on the current host::

     # ./cephadm ls

   You should see that all existing daemons have a type of ``legacy``
   in the resulting output.

#. Determine which Ceph version you will use.  You can use any Octopus
   release or later.  For example, ``docker.io/ceph/ceph:v15.2.0``.  The default
   will be the latest stable release, but if you are upgrading from an earlier
   release at the same time be sure to refer to the upgrade notes for any
   special steps to take while upgrading.

   The image is passed to cephadm with::

     # ./cephadm --image $IMAGE <rest of command goes here>

#. Adopt each monitor::

     # ./cephadm adopt --style legacy --name mon.<hostname>

#. Adopt each manager::

     # ./cephadm adopt --style legacy --name mgr.<hostname>

#. Enable cephadm::

     # ceph mgr module enable cephadm
     # ceph orch set backend cephadm

#. Generate an SSH key::

     # ceph cephadm generate-key
     # ceph cephadm get-pub-key

#. Install the SSH key on each host to be managed::

     # echo <ssh key here> | sudo tee /root/.ssh/authorized_keys

   Note that ``/root/.ssh/authorized_keys`` should have mode ``0600`` and
   ``/root/.ssh`` should have mode ``0700``.

#. Tell cephadm which hosts to manage::

     # ceph orch host add <hostname> [ip-address]

   This will perform a ``cephadm check-host`` on each host before
   adding it to ensure it is working.  The IP address argument is only
   required if DNS doesn't allow you to connect to each host by it's
   short name.

#. Verify that the monitor and manager daemons are visible::

     # ceph orch ps

#. Adopt all remainingg daemons::

      # ./cephadm adopt --style legacy --name <osd.0>
      # ./cephadm adopt --style legacy --name <osd.1>
      # ./cephadm adopt --style legacy --name <mds.foo>

   Repeat for each host and daemon.

#. Check the ``ceph health detail`` output for cephadm warnings about
   stray cluster daemons or hosts that are not yet managed.
