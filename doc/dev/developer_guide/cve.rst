.. _build and test cve:

Building and Testing a CVE Fix
==============================

Fixes for CVEs (Common Vulnerabilities and Exposures) must be developed under
embargo. This means the fix cannot be pushed to the public ``ceph/ceph``
repository, built using the public build infrastructure, or discussed in
public channels until the embargo is lifted. Instead, each CVE has its own
private fork of ``ceph/ceph`` (for example,
``ceph/ceph-ghsa-xrjv-7fcr-h485``) where the fix is developed. Builds and
tests use internal infrastructure in the `Sepia lab
<https://wiki.sepia.ceph.com/doku.php>`_.

Prerequisites
-------------

Access to a private fork is limited to Ceph GitHub organization admins and
the collaborators added to its security advisory. If you cannot view the
private fork for the CVE you are working on, ask Sage McTaggart or
Gabriella Roman to add you as a collaborator to the parent advisory
(not the fork itself).

Private CVE forks can be built using the `cve-pipeline
<https://jenkins.ceph.com/job/cve-pipeline>`_ (404s unless logged in and authorized)
Jenkins job. Access to the this job is limited to Ceph GitHub organization
admins and members of the `ceph/security <https://github.com/orgs/ceph/teams/security>`_
GitHub Team. Again, if you do not have access to see the cve-pipeline
job, ask Sage or Gabriella for access.

Developing the Fix
------------------

#. Clone the public repository as usual:

   .. prompt:: bash $

      git clone https://github.com/ceph/ceph.git

#. Add the CVE's private fork as a remote by adding the following to
   ``.git/config`` in your local working copy, substituting the fork for your
   CVE:

   .. code-block:: ini

      [remote "private"]
              url = git@github.com:ceph/ceph-ghsa-xrjv-7fcr-h485.git
              fetch = +refs/heads/*:refs/remotes/private/*

#. Create a branch and develop the fix as you would for any other bug. See
   :ref:`basic workflow dev guide` for the general development workflow.

.. note::

   A branch just needs to be pushed to the private fork in order to start a
   package and container build.

.. warning::

   Do **not** include any information pertaining to the CVE (for example, the
   CVE ID or a description of the vulnerability) in the branch name. Branch
   names are visible in public shaman build and repo metadata as well as in
   teuthology job results in paddles and Pulpito, even when the branch itself
   is pushed only to the private repository.

Building the Fix
----------------

When the fix is ready to be built, push the branch to the private fork:

.. prompt:: bash $

   git push private $BRANCH_NAME

Then manually trigger the `cve-pipeline
<https://jenkins.ceph.com/job/cve-pipeline>`_
Jenkins job.

Unlike regular builds, the resulting artifacts are not published to the public
chacra or quay repositories:

* Packages are pushed to an internal Pulp instance at
  ``pulp.front.sepia.ceph.com``.
* Containers are pushed to an internal Quay instance at
  ``quay-int.front.sepia.ceph.com``.

Testing the Fix
---------------

The built packages and containers can be tested with teuthology in the Sepia
lab, but because the artifacts live on internal infrastructure, the test run
must be pointed at the internal Pulp and Quay instances. Save the following
YAML fragment to a file on the teuthology host (for example ``~/pulp.yaml``):

.. code-block:: yaml

   package_source: pulp

   defaults:
     cephadm:
       containers:
         image: 'quay-int.front.sepia.ceph.com/ceph-ci/ceph'

``package_source: pulp`` is a per-job override that makes the scheduled jobs
locate and install packages from the internal Pulp instance instead of
Chacra/Shaman. The Pulp API credentials are supplied by the lab-wide
teuthology configuration on the teuthology hosts. Do **not** put credentials
in the fragment; job configurations are archived publicly.

Schedule the run with the fragment appended to the ``teuthology-suite``
command:

.. prompt:: bash $

   teuthology-suite \
     --ceph-repo https://github.com/ceph/ceph.git \
     --ceph $BRANCH_NAME \
     -S $SHA1 \
     --validate-sha1 false \
     --suite-repo https://github.com/ceph/ceph.git \
     --suite-branch $RELEASE_BRANCH \
     -s $SUITE --machine-type $MACHINE_TYPE \
     ~/pulp.yaml

Note the following:

* ``--ceph $BRANCH_NAME`` must match the private fork branch name.
* ``-S $SHA1`` must be the full 40-character sha1 of the commit that was built.
* ``--validate-sha1 false`` is required because the teuthology hosts do not
  (and should not) have access to ``ceph-private.git``. The build is located
  in Pulp by its sha1 label instead, so no git access to the private
  repository is needed anywhere in the test pipeline.

Remember that the warning about branch names applies to test runs as well:
scheduled jobs and their results are publicly visible in paddles and Pulpito.

Manually Testing a Container
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The built containers can also be pulled directly from the internal Quay
instance for manual testing. The containers are tagged with the branch name
that was built:

.. prompt:: bash $

   podman pull quay-int.front.sepia.ceph.com/ceph-ci/ceph:$BRANCH_NAME

Merging the fix
---------------

Once you have tested your changes and are happy with the fix, create a pull
request in the private fork. It will eventually get merged into the ceph.git
repo via the parent advisory.
