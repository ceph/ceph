.. _build and test cve:

Building and Testing a CVE Fix
==============================

Fixes for CVEs (Common Vulnerabilities and Exposures) must be developed under
embargo. This means the fix cannot be pushed to the public ``ceph/ceph``
repository, built using the public build infrastructure, or discussed in
public channels until the embargo is lifted. Instead, the fix is developed,
built, and tested against a private repository. Builds and tests use internal
infrastructure in the `Sepia lab <https://wiki.sepia.ceph.com/doku.php>`_.

Three git repositories are involved. This document refers to them by the
remote names below:

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Repository
     - Remote
     - Purpose
   * - ``ceph/ceph`` (public)
     - ``origin``
     - Clone from here and branch off its target branch. **Never** push the
       fix here until the embargo is lifted.
   * - `ceph-private <https://github.com/ceph/ceph-private>`_ (shared)
     - ``private``
     - Default repo to develop, build (via cve-pipeline), and test the fix
       under embargo.
   * - a per-CVE GitHub Security Advisory fork (for example,
       ``ceph/ceph-ghsa-xrjv-7fcr-h485``)
     - ``advisory``
     - Used instead of ``private``, for higher-sensitivity fixes only, to
       develop/build/test under embargo. Open a pull request here when done,
       but **do not merge** it — also push the finished branch to
       ``private`` (see "Merging the fix") so the Release Manager can build
       the security release from it.

Prerequisites
-------------

Access to ``private`` (``ceph-private``) is limited to Ceph GitHub
organization admins. Access to an ``advisory`` fork is limited to
organization admins plus the collaborators added to its security advisory.
If you cannot view the private repo for the CVE you are working on, ask Sage
McTaggart or Gabriella Roman to add you as a collaborator to the parent
advisory (not the fork itself).

Builds go through the `cve-pipeline
<https://jenkins.ceph.com/job/cve-pipeline>`_ (404s unless logged in and
authorized) Jenkins job. Access to this job is limited to Ceph GitHub
organization admins and members of the `ceph/security
<https://github.com/orgs/ceph/teams/security>`_ GitHub Team. Again, if you
do not have access to see the cve-pipeline job, ask Sage or Gabriella for
access.

Developing the Fix
------------------

#. Clone the public repository as usual:

   .. prompt:: bash $

      git clone https://github.com/ceph/ceph.git

#. Add a remote for whichever repo you'll develop in — ``private`` for most
   CVEs:

   .. prompt:: bash $

      git remote add private git@github.com:ceph/ceph-private.git

   or, for higher-sensitivity CVEs, ``advisory`` instead, substituting the
   fork for your CVE:

   .. prompt:: bash $

      git remote add advisory git@github.com:ceph/ceph-ghsa-xrjv-7fcr-h485.git

#. Create a branch and develop the fix as you would for any other bug. See
   :ref:`basic workflow dev guide` for the general development workflow.

.. note::

   Pushing a branch does not by itself trigger a build — the cve-pipeline
   Jenkins job must be triggered manually (see "Building the Fix" below).

.. warning::

   Do **not** include any information pertaining to the CVE (for example, the
   CVE ID or a description of the vulnerability) in the branch name. Branch
   names are visible in public shaman build and repo metadata as well as in
   teuthology job results in paddles and Pulpito, even when the branch itself
   is pushed only to the private repository.

Building the Fix
----------------

When the fix is ready to be built, push the branch to whichever remote you
set up above:

.. prompt:: bash $

   git push private $BRANCH_NAME

Then manually trigger the `cve-pipeline
<https://jenkins.ceph.com/job/cve-pipeline>`_
Jenkins job. Key parameters:

* ``CEPH_REPO`` — SSH URL of the repo to build (defaults to your ``private``
  remote's URL; use your ``advisory`` remote's URL instead if that's where
  the fix lives).
* ``BRANCH`` — the branch on ``CEPH_REPO`` to build.
* ``SHA1`` — leave blank; this is intentional.
* ``DISTROS`` / ``ARCHS`` — which distros/architectures to build.
* ``THROWAWAY`` — must stay ``false``. Setting it ``true`` silently
  overrides ``PULP_UPLOAD`` to ``false`` regardless of its own value, so
  nothing gets published and a subsequent teuthology run has nothing to
  fetch.
* ``PULP_UPLOAD`` — ``true`` (the default) to actually publish packages.
* ``CI_CONTAINER`` / ``CONTAINER_REPO_HOSTNAME`` /
  ``CONTAINER_REPO_ORGANIZATION`` — control the container build.

Unlike regular builds, the resulting artifacts are not published to the public
chacra or quay repositories:

* Packages are pushed to an internal Pulp instance at
  ``pulp.front.sepia.ceph.com``.
* Containers are pushed to an internal Quay instance at
  ``quay-int.front.sepia.ceph.com`` (``ceph-ci`` org), not the public
  ``quay.ceph.io``.

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

.. warning::

   This file is per-host, not synced between scheduling hosts. If you
   normally schedule from more than one teuthology host, create it on each
   one — a run from a second host fails with an "override file not found"
   error until it exists there too.

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

* ``--ceph $BRANCH_NAME`` must match the branch name that was built.
* ``-S $SHA1`` must be the full 40-character sha1 of the commit that was built.
* ``--validate-sha1 false`` is required because the teuthology hosts do not
  (and should not) have access to ``private`` or ``advisory``. The build is
  located in Pulp by its sha1 label instead, so no git access to either
  repository is needed anywhere in the test pipeline.
* ``--suite-repo``/``--suite-branch`` must point at the **public** repo
  (``origin``) and the real release branch (e.g. ``tentacle``), *not* the
  private branch name — the teuthology hosts have no network access to
  ``private``/``advisory``, and the suite YAML doesn't exist there. Mixing
  this up (leaving ``--suite-branch`` set to the private branch name) fails
  the suite-YAML fetch.

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

Once you have tested your changes and are happy with the fix, open a pull
request in the repo you developed in (``private`` or ``advisory``) — but
**do not merge it**. If you developed in ``advisory``, also push the
finished branch to ``private`` so the Release Manager can build the security
release from it:

.. prompt:: bash $

   git push private $BRANCH_NAME

Then notify the Release Manager, who will proceed with the `Security Release
Process Deviation
<https://docs.ceph.com/en/latest/dev/release-process/#security-release-process-deviation>`_.
