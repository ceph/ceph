.. _build and test cve:

Building and Testing a CVE Fix
==============================

Fixes for CVEs (Common Vulnerabilities and Exposures) must be developed under
embargo. This means the fix cannot be pushed to the public ``ceph/ceph``
repository, built using the public build infrastructure, or discussed in
public channels until the embargo is lifted. Instead, each CVE has its own
private fork of ``ceph/ceph`` (for example,
``ceph/ceph-ghsa-abcd-1234-beef``) where the fix is developed. Builds and
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
Jenkins job. Access to this job is limited to Ceph GitHub organization
admins and members of the `ceph/security <https://github.com/orgs/ceph/teams/security>`_
GitHub Team. Again, if you do not have access to see the cve-pipeline
job, ask Sage or Gabriella for access.

A note about terminology
------------------------

Three separate git repositories will be used when developing a CVE fix.

In this document, "private fork" and "ceph-private" will be referenced.

For clarity,

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Repository
     - Git remote
     - Purpose
   * - ``ceph/ceph`` (the public repository)
     - ``origin``
     - Clone from here and base the fix on its target branch. **Never** push
       the fix here until the embargo is lifted.
   * - ``ceph/ceph-ghsa-*`` (the "advisory fork" or "private fork")
     - ``advisory``
     - Develop, build (via cve-pipeline), and test the fix under embargo. Open a
       pull request here, but **do not merge** it.
   * - ``ceph/ceph-private``
     - ``private``
     - Push the finished branch here so the Release Manager can build the
       security release from it.

Developing the Fix
------------------

#. Clone the public repository as usual:

   .. prompt:: bash $

      git clone https://github.com/ceph/ceph.git

#. Check out the target branch (probably ``main``)

#. Create a branch and develop the fix as you would for any other bug. See
   :ref:`basic workflow dev guide` for the general development workflow.

   .. prompt:: bash $

      git checkout -b $BRANCH_NAME

   .. warning::

      Do **not** include any information pertaining to the CVE (for example, the
      CVE ID or a description of the vulnerability) in the branch name. Branch
      names are visible in public shaman build and repo metadata as well as in
      teuthology job results in Paddles and Pulpito, even when the branch itself
      is pushed only to the private repository.

#. Add the CVE's private fork to your clone:

   .. prompt:: bash $

      git remote add advisory git@github.com:ceph/ceph-ghsa-abcd-1234-beef.git

.. note::

   Pushing a branch to the private fork or ceph-private.git will not
   automatically trigger a build.  You must trigger the Jenkins job manually
   below.

Building the Fix
----------------

When the fix is ready to be built, push the branch to the private fork:

.. prompt:: bash $

   git push advisory $BRANCH_NAME

At this point, you are encouraged to open a Pull Request in the private fork
even if your changes aren't ready to be merged yet. This allows peer review to
begin.

Manually trigger the `cve-pipeline
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

Preparing the fix for release
-----------------------------

Once you have tested your changes and are happy with the fix,

#. Comment in the pull request in the private fork that it's ready for review
   but DO NOT MERGE it.

   .. note::

      Merging the fix would disclose the CVE before the release is ready.
      *Releasing* the fix is what requires pushing the branch to
      ceph-private.git: official signed releases must be built from the
      ceph-private.git repo because, for security reasons, the
      ceph-release-pipeline job can only build from ceph.git or
      ceph-private.git.

#. Create a backport branch in the advisory for each Ceph release branch the
   fix applies to. The backport is based on that release's most recent tag, not
   on the release branch itself. For example, if the latest release of tentacle
   was 20.2.3:

   .. prompt:: bash $

      git checkout -b ${BRANCH_NAME}-tentacle v20.2.3
      git cherry-pick -x $SHA1_OF_YOUR_FIX_FROM_BRANCH_NAME
      git push advisory ${BRANCH_NAME}-tentacle

#. Repeat this and open a Pull Request in the advisory for each applicable
   release branch.

#. Add the ceph-private repo to your remotes:

   .. prompt:: bash $

      git remote add private git@github.com:ceph/ceph-private.git

#. Push each of those backport branches to ceph-private.git, renamed to
   ``$RELEASE-release``. The branch name matters: the release process builds
   from ``$RELEASE-release`` branches regardless of release type, not just for
   CVEs. Continuing the tentacle example, with ``${BRANCH_NAME}-tentacle``
   checked out:

   .. prompt:: bash $

      git checkout -B tentacle-release
      git push -f private tentacle-release

#. Notify the Release Manager. They will proceed with the Release process as
   normal
   https://docs.ceph.com/en/latest/dev/release-process/#security-release-process-deviation.

What Success Looks Like
-----------------------

In the end, you should have:

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - What
     - Where
     - Contents
   * - One **pull request** targeting ``main``
     - advisory fork
     - Your fix commit(s) on top of ``main``.
   * - One **pull request** per affected release, targeting that release
       branch
     - advisory fork
     - Your fix commit(s) cherry-picked from ``$BRANCH_NAME`` onto that
       release's most recent tag.
   * - One **branch** per affected release, named ``$RELEASE-release``
       (for example ``tentacle-release``)
     - ``ceph-private``
     - That release's most recent tag (e.g., ``v20.2.3``) plus your
       cherry-picked commits.

Nothing is merged and nothing is pushed to ceph.git. The Release Manager builds
the signed release from the ``$RELEASE-release`` branches in ceph-private.git,
and the advisory pull requests are what eventually land in ceph.git once the
embargo is lifted.
