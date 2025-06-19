======================
  Ceph Release Process
======================

Prerequisites
=============

Signing Machine
---------------
The signing machine is a virtual machine in the `Sepia lab
<https://wiki.sepia.ceph.com/doku.php?id=start>`_. SSH access to the signing
machine is limited to the usual Infrastructure Admins along with a few other
component leads (e.g., nfs-ganesha, ceph-iscsi).

The ``ubuntu`` user on the machine has some `build scripts <https://github.com/ceph/ceph-build/tree/main/scripts>`_ that help with pulling, pushing, and signing packages.

The GPG signing key permanently lives on a `Nitrokey Pro <https://shop.nitrokey.com/shop/product/nkpr2-nitrokey-pro-2-3>`_ and is passed through to the VM via RHV. This helps to ensure that the key cannot be exported or leave the datacenter in any way.

New Major Releases
------------------
For each new major (alphabetical) release, you must create one ``ceph-release`` RPM for each RPM repo (e.g., one for el8 and one for el9). `chacra <https://github.com/ceph/chacra>`_ is a python service we use to store DEB and RPM repos. The chacra repos are configured to include this ceph-release RPM, but it must be built separately. You must make sure that chacra is properly configured to include this RPM for each particular release.

1. Update chacra so it is aware of the new Ceph release.  See `this PR <https://github.com/ceph/chacra/pull/219>`_ for an example.
2. Redeploy chacra (e.g., ``ansible-playbook chacra.ceph.com.yml``)
3. Run https://jenkins.ceph.com/view/all/job/ceph-release-rpm/

Summarized build process
========================

1. QE finishes testing and finds a stopping point.  That commit is pushed to the ``$release-release`` branch in ceph.git (e.g., ``squid-release``).  This allows work to continue in the working ``$release`` branch without having to freeze it during the release process.
2. The Ceph Council approves and notifies the "Build Lead".
3. The "Build Lead" starts the `Jenkins multijob <https://jenkins.ceph.com/view/all/job/ceph>`_, which triggers all builds.
4. Packages are pushed to chacra.ceph.com.
5. Packages are pulled from chacra.ceph.com to the Signer VM.
6. Packages are signed.
7. Packages are pushed to a prerelease area on download.ceph.com.
8. Prerelease containers are built and pushed to quay.ceph.io.
9. Final test and validation are done on prerelease packages and containers.
10. Prerelease packages and containers are promoted to official releases on
    download.ceph.com and quay.io.

Hotfix Release Process Deviation
--------------------------------

A hotfix release has a couple differences.

1. Check out the most recent tag. For example, if we're releasing a hotfix on top of 19.2.1, ``git checkout -f -B squid-release tags/v19.2.1``.
2. ``git cherry-pick -x`` the necessary hotfix commits (Note: only "cherry-pick" must be used).
3. ``git push -f origin squid-release``.
4. Verify the commits in the ``$release-release`` branch:

   1. To check against the previous point release (if we are making 19.2.2, this would be 19.2.1), run ``git log --pretty=oneline --no-merges tags/v19.2.1..origin/squid-release``. Verify that the commits produced are exactly what we want in the next point release.
   2. To check against the RC in the "ceph-ci" repo (``ceph-ci`` in this example), run ``git log --pretty=oneline --no-merges origin/squid-release...ceph-ci/squid-release``. There should be no output produced if the ``$release-release`` branch in the ceph repo is identical to the RC in ``ceph-ci``. Note the use of git `triple dot notation <https://git-scm.com/book/en/v2/Git-Tools-Revision-Selection>`_, which shows any commit discrepencies between both references.
5. Notify the "Build Lead" to start the build.
6. The "Build Lead" should set ``RELEASE_TYPE=HOTFIX`` instead of ``STABLE``.

Security Release Process Deviation
----------------------------------

A security/CVE release is similar to a hotfix release with two differences:

    1. The fix should be pushed to the `ceph-private <https://github.com/ceph/ceph-private>`_ repo instead of ceph.git (requires GitHub Admin Role).
    2. The tags (e.g., v19.2.3) must be manually pushed to ceph.git by the "Build Lead."

1. Check out the most recent tag. For example, if we're releasing a security fix on top of 19.2.2, ``git checkout -f -B squid-release origin/v19.2.2``
2. ``git cherry-pick -x`` the necessary security fix commits
3. ``git remote add security git@github.com:ceph/ceph-private.git``
4. ``git push -f security squid-release``
5. Notify the "Build Lead" to start the build.
6. The "Build Lead" should set ``RELEASE_TYPE=SECURITY`` instead of ``STABLE``.
7. Finally, the `ceph-tag <https://github.com/ceph/ceph-build/blob/main/ansible/roles/ceph-release/tasks/push.yml>`_ steps need to be manually run by the "Build Lead" as close to the Announcement time as possible::

    # Example using squid pretending 19.2.3 is the security release version
    # Add the ceph-releases repo (also requires GitHub Admin Role). The `ceph-setup <https://jenkins.ceph.com/job/ceph-setup>`_ job will have already created and pushed the tag to ceph-releases.git.
    git remote add releases git@github.com:ceph/ceph-releases.git
    git fetch --all
    # Check out the version commit
    git checkout -f -B squid-release releases/squid-release
    git push -f origin squid-release
    git push origin v19.2.3
    # Now create a Pull Request of squid-release targeting squid to merge the version commit and security fixes back into the squid branch

1. Preparing the release branch
===============================

Once QE has determined a stopping point in the working (e.g., ``squid``) branch, that commit should be pushed to the corresponding ``squid-release`` branch.

Notify the "Build Lead" that the release branch is ready.

2. Starting the build
=====================

We'll use a stable/regular 19.2.2 release of Squid as an example throughout this document.

1. Browse to https://jenkins.ceph.com/view/all/job/ceph/build?delay=0sec
2. Log in with GitHub OAuth
3. Set the parameters as necessary::

    BRANCH=squid
    TAG=checked
    VERSION=19.2.2
    RELEASE_TYPE=STABLE
    ARCHS=x86_64 arm64

NOTE: if for some reason the build has to be restarted (for example if one distro failed) then the ``TAG`` option has to be unchecked.

4. Use https://docs.ceph.com/en/latest/start/os-recommendations/?highlight=debian#platforms to determine the ``DISTROS`` parameter.  For example,

    +-------------------+--------------------------------------------------+
    | Release           | Distro Codemap                                   |
    +===================+==================================================+
    | pacific (16.X.X)  | ``focal bionic buster bullseye``                 |
    +-------------------+--------------------------------------------------+
    | quincy (17.X.X)   | ``jammy focal centos9 bullseye``                 |
    +-------------------+--------------------------------------------------+
    | reef (18.X.X)     | ``jammy focal centos9 windows bookworm``         |
    +-------------------+--------------------------------------------------+
    | squid (19.X.X)    | ``jammy centos9 windows bookworm``               |
    +-------------------+--------------------------------------------------+

5. Click ``Build``.

3. Release Notes
================

Packages take hours to build. Use those hours to create the Release Notes and Announcements:

1. ceph.git Release Notes (e.g., `v19.2.2's ceph.git (docs.ceph.com) PR <https://github.com/ceph/ceph/pull/62734>`_)
2. ceph.io Release Notes (e.g., `v19.2.2's ceph.io.git (www.ceph.io) PR <https://github.com/ceph/ceph.io/pull/864>`_)
3. E-mail announcement

See `the Ceph Tracker wiki page that explains how to write the release notes <https://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO_write_the_release_notes>`_.

.. _Signing and Publishing the Build:

4. Signing and Publishing the Build
===================================

#. Obtain the sha1 of the version commit from the `build job <https://jenkins.ceph.com/view/all/job/ceph>`_ or the ``sha1`` file created by the `ceph-setup <https://jenkins.ceph.com/job/ceph-setup/>`_ job.

#. Download the packages from chacra.ceph.com to the signing virtual machine. These packages get downloaded to ``/opt/repos`` where the `Sepia Lab Long Running (Ceph) Cluster <https://wiki.sepia.ceph.com/doku.php?id=services:longrunningcluster>`_ is mounted.  Note: this step will also run a command to transfer the source tarballs from chacra.ceph.com to download.ceph.com directly, by ssh'ing to download.ceph.com and running /home/signer/bin/get-tarballs.sh.

   .. prompt:: bash $

      ssh ubuntu@signer.front.sepia.ceph.com
      sync-pull ceph [pacific|quincy|etc] <sha1>

   Example::

      $ sync-pull ceph squid 0eceb0defba60152a8182f7bd87d164b639885b8
      sync for: ceph squid
      ********************************************
      + : 0eceb0defba60152a8182f7bd87d164b639885b8
      + project=ceph
      + release=squid
      + sha1=0eceb0defba60152a8182f7bd87d164b639885b8
      + echo 'sync for: ceph squid'
      sync for: ceph squid
      + echo '********************************************'
      ********************************************
      + [[ ceph == \c\e\p\h ]]
      + current_highest_count=0
      + for combo in debian/bookworm debian/bullseye ubuntu/bionic ubuntu/focal ubuntu/jammy
      ++ wc -l
      ++ curl -fs https://chacra.ceph.com/r/ceph/squid/0eceb0defba60152a8182f7bd87d164b639885b8/debian/bookworm/flavors/default/pool/main/c/ceph/
      + combo_count=161
      + [[ 0 -eq 22 ]]
      + '[' 161 -gt 0 ']'
      + current_highest_count=161
      + highest_combo=debian/bookworm

      etc...

#. Sign the DEBs:

   .. prompt:: bash

      merfi gpg /opt/repos/ceph/squid-19.2.2/debian/

   Example::

      --> Starting path collection, looking for files to sign
      --> 1 repos found
      --> signing: /opt/repos/ceph/squid-19.2.2/debian/jessie/dists/bookworm/Release
      --> Running command: gpg --batch --yes --armor --detach-sig --output Release.gpg Release
      --> Running command: gpg --batch --yes --clearsign --output InRelease Release
      --> signing: /opt/repos/ceph/squid-19.2.2/debian/jessie/dists/jammy/Release
      --> Running command: gpg --batch --yes --armor --detach-sig --output Release.gpg Release
      --> Running command: gpg --batch --yes --clearsign --output InRelease Release

      etc...

#. Sign the RPMs:

   .. prompt:: bash

      sign-rpms ceph squid

   Example::

      $ sign-rpms ceph squid

      + [[ 2 -lt 1 ]]
      + project=ceph
      + shift
      + '[' 1 -eq 0 ']'
      + releases=("$@")
      + distros=(centos rhel)
      + distro_versions=(7 8 9)
      + read -s -p 'Key Passphrase: ' GPG_PASSPHRASE
      Key Passphrase: + echo

      + for release in "${releases[@]}"
      + for distro in "${distros[@]}"
      + for distro_version in "${distro_versions[@]}"
      + for path in /opt/repos/$project/$release*
      + '[' -d /opt/repos/ceph/squid-19.1.0/centos/7 ']'
      ...
      + echo 'Checking packages in: /opt/repos/ceph/squid-19.1.0/centos/9'
      Checking packages in: /opt/repos/ceph/squid-19.1.0/centos/9
      + update_repo=0
      + cd /opt/repos/ceph/squid-19.1.0/centos/9
      ++ find -name '*.rpm'
      + for rpm in `find -name "*.rpm"`
      ++ grep '^Signature'

      etc...

#. Publish the packages to download.ceph.com:

   .. prompt:: bash $

      sync-push ceph squid-19.2.2 2

This leaves the packages, and the tarball, in a password-protected
prerelease area at https://download.ceph.com/prerelease/ceph.  Verify them
from there.  When done and ready for release, log into download.ceph.com and
mv the directories and the tarballs from the prerelease home
(/data/download.ceph.com/www/prerelease/ceph) to the release directory
(/data/download.ceph.com/www).


5. Build Containers
===================

Unlike CI builds, which have access to packages in the correct form for
the container, release builds do not, because the build does not 
sign the packages.  Thus, release builds do not build the containers.
This must be done after :ref:`Signing and Publishing the Build`.

A Jenkins job named ``ceph-release-containers`` exists so that we can test the
images before release. The job exists both for convenience and because it
requires access to both x86_64 and arm64 builders. Start the job as Build with Parameters on
the Jenkins server, set ``BRANCH``, ``SHA1`` and ``VERSION`` fields and leave other fields as defaults. 
This job:

* builds the architecture-specific container imagess and pushes them to
  ``quay.ceph.io/ceph/prerelease-amd64`` and
  ``quay.ceph.io/ceph/prerelease-arm64``

* fuses the architecture-specific images together into a "manifest-list"
  or "fat" container image and pushes it to ``quay.ceph.io/ceph/prerelease``

Finally, when all appropriate testing and verification is done on the
container images, run ``make-manifest-list.py --promote`` from the Ceph
source tree (at ``container/make-manifest-list.py``) to promote them to
their final release location on ``quay.io/ceph/ceph`` (you must ensure
that you're logged into ``quay.io/ceph`` and ``quay.ceph.io/ceph`` with appropriate permissions):

    .. prompt:: bash

       cd <ceph-checkout>/src/container
       ./make-manifest-list.py --promote

The ``--promote`` step should be performed only as the final step in releasing
containers, after the container images have been tested and have been confirmed
to be good.


6. Announce the Release
=======================

Version Commit PR
-----------------

The `ceph-tag Jenkins job <https://jenkins.ceph.com/job/ceph-tag>`_ creates a Pull Request in ceph.git that targets the release branch.

If this was a regular release (not a hotfix release or a security release), the only commit in that Pull Request should be the version commit.  For example, see `v15.2.17's version commit PR <https://github.com/ceph/ceph/pull/47520>`_.

Request a review and then merge the Pull Request.

Announcing
----------

Publish the Release Notes on ceph.io before announcing the release by email, because the e-mail announcement references the ceph.io blog post.
