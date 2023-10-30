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

1. QE finishes testing and finds a stopping point.  That commit is pushed to the ``$release-release`` branch in ceph.git (e.g., ``quincy-release``).  This allows work to continue in the working ``$release`` branch without having to freeze it during the release process.
2. The Ceph Council approves and notifies the "Build Lead".
3. The "Build Lead" starts the `Jenkins multijob <https://jenkins.ceph.com/view/all/job/ceph>`_, which triggers all builds.
4. Packages are pushed to chacra.ceph.com.
5. Packages are pulled from chacra.ceph.com to the Signer VM.
6. Packages are signed.
7. Packages are pushed to download.ceph.com.
8. Release containers are built and pushed to quay.io.

Hotfix Release Process Deviation
--------------------------------

A hotfix release has a couple differences.

1. Check out the most recent tag. For example, if we're releasing a hotfix on top of 17.2.3, ``git checkout -f -B quincy-release tags/v17.2.3``.
2. ``git cherry-pick -x`` the necessary hotfix commits (Note: only "cherry-pick" must be used).
3. ``git push -f origin quincy-release``.
4. Verify the commits in the ``$release-release`` branch:

   1. To check against the previous point release (if we are making 17.2.4, this would be 17.2.3), run ``git log --pretty=oneline --no-merges tags/v17.2.3..origin/quincy-release``. Verify that the commits produced are exactly what we want in the next point release.
   2. To check against the RC in the "ceph-ci" repo (``ceph-ci`` in this example), run ``git log --pretty=oneline --no-merges origin/quincy-release...ceph-ci/quincy-release``. There should be no output produced if the ``$release-release`` branch in the ceph repo is identical to the RC in ``ceph-ci``. Note the use of git `triple dot notation <https://git-scm.com/book/en/v2/Git-Tools-Revision-Selection>`_, which shows any commit discrepencies between both references.
5. Notify the "Build Lead" to start the build.
6. The "Build Lead" should set ``RELEASE_TYPE=HOTFIX`` instead of ``STABLE``.

Security Release Process Deviation
----------------------------------

A security/CVE release is similar to a hotfix release with two differences:

    1. The fix should be pushed to the `ceph-private <https://github.com/ceph/ceph-private>`_ repo instead of ceph.git (requires GitHub Admin Role).
    2. The tags (e.g., v17.2.4) must be manually pushed to ceph.git by the "Build Lead."

1. Check out the most recent tag. For example, if we're releasing a security fix on top of 17.2.3, ``git checkout -f -B quincy-release origin/v17.2.3``
2. ``git cherry-pick -x`` the necessary security fix commits
3. ``git remote add security git@github.com:ceph/ceph-private.git``
4. ``git push -f security quincy-release``
5. Notify the "Build Lead" to start the build.
6. The "Build Lead" should set ``RELEASE_TYPE=SECURITY`` instead of ``STABLE``.
7. Finally, the `ceph-tag <https://github.com/ceph/ceph-build/blob/main/ansible/roles/ceph-release/tasks/push.yml>`_ steps need to be manually run by the "Build Lead" as close to the Announcement time as possible::

    # Example using quincy pretending 17.2.4 is the security release version
    # Add the ceph-releases repo (also requires GitHub Admin Role). The `ceph-setup <https://jenkins.ceph.com/job/ceph-setup>`_ job will have already created and pushed the tag to ceph-releases.git.
    git remote add releases git@github.com:ceph/ceph-releases.git
    git fetch --all
    # Check out the version commit
    git checkout -f -B quincy-release releases/quincy-release
    git push -f origin quincy-release
    git push origin v17.2.4
    # Now create a Pull Request of quincy-release targeting quincy to merge the version commit and security fixes back into the quincy branch

1. Preparing the release branch
===============================

Once QE has determined a stopping point in the working (e.g., ``quincy``) branch, that commit should be pushed to the corresponding ``quincy-release`` branch.

Notify the "Build Lead" that the release branch is ready.

2. Starting the build
=====================

We'll use a stable/regular 15.2.17 release of Octopus as an example throughout this document.

1. Browse to https://jenkins.ceph.com/view/all/job/ceph/build?delay=0sec
2. Log in with GitHub OAuth
3. Set the parameters as necessary::

    BRANCH=octopus
    TAG=checked
    VERSION=15.2.17
    RELEASE_TYPE=STABLE
    ARCHS=x86_64 arm64

NOTE: if for some reason the build has to be restarted (for example if one distro failed) then the ``TAG`` option has to be unchecked.

4. Use https://docs.ceph.com/en/latest/start/os-recommendations/?highlight=debian#platforms to determine the ``DISTROS`` parameter.  For example,

    +-------------------+--------------------------------------------------+
    | Release           | Distro Codemap                                   |
    +===================+==================================================+
    | pacific (16.X.X)  | ``focal bionic centos8 buster bullseye``         |
    +-------------------+--------------------------------------------------+
    | quincy (17.X.X)   | ``focal centos8 centos9 bullseye``               |
    +-------------------+--------------------------------------------------+
    | reef (18.X.X)     | ``jammy focal centos8 centos9 windows bookworm`` |
    +-------------------+--------------------------------------------------+

5. Click ``Build``.

3. Release Notes
================

Packages take hours to build. Use those hours to create the Release Notes and Announcements:

1. ceph.git Release Notes (e.g., `v15.2.17's ceph.git (docs.ceph.com) PR <https://github.com/ceph/ceph/pull/47198>`_)
2. ceph.io Release Notes (e.g., `v15.2.17's ceph.io.git (www.ceph.io) PR <https://github.com/ceph/ceph.io/pull/427>`_)
3. E-mail announcement

See `the Ceph Tracker wiki page that explains how to write the release notes <https://tracker.ceph.com/projects/ceph-releases/wiki/HOWTO_write_the_release_notes>`_.

4. Signing and Publishing the Build
===================================

#. Obtain the sha1 of the version commit from the `build job <https://jenkins.ceph.com/view/all/job/ceph>`_ or the ``sha1`` file created by the `ceph-setup <https://jenkins.ceph.com/job/ceph-setup/>`_ job.

#. Download the packages from chacra.ceph.com to the signing virtual machine. These packages get downloaded to ``/opt/repos`` where the `Sepia Lab Long Running (Ceph) Cluster <https://wiki.sepia.ceph.com/doku.php?id=services:longrunningcluster>`_ is mounted.

   .. prompt:: bash $

      ssh ubuntu@signer.front.sepia.ceph.com
      sync-pull ceph [pacific|quincy|etc] <sha1>

   Example::

      $ sync-pull ceph octopus 8a82819d84cf884bd39c17e3236e0632ac146dc4
      sync for: ceph octopus
      ********************************************
      Found the most packages (332) in ubuntu/bionic.
      No JSON object could be decoded
      No JSON object could be decoded
      ubuntu@chacra.ceph.com:/opt/repos/ceph/octopus/8a82819d84cf884bd39c17e3236e0632ac146dc4/ubuntu/bionic/flavors/default/* /opt/repos/ceph/octopus-15.2.17/debian/jessie/
      --------------------------------------------
      receiving incremental file list
      db/
       db/checksums.db
              180.22K 100%    2.23MB/s    0:00:00 (xfr#1, to-chk=463/467)
      db/contents.cache.db
              507.90K 100%    1.95MB/s    0:00:00 (xfr#2, to-chk=462/467)
      db/packages.db

      etc...

#. Sign the DEBs:

   .. prompt:: bash

      merfi gpg /opt/repos/ceph/octopus-15.2.17/debian

   Example::

      $ merfi gpg /opt/repos/ceph/octopus-15.2.17/debian
      --> Starting path collection, looking for files to sign
      --> 18 matching paths found
      --> will sign with the following commands:
      --> gpg --batch --yes --armor --detach-sig --output Release.gpg Release
      --> gpg --batch --yes --clearsign --output InRelease Release
      --> signing: /opt/repos/ceph/octopus-15.2.17/debian/jessie/dists/bionic/Release
      --> Running command: gpg --batch --yes --armor --detach-sig --output Release.gpg Release
      --> Running command: gpg --batch --yes --clearsign --output InRelease Release
      --> signing: /opt/repos/ceph/octopus-15.2.17/debian/jessie/dists/focal/Release
      --> Running command: gpg --batch --yes --armor --detach-sig --output Release.gpg Release
      --> Running command: gpg --batch --yes --clearsign --output InRelease Release

      etc...

#. Sign the RPMs:

   .. prompt:: bash

      sign-rpms octopus

   Example::

      $ sign-rpms octopus
      Checking packages in: /opt/repos/ceph/octopus-15.2.17/centos/7
      signing:  /opt/repos/ceph/octopus-15.2.17/centos/7/SRPMS/ceph-release-1-1.el7.src.rpm
      /opt/repos/ceph/octopus-15.2.17/centos/7/SRPMS/ceph-release-1-1.el7.src.rpm:
      signing:  /opt/repos/ceph/octopus-15.2.17/centos/7/SRPMS/ceph-15.2.17-0.el7.src.rpm
      /opt/repos/ceph/octopus-15.2.17/centos/7/SRPMS/ceph-15.2.17-0.el7.src.rpm:
      signing:  /opt/repos/ceph/octopus-15.2.17/centos/7/noarch/ceph-mgr-modules-core-15.2.17-0.el7.noarch.rpm

      etc...

5. Publish the packages to download.ceph.com:

   .. prompt:: bash $

      sync-push octopus

5. Build Containers
===================

Start the following two jobs:

#. https://2.jenkins.ceph.com/job/ceph-container-build-ceph-base-push-imgs/
#. https://2.jenkins.ceph.com/job/ceph-container-build-ceph-base-push-imgs-arm64/

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
