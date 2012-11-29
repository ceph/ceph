=================
 Release Process
=================

Build environment
=================

Ceph maintains multiple build environments. 

Debian
------

We build Debian-based packages via ``pbuilder`` for multiple distributions, which identifies the build hosts in the ``deb_hosts`` file and the list of distributions in the ``deb_dist`` file. Each build host will build all distributions. Currently Ceph maintains 1 64-bit build host and 1 32-bit build host.

RPM
---

We build RPM-based packages natively. Therefore, we maintain one build host per distribution.  We build RPM-based packages via ``pbuilder``, which identifies the build hosts in the ``rpm_hosts`` file.

Prior to building, it's necessary to update the ``pbuilder`` seed tarballs::

    ./update_all_pbuilders.sh

2. Setup keyring for signing packages
=====================================

::

    export GNUPGHOME=<path to keyring dir>

    # verify it's accessible
    gpg --list-keys

The release key should be present::

  pub   4096R/17ED316D 2012-05-20
  uid                  Ceph Release Key <sage@newdream.net>


3. Set up build area
====================

Checkout ceph and ceph-build::

    git clone http://github.com/ceph/ceph.git
    git clone http://github.com/ceph/ceph-build.git

Checkout next branch::

    git checkout next

Checkout the submodules (only needed to prevent errors in recursive make)::

    git submodule update --init

4.  Update Build version numbers
================================

Edit configure.ac and change version number::

    DEBEMAIL user@host dch -v 0.xx-1

Commit the changes::

    git commit -a

Tag the release::

    ../ceph-build/tag-release v0.xx

5. Create Makefiles
===================

The actual configure options used to build packages are in the
``ceph.spec.in`` and ``debian/rules`` files.  At this point we just
need to create a Makefile.::

     ./do_autogen.sh


6. Run the release scripts
==========================

This creates tarballs and copies them, with other needed files to
the build hosts listed in deb_hosts and rpm_hosts, runs a local build
script, then rsyncs the results back tot the specified release directory.::

    ../ceph-build/do_release.sh /tmp/release

7. Create RPM Repo
==================

Copy the rpms to the destination repo, creates the yum repository
rpm and indexes.::

   ../ceph-build/push_to_rpm_repo.sh /tmp/release /tmp/repo 0.xx

8. Create debian repo
=====================

::

    mkdir /tmp/debian-repo
    ../ceph-build/gen_reprepro_conf.sh debian-testing
    ../ceph-build/push_to_deb_repo.sh /tmp/release /tmp/debian-repo 0.xx main

9.  Push repos to ceph.org
==========================

For a development release::

    rcp ceph-0.xx.tar.bz2 ceph-0.xx.tar.gz \
        ceph_site@ceph.com:ceph.com/downloads/.
    rsync -av /tmp/repo/0.52/ ceph_site@ceph.com:ceph.com/rpm-testing
    rsync -av /tmp/debian-repo/ ceph_site@ceph.com:ceph.com/debian-testing

For a stable release, replace {CODENAME} with the release codename (e.g., ``argonaut`` or ``bobtail``)::

    rcp ceph-0.xx.tar.bz2 \
        ceph_site@ceph.com:ceph.com/downloads/ceph-0.xx{CODENAME}.tar.bz2
    rcp ceph-0.xx.tar.gz  \
        ceph_site@ceph.com:ceph.com/downloads/ceph-0.xx{CODENAME}.tar.gz
    rsync -av /tmp/repo/0.52/ ceph_site@ceph.com:ceph.com/rpm-{CODENAME}
    rsync -auv /tmp/debian-repo/ ceph_site@ceph.com:ceph.com/debian-{CODENAME}

10. Update Git
==============

Development release
-------------------

For a development release, update tags for ``ceph.git``::

    git push origin v0.xx
    git push origin HEAD:testing
    git checkout master
    git merge next
    git push origin master
    git push origin HEAD:next

Similarly, for a development release, for both ``teuthology.git`` and ``ceph-qa-suite.git``::

    git checkout master
    git reset --hard origin/master
    git branch -f testing origin/next
    git push -f origin testing
    git push -f master:next

Stable release
--------------

For ``ceph.git``:

    git push origin stable
