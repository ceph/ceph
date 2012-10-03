===================
  Release Process
===================

1. Build environment
====================

There are multiple build envrionments, debian based packages are built via pbuilder for multiple distributions.  The build hosts are deb_host, and the list of distributions is deb_dist.  All distributions are build on each of the build hosts.  Currently there is 1 64 bit and 1 32 bit build host.

The rpm based packages are are built natively, so 1 distribution per build host.  The list of rpm_build hosts is found in rpm_hosts.

Prior to building, it's necessorary to update the pbuilder seed tarballs.  The update_all_pbuilder scripts is used for this.

2. Setup keyring for signing packages
=====================================

    export GNUPGHOME=<path to keyring>

    # verify it's accessible
    gpg --list-keys

3. Set up build area
====================

    #checkout ceph and ceph-build

    git clone http://github.com/ceph/ceph.git
    git clone http://github.com/ceph/ceph-build.git

    #checkout next branch

    git checkout next

     #submodules (only needed to prevent errors in recursive make)

     git submodule init
     git submodule update

4.  Update Build version numbers
================================

    edit configure.ac and change version number

    DEBEMAIL user@host dch -v 0.xx-1 

    # commit the changes

    git commit -a 

    # Tag the release 

    ../ceph-build/tag-release v0.xx

5. Create Makefiles
===================

     ./autogen

     # The actuall configure options used to build packages are in the
     # ceph.spec.in and debian/rules files.  At this point we just need
     # to create a Makefile.

     ./configure --with-debug \
                 --with-radosgw \
                 --with-fuse \
                 --with-tcmalloc \
                 --with-libatomic-ops \
                 --with-nss \
                 --without-cryptpp \
                 --with-gtk2

6. Run the release scripts
==========================

   This creates tarballs and copies them, with other needed files to
   the build hosts listed in deb_hosts and rpm_hosts, runs a local build
   script, then rsyncs the results back tot the specified release directory.

    ../ceph-build/do_release.sh /tmp/release

7. Create RPM Repo
==================

   Copies the rpms to the destination repo, creates the yum repository
   rpm and indexes.

   ../ceph-build/push_to_rpm_repo.sh /tmp/release /tmp/repo 0.xx

8. Create debian repo
=====================

    mkdir /tmp/debian-repo
    mkdir /tmp/debian-repo/conf
    ../ceph-build/gen_reprepro_conf.sh debian-testing main \
             	 `cat ceph-build/deb_dists`
    ../ceph-build/push_to_deb_repo.sh  /tmp/release /tmp/debian-repo

9.  Push repos to ceph.org
==========================

    TBD multiple repos for stable and developement

    rsync -av /tmp/repo/0.52/ ceph_site@ceph.com:ceph.com/rpms
    rsync -auv /tmp/debian-repo/ ceph_site@ceph.com:ceph.com/debian

10. Update Git
==============

    Merge work area commits (version number updates and new tag)
    Move testing tag
    Move next tag

    TBD stable release and point release
