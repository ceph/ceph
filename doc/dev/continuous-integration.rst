Continuous Integration Architecture
===================================

In Ceph, we rely on multiple CI pipelines in our development. Most of these pipelines
are centered around Jenkins. And their configurations are generated using `Jenkins Job Builder`_.

.. _Jenkins Job Builder: https://docs.openstack.org/infra/jenkins-job-builder/

Let's take the ``make check`` performed by Jenkins as an example.

ceph-pull-requests
------------------

``ceph-pull-requests`` is a jenkins job which gets triggered by a GitHub pull
request or a trigger phrase like::

    jenkins test make check

There are multiple parties involved in this jenkins job:

.. graphviz::

   digraph  {
     rankdir="LR";
     github [
      label="<git> git_repo | <webhooks> webhooks | <api> api";
      shape=record;
      href="https://github.com/ceph/ceph";
     ];
     subgraph cluster_lab {
       label="Sepia Lab";
       href="https://wiki.sepia.ceph.com/doku.php";
       shape=circle;
       apt_mirror [
         href="http://apt-mirror.front.sepia.ceph.com";
       ];
       shaman [
         href="https://shaman.ceph.com";
       ];
       chacra [
         peripheries=3;
         href="https://chacra.ceph.com";
       ];
       subgraph cluster_jenkins {
         label="jenkins";
         href="https://jenkins.ceph.com";
         jenkins_controller [ label = "controller" ];
         jenkins_agents [ label = "agents", peripheries=3 ];
       };
     };
     {
       rank=same;
       package_repos [ peripheries=3 ];
       pypi;
       npm;
     }
     github:webhooks -> jenkins_controller [ label = "notify", color = "crimson" ];
     jenkins_controller -> jenkins_agents [ label = "schedule jobs" ];
     jenkins_agents -> github:git [ label = "git pull" ];
     jenkins_agents -> shaman [ label = "query for chacra repo URL" ];
     jenkins_agents -> chacra [ label = "pull build dependencies" ];
     jenkins_agents -> package_repos [ label = "pull build dependencies" ];
     jenkins_agents -> pypi [ label = "pull Python packages" ];
     jenkins_agents -> npm [ label = "pull JavaScript packages" ];
     jenkins_agents -> apt_mirror [ label = "pull build dependencies" ];
     jenkins_agents -> github:api [ label = "update", color = "crimson" ];
   }

Where

Sepia Lab
   `Sepia Lab`_ is a test lab used by the Ceph project. This lab offers
   the storage and computing resources required by our CI infra.

Jenkins agents
   are a set of machines which perform the CI jobs. In this case, they

   #. pull the git repo from GitHub and
   #. rebase the pull request against the latest master
   #. set necessary environment variables
   #. run ``run-make-check.sh``

Chacra
   is a server offering RESTful API allowing the clients to store and
   retrieve binary packages. It also creates the repo for uploaded
   packages automatically. Once a certain repo is created on chacra, the
   configured shaman server is updated as well, then we can query shaman
   for the corresponding repo address. Chacra not only hosts Ceph packages,
   it also hosts quite a few other packages like various build dependencies.

Shaman
   is a server offering RESTful API allowing the clients to query the
   information of repos hosted by chacra nodes. Shaman is also known
   for its `Web UI`_. But please note, shaman does not build the
   packages, it just offers information on the builds.

As the following shows, `chacra`_ manages multiple projects whose metadata
are stored in a database. These metadata are exposed via Shaman as a web
service. `chacractl`_ is a utility to interact with the `chacra`_ service.

.. graphviz::

   digraph  {
     libboost [
       shape=cylinder;
     ];
     libzbd [
       shape=cylinder;
     ];
     other_repos [
       label="...";
       shape=cylinder;
     ];
     postgresql [
       shape=cylinder;
       style=filled;
     ]
     shaman -> postgresql;
     chacra -> postgresql;
     chacractl -> chacra;
     chacra -> libboost;
     chacra -> libzbd;
     chacra -> other_repos;
   }

.. _Sepia Lab: https://wiki.sepia.ceph.com/doku.php
.. _Web UI: https://shaman.ceph.com

build dependencies
------------------

Just like lots of other software projects, Ceph has both build-time and
run-time dependencies. Most of time, we are inclined to use the packages
prebuilt by the distro. But there are cases where

- the necessary dependencies are either missing in the distro, or
- their versions are too old, or
- they are packaged without some important feature enabled.
- we want to ensure that the version of a certain runtime dependency is
  identical to the one we tested in our lab.

No matter what the reason is, we either need to build them from source, or
to package them as binary packages instead of using the ones shipped by the
distro. Quite a few build-time dependencies are included as git submodules,
but in order to avoid rebuilding these dependencies repeatedly, we pre-built
some of them and uploaded them to our own repos. So, when performing
``make check``, the building hosts in our CI just pull them from our internal
repos hosting these packages instead of building them.

So far, following packages are prebuilt for ubuntu focal, and then uploaded to
`chacra`_:

libboost
    packages `boost`_. The packages' names are changed from ``libboost-*`` to
    ``ceph-libboost-*``, and they are instead installed into ``/opt/ceph``, so
    they don't interfere with the official ``libboost`` packages shipped by
    distro. Its build scripts are hosted at https://github.com/ceph/ceph-boost.

    .. prompt:: bash $

       tar xjf boost_1_76_0.tar.bz2
       git clone https://github.com/ceph/ceph-boost
       cp -ra ceph-boost/debian boost_1_76_0/
       export DEB_BUILD_OPTIONS='parallel=6 nodoc'
       dpkg-buildpackage -us -uc -b

libzbd
    packages `libzbd`_ . The upstream libzbd includes debian packaging already.

libpmem
    packages `pmdk`_ . Please note, ``ndctl`` is one of the build dependencies of
    pmdk, for an updated debian packaging, please see
    https://github.com/ceph/ceph-ndctl .

.. note::

   please ensure that the package version and the release number of the
   packaging are properly updated when updating/upgrading the packaging,
   otherwise it would be difficult to tell which version of the package
   is installed. We check the package version before trying to upgrade
   it in ``install-deps.sh``.

.. _boost: https://www.boost.org
.. _libzbd: https://github.com/westerndigitalcorporation/libzbd
.. _pmdk: https://github.com/pmem/pmdk

But in addition to these libraries, ``ceph-mgr-dashboard``'s frontend uses lots of
JavaScript packages. Quite a few of them are not packaged by distros. Not to
mention the trouble of testing different combination of versions of these
packages. So we decided to include these JavaScript packages in our dist tarball
using ``make-dist``.

Also, because our downstream might not want to use the prepackaged binaries when
redistributing the precompiled Ceph packages, we also need to include these
libraries in our dist tarball. They are

- boost
- liburing
- pmdk

``make-dist`` is a script used by our CI pipeline to create dist tarball so the
tarball can be used to build the Ceph packages in a clean room environment. When
we need to upgrade these third party libraries, we should

- update the CMake script
- rebuild the prebuilt packages and
- update this script to reflect the change.

Uploading Dependencies
----------------------

To ensure that prebuilt packages are available by the jenkins agents, we need to
upload them to either ``apt-mirror.front.sepia.ceph.com`` or `chacra`_. To upload
packages to the former would require the help our our lab administrator, so if we
want to maintain the package repositories on regular basis, a better choice would be
to manage them using `chacractl`_. `chacra`_ represents packages repositories using
a resource hierarchy, like::

  <project>/<branch>/<ref>/<distro>/<distro-version>/<arch>

In which:

project
    in general, it is used for denoting a set of related packages. For instance,
    ``libboost``.

branch
    branch of project. This mirrors the concept of a Git repo.

ref
    a unique id of a given version of a set packages. This id is used to reference
    the set packages under the ``<project>/<branch>``. It is a good practice to
    version the packaging recipes, like the ``debian`` directory for building deb
    packages and the ``spec`` for building rpm packages, and use the sha1 of the
    packaging receipe for the ``ref``. But you could also use a random string for
    ``ref``, like the tag name of the built source tree.

distro
    the distro name for which the packages are built. Currently, following distros are
    supported:

    - centos
    - debian
    - fedora
    - rhel
    - ubuntu

distro-version
    the version of the distro. For instance, if a package is built on ubuntu focal,
    the ``distro-version`` should be ``20.04``.

arch
    the architecture of the packages. It could be:

    - arm64
    - amd64
    - noarch

So, for example, we can upload the prebuilt boost packages to chacra like

.. prompt:: bash $

   ls *.deb | chacractl binary create \
     libboost/master/099c0fd56b4a54457e288a2eff8fffdc0d416f7a/ubuntu/focal/amd64/flavors/default

.. _chacra: https://github.com/ceph/chacra
.. _chacractl: https://github.com/ceph/chacractl

Update ``install-deps.sh``
--------------------------

We also need to update ``install-deps.sh`` to point the built script to the new
repo. Please refer to the `script <https://github.com/ceph/ceph/blob/master/install-deps.sh>`_,
for more details.
