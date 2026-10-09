.. _packages:

=======================
 Getting Ceph Packages
=======================

.. meta::
   :description: Add the Ceph package repository with cephadm, APT, or DNF, use development builds, or download packages for hosts without internet access.
   :ceph-page-type: procedure

Set up the Ceph package repository on each :term:`host <Host>`, or download
the packages. Hosts managed by :term:`cephadm` run the daemons in containers
and need no Ceph packages; install ``ceph-common`` on them only if you want
the command-line tools there (see :ref:`cephadm-enable-cli`).

Prerequisites
=============

- A distribution that Ceph builds packages for. See :ref:`start-platforms`.
- The :term:`release <Ceph Release>` that you want, for example
  |stable-release|, the current :term:`stable release <Ceph Stable Release>`.
  `Releases`_ lists every release.
- Root access, or a user with ``sudo``.
- Access to ``download.ceph.com``, or to a mirror near you; see
  :ref:`install-mirrors`.
- For the cephadm route, ``cephadm`` installed as described in
  :ref:`get-cephadm`.

Procedure
=========

.. list-table::
   :header-rows: 1
   :widths: 45 55

   * - Your hosts
     - Follow
   * - ``cephadm`` is installed, and you want it to add the repository
     - `Using Cephadm`_
   * - Debian or Ubuntu
     - `Debian and Ubuntu`_
   * - RHEL, CentOS Stream, Rocky Linux, or another Enterprise Linux (EL)
       distribution
     - `RHEL`_
   * - openSUSE, openEuler, or another distribution that ships its own Ceph
       packages
     - `Distribution Packages`_
   * - A :term:`release candidate <Ceph Release Candidate>` (version x.1.z),
       which is not a development build
     - `Debian and Ubuntu`_ or `RHEL`_, with the version or the name of the
       coming release in place of the release name, for example
       ``https://download.ceph.com/debian-21.1.1/`` (bookworm, jammy, noble,
       trixie)
   * - Unreleased builds, for development and testing
     - `Development Packages`_
   * - No internet access
     - `Downloading Packages Manually`_

Stable releases and release candidates are signed with the release key,
``release.asc``; development builds from shaman are not.

.. _install-packages-with-cephadm:

.. _get-packages-cephadm:

Using Cephadm
-------------

.. warning:: ``cephadm add-repo`` replaces ``/etc/yum.repos.d/ceph.repo`` on
   EL, or ``/etc/apt/sources.list.d/ceph.list`` and
   ``/etc/apt/trusted.gpg.d/ceph.release.gpg`` on Debian and Ubuntu, without
   keeping a copy. Copy these files first if you have changed them. On EL, the
   command also installs the ``epel-release`` package.

#. Add the repository for the release:

   .. prompt:: bash #
      :substitutions:

      cephadm add-repo --release |stable-release|

   ``Completed adding repo.`` is the last line of the output; on EL,
   ``Enabling EPEL...`` appears before it. To pin a specific release instead,
   give ``--version`` and the version in x.y.z form, for example
   ``cephadm add-repo --version 20.2.4``.

#. On EL, enable the CRB repository as in steps 3 and 4 of `RHEL`_. Some
   dependencies come from it: on EL 10, ``ceph-common`` needs ``lttng-ust``.

#. Install the command-line tools:

   .. prompt:: bash #

      cephadm install ceph-common

   On success, ``Installing packages ['ceph-common']...`` is the last line of
   the output, and the output of the package manager goes to
   ``/var/log/ceph/cephadm.log``. If the package manager fails, cephadm
   prints ``Non-zero exit code``, the command that failed, and its output.

.. _configure-repositories-manually:

.. _ceph-release-packages:

.. _debian-packages:

Debian and Ubuntu
-----------------

APT finds Ceph in ``https://download.ceph.com/debian-{release-name}/``.

#. Install the release key, which APT uses to check package signatures:

   .. prompt:: bash $

      wget -q -O- 'https://download.ceph.com/keys/release.asc' | sudo tee /etc/apt/trusted.gpg.d/ceph.asc

   ``tee`` prints the key, a PGP public key block.

#. Add the repository.

   .. warning:: This command replaces ``/etc/apt/sources.list.d/ceph.list``,
      including any entry for another Ceph release.

   .. prompt:: bash $
      :substitutions:

      echo "deb https://download.ceph.com/debian-|stable-release|/ $(. /etc/os-release && echo $VERSION_CODENAME) main" | sudo tee /etc/apt/sources.list.d/ceph.list

   ``tee`` prints the line that it wrote. The command reads the codename of
   your distribution release, such as ``noble`` or ``bookworm``, from
   ``/etc/os-release``. For an earlier release, replace the release name in
   the URL. To pin a specific release, use ``debian-{version}`` instead, for
   example ``debian-20.2.4``.

#. Update the package index:

   .. prompt:: bash $

      sudo apt-get update

   The output includes a line for ``download.ceph.com`` and no error lines,
   which start with ``E:``.

.. _rpm-packages:

.. _get-packages-rhel:

RHEL
----

These steps apply to RHEL and the other Enterprise Linux (EL) distributions,
such as CentOS Stream and Rocky Linux. Their package manager is DNF; ``yum``
still works as an alias of ``dnf``.

#. Import the release key:

   .. prompt:: bash $

      sudo rpm --import 'https://download.ceph.com/keys/release.asc'

   The command prints nothing on success.

#. Enable EPEL (Extra Packages for Enterprise Linux), a Fedora project
   repository that supplies dependencies such as ``gperftools-libs``. Replace
   ``{distro_release}`` with the major version of your distribution, for
   example ``10`` for EL 10:

   .. prompt:: bash $

      sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-{distro_release}.noarch.rpm

   The output recommends enabling the CodeReady Builder (CRB) repository,
   which step 4 does; ``Complete!`` is its last line.

#. Install the ``config-manager`` command of DNF:

   .. prompt:: bash $

      sudo dnf install -y dnf-plugins-core

   ``Complete!`` is the last line of the output.

#. Enable the CRB repository, which supplies other dependencies, such as
   ``lua-devel`` and, on EL 10, ``lttng-ust``. On RHEL itself, run
   ``sudo crb enable`` instead of the command below: this helper from the
   ``epel-release`` package enables the repository through
   ``subscription-manager``.

   .. prompt:: bash $

      sudo dnf config-manager --set-enabled crb

   The command prints nothing on success.

#. Create the file ``/etc/yum.repos.d/ceph.repo`` with the following content,
   replacing ``{distro}`` with ``el9`` or ``el10``.

   .. warning:: This replaces an existing ``ceph.repo``, such as one written
      by ``cephadm add-repo`` or by the ``ceph-release`` package.

   .. code-block:: ini
      :substitutions:

      [ceph]
      name=Ceph packages for $basearch
      baseurl=https://download.ceph.com/rpm-|stable-release|/{distro}/$basearch
      enabled=1
      priority=2
      gpgcheck=1
      gpgkey=https://download.ceph.com/keys/release.asc

      [ceph-noarch]
      name=Ceph noarch packages
      baseurl=https://download.ceph.com/rpm-|stable-release|/{distro}/noarch
      enabled=1
      priority=2
      gpgcheck=1
      gpgkey=https://download.ceph.com/keys/release.asc

      [ceph-source]
      name=Ceph source packages
      baseurl=https://download.ceph.com/rpm-|stable-release|/{distro}/SRPMS
      enabled=0
      priority=2
      gpgcheck=1
      gpgkey=https://download.ceph.com/keys/release.asc

   Set ``priority=2`` so that packages from the Ceph repository take
   precedence over the older ``librados2`` and ``librbd1`` packages that the
   distribution ships: DNF prefers the repository with the lower number, and
   the default is 99. DNF fills in ``$basearch`` with the CPU architecture;
   ``noarch`` holds packages for every architecture, and ``SRPMS`` holds
   source packages.

   For an earlier release, replace the release name in each ``baseurl``. To
   pin a specific release, use ``rpm-{version}`` instead, for example
   ``rpm-20.2.4``. ``https://download.ceph.com/rpm-{release-name}/`` lists the
   distributions that a release is built for.

   Instead of creating the file, you can install the ``ceph-release``
   package, which writes ``ceph.repo`` for one release and distribution.

   .. warning:: The ``ceph-release`` package replaces an existing
      ``/etc/yum.repos.d/ceph.repo`` without keeping a copy. The file that it
      writes has no ``priority`` line, uses ``http://`` URLs, and enables the
      source repository.

   .. prompt:: bash $
      :substitutions:

      su -c 'rpm -Uvh https://download.ceph.com/rpm-|stable-release|/el10/noarch/ceph-release-1-1.el10.noarch.rpm'

   The output ends with ``ceph-release`` at ``[100%]``. The file name has the
   form ``ceph-release-{version}.{distro}.noarch.rpm``, and the ``noarch``
   directory of each release and distribution holds it; for EL 9, use ``el9``
   in both places.

.. _opensuse-tumbleweed:

.. _openeuler:

Distribution Packages
---------------------

openSUSE Tumbleweed and openEuler ship Ceph packages in their standard
repositories, so you do not add a repository; install the packages with the
package manager of the distribution. The Ceph project does not build or test
these packages; see :ref:`start-platforms`.

#. On openEuler, install Ceph:

   .. prompt:: bash $

      sudo yum -y install ceph

   ``Complete!`` is the last line of the output. The openEuler packages are
   also in
   ``https://repo.openeuler.org/openEuler-{release}/everything/{arch}/Packages/``.

.. _deb-packages:

.. _ceph-development-packages:

Development Packages
--------------------

.. warning:: Development packages are untested builds, for developers and
   quality assurance only. While their repository is configured, the next
   install or upgrade replaces the installed release with a development build.
   The commands below replace ``/etc/apt/sources.list.d/shaman.list`` or
   ``/etc/yum.repos.d/shaman.repo``.

The Ceph CI builds packages for current branches of the Ceph source
repository; shaman tracks the builds and chacra hosts them. `The shaman page`_
lists the branches and distributions that are built.

#. Remove the release repository file, ``/etc/apt/sources.list.d/ceph.list``
   or ``/etc/yum.repos.d/ceph.repo``. On EL, its section names are the same as
   those of the development repository. Afterwards,
   ``ls /etc/apt/sources.list.d`` or ``ls /etc/yum.repos.d`` does not list
   the file.

#. Add the repository for the newest build of a branch with the command for
   your distribution, replacing ``{BRANCH}`` with the branch name, for example
   ``main`` or ``wip-hack``:

   - Ubuntu:

     .. prompt:: bash $

        curl -fsSL https://shaman.ceph.com/api/repos/ceph/{BRANCH}/latest/ubuntu/$(. /etc/os-release && echo $VERSION_CODENAME)/repo/ | sudo tee /etc/apt/sources.list.d/shaman.list

   - EL 10 (for EL 9, use ``centos/9`` instead of ``rocky/10``):

     .. prompt:: bash $

        curl -fsSL https://shaman.ceph.com/api/repos/ceph/{BRANCH}/latest/rocky/10/repo/ | sudo tee /etc/yum.repos.d/shaman.repo

   - Ubuntu or EL 9, with cephadm, which writes ``ceph.list`` or
     ``ceph.repo`` instead (on EL 10, cephadm asks shaman for ``centos/10``,
     which shaman does not build, so use the ``curl`` command above):

     .. prompt:: bash #

        cephadm add-repo --dev {BRANCH}

   ``tee`` prints the repository definition that it wrote. For ``cephadm``,
   ``Completed adding repo.`` is the last line of the output.

   ``latest`` in the URL selects the newest commit of the branch that shaman
   has built. To use one commit that shaman has built, replace ``latest``
   with its SHA1, the commit ID in the Ceph Git repository. With cephadm, add
   ``--dev-commit {SHA1}`` to the ``cephadm add-repo --dev {BRANCH}``
   command; ``--dev-commit`` alone fails. For example, on Ubuntu 22.04
   (jammy):

   .. prompt:: bash $

      curl -fsSL https://shaman.ceph.com/api/repos/ceph/main/{SHA1}/ubuntu/jammy/repo/ | sudo tee /etc/apt/sources.list.d/shaman.list

   The same works with ``rocky/10`` and ``/etc/yum.repos.d/shaman.repo``.
   Development repositories are no longer available after two weeks.

#. If you used ``curl`` on Ubuntu, update the package index, as in step 3 of
   `Debian and Ubuntu`_. ``cephadm`` does this for you.

.. _download-packages-manually:

Downloading Packages Manually
-----------------------------

For a host without internet access, download the packages, with every
dependency, on a host that has access, and copy them over. For a cluster that
cephadm deploys, see :ref:`cephadm-airgap` instead. Copy the release key,
``https://download.ceph.com/keys/release.asc``, as well, and install it on the
target host as in step 1 of `Debian and Ubuntu`_ or `RHEL`_; without it, the
package manager cannot verify the packages and shows security warnings.

- Debian and Ubuntu: to get every package that the host needs, mirror the
  repository; see :ref:`install-mirrors`. Single package files are in the
  ``pool`` directory of the repository, and each file name includes the
  Debian revision (``-1``), the codename, and the architecture. For example,
  this command downloads only the ``ceph`` metapackage, which depends on the
  daemon packages:

  .. prompt:: bash $

     wget -q https://download.ceph.com/debian-20.2.4/pool/main/c/ceph/ceph_20.2.4-1jammy_amd64.deb

  ``wget -q`` prints nothing, even when the download fails. ``ls`` then
  lists ``ceph_20.2.4-1jammy_amd64.deb``.

- EL: download the RPMs from the directory of your distribution and
  architecture and from the ``noarch`` directory beside it, for example:

  .. code-block:: none
     :substitutions:

     https://download.ceph.com/rpm-|stable-release|/el10/x86_64/
     https://download.ceph.com/rpm-|stable-release|/el10/noarch/

  The general form is
  ``https://download.ceph.com/rpm-{release-name}/{distro}/{arch}/``. Download
  the dependencies from EPEL and CRB as well (steps 2 to 4 of `RHEL`_).

Verification
============

- On Debian and Ubuntu, check where APT gets Ceph from:

  .. prompt:: bash $

     apt-cache policy ceph

  The ``Candidate`` line shows the Ceph version, for example
  ``20.2.4-1noble``, and the version table lists it from
  ``https://download.ceph.com``, or from a ``chacra.ceph.com`` URL for
  development packages.

- On EL, list the enabled repositories:

  .. prompt:: bash $

     dnf repolist

  The output lists the Ceph repositories, for example ``ceph`` and
  ``ceph-noarch``.

- On EL, if you downloaded RPMs by hand, check their signatures on a host
  that has the release key (step 1 of `RHEL`_):

  .. prompt:: bash $

     rpm -K *.rpm

  Each line ends with ``digests signatures OK``.

Troubleshooting
===============

- **"cephadm: command not found".** ``cephadm`` is not installed, or you
  downloaded the binary with ``curl``. Install it as described in
  :ref:`get-cephadm`, or run the binary as ``./cephadm`` from its directory.
- **"version must be in the form x.y.z (e.g., 15.2.0)".** Give ``--version``
  a full version, such as ``20.2.4``.
- **"failed to fetch repository metadata. please check the provided
  parameters are correct and try again" from cephadm.** On EL, cephadm found
  no repository for this release or version and your distribution. Check the
  name or the version, and browse ``https://download.ceph.com/rpm-{version}/``
  for the distributions that it is built for.
- **"Unable to find a match: epel-release" from cephadm.** On RHEL itself,
  ``cephadm add-repo`` writes ``ceph.repo`` and then fails to install the
  ``epel-release`` package, which RHEL does not ship. Install EPEL as in
  step 2 of `RHEL`_, then run ``cephadm add-repo`` again.
- **"Distro ... version ... not supported".** cephadm cannot add a repository
  on this distribution. Use the packages that the distribution ships, or
  follow the route for your distribution on this page.
- **"Ceph team does not build Fedora specific packages and therefore cannot
  add repos for this distro".** Fedora ships its own Ceph packages; install
  them with ``dnf`` without adding a repository.
- **"nothing provides" in the output of "cephadm install ceph-common" on
  EL.** The CRB repository is not enabled. Do step 2 of `Using Cephadm`_,
  then run the command again.
- **"NO_PUBKEY E84AC2C0460F3994" or "Missing key
  08B73419AC32B4E966C1A330E84AC2C0460F3994" from "apt-get update" or
  "cephadm add-repo".** APT has no copy of the release key that it can read.
  Do step 1 of `Debian and Ubuntu`_, then run the command again.
- **"does not have a Release file" from "apt-get update".** This Ceph release
  has no packages for your distribution release. The ``dists`` directory of
  the repository, such as
  ``https://download.ceph.com/debian-{release-name}/dists/``, lists the
  codenames that it has; see also :ref:`start-platforms`.
- **"apt-cache policy ceph" shows no version from download.ceph.com.** The
  file ``/etc/apt/sources.list.d/ceph.list`` is missing or empty. On Debian
  12, ``apt-add-repository`` writes an empty
  ``archive_uri-https_download_ceph_com_*.list`` file instead; delete that
  file and add the repository with step 2 of `Debian and Ubuntu`_.
- **A dnf or rpm warning that ends with "NOKEY".** The release key is not
  imported. Repeat step 1 of `RHEL`_.
- **"digests SIGNATURES NOT OK" from "rpm -K".** The release key is not
  imported on this host, or the file is damaged. Repeat step 1 of `RHEL`_,
  then download the file again if the check still fails.
- **"The requested URL returned error: 504" from curl.** Shaman has not built
  this repository yet, or has removed it. Try again later, or choose another
  branch or commit on `the shaman page`_.

Next Steps
==========

- :ref:`install_storage_cluster`: install the packages with APT or DNF.
- :ref:`cephadm_deploying_new_cluster`: deploy a cluster with cephadm.
- :ref:`manual-deployment`: deploy a cluster by hand on hosts that have the
  packages.

Additional Resources
====================

- :ref:`os-recommendations`
- `Releases`_
- :ref:`install-mirrors`
- :ref:`containers`

.. _the shaman page: https://shaman.ceph.com

.. Needs to be an external link because doc/releases/index.rst is not in
   stable branches and we want to always use the main branch version
.. _Releases: https://docs.ceph.com/en/latest/releases/
