.. _ceph-releases-general:

=======================
Ceph Releases (general)
=======================

.. toctree::
    :maxdepth: 1

Understanding the release cycle
-------------------------------

Starting with the Nautilus release (14.2.0), there is a new stable release cycle
every year, targeting the month of March. Each stable release series will receive a name
(e.g., 'Mimic') and a major release number (e.g., 13 for Mimic because 'M' is
the 13th letter of the alphabet).

Releases are named after a species of cephalopod (usually the common
name, since the latin names are harder to remember or pronounce).

Version numbers have three components, *x.y.z*.  *x* identifies the release
cycle (e.g., 13 for Mimic).  *y* identifies the release type:

* x.0.z - development versions
* x.1.z - release candidates (for test clusters, brave users)
* x.2.z - stable/bugfix releases (for users)

Release candidates (x.1.z)
^^^^^^^^^^^^^^^^^^^^^^^^^^

There is a feature freeze roughly two months prior to the planned
initial stable release, after which focus shifts to stabilization and
bug fixes only.

* Release candidate release every 1-2 weeks
* Intended for final testing and validation of the upcoming stable release

Stable releases (x.2.z)
^^^^^^^^^^^^^^^^^^^^^^^

Once the initial stable release is made (x.2.0), there are
semi-regular bug-fix point releases with bug fixes and (occasionally)
feature backports.  Bug fixes are accumulated and included in
the next point release.

* Stable point release every 4 to 6 weeks
* Intended for production deployments
* Bug fix backports for 2 full release cycles (2 years).
* Online, rolling upgrade support and testing from the last two (2)
  stable release(s) (starting from Luminous).
* Online, rolling upgrade support and testing from prior stable point
  releases

For each stable release:

* `Integration and upgrade tests
  <https://github.com/ceph/ceph/tree/master/qa/suites/>`_ are run on a regular basis
  and `their results <http://pulpito.ceph.com/>`_ analyzed by Ceph
  developers.
* `Issues <http://tracker.ceph.com/projects/ceph/issues?query_id=27>`_
  fixed in the development branch (master) are scheduled to be backported.
* When an issue found in the stable release is `reported
  <http://tracker.ceph.com/projects/ceph/issues/new>`_, it is
  triaged by Ceph developers.
* The `stable releases and backport team <http://tracker.ceph.com/projects/ceph-releases/wiki>`_
  publishes ``point releases`` including fixes that have been backported to the stable release.

Lifetime of stable releases
---------------------------

The lifetime of a stable release series is calculated to be approximately 24
months (i.e., two 12 month release cycles) after the month of the first release.
For example, Mimic (13.2.z) will reach end of life (EOL) shortly after Octopus
(15.2.0) is released. The lifetime of a release may vary because it depends on
how quickly the stable releases are published.

Detailed information on all releases, past and present, can be found at :ref:`ceph-releases-index`

