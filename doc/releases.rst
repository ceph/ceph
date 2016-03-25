=============
Ceph Releases
=============

Timeline
--------

+----------------------------+-----------+-----------+-----------+-----------+-----------+--------------+
|                            |`Dumpling`_|`Emperor`_ |`Firefly`_ |`Giant`_   |`Hammer`_  |`Infernalis`_ |
|                            |LTS        |Stable     |LTS        |Stable     |LTS        |Stable        |
+----------------------------+-----------+-----------+-----------+-----------+-----------+--------------+
|     First release          | August    | November  | May       | October   | April     | November     |
|                            | 2013      | 2013      | 2014      | 2014      | 2015      | 2015         |
+----------------------------+-----------+-----------+-----------+-----------+-----------+--------------+
|  Estimated retirement      | March     |           | January   |           | November  | June 2016    |
|                            | 2015      |           | 2016      |           | 2016      |              |
+----------------------------+-----------+-----------+-----------+-----------+-----------+--------------+
|    Actual retirement       | May       | May       |           | April     |           |              |
|                            | 2015      | 2014      |           | 2015      |           |              |
+----------------------------+-----------+-----------+-----------+-----------+-----------+--------------+

+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |Development|`Dumpling`_|`Emperor`_ |`Firefly`_ |`Giant`_   |`Hammer`_  |`Infernalis`_ |
|                |Testing    |LTS        |Stable     |LTS        |Stable     |LTS        |Stable        |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| March     2016 |`10.1.0`_  |           |           |           |           |           |              |
|                |`10.0.4`_  |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| February  2016 |`10.0.3`_  |           |           |           |           |`0.94.6`_  |`9.2.1`_      |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| January   2016 |`10.0.2`_  |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| December  2015 |`10.0.1`_  |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| November  2015 |`10.0.0`_  |           |           |`0.80.11`_ |           |           |`9.2.0`_      |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| October   2015 |`9.1.0`_   |           |           |           |           |`0.94.5`_  |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |           |           |           |           |`0.94.4`_  |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| August    2015 |`9.0.3`_   |           |           |           |           |`0.94.3`_  |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| July      2015 |`9.0.2`_   |           |           |`0.80.10`_ |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| June      2015 |`9.0.1`_   |           |           |           |           |`0.94.2`_  |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| May       2015 |`9.0.0`_   |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| April     2015 |           |           |           |           |`0.87.2`_  |`0.94.1`_  |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |           |           |           |           |`0.94`_    |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| March     2015 |           |           |           |`0.80.9`_  |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| February  2015 |`0.93`_    |           |           |           |`0.87.1`_  |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |`0.92`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| January   2015 |`0.91`_    |           |           |`0.80.8`_  |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| December  2014 |`0.90`_    |           |           |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |`0.89`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| November  2014 |`0.88`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| October   2014 |`0.86`_    |           |           |`0.80.7`_  |`0.87`_    |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |           |           |`0.80.6`_  |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| September 2014 |`0.85`_    |`0.67.11`_ |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| August    2014 |`0.84`_    |`0.67.10`_ |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| July      2014 |`0.83`_    |           |           |`0.80.5`_  |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |           |           |`0.80.4`_  |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |           |           |`0.80.3`_  |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |           |           |`0.80.2`_  |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| June      2014 |`0.82`_    |           |           |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |`0.81`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| May       2014 |           |`0.67.9`_  |           |`0.80.1`_  |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |`0.67.8`_  |           |`0.80`_    |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| April     2014 |`0.79`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| March     2014 |`0.78`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| February  2014 |`0.77`_    |`0.67.7`_  |           |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |`0.67.6`_  |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| January   2014 |`0.76`_    |           |           |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |`0.75`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| December  2013 |`0.74`_    |`0.67.5`_  |`0.72.2`_  |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |`0.73`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| November  2013 |           |           |`0.72.1`_  |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |           |`0.72`_    |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| October   2013 |`0.71`_    |`0.67.4`_  |           |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |`0.70`_    |           |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| September 2013 |`0.69`_    |           |           |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |`0.68`_    |`0.67.3`_  |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+
| August    2013 |           |`0.67.2`_  |           |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |`0.67.1`_  |           |           |           |           |              |
|                +-----------+-----------+-----------+-----------+-----------+-----------+--------------+
|                |           |`0.67`_    |           |           |           |           |              |
+----------------+-----------+-----------+-----------+-----------+-----------+-----------+--------------+

.. _10.0.4: ../release-notes#v10-0-4
.. _10.0.3: ../release-notes#v10-0-3
.. _10.0.2: ../release-notes#v10-0-2
.. _10.0.1: ../release-notes#v10-0-1
.. _10.0.0: ../release-notes#v10-0-0

.. _9.2.1: ../release-notes#v9-2-1-infernalis
.. _9.2.0: ../release-notes#v9-2-0-infernalis
.. _Infernalis: ../release-notes#v9.2.0-infernalis

.. _9.1.0: ../release-notes#v9-1-0
.. _9.0.3: ../release-notes#v9-0-3
.. _9.0.2: ../release-notes#v9-0-2
.. _9.0.1: ../release-notes#v9-0-1
.. _9.0.0: ../release-notes#v9-0-0

.. _0.94.6: ../release-notes#v0-94-6-hammer
.. _0.94.5: ../release-notes#v0-94-5-hammer
.. _0.94.4: ../release-notes#v0-94-4-hammer
.. _0.94.3: ../release-notes#v0-94-3-hammer
.. _0.94.2: ../release-notes#v0-94-2-hammer
.. _0.94.1: ../release-notes#v0-94-1-hammer
.. _0.94: ../release-notes#v0-94-hammer
.. _Hammer: ../release-notes#v0-94-hammer

.. _0.93: ../release-notes#v0-93
.. _0.92: ../release-notes#v0-92
.. _0.91: ../release-notes#v0-91
.. _0.90: ../release-notes#v0-90
.. _0.89: ../release-notes#v0-89
.. _0.88: ../release-notes#v0-88

.. _0.87.2: ../release-notes#v0-87-2-giant
.. _0.87.1: ../release-notes#v0-87-1-giant
.. _0.87: ../release-notes#v0-87-giant
.. _Giant: ../release-notes#v0-87-giant

.. _0.86: ../release-notes#v0-86
.. _0.85: ../release-notes#v0-85
.. _0.84: ../release-notes#v0-84
.. _0.83: ../release-notes#v0-83
.. _0.82: ../release-notes#v0-82
.. _0.81: ../release-notes#v0-81

.. _0.80.11: ../release-notes#v0-80-11-firefly
.. _0.80.10: ../release-notes#v0-80-10-firefly
.. _0.80.9: ../release-notes#v0-80-9-firefly
.. _0.80.8: ../release-notes#v0-80-8-firefly
.. _0.80.7: ../release-notes#v0-80-7-firefly
.. _0.80.6: ../release-notes#v0-80-6-firefly
.. _0.80.5: ../release-notes#v0-80-5-firefly
.. _0.80.4: ../release-notes#v0-80-4-firefly
.. _0.80.3: ../release-notes#v0-80-3-firefly
.. _0.80.2: ../release-notes#v0-80-2-firefly
.. _0.80.1: ../release-notes#v0-80-1-firefly
.. _0.80: ../release-notes#v0-80-firefly
.. _Firefly: ../release-notes#v0-80-firefly

.. _0.79: ../release-notes#v0-79
.. _0.78: ../release-notes#v0-78
.. _0.77: ../release-notes#v0-77
.. _0.76: ../release-notes#v0-76
.. _0.75: ../release-notes#v0-75
.. _0.74: ../release-notes#v0-74
.. _0.73: ../release-notes#v0-73

.. _0.72.2: ../release-notes#v0-72-2-emperor
.. _0.72.1: ../release-notes#v0-72-1-emperor
.. _0.72: ../release-notes#v0-72-emperor
.. _Emperor: ../release-notes#v0-72-emperor

.. _0.71: ../release-notes#v0-71
.. _0.70: ../release-notes#v0-70
.. _0.69: ../release-notes#v0-69
.. _0.68: ../release-notes#v0-68

.. _0.67.11: ../release-notes#v0-67-11-dumpling
.. _0.67.10: ../release-notes#v0-67-10-dumpling
.. _0.67.9: ../release-notes#v0-67-9-dumpling
.. _0.67.8: ../release-notes#v0-67-8-dumpling
.. _0.67.7: ../release-notes#v0-67-7-dumpling
.. _0.67.6: ../release-notes#v0-67-6-dumpling
.. _0.67.5: ../release-notes#v0-67-5-dumpling
.. _0.67.4: ../release-notes#v0-67-4-dumpling
.. _0.67.3: ../release-notes#v0-67-3-dumpling
.. _0.67.2: ../release-notes#v0-67-2-dumpling
.. _0.67.1: ../release-notes#v0-67-1-dumpling
.. _0.67: ../release-notes#v0-67-dumpling
.. _Dumpling:  ../release-notes#v0-67-dumpling

Understanding the release cycle
-------------------------------

The development release cycle is two to four weeks long.  Each cycle
freezes the master development branch and applies `integration and
upgrade tests <https://github.com/ceph/ceph-qa-suite>`_ for the
duration of one cycle before it is released and the next release's
code is frozen for testing.  Once released, there is no effort to
backport fixes; developer focus in on the next development release
which is usually only a few weeks away.

There are three to four stable releases a year.  Each stable release
will receive a name (e.g., 'Firefly') and bug fix backports at least
until the next stable release is out.

Every other stable releases is a LTS (Long Term Stable) and will
receive updates until two LTS are published. For instance Dumpling is
retired when Hammer is published, Firefly is retired when Jewel is
published etc. The rationale is that backports to a LTS (Dumpling for
instance) are expected to happen until the next LTS is published
(Firefly is the LTS following Dumpling), to fix bugs and possibly
backport important features. After the next LTS is published, there
backports are still expected to fix bugs with a focus on whatever can
prevent upgrades to the next LTS (in our example, fixes to Dumpling
were published after Firefly was released and until Hammer was
published, primarily to ensure Dumpling cluster can smoothly migrate
to Firefly).

* LTS : until the next two LTS are published
* Stable release : until the next stable release is published
* Development / testing release : no backports

For each stable release:

* `Integration and upgrade tests
  <https://github.com/ceph/ceph-qa-suite>`_ are run on a regular basis
  and `their results <http://pulpito.ceph.com/>`_ analyzed by Ceph
  developers.
* `Issues <http://tracker.ceph.com/projects/ceph/issues?query_id=27>`_
  fixed in the development branch is scheduled to be backported to the
  release.
* When an issue found in the release is `reported
  <http://tracker.ceph.com/projects/ceph/issues/new>`_ it will be
  triaged by Ceph developers.
* The `stable releases and backport team <http://tracker.ceph.com/projects/ceph-releases>`_
  publishes ``point releases`` including fixes that have been backported to the release.

In the timeline, the life time of a LTS is calculated to be
approximately 18 months after the month of the first release. For
instance, Dumpling is published August 2013 and 18 months starting
September 2013 is February 2015, therefore by March 2015 Dumpling
should be retired. The lifetime of a release may vary because it
depend on how quickly the stable releases are published. For instance
although Dumpling theoritical retirement was March 2015, it was
extended to May 2015.

Release numbers conventions
---------------------------

The first Ceph release back in Jan of 2008 was 0.1.  That made sense at
the time.  The versioning scheme did not change until April 2015,
when 0.94.1 (the first Hammer point release) was published.  To avoid reaching
0.99 (and 0.100 or 1.00?) we have a new strategy.

* x.0.z - development releases (for early testers and the brave at heart)
* x.1.z - release candidates (for test clusters, brave users)
* x.2.z - stable/bugfix releases (for users)

``x`` will start at 9 for Infernalis (``I`` is the 9th letter), making
our first development release of the 9th release cycle 9.0.0.
Subsequent development releases will be 9.0.1, 9.0.2, etc.

After a couple months we'll have a 9.1.0 (and maybe 9.1.1) release candidate.

A few weeks after that we'll have the Infernalis release 9.2.0, followed
by stable bug fix updates 9.2.1, 9.2.2, etc., and then begin work on the
Jewel (10.y.z) release.
