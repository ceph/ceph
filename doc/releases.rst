================
Release timeline
================

There are approximately four stable releases a year. Every other
release is a LTS (Long Term Support). A LTS release is supported for 18
months. A stable release that is not LTS is supported until the next
stable release is published. A development / testing release is not
supported.

* Long Term Support release : 18 months
* Stable release : until the next stable release is published
* Development / testing release : not supported

Supporting a release means:

* `Integration and upgrade tests
  <https://github.com/ceph/ceph-qa-suite>`_ are run on a regular basis
  and `their results <http://pulpito.ceph.com/>`_ analyzed by Ceph
  developers.
* `Issues <http://tracker.ceph.com/projects/ceph/issues?query_id=27>`_
  fixed in the development branch is scheduled to be backported to the
  release.
* When an issue found in the release is `reported
  <http://tracker.ceph.com/projects/ceph/issues/new>`_ it will be
  handled by Ceph developers.
* The `stable releases and backport team <http://tracker.ceph.com/projects/ceph-releases>`_
  publishes ``point releases`` including fixes that have been backported to the release.

+----------------+-----------+-----------+-----------+-----------+-----------+
|                |Development|`Dumpling`_|`Emperor`_ |`Firefly`_ |`Giant`_   |
|                |Testing    |LTS        |Stable     |LTS        |Stable     |
+----------------+-----------+-----------+-----------+-----------+-----------+
| March     2015 |           |           |           |`0.80.9`_  |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| February  2015 |`0.93`_    |           |           |           |`0.87.1`_  |
|                +-----------+-----------+-----------+-----------+-----------+
|                |`0.92`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| January   2015 |`0.91`_    |           |           |`0.80.8`_  |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| December  2014 |`0.90`_    |           |           |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |`0.89`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| November  2014 |`0.88`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| October   2014 |`0.86`_    |           |           |`0.80.7`_  |`0.87`_    |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |           |           |`0.80.6`_  |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| September 2014 |`0.85`_    |`0.67.11`_ |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| August    2014 |`0.84`_    |`0.67.10`_ |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| July      2014 |`0.83`_    |           |           |`0.80.5`_  |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |           |           |`0.80.4`_  |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |           |           |`0.80.3`_  |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |           |           |`0.80.2`_  |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| June      2014 |`0.82`_    |           |           |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |`0.81`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| May       2014 |           |`0.67.9`_  |           |`0.80.1`_  |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |`0.67.8`_  |           |`0.80`_    |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| April     2014 |`0.79`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| March     2014 |`0.78`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| February  2014 |`0.77`_    |`0.67.7`_  |           |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |`0.67.6`_  |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| January   2014 |`0.76`_    |           |           |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |`0.75`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| December  2013 |`0.74`_    |`0.67.5`_  |`0.72.2`_  |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |`0.73`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| November  2013 |           |           |`0.72.1`_  |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |           |`0.72`_    |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| October   2013 |`0.71`_    |`0.67.4`_  |           |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |`0.70`_    |           |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| September 2013 |`0.69`_    |           |           |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |`0.68`_    |`0.67.3`_  |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+
| August    2013 |           |`0.67.2`_  |           |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |`0.67.1`_  |           |           |           |
|                +-----------+-----------+-----------+-----------+-----------+
|                |           |`0.67`_    |           |           |           |
+----------------+-----------+-----------+-----------+-----------+-----------+

.. _0.93: ../release-notes#v0-93
.. _0.92: ../release-notes#v0-92
.. _0.91: ../release-notes#v0-91
.. _0.90: ../release-notes#v0-90
.. _0.89: ../release-notes#v0-89
.. _0.88: ../release-notes#v0-88

.. _0.87.1: ../release-notes#v0-87-1-giant
.. _0.87: ../release-notes#v0-87-giant
.. _Giant: release-notes#v0-87-giant

.. _0.86: ../release-notes#v0-86
.. _0.85: ../release-notes#v0-85
.. _0.84: ../release-notes#v0-84
.. _0.83: ../release-notes#v0-83
.. _0.82: ../release-notes#v0-82
.. _0.81: ../release-notes#v0-81

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
