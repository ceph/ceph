=====================
Erasure code profiles
=====================

Erasure code is defined by a **profile** and is used when creating an
erasure coded pool and the associated crush ruleset.

The **default** erasure code profile (which is created when the Ceph
cluster is initialized) provides the same level of redundancy as two
copies but requires 25% less disk space. It is described as a profile
with **k=2** and **m=1**, meaning the information is spread over three
OSD (k+m == 3) and one of them can be lost.

To improve redundancy without increasing raw storage requirements, a
new profile can be created. For instance, a profile with **k=10** and
**m=4** can sustain the loss of four (**m=4**) OSDs by distributing an
object on fourteen (k+m=14) OSDs. The object is first divided in
**10** chunks (if the object is 10MB, each chunk is 1MB) and **4**
coding chunks are computed, for recovery (each coding chunk has the
same size as the data chunk, i.e. 1MB). The raw space overhead is only
40% and the object will not be lost even if four OSDs break at the
same time.

.. _list of available plugins:

.. toctree::
	:maxdepth: 1

	erasure-code-jerasure
	erasure-code-isa
	erasure-code-lrc
	erasure-code-shec

osd erasure-code-profile set
============================

To create a new erasure code profile::

	ceph osd erasure-code-profile set {name} \
             [{directory=directory}] \
             [{plugin=plugin}] \
             [{key=value} ...] \
             [--force]

Where:

``{directory=directory}``

:Description: Set the **directory** name from which the erasure code
              plugin is loaded.

:Type: String
:Required: No.
:Default: /usr/lib/ceph/erasure-code

``{plugin=plugin}``

:Description: Use the erasure code **plugin** to compute coding chunks
              and recover missing chunks. See the `list of available
              plugins`_ for more information.

:Type: String
:Required: No. 
:Default: jerasure

``{key=value}``

:Description: The semantic of the remaining key/value pairs is defined
              by the erasure code plugin.

:Type: String
:Required: No. 

``--force``

:Description: Override an existing profile by the same name.

:Type: String
:Required: No. 

osd erasure-code-profile rm
============================

To remove an erasure code profile::

	ceph osd erasure-code-profile rm {name}

If the profile is referenced by a pool, the deletion will fail.

osd erasure-code-profile get
============================

To display an erasure code profile::

	ceph osd erasure-code-profile get {name}

osd erasure-code-profile ls
===========================

To list the names of all erasure code profiles::

	ceph osd erasure-code-profile ls

