=======================
ISA erasure code plugin
=======================

The *isa* plugin encapsulates the `ISA
<https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version/>`_
library. It only runs on Intel processors.

Create an isa profile
=====================

To create a new *isa* erasure code profile::

        ceph osd erasure-code-profile set {name} \
             plugin=isa \
             technique={reed_sol_van|cauchy} \
             [k={data-chunks}] \
             [m={coding-chunks}] \
             [ruleset-root={root}] \
             [ruleset-failure-domain={bucket-type}] \
             [directory={directory}] \
             [--force]

Where:

``k={data chunks}``

:Description: Each object is split in **data-chunks** parts,
              each stored on a different OSD.

:Type: Integer
:Required: No.
:Default: 7

``m={coding-chunks}``

:Description: Compute **coding chunks** for each object and store them
              on different OSDs. The number of coding chunks is also
              the number of OSDs that can be down without losing data.

:Type: Integer
:Required: No.
:Default: 3

``technique={reed_sol_van|cauchy}``

:Description: The ISA plugin comes in two `Reed Solomon
              <https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction>`_
              forms. If *reed_sol_van* is set, it is `Vandermonde
              <https://en.wikipedia.org/wiki/Vandermonde_matrix>`_, if
              *cauchy* is set, it is `Cauchy
              <https://en.wikipedia.org/wiki/Cauchy_matrix>`_.

:Type: String
:Required: No.
:Default: reed_sol_van

``ruleset-root={root}``

:Description: The name of the crush bucket used for the first step of
              the ruleset. For intance **step take default**.

:Type: String
:Required: No.
:Default: default

``ruleset-failure-domain={bucket-type}``

:Description: Ensure that no two chunks are in a bucket with the same
              failure domain. For instance, if the failure domain is
              **host** no two chunks will be stored on the same
              host. It is used to create a ruleset step such as **step
              chooseleaf host**.

:Type: String
:Required: No.
:Default: host

``directory={directory}``

:Description: Set the **directory** name from which the erasure code
              plugin is loaded.

:Type: String
:Required: No.
:Default: /usr/lib/ceph/erasure-code

``--force``

:Description: Override an existing profile by the same name.

:Type: String
:Required: No.

