============================
Jerasure erasure code plugin
============================

The *jerasure* plugin is the most generic and flexible plugin, it is
also the default for Ceph erasure coded pools. 

The *jerasure* plugin encapsulates the `Jerasure
<https://bitbucket.org/jimplank/jerasure/>`_ library. It is
recommended to read the *jerasure* documentation to get a better
understanding of the parameters.

Create a jerasure profile
=========================

To create a new *jerasure* erasure code profile::

        ceph osd erasure-code-profile set {name} \
             plugin=jerasure \
             k={data-chunks} \
             m={coding-chunks} \
             technique={reed_sol_van|reed_sol_r6_op|cauchy_orig|cauchy_good|liberation|blaum_roth|liber8tion} \
             [ruleset-root={root}] \
             [ruleset-failure-domain={bucket-type}] \
             [directory={directory}] \
             [--force]

Where:

``k={data chunks}``

:Description: Each object is split in **data-chunks** parts,
              each stored on a different OSD.

:Type: Integer
:Required: Yes.
:Example: 4

``m={coding-chunks}``

:Description: Compute **coding chunks** for each object and store them
              on different OSDs. The number of coding chunks is also
              the number of OSDs that can be down without losing data.

:Type: Integer
:Required: Yes.
:Example: 2

``technique={reed_sol_van|reed_sol_r6_op|cauchy_orig|cauchy_good|liberation|blaum_roth|liber8tion}``

:Description: The more flexible technique is *reed_sol_van* : it is
              enough to set *k* and *m*. The *cauchy_good* technique
              can be faster but you need to chose the *packetsize*
              carefully. All of *reed_sol_r6_op*, *liberation*,
              *blaum_roth*, *liber8tion* are *RAID6* equivalents in
              the sense that they can only be configured with *m=2*. 

:Type: String
:Required: No.
:Default: reed_sol_van

``packetsize={bytes}``

:Description: The encoding will be done on packets of *bytes* size at
              a time. Chosing the right packet size is difficult. The
              *jerasure* documentation contains extensive information
              on this topic.

:Type: Integer
:Required: No.
:Default: 2048

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

