============================
Jerasure erasure code plugin
============================

The *jerasure* plugin is the most generic and flexible plugin, it is
also the default for Ceph erasure coded pools. 

The *jerasure* plugin encapsulates the `Jerasure
<https://github.com/ceph/jerasure>`_ library. It is
recommended to read the ``jerasure`` documentation to
understand the parameters. Note that the ``jerasure.org``
web site as of 2023 may no longer be connected to the original
project or legitimate.

Create a jerasure profile
=========================

To create a new *jerasure* erasure code profile:
 
.. prompt:: bash $

   ceph osd erasure-code-profile set {name} \
     plugin=jerasure \
     k={data-chunks} \
     m={coding-chunks} \
     technique={reed_sol_van|reed_sol_r6_op|cauchy_orig|cauchy_good|liberation|blaum_roth|liber8tion} \
     [crush-root={root}] \
     [crush-failure-domain={bucket-type}] \
     [crush-device-class={device-class}] \
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

              .. note:: When using ``blaum_roth`` coding, the default 
                 word size of ``w=7`` is suboptimal because ``blaum_roth`` 
                 works best when ``w+1`` is prime. When creating a new 
                 erasure-code profile with ``technique=blaum_roth``, 
                 set ``w`` to a number that is one integer less than a prime 
                 number (for example, ``6``). See `Loic Dachary's 
                 commit f51d21b to ceph/ceph <https://github.com/ceph/ceph/commit/f51d21b53d26d4f27c950cb1ba3f989e713ab325>`_ for information about
                 why this default cannot be changed easily in the
                 source code, and see `the second bullet point on
                 page 29 of Plank and Greenan's "Jerasure: A Library
                 in C Facilitating Erasure Coding for Storage
                 Applications" <https://github.com/ceph/jerasure/blob/master/Manual.pdf>`_ for an unequivocal statement of the restriction that applies 
                 to ``w`` when using Blaum-Roth coding.
                 (Information about the proper value of ``w`` when
                 using ``blaum_roth`` coding was provided to the
                 Ceph upstream in September of 2024 by Benjamin
                 Mare.)

:Type: String
:Required: No.
:Default: reed_sol_van

``packetsize={bytes}``

:Description: The encoding will be done on packets of *bytes* size at
              a time. Choosing the right packet size is difficult. The
              *jerasure* documentation contains extensive information
              on this topic.

:Type: Integer
:Required: No.
:Default: 2048

``crush-root={root}``

:Description: The name of the crush bucket used for the first step of
              the CRUSH rule. For instance **step take default**.

:Type: String
:Required: No.
:Default: default

``crush-failure-domain={bucket-type}``

:Description: Ensure that no two chunks are in a bucket with the same
              failure domain. For instance, if the failure domain is
              **host** no two chunks will be stored on the same
              host. It is used to create a CRUSH rule step such as **step
              chooseleaf host**.

:Type: String
:Required: No.
:Default: host

``crush-device-class={device-class}``

:Description: Restrict placement to devices of a specific class (e.g.,
              ``ssd`` or ``hdd``), using the crush device class names
              in the CRUSH map.

:Type: String
:Required: No.

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

