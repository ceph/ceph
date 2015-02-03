===============
jerasure plugin
===============

Introduction
------------

The parameters interpreted by the jerasure plugin are:

::
 
  ceph osd erasure-code-profile set myprofile \
     directory=<dir>         \ # plugin directory absolute path
     plugin=jerasure         \ # plugin name (only jerasure)
     k=<k>                   \ # data chunks (default 2)
     m=<m>                   \ # coding chunks (default 2)
     technique=<technique>   \ # coding technique

The coding techniques can be chosen among *reed_sol_van*,
*reed_sol_r6_op*, *cauchy_orig*, *cauchy_good*, *liberation*,
*blaum_roth* and *liber8tion*.

The *src/erasure-code/jerasure* directory contains the
implementation. It is a wrapper around the code found at
`https://github.com/ceph/jerasure <https://github.com/ceph/jerasure>`_
and `https://github.com/ceph/gf-complete
<https://github.com/ceph/gf-complete>`_ , pinned to the latest stable
version in *.gitmodules*. These repositories are copies of the
upstream repositories `http://jerasure.org/jerasure/jerasure
<http://jerasure.org/jerasure/jerasure>`_ and
`http://jerasure.org/jerasure/gf-complete
<http://jerasure.org/jerasure/gf-complete>`_ . The difference
between the two, if any, should match pull requests against upstream.
