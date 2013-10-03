===============
jerasure plugin
===============

Introduction
------------

The parameters interpreted by the jerasure plugin are:

::
 
  ceph osd pool create <pool> \
     erasure-code-directory=<dir>         \ # plugin directory absolute path
     erasure-code-plugin=jerasure         \ # plugin name (only jerasure)
     erasure-code-k=<k>                   \ # data chunks (default 2)
     erasure-code-m=<m>                   \ # coding chunks (default 2)
     erasure-code-technique=<technique>   \ # coding technique

The coding techniques can be chosen among *reed_sol_van*,
*reed_sol_r6_op*, *cauchy_orig*, *cauchy_good*, *liberation*,
*blaum_roth* and *liber8tion*.

