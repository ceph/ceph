:orphan:

==========================================================
 ceph-diff-sorted -- compare two sorted files line by line
==========================================================

.. program:: ceph-diff-sorted

Synopsis
========

| **ceph-diff-sorted** *file1* *file2*

Description
===========

:program:`ceph-diff-sorted` is a simplifed *diff* utility optimized
for comparing two files with lines that are lexically sorted.

The output is simplified in comparison to that of the standard `diff`
tool available in POSIX systems. Angle brackets ('<' and '>') are used
to show lines that appear in one file but not the other. The output is
not compatible with the `patch` tool.

This tool was created in order to perform diffs of large files (e.g.,
containing billions of lines) that the standard `diff` tool cannot
handle efficiently. Knowing that the lines are sorted allows this to
be done efficiently with minimal memory overhead.

The sorting of each file needs to be done lexcially. Most POSIX
systems use the *LANG* environment variable to determine the `sort`
tool's sorting order. To sort lexically we would need something such
as:

        $ LANG=C sort some-file.txt >some-file-sorted.txt

Examples
========

Compare two files::

        $ ceph-diff-sorted fileA.txt fileB.txt

Exit Status
===========

When complete, the exit status will be set to one of the following:

0
  files same
1
  files different
2
  usage problem (e.g., wrong number of command-line arguments)
3
  problem opening input file
4
  bad file content (e.g., unsorted order or empty lines)


Availability
============

:program:`ceph-diff-sorted` is part of Ceph, a massively scalable,
open-source, distributed storage system.  Please refer to the Ceph
documentation at http://ceph.com/docs for more information.

See also
========

:doc:`rgw-orphan-list <rgw-orphan-list>`\(8)
