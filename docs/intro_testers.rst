.. _intro_testers:

========================
Introduction for Testers
========================

This document is aimed at providing an introduction to running existing test suites.

We assume here that you have access to an operational test lab; if not, ask
your local admin for access!

If you're here to test upstream Ceph, start `here
<http://ceph.github.io/sepia/adding_users/>`__.


Terminology
===========

In the abstract, each set of tests is defined by a `suite`. All of our suites
live in the `ceph-qa-suite repository
<https://github.com/ceph/ceph-qa-suite/>`__, in the `suites` subdirectory.

In concrete terms, a `run` is what is created by assembling the contents of a
`suite` into a number of `jobs`. A `job` is created by assembling a number of
`fragments` (also known as `facets`) together. Each `fragment` is in `YAML
<http://yaml.org/>`__ format.

Each `job` definition contains a list of `tasks` to execute, along with
`roles`. `Roles` tell `teuthology` how many nodes to use for each `job` along
with what functions each node will perform.

To go into more depth regarding suite design, see `ceph-qa-suite`'s `README`.

One example of this is the `smoke
<https://github.com/ceph/ceph-qa-suite/tree/master/suites/smoke>`__ suite.


Scheduling
==========
Most testing happens by scheduling `runs`. The way we do that is using the
`teuthology-suite` command.

To get a preview of what `teuthology-suite` might do, try::

    teuthology-suite -v -s smoke -c master -m mira --dry-run

Assuming a build is available, that should pretend to schedule several jobs. If
it complains about missing packages, try swapping `master` with `jewel` or one
of the other Ceph stable branches.

To see even more detail, swap `-v` with `-vv`. It will print out each job
definition in full. To limit the number of jobs scheduled, you may want to use
the `--limit`, `--filter`, or `--filter-out` flags.

To actually schedule, drop `--dry-run` and optionally use the `--email` flag to
get an email when the test run completes.

`teuthology-suite` also prints out a link to the run in `pulpito
<https://github.com/ceph/pulpito/>`__ that will display the current status of
each job. The Sepia lab's pulpito instance is `here
<http://pulpito.ceph.com/>`__.
