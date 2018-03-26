.. _intro_testers:

========================
Introduction for Testers
========================

This document is aimed at providing an introduction to running existing test suites.

We assume here that you have access to an operational test lab; if not, ask
your local admin for access!

If you're here to test upstream Ceph, start `here
<https://wiki.sepia.ceph.com/doku.php?id=gettingstarted>`__.


Terminology
===========

In the abstract, each set of tests is defined by a `suite`. All of our suites
live in the `ceph-qa-suite repository
<https://github.com/ceph/ceph-qa-suite/>`__, in the `suites` subdirectory. Each
subdirectory in `suites` is a suite; they may also have "sub-suites" which may
aid in scheduling, for example, tests for a specific feature.

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

    teuthology-suite -v -m mira --ceph-repo http://github.com/ceph/ceph.git -c master --suite-repo http://github.com/ceph/ceph.git -s smoke --dry-run

The `-m mira` specifies `mira` as the machine type. Machine types are dependent
on the specific lab in use. The `--ceph-repo http://github.com/ceph/ceph.git`
specifies from which git repository to pull `-c master`. Similarly,
`--suite-repo` is specifying where to find the QA branch. The default for
`--ceph-repo` and `--suite-repo` is `http://github.com/ceph/ceph-ci.git` which
is usually what you will want. For `master`, you must always use
`http://github.com/ceph/ceph.git` as it does not exist on the ceph-ci
repository.

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

There may be times when, after scheduling a run containing a large number of
jobs, that you want to reschedule only those jobs which have failed or died for
some other reason. For that use-case, `teuthology-suite` has a `--rerun`/`-r`
flag, and an optional `--rerun-statuses`/`-R` flag. An example of its usage
is::

    teuthology-suite -m smithi -c wip-pdonnell-testing-20170718 --rerun pdonnell-2017-07-19_19:04:52-multimds-wip-pdonnell-testing-20170718-testing-basic-smithi -R dead --dry-run
