The `upgrade` suite is used to verify that upgrades can complete successfully
without disrupting any ongoing workloads.

The diagram below represents the upgrade test directory from the squid release
branch. Each release branch upgrade directory includes X-2 upgrade testing. That
means, we can test the upgrade from 2 previous releases to the current one.

```
upgrade
├── quincy-x
│   ├── filestore-remove-check
│   ├── parallel
│   │   ├── 0-start.yaml
│   │   ├── 1-tasks.yaml
│   │   ├── upgrade-sequence.yaml
│   │   └── workload
│   └── stress-split
|
├── reef-x
│   ├── parallel
│   │   └── workload
│   └── stress-split
|
├── squid-p2p
│   ├── squid-p2p-parallel
│   └── squid-p2p-stress-split
|
└── telemetry-upgrade
    ├── quincy-x
    └── reef-x

```

Based on the above example where X=squid, it is possible to test the upgrades
from Quincy (X-2) or from Reef (X-1) to Squid (X).

- The `upgrade/quincy-x/parallel` and `upgrade/reef-x/parallel` sub-suite
  installs a Quincy or Reef cluster, then upgrades the cluster to Squid (X). In
  parallel, some workloads are run against the cluster, including telemetry
  workunits.
- The `upgrade/telemetry-upgrade` sub-suite is identical to
  `upgrade/quincy-x/parallel` and `upgrade/reef-x/parallel` sub-suites above,
  but these only test the telemetry workunits and do not run any other
  workloads.

A simple upgrade test contains these steps in order, divided into separate yaml
files:
```
├── 0-start.yaml
├── 1-tasks.yaml
├── upgrade-sequence.yaml
└── workload
```

- `0-start.yaml`: This file contains the information about the ceph cluster
  configuration (number of osds, monitors etc) for the test
- `1-tasks.yaml`: This file contains the information of the tasks we want to run
  on the cluster. It is here that we install an older release, then begin
  running the given `workload` and `upgrade-sequence` in parallel.
- `upgrade-sequence.yaml`: This file contains the steps for upgrading the
  cluster to the designated release
- `workloads`: A set of yaml file with workloads we want to run while the
  upgrade is in progress

```
- print: "**** done start parallel"
- parallel:
    - workload
    - upgrade-sequence
- print: "**** done end parallel"
```

The `workload` directory contains the workload yaml files just like any other
suite and the `upgrade-sequence` is responsible for initiating the upgrade and
waiting for it to complete.

```
# renamed tasks: to upgrade-sequence:
upgrade-sequence:
   sequential:
   - print: "**** done start upgrade, wait"
   ...
       mon.a:
         - ceph config set global log_to_journald false --force
         - ceph orch upgrade start --image quay.ceph.io/ceph-ci/ceph:$sha1
         - while ceph orch upgrade status | jq '.in_progress' | grep true && ! ceph orch upgrade status | jq '.message' | grep Error ; do ceph orch ps ; ceph versions ; ceph orch upgrade status ; sleep 30 ; done
   ...
   - print: "**** done end upgrade, wait..."
```

## Telemetry Upgrade tests

The telemetry upgrade sub-suite verifies that telemetry is emitting the correct
collections after the upgrade. This integration test coverage is done via
workunits. Workunits are basically bash scripts that run commands against a Ceph
Cluster.

In the same manner as the `upgrade/parallel` tests, each release branch
references the `qa/workunits` directory, which includes telemetry bash scripts
for X-2 releases. That means we can test telemetry before and after the upgrade
from previous two releases to the current one.

For instance, the relevant telemetry workunits for the `squid` release are:
```
qa/workunits
├── test_telemetry_quincy.sh
├── test_telemetry_quincy_x.sh
├── test_telemetry_reef.sh
└── test_telemetry_reef_x.sh
```

- `test_telemetry_quincy.sh`, tests the presence of telemetry collection on a
  Quincy cluster before the upgrade.
- `test_telemetry_quincy_x.sh`, tests the presence of new telemetry collection
  on the X-version cluster after it has been upgraded from Quincy.
- `test_telemetry_reef.sh`, tests the presence of telemetry collection on a Reef
  cluster before the upgrade
- `test_telemetry_reef_x.sh`, tests the presence of new telemetry collection on
  the X-version cluster after it has been upgraded from Reef.

A sample telemetry upgrade test file contains the following test:
```
...
# Assert that new collections are available
COLLECTIONS=$(ceph telemetry collection ls)
NEW_COLLECTIONS=("perf_perf" "basic_mds_metadata" "basic_pool_usage"
                 "basic_rook_v01" "perf_memory_metrics" "basic_pool_options_bluestore")
for col in ${NEW_COLLECTIONS[@]}; do
    if ! [[ $COLLECTIONS == *$col* ]];
    then
        echo "COLLECTIONS does not contain" "'"$col"'."
	exit 1
    fi
done
...
```

These workunits are used in the `upgrade` suite, specifically in:
- [upgrade/quincy-x/parallel](https://github.com/ceph/ceph/blob/squid/qa/suites/upgrade/quincy-x/parallel/1-tasks.yaml)
- [upgrade/reef-x/parallel](https://github.com/ceph/ceph/blob/squid/qa/suites/upgrade/reef-x/parallel/1-tasks.yaml)
- [upgrade/telemetry-upgrade](https://github.com/ceph/ceph/tree/squid/qa/suites/upgrade/telemetry-upgrade)

```

upgrade
├── reef-x
│   ├── parallel
│   │   └──  1-tasks.yaml
├── squid-x
│   ├── parallel
│   │   └── 1-tasks.yaml
└── telemetry-upgrade
    ├── quincy-x
    └── reef-x


```

The `upgrade/quincy-x/parallel` and `upgrade/reef-x/parallel` sub-suite installs
a Quincy or Reef cluster, then upgrades the cluster to Squid. In parallel, some
workloads are run against the cluster, including telemetry workunits. The
`1-tasks.yaml` file is the place where the workunits are run.

For instance, the `upgrade/quincy-x/parallel/1-tasks.yaml`  file from the
`squid` release branch looks like this:

```
...
- print: "**** done start telemetry quincy..."
- workunit:
    clients:
      client.0:
        - test_telemetry_quincy.sh
- print: "**** done end telemetry quincy..."

- print: "**** done start parallel"
- parallel:
    - workload
    - upgrade-sequence
- print: "**** done end parallel"

- print: "**** done start telemetry x..."
- workunit:
    clients:
      client.0:
        - test_telemetry_quincy_x.sh
- print: "**** done end telemetry x..."
```

The `test_telemetry_quincy.sh` workunit is run on the Quincy cluster before the
upgrade and `test_telemetry_quincy_x.sh` is run on the X-version cluster (in
this example `squid`) after the upgrade.

The `upgrade/telemetry-upgrade` sub-suite is identical to the
`upgrade/quincy-x/parallel` and `upgrade/reef-x/parallel` suites as above, but
these tests ONLY test the telemetry workunits and do not run with any other
workloads.

So `upgrade/telemetry-upgrade` is nice to schedule when you just want to verify
that the telemetry workunits are working as expected (they complete much
faster). The `upgrade/[quincy|reef]-x/parallel` suites are nice to schedule if
you want to verify that the workunits are working fine with all the other
workloads also running.

### What tests to update when a telemetry collection is added/removed

- If the collection is added only to the `main` branch or the current release
	(`tentacle`), - only update the `test_telemetry_{X-2}_x.sh` and
	`test_telemetry_{X-1}_x.sh`,  (where *`x-2` is Reef and `x-1` is Squid for
	`tentacle` release branch*)
- If the collection are backported to the `X-2` releases then update the -
	`test_telemetry_{X-2}.sh` and `test_telemetry_{X-1}.sh` (where *`x-2` is Reef
	and `x-1` is Squid for `tentacle` release branch*) files to reflect the
	collection changes there
