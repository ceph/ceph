# radosgw-admin command-line tests

Three suites that run `radosgw-admin` and check the exit code of each command.
They cover how the tool reads its command line: which flags each command takes,
where those flags can appear, and which inputs it rejects.

| file | what it covers |
| --- | --- |
| `test-bucket-exit-codes.sh` | the `bucket` commands |
| `test-script-exit-codes.sh` | the `script` and `script-package` commands |
| `test-globals.sh` | the ceph global flags, which are stripped before radosgw-admin reads its own |

## Running them

Run from the build directory:

```
cd /path/to/ceph/build
bash ../src/test/rgw/radosgw-admin/test-bucket-exit-codes.sh
```

Each suite prints one line per test and a total at the end. It exits 0 when
nothing failed, 1 otherwise.

## What you need

Most rows need no cluster. They pass `--no-mon-config`, so the command fails on
its arguments before it tries to connect.

The rest need a running cluster. When no radosgw process is running they are
skipped, not failed. To run them, start a vstart cluster from the build
directory:

```
MON=1 OSD=1 MDS=0 MGR=1 RGW=1 ../src/vstart.sh -n -d
```

The bucket suite also has a group of rows that need a real bucket. It creates
one over S3 with the `aws` CLI, and removes the test user afterwards. Without
`aws` installed those rows are skipped.

## Using a different build or cluster

The suites run `./bin/radosgw-admin` and read `./ceph.conf`, so from the build
directory there is nothing to set.

Set `RGW_ADMIN` to test a different build of the tool, or `CEPH_CONF` to use a
different cluster. Put either one on the line that runs the suite. For example:

```
RGW_ADMIN=/other/ceph/build/bin/radosgw-admin bash ../src/test/rgw/radosgw-admin/test-globals.sh
```

The bucket suite reaches radosgw at `http://localhost:8000`, the vstart port.
Set `RGW_ENDPOINT` when it listens somewhere else. It creates its bucket in
the `default` zonegroup; set `AWS_DEFAULT_REGION` to the api name of the
zonegroup when the cluster uses another one.

## Running them in teuthology

The `rgw/tools` suite runs all three through the
`qa/workunits/rgw/run-radosgw-admin-exit-codes.sh` workunit. It points the
suites at the installed `radosgw-admin`, the cluster in `/etc/ceph/ceph.conf`
and the radosgw the `rgw` task started. The suite's `install` task adds the
`aws` CLI package so the bucket rows are not skipped, and its `workunit` task
passes the zonegroup the `rgw` task created as `AWS_DEFAULT_REGION`.
