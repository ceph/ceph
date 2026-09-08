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

## Known failures

The bucket suite has 42 rows that fail on purpose.

They cover errors where radosgw-admin returns the error code still negative.
The shell keeps only the low byte, so `-EINVAL` (-22) shows as 234 and
`-ENOENT` (-2) shows as 254. Other commands return those same two errors
positive, and exit 22 or 2.

Those rows expect 22 and 2. They will pass once the tool is fixed.
