# rgw workunits

Scripts that teuthology's `workunit` task runs (see `qa/suites/rgw/`). They can
also be run by hand, e.g. against a vstart cluster, from the build directory:

    PATH=$PWD/bin:$PATH python3 ../qa/workunits/rgw/test_rgw_bucket_check.py

## Choosing the radosgw

The Python scripts that connect through `common.py` try localhost on port 80,
then 8000, then 443 (https). To use a radosgw elsewhere, set:

- `AWS_ENDPOINT_URL=http://host:port`: use exactly this endpoint;
- `RGW_HOST=host`: try `host` instead of localhost, on the same ports.

`AWS_ENDPOINT_URL` wins if both are set. Set them on the command line, or in a
teuthology suite with the workunit task's `env:`:

    - workunit:
        clients:
          client.0: [rgw/test_rgw_bucket_check.py]
        env:
          AWS_ENDPOINT_URL: http://rgw.example:8080

## Writing a new one

Don't hard-code localhost or a port:

- Python: connect with `boto_connect()` from `common.py`, or build on
  `rgw_endpoints()` if you need a client or the endpoint itself;
- shell: honour `$AWS_ENDPOINT_URL`, as `test_account_migration.sh` does (the
  `aws` CLI reads it too).
