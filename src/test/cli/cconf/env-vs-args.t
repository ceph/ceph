# we can use CEPH_CONF to decide what config to read; this doesn't
# output the same error as below because with just the environment,
# the config is not considered explicitly specified
  $ env CEPH_CONF=from-env cconf -s foo bar
  [2]

# command-line arguments should override environment
  $ env -u CEPH_CONF cconf -c from-args
  error reading config file(s) from-args
  [1]

