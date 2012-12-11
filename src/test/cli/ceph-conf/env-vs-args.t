# we can use CEPH_CONF to override the normal configuration file location.
  $ env CEPH_CONF=from-env ceph-conf -s foo bar
  .* \-1 did not load config file, using default settings. (re)
  [1]

# command-line arguments should override environment
  $ env -u CEPH_CONF ceph-conf -c from-args
  global_init: unable to open config file from search list from-args
  [1]

