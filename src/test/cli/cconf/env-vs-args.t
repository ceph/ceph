# we can use CEPH_CONF to override the normal configuration file location.
  $ env CEPH_CONF=from-env cconf -s foo bar
  common_init: unable to open config file. (re)
  [1]

# command-line arguments should override environment
  $ env -u CEPH_CONF cconf -c from-args
  common_init: unable to open config file. (re)
  [1]

