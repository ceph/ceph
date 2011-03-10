# we can use CEPH_CONF to override the normal configuration file location.
  $ env CEPH_CONF=from-env cconf -s foo bar
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{8,} common_init: unable to open config file. (re)
  [1]

# command-line arguments should override environment
  $ env -u CEPH_CONF cconf -c from-args
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{8,} common_init: unable to open config file. (re)
  [1]

