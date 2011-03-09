# TODO conflict with -c, it's not --create-keyring; fix manpage
  $ cauthtool -c foo
  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ [0-9a-f]{8,} common_init: unable to open config file. (re)
  [1]

# demonstrate that manpage examples fail without config
# TODO fix the manpage
  $ cauthtool --create-keyring -n foo --gen-key keyring
  creating keyring

# work around the above
  $ touch ceph.conf

To create a new keyring containing a key for client.foo:

  $ cauthtool -C -n foo --gen-key keyring.bin
  creating keyring.bin

  $ cauthtool --create-keyring -t client -n foo --gen-key keyring.bin
  creating keyring.bin

To associate some capabilities with the key (namely, the ability to mount a Ceph filesystem):

  $ cauthtool -t client -n foo --cap mds 'allow' --cap osd 'allow rw pool=data' --cap mon 'allow r' keyring.bin

To display the contents of the keyring:

  $ cauthtool -l keyring.bin
  [client.foo]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid = [0-9]{20} \(esc\) (re)
  \tcaps mds = "allow" (esc)
  \tcaps mon = "allow r" (esc)
  \tcaps osd = "allow rw pool=data" (esc)
