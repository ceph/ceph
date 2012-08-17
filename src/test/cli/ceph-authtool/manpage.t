  $ ceph-authtool
  ceph-authtool: must specify filename
  usage: ceph-authtool keyringfile [OPTIONS]...
  where the options are:
    -l, --list                    will list all keys and capabilities present in
                                  the keyring
    -p, --print-key               will print an encoded key for the specified
                                  entityname. This is suitable for the
                                  'mount -o secret=..' argument
    -C, --create-keyring          will create a new keyring, overwriting any
                                  existing keyringfile
    --gen-key                     will generate a new secret key for the
                                  specified entityname
    --add-key                     will add an encoded key to the keyring
    --cap subsystem capability    will set the capability for given subsystem
    --caps capsfile               will set all of capabilities associated with a
                                  given key, for all subsystems
  [1]

# demonstrate that manpage examples fail without config
# TODO fix the manpage
  $ ceph-authtool --create-keyring --name client.foo --gen-key keyring
  creating keyring

# work around the above
  $ touch ceph.conf

To create a new keyring containing a key for client.foo:

  $ ceph-authtool --create-keyring --id foo --gen-key keyring
  creating keyring

  $ ceph-authtool --create-keyring --name client.foo --gen-key keyring
  creating keyring

To associate some capabilities with the key (namely, the ability to mount a Ceph filesystem):

  $ ceph-authtool -n client.foo --cap mds 'allow' --cap osd 'allow rw pool=data' --cap mon 'allow r' keyring

To display the contents of the keyring:

  $ ceph-authtool -l keyring
  [client.foo]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \tcaps mds = "allow" (esc)
  \tcaps mon = "allow r" (esc)
  \tcaps osd = "allow rw pool=data" (esc)
