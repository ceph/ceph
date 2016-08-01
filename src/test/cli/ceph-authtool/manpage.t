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
    -g, --gen-key                 will generate a new secret key for the
                                  specified entityname
    --gen-print-key               will generate a new secret key without set it
                                  to the keyringfile, prints the secret to stdout
    --import-keyring FILE         will import the content of a given keyring
                                  into the keyringfile
    -n NAME, --name NAME          specify entityname to operate on
    -u AUID, --set-uid AUID       sets the auid (authenticated user id) for the
                                  specified entityname
    -a BASE64, --add-key BASE64   will add an encoded key to the keyring
    --cap SUBSYSTEM CAPABILITY    will set the capability for given subsystem
    --caps CAPSFILE               will set all of capabilities associated with a
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
