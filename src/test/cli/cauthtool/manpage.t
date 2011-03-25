  $ cauthtool
  cauthtool: must specify filename
  usage: cauthtool keyringfile [OPTIONS]...
  where the options are:
    -l, --list                    will list all keys and capabilities present in
                                  the keyring
    -p, --print                   will print an encoded key for the specified
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
    -b, --bin                     will create a binary formatted keyring
  [1]

# demonstrate that manpage examples fail without config
# TODO fix the manpage
  $ cauthtool --create-keyring --name client.foo --gen-key keyring
  creating keyring

# work around the above
  $ touch ceph.conf

To create a new keyring containing a key for client.foo:

  $ cauthtool --create-keyring --id foo --gen-key keyring.bin
  creating keyring.bin

  $ cauthtool --create-keyring --name client.foo --gen-key keyring.bin
  creating keyring.bin

To associate some capabilities with the key (namely, the ability to mount a Ceph filesystem):

  $ cauthtool -n client.foo --cap mds 'allow' --cap osd 'allow rw pool=data' --cap mon 'allow r' keyring.bin

To display the contents of the keyring:

  $ cauthtool -l keyring.bin
  [client.foo]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid = [0-9]{20} \(esc\) (re)
  \tcaps mds = "allow" (esc)
  \tcaps mon = "allow r" (esc)
  \tcaps osd = "allow rw pool=data" (esc)
