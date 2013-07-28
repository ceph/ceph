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
    --import-keyring              will import the content of a given keyring
                                  into the keyringfile
    -u, --set-uid                 sets the auid (authenticated user id) for the
                                  specified entityname
    -a, --add-key                 will add an encoded key to the keyring
    --cap subsystem capability    will set the capability for given subsystem
    --caps capsfile               will set all of capabilities associated with a
                                  given key, for all subsystems
  [1]
