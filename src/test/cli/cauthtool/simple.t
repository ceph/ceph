  $ cauthtool
  cauthtool: must specify filename
  usage: cauthtool keyringfile [OPTIONS]...
  where the options are:
    -l, --list                    will list all keys and capabilities present in
                                  the keyring
    -p, --print                   will print an encoded key for the specified
                                  entityname. This is suitable for the
                                  'mount -o secret=..' argument
    -c, --create-keyring          will create a new keyring, overwriting any
                                  existing keyringfile
    --gen-key                     will generate a new secret key for the
                                  specified entityname
    --add-key                     will add an encoded key to the keyring
    --cap subsystem capability    will set the capability for given subsystem
    --caps capsfile               will set all of capabilities associated with a
                                  given key, for all subsystems
    -b, --bin                     will create a binary formatted keyring
  [1]
