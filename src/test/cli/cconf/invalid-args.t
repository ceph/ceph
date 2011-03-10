  $ cat >test.conf <<EOF
  > [bar]
  > bar = green
  > EOF

# TODO output an error
  $ cconf -c test.conf broken
  [1]

# TODO output an error (missing key), not the whole usage
  $ cconf -c test.conf -s bar
  lookup: expected exactly one argument
  Ceph configuration query tool
  
  USAGE
  cconf <flags> <action>
  
  ACTIONS
    -l|--list-sections <prefix>     List sections in prefix
    --lookup <key>                  Print a configuration setting to stdout.
                                    Returns 0 (success) if the configuration setting is
                                    found; 1 otherwise.
    -r|--resolve-search             search for the first file that exists and
                                    can be opened in the resulted comma
                                    delimited search list.
  
  FLAGS
    --name name                     Set type.id
    [-s <section>]                  Add to list of sections to search
  
  If there is no action given, the action will default to --lookup.
  
  EXAMPLES
  [$] cconf --name client.cconf -c /etc/ceph/ceph.conf -t mon -i 0 'mon addr' (re)
  Find out if there is a 'mon addr' defined in /etc/ceph/ceph.conf
  
  [$] cconf -l mon (re)
  List sections beginning with 'mon'.
  
  RETURN CODE
  Return code will be 0 on success; error code otherwise.
  [1]
