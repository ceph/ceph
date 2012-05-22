  $ ceph-conf --help
  Ceph configuration query tool
  
  USAGE
  ceph-conf <flags> <action>
  
  ACTIONS
    -L|--list-all-sections          List all sections
    -l|--list-sections <prefix>     List sections with the given prefix
    --filter-key <key>              Filter section list to only include sections
                                    with given key defined.
    --filter-key-value <key>=<val>  Filter section list to only include sections
                                    with given key/value pair.
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
  [$] ceph-conf --name mon.0 -c /etc/ceph/ceph.conf 'mon addr' (re)
  Find out what the value of 'mon add' is for monitor 0.
  
  [$] ceph-conf -l mon (re)
  List sections beginning with 'mon'.
  
  RETURN CODE
  Return code will be 0 on success; error code otherwise.
  [1]
