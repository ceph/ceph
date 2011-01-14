#TODO just show an error, not the whole usage
  $ cconf
  Ceph configuration query tool
  
  USAGE
  cconf <flags> <action>
  
  ACTIONS
    -l|--list-sections <prefix>     List sections in prefix
    --lookup <key> [defval]         Print a configuration setting to stdout.
  \t\t\t\t  If the setting is not defined, and the (esc)
  \t\t\t\t  optional argument defval is provide, it will (esc)
  \t\t\t\t  be printed instead. variables in defval are (esc)
  \t\t\t\t  interpolated. (esc)
    -r|--resolve-search             search for the first file that exists and
                                    can be opened in the resulted comma
                                    delimited search list.
  
  FLAGS
    -i id                           Set id
    [-s <section>]                  Add to list of sections to search
  
  If there is no action given, the action will default to --lookup.
  
  EXAMPLES
  [$] cconf -i cconf -c /etc/ceph/ceph.conf -t mon -i 0 'mon addr' (re)
  Find out if there is a 'mon addr' defined in /etc/ceph/ceph.conf
  
  [$] cconf -l mon (re)
  List sections beginning with 'mon'.
  
  RETURN CODE
  Return code will be 0 on success; error code otherwise.
  [1]
