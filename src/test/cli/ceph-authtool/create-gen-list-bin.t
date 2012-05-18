  $ ceph-authtool kring --create-keyring
  creating kring

  $ ceph-authtool kring --list

  $ ceph-authtool kring --gen-key

# cram makes matching escape-containing lines with regexps a bit ugly
  $ ceph-authtool kring --list
  [client.admin]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)

# synonym
  $ ceph-authtool kring -l
  [client.admin]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)

