  $ ceph-authtool kring --create-keyring --bin
  creating kring

  $ ceph-authtool kring --list --bin

# --list actually does not use --bin, but autodetects; run it both
# ways just to trigger that
  $ ceph-authtool kring --list

  $ ceph-authtool kring --gen-key --bin

# cram makes matching escape-containing lines with regexps a bit ugly
  $ ceph-authtool kring --list
  [client.admin]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid = [0-9]{20} \(esc\) (re)

# synonym
  $ ceph-authtool kring -l
  [client.admin]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid = [0-9]{20} \(esc\) (re)

