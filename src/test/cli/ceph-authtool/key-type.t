The encoded key starts with its type: "AQ" is aes, "Ag" is aes256k.

  $ ceph-authtool kring --create-keyring --gen-key --key-type aes
  creating kring

  $ ceph-authtool kring --list
  [client.admin]
  \\tkey = AQ[a-zA-Z0-9+/]+=* \(esc\) (re)

  $ ceph-authtool kring --create-keyring --gen-key --key-type aes256k
  creating kring

  $ ceph-authtool kring --list
  [client.admin]
  \\tkey = Ag[a-zA-Z0-9+/]+=* \(esc\) (re)

  $ ceph-authtool kring --create-keyring --gen-key -t aes
  creating kring

  $ ceph-authtool kring --list
  [client.admin]
  \\tkey = AQ[a-zA-Z0-9+/]+=* \(esc\) (re)

  $ ceph-authtool kring --create-keyring --gen-key --key-type bogus
  invalid key type: bogus
  [1]
