  $ ceph-authtool kring --create-keyring --mode 0644
  creating kring

  $ ceph-authtool kring --add-key 'FAKEBASE64 foo'
  can't decode key 'FAKEBASE64 foo'
  [1]
