  $ ceph-authtool kring --create-keyring
  creating kring

  $ ceph-authtool kring --add-key 'AQAK7yxNeF+nHBAA0SgSdbs8IkJrxroDeJ6SwQ== 18446744073709551615'
  added entity client.admin auth auth(auid = 18446744073709551615 key=AQAK7yxNeF+nHBAA0SgSdbs8IkJrxroDeJ6SwQ== with 0 caps)

# cram makes matching escape-containing lines with regexps a bit ugly
  $ ceph-authtool kring --list
  [client.admin]
  \tkey = AQAK7yxNeF+nHBAA0SgSdbs8IkJrxroDeJ6SwQ== (esc)

  $ cat kring
  [client.admin]
  \tkey = AQAK7yxNeF+nHBAA0SgSdbs8IkJrxroDeJ6SwQ== (esc)
