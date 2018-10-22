  $ rbd create foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd flatten foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd resize foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd rm foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd import-diff /tmp/diff foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd mv foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd mv foo@snap bar
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd mv foo@snap bar@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd image-meta list foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd image-meta get foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd image-meta get foo@snap key
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd image-meta set foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd image-meta set foo@snap key
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd image-meta set foo@snap key val
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd image-meta remove foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd image-meta remove foo@snap key
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd snap ls foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd snap purge foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd watch foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd status foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd feature disable foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd feature disable foo@snap layering
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd feature enable foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd feature enable foo@snap layering
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd lock list foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd lock add foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd lock add foo@snap id
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd lock remove foo@snap
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd lock remove foo@snap id
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd lock remove foo@snap id client.1234
  rbd: snapshot name specified for a command that doesn't use it
  [22]
  $ rbd bench foo@snap --io-type write
  rbd: snapshot name specified for a command that doesn't use it
  [22]

  $ rbd clone foo@snap bar@snap
  rbd: destination snapshot name specified for a command that doesn't use it
  [22]
  $ rbd import /bin/ls ls@snap
  rbd: destination snapshot name specified for a command that doesn't use it
  [22]
  $ rbd cp foo bar@snap
  rbd: destination snapshot name specified for a command that doesn't use it
  [22]
  $ rbd cp foo@snap bar@snap
  rbd: destination snapshot name specified for a command that doesn't use it
  [22]
  $ rbd deep cp foo bar@snap
  rbd: destination snapshot name specified for a command that doesn't use it
  [22]
  $ rbd deep cp foo@snap bar@snap
  rbd: destination snapshot name specified for a command that doesn't use it
  [22]
  $ rbd mv foo bar@snap
  rbd: destination snapshot name specified for a command that doesn't use it
  [22]
