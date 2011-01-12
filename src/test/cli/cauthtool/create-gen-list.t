  $ cauthtool kring --create-keyring
  creating kring

  $ cauthtool kring --list

  $ cauthtool kring --gen-key

# cram makes matching escape-containing lines with regexps a bit ugly
  $ cauthtool kring --list
  client.admin
  \\t key: [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid: [0-9]{20} \(esc\) (re)

# synonym
  $ cauthtool kring -l
  client.admin
  \\t key: [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid: [0-9]{20} \(esc\) (re)
