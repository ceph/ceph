  $ cauthtool kring --create-keyring --bin
  creating kring

  $ cauthtool kring --list --bin

# --list actually does not use --bin, but autodetects; run it both
# ways just to trigger that
  $ cauthtool kring --list

  $ cauthtool kring --gen-key --bin

# cram makes matching escape-containing lines with regexps a bit ugly
  $ cauthtool kring --list
  [client.admin]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid = [0-9]{20} \(esc\) (re)

# synonym
  $ cauthtool kring -l
  [client.admin]
  \\tkey = [a-zA-Z0-9+/]+=* \(esc\) (re)
  \\tauid = [0-9]{20} \(esc\) (re)

