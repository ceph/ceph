  $ cauthtool kring --create-keyring --gen-key
  creating kring

# TODO is this nice?
  $ cauthtool --cap osd 'broken' kring
  $ cauthtool kring --list|grep caps:
  \tcaps: [osd] broken (esc)

# TODO is this nice?
  $ cauthtool --cap xyzzy 'broken' kring
  $ cauthtool kring --list|grep caps:
  \tcaps: [xyzzy] broken (esc)
