  $ cauthtool kring --create-keyring --gen-key
  creating kring

  $ cauthtool --cap osd 'allow rx pool=swimming' kring
  $ cauthtool kring --list|grep caps:
  \tcaps: [osd] allow rx pool=swimming (esc)
