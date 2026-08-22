Credentials can be read from a file so that they never appear in argv, where
any local user could read them out of /proc/<pid>/cmdline.

A file that cannot be opened is reported rather than silently ignored:

  $ radosgw-admin --access-key-file=missing
  ERROR: failed to open missing: (2)* (glob)
  [2]

  $ radosgw-admin --secret-key-file=missing
  ERROR: failed to open missing: (2)* (glob)
  [2]

Only regular files hold credentials:

  $ mkdir adir
  $ radosgw-admin --access-key-file=adir
  ERROR: adir is not a regular file
  [22]

An empty file holds no credential, and a mode that exposes it to other users
is called out before the file is used:

  $ : > empty
  $ chmod 0644 empty
  $ radosgw-admin --access-key-file=empty
  WARNING: credential file empty is accessible by group or others, recommended permissions are 0600
  ERROR: empty contains no credential
  [22]

A file holding only whitespace is empty once the trailing newline is stripped:

  $ printf '\n' > blank
  $ chmod 0600 blank
  $ radosgw-admin --secret-file=blank
  ERROR: blank contains no credential
  [22]

A well-formed file is accepted quietly, including its trailing newline. Here
the access key is read successfully and the run proceeds far enough to fail on
the secret instead:

  $ printf 'AKIAIOSFODNN7EXAMPLE\n' > access
  $ chmod 0600 access
  $ radosgw-admin --access-key-file=access --secret-key-file=missing
  ERROR: failed to open missing: (2)* (glob)
  [2]

A credential cannot be given both inline and in a file. The conflict is
reported without the file being opened, so a path that does not exist is
still enough to trigger it:

  $ radosgw-admin --access-key=AKIAIOSFODNN7EXAMPLE --access-key-file=missing
  ERROR: --access-key and --access-key-file are mutually exclusive
  [22]

  $ radosgw-admin --secret-key=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY --secret-key-file=missing
  ERROR: --secret/--secret-key and --secret-file/--secret-key-file are mutually exclusive
  [22]

The options may be given in either order:

  $ radosgw-admin --access-key-file=missing --access-key=AKIAIOSFODNN7EXAMPLE
  ERROR: --access-key and --access-key-file are mutually exclusive
  [22]

The aliases behave the same as the names they mirror:

  $ radosgw-admin --secret=wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY --secret-file=missing
  ERROR: --secret/--secret-key and --secret-file/--secret-key-file are mutually exclusive
  [22]
