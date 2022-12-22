# Random LSVD tools

This is a bit of a mess right now, since it started out as a ctypes-based Python unit test framework for the library but that's all been stripped out.

## cache.py - read cache contents
Note that you might need to parse the superblock with `parse.py` to find the cache file name - it's typically "<cache dir>/<uuid>.cache"

```
usage: cache.py [-h] [--write] [--writemap] [--read] [--nowrap] device

Read SSD cache

positional arguments:
  device      cache device/file

options:
  -h, --help  show this help message and exit
  --write     print write cache details
  --writemap  print write cache map
  --read      print read cache details
  --nowrap    one entry per line
```

## clone.py - create image clone

(note - needs to be moved into RBD API)

```
usage: clone.py [-h] [--uuid UUID] [--rados] base image

create clone of LSVD disk image

positional arguments:
  base         base image
  image        new (clone) image

options:
  -h, --help   show this help message and exit
  --uuid UUID  volume UUID
```

## dumpobj

Translate backend objects to/from text. Note that this includes the structure definitions, rather than importing them from `lsvd_types.py`, so that it can be extended to handle different versions.

```
usage: dumpobj.py [-h] [--rados] [--encode] [--decode] src [dst]

LSVD object to/from text

positional arguments:
  src         source
  dst         destination

options:
  -h, --help  show this help message and exit
  --rados     use RADOS
  --encode    text -> binary
  --decode    binary -> text

```

## parse.py - parse and pretty-print backend objects
```
usage: parse.py [-h] [--rados] [--nowrap] object

Read backend object

positional arguments:
  object      object path

options:
  -h, --help  show this help message and exit
  --rados     fetch from RADOS
  --nowrap    one entry per line
```

## mkcache.py, mkdisk.py - historic use only

These should be superceded by imgtool, which uses the RBD API plus an extension or two.
