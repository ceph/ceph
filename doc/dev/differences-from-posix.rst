========================
 Differences from POSIX
========================

.. todo:: delete http://ceph.com/wiki/Differences_from_POSIX

Ceph does have a few places where it diverges from strict POSIX semantics for various reasons:

- Sparse files propagate incorrectly to tools like df. They will only
  use up the required space, but in df will increase the "used" space
  by the full file size. We do this because actually keeping track of
  the space a large, sparse file uses is very expensive.
- In shared simultaneous writer situations, a write that crosses
  object boundaries is not necessarily atomic. This means that you
  could have writer A write "aa|aa" and writer B write "bb|bb"
  simultaneously (where | is the object boundary), and end up with
  "aa|bb" rather than the proper "aa|aa" or "bb|bb".
