MDS internal data structures
==============================

*CInode*
  CInode contains the metadata of a file, there is one CInode for each file.
  The CInode stores information like who owns the file, how big the file is.

*CDentry*
  CDentry is the glue that holds inodes and files together by relating inode to
  file/directory names. A CDentry links to at most one CInode (it may not link
  to any CInode). A CInode may be linked by multiple CDentries.

*CDir*
  CDir only exists for directory inode, it's used to link CDentries under the
  directory. A CInode can have multiple CDir when the directory is fragmented.

These data structures are linked together as::

  CInode
  CDir
   |   \
   |      \
   |         \
  CDentry   CDentry
  CInode    CInode
  CDir      CDir
   |         |  \
   |         |     \
   |         |        \
  CDentry   CDentry  CDentry
  CInode    CInode   CInode

As this doc is being written, size of CInode is about 1400 bytes, size of CDentry
is about 400 bytes, size of CDir is about 700 bytes. These data structures are
quite large. Please be careful if you want to add new fields to them.

