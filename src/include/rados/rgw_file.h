// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * convert RGW commands to file commands
 *
 * Copyright (C) 2015 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef RADOS_RGW_FILE_H
#define RADOS_RGW_FILE_H

#include <sys/stat.h>
#include <sys/types.h>
#include <stdint.h>
#include <stdbool.h>

#include "librgw.h"

#ifdef __cplusplus
extern "C" {
#endif

#define LIBRGW_FILE_VER_MAJOR 1
#define LIBRGW_FILE_VER_MINOR 4
#define LIBRGW_FILE_VER_EXTRA 1

#define LIBRGW_FILE_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)
#define LIBRGW_FILE_VERSION_CODE LIBRGW_FILE_VERSION(LIBRGW_FILE_VER_MAJOR, LIBRGW_FILE_VER_MINOR, LIBRGW_FILE_VER_EXTRA)

/*
 * Flags
 *
 * Every call takes a uint32_t flags as its last argument, and each call has
 * its *own* namespace of flag values -- RGW_LOOKUP_FLAG_* for rgw_lookup(),
 * RGW_OPEN_FLAG_* for the open calls, and so on.  The values are small and
 * deliberately reused across namespaces, so a word from the wrong namespace
 * is not detectable by value:  passing RGW_LOOKUP_FLAG_DIR to rgw_open()
 * would read as RGW_OPEN_FLAG_V3.  Pass the namespace belonging to the call.
 *
 * Where a call defines only RGW_..._FLAG_NONE it takes no flags;  pass NONE.
 * Where a call defines real values, a _FLAG_MASK is given beside them and the
 * call rejects any bit outside it with -EINVAL, rather than letting an
 * unexpected bit reach the shared internals and change behaviour there.
 *
 * Two calls share a namespace by intent, because they share an
 * implementation:  rgw_open() and rgw_open2() both take RGW_OPEN_FLAG_*, and
 * rgw_close() and rgw_close2() both take RGW_CLOSE_FLAG_*.
 */

/*
 * object types
 */
enum rgw_fh_type {
  RGW_FS_TYPE_NIL = 0,
  RGW_FS_TYPE_FILE,
  RGW_FS_TYPE_DIRECTORY,
  RGW_FS_TYPE_SYMBOLIC_LINK,
};

/*
 * dynamic allocated handle to support nfs handle
 */

/* content-addressable hash */
struct rgw_fh_hk {
  uint64_t bucket;
  uint64_t object;
};

struct rgw_file_handle
{
  /* content-addressable hash */
  struct rgw_fh_hk fh_hk;
  void *fh_private; /* librgw private data */
  /* object type */
  enum rgw_fh_type fh_type;
};

struct rgw_fs
{
  librgw_t rgw;
  void *fs_private;
  struct rgw_file_handle* root_fh;
};


/* XXX mount info hypothetical--emulate Unix, support at least
 * UUID-length fsid */
struct rgw_statvfs {
    uint64_t  f_bsize;    /* file system block size */
    uint64_t  f_frsize;   /* fragment size */
    uint64_t     f_blocks;   /* size of fs in f_frsize units */
    uint64_t     f_bfree;    /* # free blocks */
    uint64_t     f_bavail;   /* # free blocks for unprivileged users */
    uint64_t     f_files;    /* # inodes */
    uint64_t     f_ffree;    /* # free inodes */
    uint64_t     f_favail;   /* # free inodes for unprivileged users */
    uint64_t     f_fsid[2];     /* file system ID */
    uint64_t     f_flag;     /* mount flags */
    uint64_t     f_namemax;  /* maximum filename length */
};


void rgwfile_version(int *major, int *minor, int *extra);

/*
  lookup object by name (POSIX style)
*/
#define RGW_LOOKUP_FLAG_NONE    0x0000
#define RGW_LOOKUP_FLAG_CREATE  0x0001
#define RGW_LOOKUP_FLAG_RCB     0x0002 /* readdir callback hint */
#define RGW_LOOKUP_FLAG_DIR     0x0004
#define RGW_LOOKUP_FLAG_FILE    0x0008

#define RGW_LOOKUP_FLAG_MASK \
  (RGW_LOOKUP_FLAG_CREATE|RGW_LOOKUP_FLAG_RCB|RGW_LOOKUP_FLAG_DIR| \
   RGW_LOOKUP_FLAG_FILE)

#define RGW_LOOKUP_TYPE_FLAGS \
  (RGW_LOOKUP_FLAG_DIR|RGW_LOOKUP_FLAG_FILE)

int rgw_lookup(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh, const char *path,
	      struct rgw_file_handle **fh,
	      struct stat *st, uint32_t mask, uint32_t flags);

/*
  lookup object by handle (NFS style)
*/
int rgw_lookup_handle(struct rgw_fs *rgw_fs, struct rgw_fh_hk *fh_hk,
		      struct rgw_file_handle **fh, uint32_t flags);

/*
 * release file handle
 */
#define RGW_FH_RELE_FLAG_NONE   0x0000

int rgw_fh_rele(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		uint32_t flags);

/*
 attach rgw namespace
*/
#define RGW_MOUNT_FLAG_NONE     0x0000

int rgw_mount(librgw_t rgw, const char *uid, const char *key,
	      const char *secret, struct rgw_fs **rgw_fs,
	      uint32_t flags);

int rgw_mount2(librgw_t rgw, const char *uid, const char *key,
               const char *secret, const char *root, struct rgw_fs **rgw_fs,
               uint32_t flags);

/*
 register invalidate callbacks
*/
#define RGW_REG_INVALIDATE_FLAG_NONE    0x0000

typedef void (*rgw_fh_callback_t)(void *handle, struct rgw_fh_hk fh_hk);

int rgw_register_invalidate(struct rgw_fs *rgw_fs, rgw_fh_callback_t cb,
			    void *arg, uint32_t flags);

/*
 detach rgw namespace
*/
#define RGW_UMOUNT_FLAG_NONE    0x0000

int rgw_umount(struct rgw_fs *rgw_fs, uint32_t flags);


/*
  get filesystem attributes
*/
#define RGW_STATFS_FLAG_NONE     0x0000

int rgw_statfs(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh,
	       struct rgw_statvfs *vfs_st,
	       uint32_t flags);


/* XXX (get|set)attr mask bits */
#define RGW_SETATTR_MODE   1
#define RGW_SETATTR_UID    2
#define RGW_SETATTR_GID    4
#define RGW_SETATTR_MTIME  8
#define RGW_SETATTR_ATIME 16
#define RGW_SETATTR_SIZE  32
#define RGW_SETATTR_CTIME 64

/*
  create file
*/
#define RGW_CREATE_FLAG_NONE     0x0000

int rgw_create(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
	       const char *name, struct stat *st, uint32_t mask,
	       struct rgw_file_handle **fh, uint32_t posix_flags,
	       uint32_t flags);

/*
  create a symbolic link
 */
#define RGW_CREATELINK_FLAG_NONE     0x0000
int rgw_symlink(struct rgw_fs *rgw_fs, struct rgw_file_handle *parent_fh,
               const char *name, const char *link_path, struct stat *st, 
               uint32_t mask, struct rgw_file_handle **fh, uint32_t posix_flags,
               uint32_t flags);

/*
  create a new directory
*/
#define RGW_MKDIR_FLAG_NONE      0x0000

int rgw_mkdir(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *parent_fh,
	      const char *name, struct stat *st, uint32_t mask,
	      struct rgw_file_handle **fh, uint32_t flags);

/*
  rename object
*/
#define RGW_RENAME_FLAG_NONE      0x0000
/* Move only the current version, discarding the object's history.  Needed
 * to move a versioned object into a bucket which cannot hold versions;
 * without it such a rename is refused rather than silently losing history.
 * Intended to be bound as export policy, not requested per operation --
 * rename(2) has no way to ask for it, so no filesystem client can. */
#define RGW_RENAME_FLAG_SLICE_VERSIONS 0x0001

#define RGW_RENAME_FLAG_MASK (RGW_RENAME_FLAG_SLICE_VERSIONS)

int rgw_rename(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *olddir, const char* old_name,
	       struct rgw_file_handle *newdir, const char* new_name,
	       uint32_t flags);

/*
  remove file or directory
*/
#define RGW_UNLINK_FLAG_NONE      0x0000

int rgw_unlink(struct rgw_fs *rgw_fs,
	       struct rgw_file_handle *parent_fh, const char* path,
	       uint32_t flags);

/*
    read  directory content
*/
typedef int (*rgw_readdir_cb)(const char *name, void *arg, uint64_t offset,
			       struct stat *st, uint32_t mask,
			       uint32_t flags);

#define RGW_READDIR_FLAG_NONE      0x0000
#define RGW_READDIR_FLAG_DOTDOT    0x0001 /* send dot names */

#define RGW_READDIR_FLAG_MASK (RGW_READDIR_FLAG_DOTDOT)

int rgw_readdir(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *parent_fh, uint64_t *offset,
		rgw_readdir_cb rcb, void *cb_arg, bool *eof,
		uint32_t flags);

/* enumeration continuing from name */
int rgw_readdir2(struct rgw_fs *rgw_fs,
		 struct rgw_file_handle *parent_fh, const char *name,
		 rgw_readdir_cb rcb, void *cb_arg, bool *eof,
		 uint32_t flags);

/* project offset of dirent name */
#define RGW_DIRENT_OFFSET_FLAG_NONE 0x0000

int rgw_dirent_offset(struct rgw_fs *rgw_fs,
		      struct rgw_file_handle *parent_fh,
		      const char *name, int64_t *offset,
		      uint32_t flags);

/*
   get unix attributes for object
*/
#define RGW_GETATTR_FLAG_NONE      0x0000

int rgw_getattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st,
		uint32_t flags);

/*
   set unix attributes for object
*/
#define RGW_SETATTR_FLAG_NONE      0x0000

int rgw_setattr(struct rgw_fs *rgw_fs,
		struct rgw_file_handle *fh, struct stat *st,
		uint32_t mask, uint32_t flags);

/*
   truncate file
*/
#define RGW_TRUNCATE_FLAG_NONE     0x0000

int rgw_truncate(struct rgw_fs *rgw_fs,
		 struct rgw_file_handle *fh, uint64_t size,
		 uint32_t flags);

/*
   open file
*/
#define RGW_OPEN_FLAG_NONE         0x0000
#define RGW_OPEN_FLAG_CREATE       0x0001
#define RGW_OPEN_FLAG_V3           0x0002 /* ops have v3 semantics */
#define RGW_OPEN_FLAG_STATELESS    0x0002 /* alias it */

#define RGW_OPEN_FLAG_MASK (RGW_OPEN_FLAG_CREATE|RGW_OPEN_FLAG_V3)

int rgw_open(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	     uint32_t posix_flags, uint32_t flags);


typedef void* rgw_open_fd;

/*
  create disposition for rgw_open2
*/
#define RGW_CREATEMODE_NONE        0 /* do not create */
#define RGW_CREATEMODE_UNCHECKED   1 /* create, or open and apply attrs */
#define RGW_CREATEMODE_GUARDED     2 /* create, fail if it exists */
#define RGW_CREATEMODE_EXCLUSIVE   3 /* guarded, and attrs carry a verifier */
#define RGW_CREATEMODE_EXCLUSIVE41 4 /* exclusive, verifier separate from attrs */

/*
  encodings an access-control list may arrive in.  reserved:  none is
  accepted yet, and a non-empty acl is refused rather than ignored
*/
#define RGW_ACL_ENCODING_NONE      0
#define RGW_ACL_ENCODING_NFS4      1 /* XDR nfsace4 */
#define RGW_ACL_ENCODING_POSIX     2 /* POSIX.1e */
#define RGW_ACL_ENCODING_RGW       3 /* RGWAccessControlPolicy */

/*
  Optional arguments to rgw_open2.

  Carries no version of its own.  This interface is versioned as a whole --
  LIBRGW_FILE_VER_MAJOR/MINOR/EXTRA at compile time, rgwfile_version() at
  run time -- and a consumer is expected to build against the header
  belonging to the library it links, as it already must for every other
  structure here.  Adding a field to this one is an interface change like
  any other, not something to be negotiated per call.

  Zero the structure and set what you need.  Every member is independent:
  none of them is selected by createmode, which is why this is a flat
  structure rather than a tagged union.  When to set each:

  createmode
      The create disposition.  Leave it zero (RGW_CREATEMODE_NONE) to open
      without creating, in which case RGW_OPEN_FLAG_CREATE and O_EXCL decide
      as they always did.  Set it to make the disposition explicit, and it
      governs instead.  Note UNCHECKED does not truncate:  ask for that with
      RGW_SETATTR_SIZE below, or with O_TRUNC in posix_flags.

  attr_mask, attrs
      Set together or not at all -- a mask with no attrs is -EINVAL.  These
      are the attributes the object should have, applied while a created
      object is still invisible under its name, so a second caller never
      observes it without them.  Meaningful for *any* createmode, not only a
      creating one:  UNCHECKED applies them to an object that already
      existed.

      RGW_SETATTR_SIZE here is a data operation, applied after any O_TRUNC,
      so an explicit size wins over the flag.

      For an exclusive create this is also how the verifier arrives:  the
      caller folds it into atime and mtime (as ganesha's
      set_common_verifier() does) and sets RGW_SETATTR_ATIME|RGW_SETATTR_MTIME.
      Those two are stored and returned byte-exact, so do not expect server
      time to be substituted for them.  They survive until the first write,
      which moves mtime as it would on any filesystem -- long enough for a
      retransmitted create, which arrives before the client's writes.

  attrs_out
      Set it to receive the resulting attributes, NULL if you do not want
      them.  Independent of everything above:  useful on a plain open as well
      as a create, and it saves a following rgw_getattr() since these were
      just written.

  acl, acl_len, acl_encoding
      Reserved.  A non-empty ACL is refused with -ENOTSUP rather than
      ignored, because a caller which believes it set one must not be told
      the open succeeded.  Leave all three zero.  The encoding enum records
      the intended alternatives (NFSv4 XDR, POSIX.1e, RGW's own policy);
      which are accepted, and what RGW stores natively, is unsettled.
*/
struct rgw_open_args
{
  uint32_t createmode;   /* RGW_CREATEMODE_*;  zero means "do not create" */
  uint32_t attr_mask;    /* RGW_SETATTR_* describing which of *attrs to use */
  struct stat* attrs;    /* IN:  attributes to apply;  NULL if attr_mask==0 */
  struct stat* attrs_out; /* OUT: resulting attributes;  NULL if not wanted */
  void* acl;             /* reserved -- must be NULL */
  uint32_t acl_len;      /* reserved -- must be 0 */
  uint32_t acl_encoding; /* reserved -- must be RGW_ACL_ENCODING_NONE */
};

/*
  args may be NULL, in which case the create disposition is taken from
  RGW_OPEN_FLAG_CREATE and O_EXCL as it always was.  When
  args->createmode is not RGW_CREATEMODE_NONE it governs instead.
*/
int rgw_open2(struct rgw_fs* rgw_fs, struct rgw_file_handle* fh,
              rgw_open_fd* open_fd /* OUT */,
              struct rgw_open_args* args,
              uint32_t posix_flags,
              uint32_t flags);

/*
   close file
*/

#define RGW_CLOSE_FLAG_NONE        0x0000
#define RGW_CLOSE_FLAG_RELE        0x0001
#define RGW_CLOSE_FLAG_DETACH      0x0002

#define RGW_CLOSE_FLAG_MASK (RGW_CLOSE_FLAG_RELE|RGW_CLOSE_FLAG_DETACH)

/*
  RGW_CLOSE_FLAG_DETACH declines to finalize on this close's account:
  the close does not publish, even when it returns the last write open.

  It is not an abort.  There is one mutable view of an object, shared by
  every open on it, so one writer's bytes are indistinguishable from
  another's and cannot be withdrawn.  What follows from that:

    - another writer is still open:  they publish the shared view when
      they close, including everything this caller wrote.  DETACH
      suppressed the trigger, nothing more.
    - this is the last open on the object:  no one else has a claim, so
      the view is discarded rather than published.  This is the only
      case in which DETACH abandons anything.
    - only readers remain:  nothing is published and nothing is
      discarded -- readers are reading that view, and taking it from
      them would be worse than leaving it.  It persists, unpublished,
      until a later writer finishes it or it is unlinked.

  The sense is pthread_detach()'s:  it changes who is responsible for
  finalizing, not whether the work happened.  A caller wanting
  transactional abort does not have it here;  only an intersecting
  unlink discards work another open is still holding.

  NOT YET IMPLEMENTED.  The flag is accepted and currently has no
  effect -- librgw finalizes by the ordinary rules.  Semantics were
  settled 2026-09-08 so that the meaning is fixed before a caller
  depends on it.
*/
  
int rgw_close(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	      uint32_t flags);

int rgw_close2(rgw_open_fd open_fd, uint32_t flags);

/*
  change the access mode of an open, without returning it

  An upgrade to write establishes the object's mutable view, since a
  reader may be bound to the published object.  A downgrade which
  returns the last write open publishes, exactly as closing it would:
  giving up write intent and closing both return it.
*/
/* flags is reserved and must be RGW_OPEN_FLAG_NONE:  the access mode comes
   entirely from posix_flags, and nothing in the open namespace applies to a
   mode change */
int rgw_reopen2(rgw_open_fd open_fd, uint32_t posix_flags, uint32_t flags);

/*
   read data from file
*/
#define RGW_READ_FLAG_NONE 0x0000

int rgw_read(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint64_t offset,
	     size_t length, size_t *bytes_read, void *buffer,
	     uint32_t flags);

int rgw_readv(rgw_open_fd open_fd,
              const struct iovec* iov, int iov_cnt,
              uint64_t offset, uint64_t* bytes_read,
              uint32_t flags);

/*
   read symbolic link
*/
#define RGW_READLINK_FLAG_NONE 0x0000

int rgw_readlink(struct rgw_fs *rgw_fs,
	     struct rgw_file_handle *fh, uint64_t offset,
	     size_t length, size_t *bytes_read, void *buffer,
	     uint32_t flags);

/*
   write data to file
*/
#define RGW_WRITE_FLAG_NONE      0x0000

int rgw_write(struct rgw_fs *rgw_fs,
	      struct rgw_file_handle *fh, uint64_t offset,
	      size_t length, size_t *bytes_written, void *buffer,
	      uint32_t flags);

int rgw_writev(rgw_open_fd open_fd,
               const struct iovec* iov, int iov_cnt,
               uint64_t offset, uint64_t* bytes_written,
               uint32_t flags);
/*
   sync written data
*/
#define RGW_FSYNC_FLAG_NONE        0x0000

int rgw_fsync(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	      uint32_t flags);

/*
   NFS commit operation
*/

#define RGW_COMMIT_FLAG_NONE        0x0000

int rgw_commit(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
	       uint64_t offset, uint64_t length, uint32_t flags);

/*
  extended attributes
 */
typedef struct rgw_xattrstr
{
  char *val;
  uint32_t len;
} rgw_xattrstr;

typedef struct rgw_xattr
{
  rgw_xattrstr key;
  rgw_xattrstr val;
} rgw_xattr;

typedef struct rgw_xattrlist
{
  rgw_xattr *xattrs;
  uint32_t xattr_cnt;
} rgw_xattrlist;

#define RGW_GETXATTR_FLAG_NONE      0x0000

typedef int (*rgw_getxattr_cb)(rgw_xattrlist *attrs, void *arg,
			       uint32_t flags);

int rgw_getxattrs(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		  rgw_xattrlist *attrs, rgw_getxattr_cb cb, void *cb_arg,
		  uint32_t flags);

#define RGW_LSXATTR_FLAG_NONE       0x0000
/* returned by the caller's rgw_xattrlist_cb to stop enumeration;  it is not
 * an input to rgw_lsxattrs(), which takes no flags */
#define RGW_LSXATTR_FLAG_STOP       0x0001

int rgw_lsxattrs(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		 rgw_xattrstr *filter_prefix /* unimplemented for now */,
		 rgw_getxattr_cb cb, void *cb_arg, uint32_t flags);

#define RGW_SETXATTR_FLAG_NONE      0x0000

int rgw_setxattrs(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		 rgw_xattrlist *attrs, uint32_t flags);

#define RGW_RMXATTR_FLAG_NONE       0x0000

int rgw_rmxattrs(struct rgw_fs *rgw_fs, struct rgw_file_handle *fh,
		 rgw_xattrlist *attrs, uint32_t flags);

#ifdef __cplusplus
}
#endif

#endif /* RADOS_RGW_FILE_H */
