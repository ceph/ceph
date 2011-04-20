// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */



#include "FileStore.h"
#include "common/BackTrace.h"
#include "include/types.h"

#include "FileJournal.h"

#include "osd/osd_types.h"

#include "include/color.h"

#include "common/Timer.h"
#include "common/errno.h"
#include "common/run_cmd.h"
#include "common/safe_io.h"
#include "common/ProfLogger.h"
#include "common/sync_filesystem.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <iostream>
#include <errno.h>
#include <dirent.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

#include "include/fiemap.h"

#ifndef __CYGWIN__
# include <sys/xattr.h>
#endif

#ifdef DARWIN
#include <sys/param.h>
#include <sys/mount.h>
#endif // DARWIN

#include <sstream>

#define ATTR_MAX_NAME_LEN  128
#define ATTR_MAX_BLOCK_LEN 2048

#define COMMIT_SNAP_ITEM "snap_%lld"

#ifndef __CYGWIN__
# ifndef DARWIN
#  include "btrfs_ioctl.h"



# endif
#endif

#include "common/config.h"

#define DOUT_SUBSYS filestore
#undef dout_prefix
#define dout_prefix *_dout << "filestore(" << basedir << ") "

#include "include/buffer.h"

#include <map>

/*
 * long file names will have the following format:
 *
 * prefix_cookie_hash_index
 *
 * The prefix will just be the first X bytes of the original file name.
 * The cookie is a constant string that hints whether this file name
 * is hashed
 */
#define FILENAME_MAX_LEN 4096 // the long file name size

#define FILENAME_SHORT_LEN 16 // the short file name size
#define FILENAME_COOKIE "cLFN"  // ceph long file name
#define FILENAME_HASH_LEN 3

#define FILENAME_PREFIX_LEN (FILENAME_SHORT_LEN - FILENAME_HASH_LEN - 1 - (sizeof(FILENAME_COOKIE) - 1) - 1)

#ifdef DARWIN
static int sys_getxattr(const char *fn, const char *name, void *val, size_t size)
{
  int r = ::getxattr(fn, name, val, size, 0, 0);
  return (r < 0 ? -errno : r);
}

static int sys_setxattr(const char *fn, const char *name, const void *val, size_t size)
{
  int r = ::setxattr(fn, name, val, size, 0, 0);
  return (r < 0 ? -errno : r);
}

static int sys_removexattr(const char *fn, const char *name)
{
  int r = ::removexattr(fn, name, 0);
  return (r < 0 ? -errno : r);
}

static int sys_listxattr(const char *fn, char *names, size_t len)
{
  int r = ::listxattr(fn, names, len, 0);
  return (r < 0 ? -errno : r);
}
#else

static int sys_getxattr(const char *fn, const char *name, void *val, size_t size)
{
  int r = ::getxattr(fn, name, val, size);
  return (r < 0 ? -errno : r);
}

static int sys_setxattr(const char *fn, const char *name, const void *val, size_t size)
{
  int r = ::setxattr(fn, name, val, size, 0);
  return (r < 0 ? -errno : r);
}

static int sys_removexattr(const char *fn, const char *name)
{
  int r = ::removexattr(fn, name);
  return (r < 0 ? -errno : r);
}

int sys_listxattr(const char *fn, char *names, size_t len)
{
  int r = ::listxattr(fn, names, len);
  return (r < 0 ? -errno : r);
}
#endif

static int hash_filename(const char *filename, char *hash, int buf_len)
{
  if (buf_len < FILENAME_HASH_LEN + 1)
    return -EINVAL;
  sprintf(hash, "zzz");
  return 0;
}

static void build_filename(char *filename, int len, const char *old_filename, int i)
{
  char hash[FILENAME_HASH_LEN + 1];

  assert(len >= FILENAME_SHORT_LEN + 4);

  strncpy(filename, old_filename, FILENAME_PREFIX_LEN);
  filename[FILENAME_PREFIX_LEN] = '\0';
  if (strlen(filename) < FILENAME_PREFIX_LEN)
    return;
  if (old_filename[FILENAME_PREFIX_LEN] == '\0')
    return;

  hash_filename(old_filename, hash, sizeof(hash));
  sprintf(filename + FILENAME_PREFIX_LEN, "_" FILENAME_COOKIE "_%s_%d", hash, i);
}

/* is this a candidate? */
static int lfn_is_hashed_filename(const char *filename)
{
  if (strlen(filename) < FILENAME_SHORT_LEN)
    return 0;
  return (strncmp(filename + FILENAME_PREFIX_LEN, "_" FILENAME_COOKIE "_", sizeof(FILENAME_COOKIE) -1 + 2) == 0);
}

static void lfn_translate(const char *path, const char *name, char *new_name, int len)
{
  if (!lfn_is_hashed_filename(name)) {
    strncpy(new_name, name, len);
    return;
  }

  char buf[PATH_MAX];

  snprintf(buf, sizeof(buf), "%s/%s", path, name);
  int r = sys_getxattr(buf, "user.ceph._lfn", new_name, sizeof(buf) - 1);
  if (r < 0)
    strncpy(new_name, name, len);
  else
    buf[r] = '\0';
  return;
}

static int prepare_short_path(const char *pathname, char *short_path, const char **pfilename, int *is_lfn)
{
  const char *filename = strrchr(pathname, '/');
  int path_len;
  int len;

  if (!filename)
    filename = pathname;
  else
    filename++;

  *pfilename = filename;

  len = strlen(filename);
  *is_lfn = (len >= (int)FILENAME_PREFIX_LEN);
  if (!*is_lfn)
    return 0;

  path_len = filename - pathname;
  if (short_path)
    memcpy(short_path, pathname, path_len);

  return path_len;
}

int lfn_get(const char *pathname, char *short_fn, int len, const char **long_fn, int *exist, int *is_lfn)
{
  int i = 0;
  const char *filename;
  int path_len;

  path_len = prepare_short_path(pathname, short_fn, &filename, is_lfn);
  if (!*is_lfn)
    return 0;

  *long_fn = filename;

  *exist = 0;

  path_len = filename - pathname;

  while (1) {
    char buf[PATH_MAX];
    int r;

    build_filename(&short_fn[path_len], len - path_len, filename, i);
    r = getxattr(short_fn, "user.ceph._lfn", buf, sizeof(buf));
    if (r < 0)
      r = -errno;
    if (r > 0) {
      buf[MAX((int)sizeof(buf)-1, r)] = '\0';
      if (strcmp(buf, filename) == 0) { // a match?
        *exist = 1;
        return i;
      }
    }
    switch (r) {
    case -ENOENT:
      return i;
    case -ERANGE:
      assert(0); // shouldn't happen
    default:
      break;
    }
    i++;
  }

  return 0; // unreachable anyway
}

static int lfn_find(const char *pathname, char *new_pathname, int len)
{
  const char *long_fn;
  int r, exist;
  int is_lfn;

  r = lfn_get(pathname, new_pathname, len, &long_fn, &exist, &is_lfn);
  if (r < 0)
    return r;
  if (!is_lfn)
    strncpy(new_pathname, pathname, len);
  else if (!exist)
    return -ENOENT;
  return 0;
}

static int lfn_getxattr(const char *fn, const char *name, void *val, size_t size)
{
  char new_fn[PATH_MAX];
  int r;

  r = lfn_find(fn, new_fn, sizeof(new_fn));
  if (r < 0)
    return r;
  return sys_getxattr(new_fn, name, val, size);
}

static int lfn_setxattr(const char *fn, const char *name, const void *val, size_t size)
{
  char new_fn[PATH_MAX];
  int r;

  r = lfn_find(fn, new_fn, sizeof(new_fn));
  if (r < 0)
    return r;
  return sys_setxattr(new_fn, name, val, size);
}

static int lfn_removexattr(const char *fn, const char *name)
{
  char new_fn[PATH_MAX];
  int r;

  r = lfn_find(fn, new_fn, sizeof(new_fn));
  if (r < 0)
    return r;
  return sys_removexattr(new_fn, name);
}

int lfn_listxattr(const char *fn, char *names, size_t len)
{
  char new_fn[PATH_MAX];
  int r;

  r = lfn_find(fn, new_fn, sizeof(new_fn));
  if (r < 0)
    return r;
  return sys_listxattr(new_fn, names, len);
}

static int lfn_truncate(const char *fn, off_t length)
{
 char new_fn[PATH_MAX];
  int r;

  r = lfn_find(fn, new_fn, sizeof(new_fn));
  if (r < 0)
    return r;
  r = ::truncate(fn, length);
  if (r < 0)
    return -errno;
  return r;
}

static int lfn_stat(const char *fn, struct stat *buf)
{
  char new_fn[PATH_MAX];
  int r;

  r = lfn_find(fn, new_fn, sizeof(new_fn));
  if (r < 0)
    return r;
  return ::stat(new_fn, buf);
}

static int lfn_open(const char *pathname, int flags, mode_t mode)
{
  const char *long_fn;
  char short_fn[PATH_MAX];
  int r, fd, exist;
  int is_lfn;

  r = lfn_get(pathname, short_fn, sizeof(short_fn), &long_fn, &exist, &is_lfn);
  if (r < 0)
    return r;
  if (!is_lfn)
    return open(pathname, flags);

  r = ::open(short_fn, flags, mode);

  if (r < 0)
    return r;

  fd = r;

  if (flags & O_CREAT) {
    r = fsetxattr(fd, "user.ceph._lfn", long_fn, strlen(long_fn), 0);
    if (r < 0) {
      close(fd);
      return r;
    }
  }
  return fd;
}

static int lfn_open(const char *pathname, int flags)
{
  return lfn_open(pathname, flags, 0);
}

static int lfn_link(const char *oldpath, const char *newpath)
{
  char short_fn_old[PATH_MAX];
  char short_fn_new[PATH_MAX];
  int exist, is_lfn;
  const char *filename;
  int r;

  r = lfn_get(oldpath, short_fn_old, sizeof(short_fn_old), &filename, &exist, &is_lfn);
  if (r < 0)
    return r;
  if (!exist)
    return -ENOENT;
  if (!is_lfn)
    strncpy(short_fn_old, oldpath, sizeof(short_fn_old));

  r = lfn_get(newpath, short_fn_new, sizeof(short_fn_new), &filename, &exist, &is_lfn);
  if (r < 0)
    return r;
  if (exist)
    return -EEXIST;
  if (!is_lfn)
    strncpy(short_fn_new, newpath, sizeof(short_fn_new));

  r = link(short_fn_old, short_fn_new);
  if (r < 0)
    return -errno;
  return 0;
}

static int lfn_unlink(const char *pathname)
{
  const char *filename;
  char short_fn[PATH_MAX];
  char short_fn2[PATH_MAX];
  int r, i, exist, err;
  int path_len;
  int is_lfn;

  r = lfn_get(pathname, short_fn, sizeof(short_fn), &filename, &exist, &is_lfn);
  if (r < 0)
    return r;
  if (!is_lfn)
    return unlink(pathname);
  if (!exist) {
    errno = ENOENT;
    return -1;
  }

  err = unlink(short_fn);
  if (err < 0)
    return err;


  path_len = filename - pathname;
  memcpy(short_fn2, pathname, path_len);

  for (i = r + 1; ; i++) {
    struct stat buf;
    int ret;

    build_filename(&short_fn2[path_len], sizeof(short_fn2) - path_len, filename, i);
    ret = stat(short_fn2, &buf);
    if (ret < 0) {
      if (i == r + 1)
        return 0;

      break;
    }
  }

  build_filename(&short_fn2[path_len], sizeof(short_fn2) - path_len, filename, i - 1);
  generic_dout(0) << "renaming " << short_fn2 << " -> " << short_fn << dendl;

  if (rename(short_fn2, short_fn) < 0) {
    generic_derr << "ERROR: could not rename " << short_fn2 << " -> " << short_fn << dendl;
    assert(0);
  }

  return 0;
}

static void get_raw_xattr_name(const char *name, int i, char *raw_name, int raw_len)
{
  int r;
  int pos = 0;

  while (*name) {
    switch (*name) {
    case '@': /* escape it */
      pos += 2;
      assert (pos < raw_len - 1);
      *raw_name = '@';
      raw_name++;
      *raw_name = '@';
      break;
    default:
      pos++;
      assert(pos < raw_len - 1);
      *raw_name = *name;
      break;
    }
    name++;
    raw_name++;
  }

  if (!i) {
    *raw_name = '\0';
  } else {
    r = snprintf(raw_name, raw_len, "@%d", i);
    assert(r < raw_len - pos);
  }
}

static int translate_raw_name(const char *raw_name, char *name, int name_len, bool *is_first)
{
  int pos = 0;

  generic_dout(10) << "translate_raw_name raw_name=" << raw_name << dendl;
  const char *n = name;

  *is_first = true;
  while (*raw_name) {
    switch (*raw_name) {
    case '@': /* escape it */
      raw_name++;
      if (!*raw_name)
        break;
      if (*raw_name != '@') {
        *is_first = false;
        goto done;
      }

    /* fall through */
    default:
      *name = *raw_name;
      break;
    }
    pos++;
    assert(pos < name_len);
    name++;
    raw_name++;
  }
done:
  *name = '\0';
  generic_dout(10) << "translate_raw_name name=" << n << dendl;
  return pos;
}

int do_getxattr_len(const char *fn, const char *name)
{
  int i = 0, total = 0;
  char raw_name[ATTR_MAX_NAME_LEN * 2 + 16];
  int r;

  do {
    get_raw_xattr_name(name, i, raw_name, sizeof(raw_name));
    r = lfn_getxattr(fn, raw_name, 0, 0);
    if (!i && r < 0) {
      return r;
    }
    if (r < 0)
      break;
    total += r;
    i++;
  } while (r == ATTR_MAX_BLOCK_LEN);

  return total;
}

int do_getxattr(const char *fn, const char *name, void *val, size_t size)
{
  int i = 0, pos = 0;
  char raw_name[ATTR_MAX_NAME_LEN * 2 + 16];
  int ret = 0;
  int r;
  size_t chunk_size;

  if (!size)
    return do_getxattr_len(fn, name);

  do {
    chunk_size = (size < ATTR_MAX_BLOCK_LEN ? size : ATTR_MAX_BLOCK_LEN);
    get_raw_xattr_name(name, i, raw_name, sizeof(raw_name));
    size -= chunk_size;

    r = lfn_getxattr(fn, raw_name, (char *)val + pos, chunk_size);
    if (r < 0) {
      ret = r;
      break;
    }

    if (r > 0)
      pos += r;

    i++;
  } while (size && r == ATTR_MAX_BLOCK_LEN);

  if (r >= 0) {
    ret = pos;
    /* is there another chunk? that can happen if the last read size span over
       exactly one block */
    if (chunk_size == ATTR_MAX_BLOCK_LEN) {
      get_raw_xattr_name(name, i, raw_name, sizeof(raw_name));
      r = lfn_getxattr(fn, raw_name, 0, 0);
      if (r > 0) { // there's another chunk.. the original buffer was too small
        ret = -ERANGE;
      }
    }
  }
  return ret;
}

int do_setxattr(const char *fn, const char *name, const void *val, size_t size) {
  int i = 0, pos = 0;
  char raw_name[ATTR_MAX_NAME_LEN * 2 + 16];
  int ret = 0;
  size_t chunk_size;

  do {
    chunk_size = (size < ATTR_MAX_BLOCK_LEN ? size : ATTR_MAX_BLOCK_LEN);
    get_raw_xattr_name(name, i, raw_name, sizeof(raw_name));
    size -= chunk_size;

    int r = lfn_setxattr(fn, raw_name, (char *)val + pos, chunk_size);
    if (r < 0) {
      ret = r;
      break;
    }
    pos  += chunk_size;
    ret = pos;
    i++;
  } while (size);

  /* if we're exactly at a chunk size, remove the next one (if wasn't removed
     before) */
  if (ret >= 0 && chunk_size == ATTR_MAX_BLOCK_LEN) {
    get_raw_xattr_name(name, i, raw_name, sizeof(raw_name));
    lfn_removexattr(fn, raw_name);
  }
  
  return ret;
}

int do_removexattr(const char *fn, const char *name) {
  int i = 0;
  char raw_name[ATTR_MAX_NAME_LEN * 2 + 16];
  int r;

  do {
    get_raw_xattr_name(name, i, raw_name, sizeof(raw_name));
    r = lfn_removexattr(fn, raw_name);
    if (!i && r < 0) {
      return r;
    }
    i++;
  } while (r >= 0);
  return 0;
}

int do_listxattr(const char *fn, char *names, size_t len) {
  int r;

  if (!len)
   return lfn_listxattr(fn, names, len);

  r = lfn_listxattr(fn, 0, 0);
  if (r < 0)
    return r;

  size_t total_len = r  * 2; // should be enough
  char *full_buf = (char *)malloc(total_len * 2);
  if (!full_buf)
    return -ENOMEM;

  r = lfn_listxattr(fn, full_buf, total_len);
  if (r < 0)
    return r;

  char *p = full_buf;
  char *end = full_buf + r;
  char *dest = names;
  char *dest_end = names + len;

  while (p < end) {
    char name[ATTR_MAX_NAME_LEN * 2 + 16];
    int attr_len = strlen(p);
    bool is_first;
    int name_len = translate_raw_name(p, name, sizeof(name), &is_first);
    if (is_first)  {
      if (dest + name_len > dest_end) {
        r = -ERANGE;
        goto done;
      }
      strcpy(dest, name);
      dest += name_len + 1;
    }
    p += attr_len + 1;
  }
  r = dest - names;

done:
  free(full_buf);
  return r;
}

FileStore::FileStore(const std::string &base, const std::string &jdev) :
  basedir(base), journalpath(jdev),
  fsid(0),
  btrfs(false), btrfs_trans_start_end(false), btrfs_clone_range(false),
  btrfs_snap_create(false),
  btrfs_snap_destroy(false),
  btrfs_snap_create_v2(false),
  btrfs_wait_sync(false),
  ioctl_fiemap(false),
  fsid_fd(-1), op_fd(-1),
  basedir_fd(-1), current_fd(-1),
  attrs(this), fake_attrs(false),
  collections(this), fake_collections(false),
  lock("FileStore::lock"),
  force_sync(false), sync_epoch(0),
  sync_entry_timeo_lock("sync_entry_timeo_lock"),
  timer(sync_entry_timeo_lock),
  stop(false), sync_thread(this),
  op_queue_len(0), op_queue_bytes(0), next_finish(0),
  op_tp("FileStore::op_tp", g_conf.filestore_op_threads), op_wq(this, &op_tp),
  flusher_queue_len(0), flusher_thread(this),
  logger(NULL)
{
  ostringstream oss;
  oss << basedir << "/current";
  current_fn = oss.str();

  ostringstream sss;
  sss << basedir << "/current/commit_op_seq";
  current_op_seq_fn = sss.str();
}

static void get_attrname(const char *name, char *buf, int len)
{
  snprintf(buf, len, "user.ceph.%s", name);
}

bool parse_attrname(char **name)
{
  if (strncmp(*name, "user.ceph.", 10) == 0) {
    *name += 10;
    return true;
  }
  return false;
}

static int do_fiemap(int fd, off_t start, size_t len, struct fiemap **pfiemap)
{
  struct fiemap *fiemap = NULL;
  int size;
  int ret;

  fiemap = (struct fiemap*)calloc(sizeof(struct fiemap), 1);
  if (!fiemap)
    return -ENOMEM;

  fiemap->fm_start = start;
  fiemap->fm_length = len;

  fsync(fd); /* flush extents to disk if needed */

  if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
    ret = -errno;
    goto done_err;
  }

  size = sizeof(struct fiemap_extent) * (fiemap->fm_mapped_extents);

  fiemap = (struct fiemap *)realloc(fiemap, sizeof(struct fiemap) +
                                    size);
  if (!fiemap) {
    ret = -ENOMEM;
    goto done_err;
  }

  memset(fiemap->fm_extents, 0, size);

  fiemap->fm_extent_count = fiemap->fm_mapped_extents;
  fiemap->fm_mapped_extents = 0;

  if (ioctl(fd, FS_IOC_FIEMAP, fiemap) < 0) {
    ret = -errno;
    goto done_err;
  }
  *pfiemap = fiemap;

  return 0;

done_err:
  *pfiemap = NULL;
  free(fiemap);
  return ret;
}

int FileStore::statfs(struct statfs *buf)
{
  if (::statfs(basedir.c_str(), buf) < 0)
    return -errno;
  return 0;
}


/* 
 * sorry, these are sentitive to the sobject_t and coll_t typing.
 */ 

  //           11111111112222222222333333333344444444445555555555
  // 012345678901234567890123456789012345678901234567890123456789
  // yyyyyyyyyyyyyyyy.zzzzzzzz.a_s

void FileStore::append_oname(const sobject_t &oid, char *s, int len)
{
  //assert(sizeof(oid) == 28);
  char *t = s + strlen(s);

  *t++ = '/';
  const char *i = oid.oid.name.c_str();
  while (*i) {
    if (*i == '\\') {
      *t++ = '\\';
      *t++ = '\\';      
    } else if (*i == '.' && i == oid.oid.name.c_str()) {  // only escape leading .
      *t++ = '\\';
      *t++ = '.';
    } else if (*i == '/') {
      *t++ = '\\';
      *t++ = 's';
    } else
      *t++ = *i;
    i++;
  }

  if (oid.snap == CEPH_NOSNAP)
    snprintf(t, len, "_head");
  else if (oid.snap == CEPH_SNAPDIR)
    snprintf(t, len, "_snapdir");
  else
    snprintf(t, len, "_%llx", (long long unsigned)oid.snap);
  //parse_object(t+1);
}

bool FileStore::parse_object(char *s, sobject_t& o)
{
  char *bar = s + strlen(s) - 1;
  while (*bar != '_' &&
	 bar > s)
    bar--;
  if (*bar == '_') {
    char buf[bar-s + 1];
    char *t = buf;
    char *i = s;
    while (i < bar) {
      if (*i == '\\') {
	i++;
	switch (*i) {
	case '\\': *t++ = '\\'; break;
	case '.': *t++ = '.'; break;
	case 's': *t++ = '/'; break;
	default: assert(0);
	}
      } else {
	*t++ = *i;
      }
      i++;
    }
    *t = 0;
    o.oid.name = string(buf, t-buf);
    if (strcmp(bar+1, "head") == 0)
      o.snap = CEPH_NOSNAP;
    else if (strcmp(bar+1, "snapdir") == 0)
      o.snap = CEPH_SNAPDIR;
    else
      o.snap = strtoull(bar+1, &s, 16);
    return true;
  }
  return false;
}

  //           11111111112222222222333
  // 012345678901234567890123456789012
  // pppppppppppppppp.ssssssssssssssss

void FileStore::get_cdir(coll_t cid, char *s, int len) 
{
  const string &cid_str(cid.to_str());
  snprintf(s, len, "%s/current/%s", basedir.c_str(), cid_str.c_str());
}

void FileStore::get_coname(coll_t cid, const sobject_t& oid, char *s, int len) 
{
  get_cdir(cid, s, len);
  append_oname(oid, s, len);
}

int FileStore::open_journal()
{
  if (journalpath.length()) {
    dout(10) << "open_journal at " << journalpath << dendl;
    journal = new FileJournal(fsid, &finisher, &sync_cond, journalpath.c_str(), g_conf.journal_dio);
  }
  return 0;
}

int FileStore::wipe_subvol(const char *s)
{
  struct btrfs_ioctl_vol_args volargs;
  memset(&volargs, 0, sizeof(volargs));
  strcpy(volargs.name, s);
  int fd = ::open(basedir.c_str(), O_RDONLY);
  if (fd < 0) {
    int err = errno;
    derr << "FileStore::wipe_subvol: failed to open " << basedir << ": "
	 << cpp_strerror(err) << dendl;
    return -err;
  }
  int r = ::ioctl(fd, BTRFS_IOC_SNAP_DESTROY, &volargs);
  TEMP_FAILURE_RETRY(::close(fd));
  if (r == 0) {
    dout(0) << "mkfs  removed old subvol " << s << dendl;
    return 0;
  }

  dout(0) << "mkfs  removing old directory " << s << dendl;
  ostringstream old_dir;
  old_dir << basedir << "/" << s;
  DIR *dir = ::opendir(old_dir.str().c_str());
  if (!dir) {
    int err = errno;
    derr << "FileStore::wipe_subvol: failed to opendir " << old_dir << ": "
	 << cpp_strerror(err) << dendl;
    return -err;
  }
  struct dirent *de;
  char buf[PATH_MAX];
  while (::readdir_r(dir, (struct dirent*)buf, &de) == 0) {
    if (!de)
      break;
    if (strcmp(de->d_name, ".") == 0 ||
	strcmp(de->d_name, "..") == 0)
      continue;
    ostringstream oss;
    oss << old_dir.str().c_str() << "/" << de->d_name;
    int ret = run_cmd("rm", "-rf", oss.str().c_str(), (char*)NULL);
    if (ret) {
      derr << "FileStore::wipe_subvol: failed to remove " << oss.str() << ": "
	   << "error " << ret << dendl;
      ::closedir(dir);
      return ret;
    }
  }
  ::closedir(dir);
  return 0;
}

int FileStore::mkfs()
{
  int ret = 0;
  char buf[PATH_MAX];
  DIR *dir;
  struct dirent *de;
  int basedir_fd;
  struct btrfs_ioctl_vol_args volargs;

  if (!g_conf.filestore_dev.empty()) {
    dout(0) << "mounting" << dendl;
    ret = run_cmd("mount", g_conf.filestore_dev.c_str(), (char*)NULL);
    if (ret) {
      derr << "FileStore::mkfs: failed to mount g_conf.filestore_dev "
	   << "'" << g_conf.filestore_dev << "'. Error code " << ret << dendl;
      goto out;
    }
  }

  dout(1) << "mkfs in " << basedir << dendl;

  snprintf(buf, sizeof(buf), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(buf, O_CREAT|O_WRONLY, 0644);

  if (fsid_fd < 0) {
    ret = -errno;
    derr << "FileStore::mkfs: failed to open " << buf << ": "
	 << cpp_strerror(ret) << dendl;
    goto out;
  }
  if (lock_fsid() < 0) {
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  // wipe
  dir = ::opendir(basedir.c_str());
  if (!dir) {
    ret = -errno;
    derr << "FileStore::mkfs: failed to opendir " << basedir << dendl;
    goto close_fsid_fd;
  }
  while (::readdir_r(dir, (struct dirent*)buf, &de) == 0) {
    if (!de)
      break;
    if (strcmp(de->d_name, ".") == 0 ||
	strcmp(de->d_name, "..") == 0)
      continue;
    if (strcmp(de->d_name, "fsid") == 0)
      continue;

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s", basedir.c_str(), de->d_name);
    struct stat st;
    if (::stat(path, &st)) {
      ret = -errno;
      derr << "FileStore::mkfs: failed to stat " << de->d_name << dendl;
      goto close_dir;
    }
    if (S_ISDIR(st.st_mode)) {
      ret = wipe_subvol(de->d_name);
      if (ret)
	goto close_dir;
    }
    else {
      if (::unlink(path)) {
	ret = -errno;
	derr << "FileStore::mkfs: failed to remove old file "
	     << de->d_name << dendl;
	goto close_dir;
      }
      dout(0) << "mkfs  removing old file " << de->d_name << dendl;
    }
  }

  // fsid
  srand(time(0) + getpid());
  fsid = rand();
  ret = safe_write(fsid_fd, &fsid, sizeof(fsid));
  if (ret) {
    derr << "FileStore::mkfs: failed to write fsid: "
	 << cpp_strerror(ret) << dendl;
    goto close_fsid_fd;
  }
  if (TEMP_FAILURE_RETRY(::close(fsid_fd))) {
    ret = errno;
    derr << "FileStore::mkfs: close failed: can't write fsid: "
	 << cpp_strerror(ret) << dendl;
    fsid_fd = -1;
    goto close_fsid_fd;
  }
  fsid_fd = -1;
  dout(10) << "mkfs fsid is " << fsid << dendl;

  // current
  memset(&volargs, 0, sizeof(volargs));
  basedir_fd = ::open(basedir.c_str(), O_RDONLY);
  volargs.fd = 0;
  strcpy(volargs.name, "current");
  if (::ioctl(basedir_fd, BTRFS_IOC_SUBVOL_CREATE, (unsigned long int)&volargs)) {
    ret = errno;
    if (ret == EEXIST) {
      dout(2) << " current already exists" << dendl;
      // not fatal
    }
    else if (ret == EOPNOTSUPP || ret == ENOTTY) {
      dout(2) << " BTRFS_IOC_SUBVOL_CREATE ioctl failed, trying mkdir "
	      << current_fn << dendl;
      if (::mkdir(current_fn.c_str(), 0755)) {
	ret = errno;
	if (ret != EEXIST) {
	  derr << "FileStore::mkfs: mkdir " << current_fn << " failed: "
	       << cpp_strerror(ret) << dendl;
	  goto close_basedir_fd;
	}
      }
    }
    else {
      derr << "FileStore::mkfs: BTRFS_IOC_SUBVOL_CREATE failed with error "
	   << cpp_strerror(ret) << dendl;
      goto close_basedir_fd;
    }
  }
  else {
    // ioctl succeeded. yay
    dout(2) << " created btrfs subvol " << current_fn << dendl;
    if (::chmod(current_fn.c_str(), 0755)) {
      ret = -errno;
      derr << "FileStore::mkfs: failed to chmod " << current_fn << " to 0755: "
	   << cpp_strerror(ret) << dendl;
      goto close_basedir_fd;
    }
  }

  // journal?
  ret = mkjournal();
  if (ret)
    goto close_basedir_fd;

  if (!g_conf.filestore_dev.empty()) {
    dout(0) << "umounting" << dendl;
    snprintf(buf, sizeof(buf), "umount %s", g_conf.filestore_dev.c_str());
    //system(cmd);
  }

  dout(1) << "mkfs done in " << basedir << dendl;
  ret = 0;

close_basedir_fd:
  TEMP_FAILURE_RETRY(::close(basedir_fd));
close_dir:
  ::closedir(dir);
close_fsid_fd:
  if (fsid_fd != -1) {
    TEMP_FAILURE_RETRY(::close(fsid_fd));
    fsid_fd = -1;
  }
out:
  return ret;
}

int FileStore::mkjournal()
{
  // read fsid
  int ret;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());
  int fd = ::open(fn, O_RDONLY, 0644);
  if (fd < 0) {
    int err = errno;
    derr << "FileStore::mkjournal: open error: " << cpp_strerror(err) << dendl;
    return -err;
  }
  ret = safe_read(fd, &fsid, sizeof(fsid));
  if (ret < 0) {
    derr << "FileStore::mkjournal: read error: " << cpp_strerror(ret) << dendl;
    TEMP_FAILURE_RETRY(::close(fd));
    return ret;
  }
  if (TEMP_FAILURE_RETRY(::close(fd))) {
    int err = errno;
    derr << "FileStore::mkjournal: close error: " << cpp_strerror(err) << dendl;
    return -err;
  }

  ret = 0;

  open_journal();
  if (journal) {
    ret = journal->create();
    if (ret)
      dout(0) << "mkjournal error creating journal on " << journalpath << dendl;
    else
      dout(0) << "mkjournal created journal on " << journalpath << dendl;
    delete journal;
    journal = 0;
  }
  return ret;
}


int FileStore::lock_fsid()
{
  struct flock l;
  memset(&l, 0, sizeof(l));
  l.l_type = F_WRLCK;
  l.l_whence = SEEK_SET;
  l.l_start = 0;
  l.l_len = 0;
  int r = ::fcntl(fsid_fd, F_SETLK, &l);
  if (r < 0) {
    char buf[80];
    dout(0) << "lock_fsid failed to lock " << basedir << "/fsid, is another cosd still running? " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    return -errno;
  }
  return 0;
}

bool FileStore::test_mount_in_use()
{
  dout(5) << "test_mount basedir " << basedir << " journal " << journalpath << dendl;
  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());

  // verify fs isn't in use

  fsid_fd = ::open(fn, O_RDWR, 0644);
  if (fsid_fd < 0)
    return 0;   // no fsid, ok.
  bool inuse = lock_fsid() < 0;
  ::close(fsid_fd);
  fsid_fd = -1;
  return inuse;
}


int FileStore::_detect_fs()
{
  char buf[80];
  
  // fake collections?
  if (g_conf.filestore_fake_collections) {
    dout(0) << "faking collections (in memory)" << dendl;
    fake_collections = true;
  }

  // xattrs?
  if (g_conf.filestore_fake_attrs) {
    dout(0) << "faking xattrs (in memory)" << dendl;
    fake_attrs = true;
  } else {
    char fn[PATH_MAX];
    int x = rand();
    int y = x+1;
    snprintf(fn, sizeof(fn), "%s/fsid", basedir.c_str());
    int ret = do_setxattr(fn, "user.test", &x, sizeof(x));
    if (ret >= 0)
      ret = do_getxattr(fn, "user.test", &y, sizeof(y));
    if ((ret < 0) || (x != y)) {
      derr << "Extended attributes don't appear to work. ";
      if (ret)
	*_dout << "Got error " + cpp_strerror(ret) + ". ";
      *_dout << "If you are using ext3 or ext4, be sure to mount the underlying "
	     << "file system with the 'user_xattr' option." << dendl;
      return -ENOTSUP;
    }
  }

  int fd = ::open(basedir.c_str(), O_RDONLY);
  if (fd < 0)
    return -errno;

  // fiemap?
  struct fiemap *fiemap;
  int r = do_fiemap(fd, 0, 1, &fiemap);
  if (r == -EOPNOTSUPP) {
    dout(0) << "mount FIEMAP ioctl is NOT supported" << dendl;
    ioctl_fiemap = false;
  } else {
    dout(0) << "mount FIEMAP ioctl is supported" << dendl;
    ioctl_fiemap = true;
  }

  struct statfs st;
  r = ::fstatfs(fd, &st);
  if (r < 0)
    return -errno;

  static const __SWORD_TYPE BTRFS_F_TYPE(0x9123683E);
  if (st.f_type == BTRFS_F_TYPE) {
    dout(0) << "mount detected btrfs" << dendl;      
    btrfs = true;

    // clone_range?
    if (g_conf.filestore_btrfs_clone_range) {
      btrfs_clone_range = true;
      int r = _do_clone_range(fsid_fd, -1, 0, 1);
      if (r == -EBADF) {
	dout(0) << "mount btrfs CLONE_RANGE ioctl is supported" << dendl;
      } else {
	btrfs_clone_range = false;
	dout(0) << "mount btrfs CLONE_RANGE ioctl is NOT supported: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      }
    } else {
      dout(0) << "mount btrfs CLONE_RANGE ioctl is DISABLED via 'filestore btrfs clone range' option" << dendl;
    }

    // snap_create and snap_destroy?
    struct btrfs_ioctl_vol_args volargs;
    volargs.fd = fd;
    strcpy(volargs.name, "sync_snap_test");
    r = ::ioctl(fd, BTRFS_IOC_SNAP_CREATE, &volargs);
    if (r == 0 || errno == EEXIST) {
      dout(0) << "mount btrfs SNAP_CREATE is supported" << dendl;
      btrfs_snap_create = true;

      r = ::ioctl(fd, BTRFS_IOC_SNAP_DESTROY, &volargs);
      if (r == 0) {
	dout(0) << "mount btrfs SNAP_DESTROY is supported" << dendl;
	btrfs_snap_destroy = true;
      } else {
	dout(0) << "mount btrfs SNAP_DESTROY failed: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      }
    } else {
      dout(0) << "mount btrfs SNAP_CREATE failed: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
    }

    if (g_conf.filestore_btrfs_snap && !btrfs_snap_destroy) {
      dout(0) << "mount btrfs snaps enabled, but no SNAP_DESTROY ioctl (from kernel 2.6.32+)" << dendl;
      cerr << TEXT_YELLOW
	   << " ** WARNING: 'filestore btrfs snap' was enabled (for safe transactions, rollback),\n"
	   << "             but btrfs does not support the SNAP_DESTROY ioctl (added in\n"
	   << "             Linux 2.6.32).  Disabling.\n"
	   << TEXT_NORMAL;
      g_conf.filestore_btrfs_snap = false;
    }

    // start_sync?
    __u64 transid = 0;
    r = ::ioctl(fd, BTRFS_IOC_START_SYNC, &transid);
    dout(0) << "mount btrfs START_SYNC got " << r << " " << strerror_r(-r, buf, sizeof(buf)) << dendl;
    if (r == 0 && transid > 0) {
      dout(0) << "mount btrfs START_SYNC is supported (transid " << transid << ")" << dendl;

      // do we have wait_sync too?
      r = ::ioctl(fd, BTRFS_IOC_WAIT_SYNC, &transid);
      if (r == 0 || r == -ERANGE) {
	dout(0) << "mount btrfs WAIT_SYNC is supported" << dendl;
	btrfs_wait_sync = true;
      } else {
	dout(0) << "mount btrfs WAIT_SYNC is NOT supported: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      }
    } else {
      dout(0) << "mount btrfs START_SYNC is NOT supported: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
    }

    if (btrfs_wait_sync) {
      // async snap creation?
      struct btrfs_ioctl_vol_args vol_args;
      vol_args.fd = 0;
      strcpy(vol_args.name, "async_snap_test");

      struct btrfs_ioctl_vol_args_v2 async_args;
      async_args.fd = fd;
      async_args.flags = BTRFS_SUBVOL_CREATE_ASYNC;
      strcpy(async_args.name, "async_snap_test");

      // remove old one, first
      struct stat st;
      if (::fstatat(fd, vol_args.name, &st, 0) == 0) {
	dout(0) << "mount btrfs removing old async_snap_test" << dendl;
	r = ::ioctl(fd, BTRFS_IOC_SNAP_DESTROY, &vol_args);
	if (r != 0)
	  dout(0) << "mount  failed to remove old async_snap_test: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      }

      r = ::ioctl(fd, BTRFS_IOC_SNAP_CREATE_V2, &async_args);
      dout(0) << "mount btrfs SNAP_CREATE_V2 got " << r << " " << strerror_r(-r, buf, sizeof(buf)) << dendl;
      if (r == 0 || errno == EEXIST) {
	dout(0) << "mount btrfs SNAP_CREATE_V2 is supported" << dendl;
	btrfs_snap_create_v2 = true;
      
	// clean up
	r = ::ioctl(fd, BTRFS_IOC_SNAP_DESTROY, &vol_args);
	if (r != 0) {
	  dout(0) << "mount btrfs SNAP_DESTROY failed: " << strerror_r(-r, buf, sizeof(buf)) << dendl;
	}
      } else {
	dout(0) << "mount btrfs SNAP_CREATE_V2 is NOT supported: "
		<< strerror_r(-r, buf, sizeof(buf)) << dendl;
      }
    }

    if (g_conf.filestore_btrfs_snap && !btrfs_snap_create_v2) {
      dout(0) << "mount WARNING: btrfs snaps enabled, but no SNAP_CREATE_V2 ioctl (from kernel 2.6.37+)" << dendl;
      cerr << TEXT_YELLOW
	   << " ** WARNING: 'filestore btrfs snap' is enabled (for safe transactions,\n"	 
	   << "             rollback), but btrfs does not support the SNAP_CREATE_V2 ioctl\n"
	   << "             (added in Linux 2.6.37).  Expect slow btrfs sync/commit\n"
	   << "             performance.\n"
	   << TEXT_NORMAL;
    }

  } else {
    dout(0) << "mount did NOT detect btrfs" << dendl;
    btrfs = false;
  }
  ::close(fd);
  return 0;
}

int FileStore::_sanity_check_fs()
{
  // sanity check(s)

  if ((int)g_conf.filestore_journal_writeahead +
      (int)g_conf.filestore_journal_parallel +
      (int)g_conf.filestore_journal_trailing > 1) {
    dout(0) << "mount ERROR: more than one of filestore journal {writeahead,parallel,trailing} enabled" << dendl;
    cerr << TEXT_RED 
	 << " ** WARNING: more than one of 'filestore journal {writeahead,parallel,trailing}'\n"
	 << "             is enabled in ceph.conf.  You must choose a single journal mode."
	 << TEXT_NORMAL << std::endl;
    return -EINVAL;
  }

  if (!btrfs) {
    if (!journal || !g_conf.filestore_journal_writeahead) {
      dout(0) << "mount WARNING: no btrfs, and no journal in writeahead mode; data may be lost" << dendl;
      cerr << TEXT_RED 
	   << " ** WARNING: no btrfs AND (no journal OR journal not in writeahead mode)\n"
	   << "             For non-btrfs volumes, a writeahead journal is required to\n"
	   << "             maintain on-disk consistency in the event of a crash.  Your conf\n"
	   << "             should include something like:\n"
	   << "        osd journal = /path/to/journal_device_or_file\n"
	   << "        filestore journal writeahead = true\n"
	   << TEXT_NORMAL;
    }

    // ext3?
    struct statfs buf;
    int r = ::statfs(basedir.c_str(), &buf);
    if (r == 0 && buf.f_type != 0xEF53 /*EXT3_SUPER_MAGIC*/) {
      dout(0) << "mount WARNING: not btrfs or ext3; data may be lost" << dendl;
      cerr << TEXT_YELLOW
	   << " ** WARNING: not btrfs or ext3.  We don't currently support file systems other\n"
	   << "             than btrfs and ext3 (data=journal or data=ordered).  Data may be\n"
	   << "             lost in the event of a crash.\n"
	   << TEXT_NORMAL;
    }    
  }

  if (!journal) {
    dout(0) << "mount WARNING: no journal" << dendl;
    cerr << TEXT_YELLOW
	 << " ** WARNING: No osd journal is configured: write latency may be high.\n"
	 << "             If you will not be using an osd journal, write latency may be\n"
	 << "             relatively high.  It can be reduced somewhat by lowering\n"
	 << "             filestore_max_sync_interval, but lower values mean lower write\n"
	 << "             throughput, especially with spinning disks.\n"
	 << TEXT_NORMAL;
  }

  return 0;
}


int FileStore::read_op_seq(const char *fn, uint64_t *seq)
{
  int op_fd = lfn_open(current_op_seq_fn.c_str(), O_CREAT|O_RDWR, 0644);
  if (op_fd < 0)
    return -errno;
  char s[40];
  memset(s, 0, sizeof(s));
  int ret = safe_read(op_fd, s, sizeof(s) - 1);
  if (ret < 0) {
    derr << "error reading " << current_op_seq_fn << ": "
	 << cpp_strerror(ret) << dendl;
    TEMP_FAILURE_RETRY(::close(op_fd));
    return ret;
  }
  *seq = atoll(s);
  return op_fd;
}

int FileStore::write_op_seq(int fd, uint64_t seq)
{
  char s[30];
  int ret;
  snprintf(s, sizeof(s), "%" PRId64 "\n", seq);
  ret = TEMP_FAILURE_RETRY(::pwrite(fd, s, strlen(s), 0));
  return ret;
}

int FileStore::mount() 
{
  int ret;
  char buf[PATH_MAX];
  uint64_t initial_op_seq;

  if (!g_conf.filestore_dev.empty()) {
    dout(0) << "mounting" << dendl;
    //run_cmd("mount", g_conf.filestore_dev, (char*)NULL);
  }

  dout(5) << "basedir " << basedir << " journal " << journalpath << dendl;
  
  // make sure global base dir exists
  if (::access(basedir.c_str(), R_OK | W_OK)) {
    ret = -errno;
    derr << "FileStore::mount: unable to access basedir '" << basedir << "': "
	    << cpp_strerror(ret) << dendl;
    goto done;
  }

  // test for btrfs, xattrs, etc.
  ret = _detect_fs();
  if (ret)
    goto done;

  // get fsid
  snprintf(buf, sizeof(buf), "%s/fsid", basedir.c_str());
  fsid_fd = ::open(buf, O_RDWR|O_CREAT, 0644);
  if (fsid_fd < 0) {
    ret = -errno;
    derr << "FileStore::mount: error opening '" << buf << "': "
	 << cpp_strerror(ret) << dendl;
    goto done;
  }

  fsid = 0;
  ret = safe_read_exact(fsid_fd, &fsid, sizeof(fsid));
  if (ret) {
    derr << "FileStore::mount: error reading fsid_fd: " << cpp_strerror(ret)
	 << dendl;
    goto close_fsid_fd;
  }

  if (lock_fsid() < 0) {
    derr << "FileStore::mount: lock_fsid failed" << dendl;
    ret = -EBUSY;
    goto close_fsid_fd;
  }

  dout(10) << "mount fsid is " << fsid << dendl;

  // open some dir handles
  basedir_fd = ::open(basedir.c_str(), O_RDONLY);
  if (basedir_fd < 0) {
    ret = -errno;
    derr << "FileStore::mount: failed to open " << basedir << ": "
	 << cpp_strerror(ret) << dendl;
    basedir_fd = -1;
    goto close_fsid_fd;
  }

  {
    // get snap list
    DIR *dir = ::opendir(basedir.c_str());
    if (!dir) {
      ret = -errno;
      derr << "FileStore::mount: opendir '" << basedir << "' failed: "
	   << cpp_strerror(ret) << dendl;
      goto close_basedir_fd;
    }

    struct dirent *de;
    while (::readdir_r(dir, (struct dirent *)buf, &de) == 0) {
      if (!de)
	break;
      long long unsigned c;
      if (sscanf(de->d_name, COMMIT_SNAP_ITEM, &c) == 1)
	snaps.push_back(c);
    }
    
    if (::closedir(dir) < 0) {
      ret = -errno;
      derr << "FileStore::closedir(basedir) failed: error " << cpp_strerror(ret)
	   << dendl;
      goto close_basedir_fd;
    }

    dout(0) << "mount found snaps " << snaps << dendl;
  }

  if (btrfs && g_conf.filestore_btrfs_snap) {
    if (snaps.empty()) {
      dout(0) << "mount WARNING: no consistent snaps found, store may be in inconsistent state" << dendl;
    } else if (!btrfs) {
      dout(0) << "mount WARNING: not btrfs, store may be in inconsistent state" << dendl;
    } else {
      uint64_t cp = snaps.back();
      uint64_t curr_seq;

      {
	int fd = read_op_seq(current_op_seq_fn.c_str(), &curr_seq);
	assert(fd >= 0);
	TEMP_FAILURE_RETRY(::close(fd));
      }
      dout(10) << "*** curr_seq=" << curr_seq << " cp=" << cp << dendl;
     
      if (cp != curr_seq && !g_conf.osd_use_stale_snap) { 
        derr << TEXT_RED
	     << " ** ERROR: current volume data version is not equal to snapshotted version\n"
	     << "           which can lead to data inconsistency. \n"
	     << "           Current version=" << curr_seq << " snapshot version=" << cp << "\n"
	     << "           Startup with snapshotted version can be forced using the\n"
             <<"            'osd use stale snap = true' config option.\n"
	     << TEXT_NORMAL << dendl;
	ret = -ENOTSUP;
	goto close_basedir_fd;
      }

      if (cp != curr_seq) {
        derr << "WARNING: user forced start with data sequence mismatch: curr=" << curr_seq << " snap_seq=" << cp << dendl;
        cerr << TEXT_YELLOW
	     << " ** WARNING: forcing the use of stale snapshot data\n" << TEXT_NORMAL;
      }

      // drop current
      btrfs_ioctl_vol_args vol_args;
      vol_args.fd = 0;
      strcpy(vol_args.name, "current");
      ret = ::ioctl(basedir_fd,
		      BTRFS_IOC_SNAP_DESTROY,
		      &vol_args);
      if (ret) {
	ret = errno;
	derr << "FileStore::mount: error removing old current subvol: "
	     << cpp_strerror(ret) << dendl;
	char s[PATH_MAX];
	snprintf(s, sizeof(s), "%s/current.remove.me.%d", basedir.c_str(), rand());
	if (::rename(current_fn.c_str(), s)) {
	  ret = -errno;
	  derr << "FileStore::mount: error renaming old current subvol: "
	       << cpp_strerror(ret) << dendl;
	  goto close_basedir_fd;
	}
      }

      // roll back
      char s[PATH_MAX];
      snprintf(s, sizeof(s), "%s/" COMMIT_SNAP_ITEM, basedir.c_str(), (long long unsigned)cp);
      vol_args.fd = lfn_open(s, O_RDONLY);
      if (vol_args.fd < 0) {
	ret = -errno;
	derr << "FileStore::mount: error opening '" << s << "': "
	     << cpp_strerror(ret) << dendl;
	goto close_basedir_fd;
      }
      if (::ioctl(basedir_fd, BTRFS_IOC_SNAP_CREATE, &vol_args)) {
	ret = -errno;
	derr << "FileStore::mount: error ioctl(BTRFS_IOC_SNAP_CREATE) failed: "
	     << cpp_strerror(ret) << dendl;
	TEMP_FAILURE_RETRY(::close(vol_args.fd));
	goto close_basedir_fd;
      }
      TEMP_FAILURE_RETRY(::close(vol_args.fd));
      dout(10) << "mount rolled back to consistent snap " << cp << dendl;
      snaps.pop_back();

      if (cp != curr_seq) {
        int fd = read_op_seq(current_op_seq_fn.c_str(), &curr_seq);
	assert(fd >= 0);
        /* we'll use the higher version from now on */
        curr_seq = cp;
        write_op_seq(fd, curr_seq);
	TEMP_FAILURE_RETRY(::close(fd));
      }
    }
  }
  initial_op_seq = 0;

  current_fd = lfn_open(current_fn.c_str(), O_RDONLY);
  if (current_fd < 0) {
    derr << "FileStore::mount: error opening: " << current_fn << ": "
	 << cpp_strerror(ret) << dendl;
    goto close_basedir_fd;
  }

  assert(current_fd >= 0);

  op_fd = read_op_seq(current_op_seq_fn.c_str(), &initial_op_seq);
  if (op_fd < 0) {
    derr << "FileStore::mount: read_op_seq failed" << dendl;
    goto close_current_fd;
  }

  dout(5) << "mount op_seq is " << initial_op_seq << dendl;

  // journal
  open_journal();

  // select journal mode?
  if (journal) {
    if (!g_conf.filestore_journal_writeahead &&
	!g_conf.filestore_journal_parallel &&
	!g_conf.filestore_journal_trailing) {
      if (!btrfs) {
	g_conf.filestore_journal_writeahead = true;
	dout(0) << "mount: enabling WRITEAHEAD journal mode: btrfs not detected" << dendl;
      } else if (!g_conf.filestore_btrfs_snap) {
	g_conf.filestore_journal_writeahead = true;
	dout(0) << "mount: enabling WRITEAHEAD journal mode: 'filestore btrfs snap' mode is not enabled" << dendl;
      } else if (!btrfs_snap_create_v2) {
	g_conf.filestore_journal_writeahead = true;
	dout(0) << "mount: enabling WRITEAHEAD journal mode: btrfs SNAP_CREATE_V2 ioctl not detected (v2.6.37+)" << dendl;
      } else {
	g_conf.filestore_journal_parallel = true;
	dout(0) << "mount: enabling PARALLEL journal mode: btrfs, SNAP_CREATE_V2 detected and 'filestore btrfs snap' mode is enabled" << dendl;
      }
    } else {
      if (g_conf.filestore_journal_writeahead)
	dout(0) << "mount: WRITEAHEAD journal mode explicitly enabled in conf" << dendl;
      if (g_conf.filestore_journal_parallel)
	dout(0) << "mount: PARALLEL journal mode explicitly enabled in conf" << dendl;
      if (g_conf.filestore_journal_trailing)
	dout(0) << "mount: TRAILING journal mode explicitly enabled in conf" << dendl;
    }
    if (g_conf.filestore_journal_writeahead)
      journal->set_wait_on_full(true);
  }

  ret = _sanity_check_fs();
  if (ret) {
    derr << "FileStore::mount: _sanity_check_fs failed with error "
	 << ret << dendl;
    goto close_current_fd;
  }

  ret = journal_replay(initial_op_seq);
  if (ret < 0) {
    derr << "mount failed to open journal " << journalpath << ": "
	 << cpp_strerror(ret) << dendl;
    if (ret == -ENOTTY) {
      derr << "maybe journal is not pointing to a block device and its size "
	   << "wasn't configured?" << dendl;
    }
    goto close_current_fd;
  }

  journal_start();

  sync_thread.create();
  op_tp.start();
  flusher_thread.create();
  op_finisher.start();
  ondisk_finisher.start();

  timer.init();

  // all okay.
  return 0;

close_current_fd:
  TEMP_FAILURE_RETRY(::close(current_fd));
  current_fd = -1;
close_basedir_fd:
  TEMP_FAILURE_RETRY(::close(basedir_fd));
  basedir_fd = -1;
close_fsid_fd:
  fsid = 0;
  TEMP_FAILURE_RETRY(::close(fsid_fd));
  fsid_fd = -1;
done:
  return ret;
}

int FileStore::umount() 
{
  dout(5) << "umount " << basedir << dendl;
  
  start_sync();

  lock.Lock();
  stop = true;
  sync_cond.Signal();
  flusher_cond.Signal();
  lock.Unlock();
  sync_thread.join();
  op_tp.stop();
  flusher_thread.join();

  journal_stop();
  stop_logger();

  op_finisher.stop();
  ondisk_finisher.stop();

  if (fsid_fd >= 0) {
    TEMP_FAILURE_RETRY(::close(fsid_fd));
    fsid_fd = -1;
  }
  if (op_fd >= 0) {
    TEMP_FAILURE_RETRY(::close(op_fd));
    op_fd = -1;
  }
  if (current_fd >= 0) {
    TEMP_FAILURE_RETRY(::close(current_fd));
    current_fd = -1;
  }
  if (basedir_fd >= 0) {
    TEMP_FAILURE_RETRY(::close(basedir_fd));
    basedir_fd = -1;
  }

  if (!g_conf.filestore_dev.empty()) {
    dout(0) << "umounting" << dendl;
    //run_cmd("umount", g_conf.filestore_dev, (char*)NULL);
  }

  {
    Mutex::Locker l(sync_entry_timeo_lock);
    timer.shutdown();
  }

  // nothing
  return 0;
}


int FileStore::get_max_object_name_length()
{
  lock.Lock();
  int ret = pathconf(basedir.c_str(), _PC_NAME_MAX);
  if (ret < 0) {
    int err = errno;
    lock.Unlock();
    if (err == 0)
      return -EDOM;
    return -err;
  }
  lock.Unlock();
  return ret;
}


void FileStore::start_logger(int whoami, utime_t tare)
{
  dout(10) << "start_logger" << dendl;
  assert(!logger);

  static ProfLogType fs_logtype(l_os_first, l_os_last);
  static bool didit = false;
  if (!didit) {
    didit = true;

    fs_logtype.add_inc(l_os_in_ops, "in_o");
    //fs_logtype.add_inc(l_os_in_bytes, "in_b");
    fs_logtype.add_inc(l_os_readable_ops, "or_o");
    fs_logtype.add_inc(l_os_readable_bytes, "or_b");
    //fs_logtype.add_inc(l_os_commit_bytes, "com_o");
    //fs_logtype.add_inc(l_os_commit_bytes, "com_b");

    fs_logtype.add_set(l_os_jq_max_ops, "jq_mo");
    fs_logtype.add_set(l_os_jq_ops, "jq_o");
    fs_logtype.add_inc(l_os_j_ops, "j_o");
    fs_logtype.add_set(l_os_jq_max_bytes, "jq_mb");
    fs_logtype.add_set(l_os_jq_bytes, "jq_b");
    fs_logtype.add_inc(l_os_j_bytes, "j_b");
    fs_logtype.add_set(l_os_oq_max_ops, "oq_mo");
    fs_logtype.add_set(l_os_oq_ops, "oq_o");
    fs_logtype.add_inc(l_os_ops, "o");
    fs_logtype.add_set(l_os_oq_max_bytes, "oq_mb");
    fs_logtype.add_set(l_os_oq_bytes, "oq_b");
    fs_logtype.add_inc(l_os_bytes, "b");
    fs_logtype.add_set(l_os_committing, "comitng");
  }

  char name[80];
  snprintf(name, sizeof(name), "osd.%d.fs.log", whoami);
  logger = new ProfLogger(name, (ProfLogType*)&fs_logtype);
  if (journal)
    journal->logger = logger;
  logger_add(logger);  
  logger_tare(tare);
  logger_start();
}

void FileStore::stop_logger()
{
  dout(10) << "stop_logger" << dendl;
  if (logger) {
    if (journal)
      journal->logger = NULL;
    logger_remove(logger);
    delete logger;
    logger = NULL;
  }
}




/// -----------------------------

FileStore::Op *FileStore::build_op(list<Transaction*>& tls,
				   Context *onreadable, Context *onreadable_sync)
{
  uint64_t bytes = 0, ops = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       p++) {
    bytes += (*p)->get_num_bytes();
    ops += (*p)->get_num_ops();
  }

  Op *o = new Op;
  o->tls.swap(tls);
  o->onreadable = onreadable;
  o->onreadable_sync = onreadable_sync;
  o->ops = ops;
  o->bytes = bytes;
  return o;
}



void FileStore::queue_op(OpSequencer *osr, Op *o)
{
  assert(journal_lock.is_locked());
  // initialize next_finish on first op
  if (next_finish == 0)
    next_finish = op_seq;

  // mark apply start _now_, because we need to drain the entire apply
  // queue during commit in order to put the store in a consistent
  // state.
  _op_apply_start(o->op);
  op_tp.lock();

  osr->queue(o);

  if (logger) {
    logger->inc(l_os_ops);
    logger->inc(l_os_bytes, o->bytes);
    logger->set(l_os_oq_ops, op_queue_len);
    logger->set(l_os_oq_bytes, op_queue_bytes);
  }

  op_tp.unlock();

  dout(5) << "queue_op " << o << " seq " << o->op << " " << o->bytes << " bytes"
	   << "   (queue has " << op_queue_len << " ops and " << op_queue_bytes << " bytes)"
	   << dendl;
  op_wq.queue(osr);
}

void FileStore::op_queue_reserve_throttle(Op *o)
{
  op_tp.lock();
  _op_queue_reserve_throttle(o, "op_queue_reserve_throttle");
  op_tp.unlock();
}

void FileStore::_op_queue_reserve_throttle(Op *o, const char *caller)
{
  // Do not call while holding the journal lock!
  uint64_t max_ops = g_conf.filestore_queue_max_ops;
  uint64_t max_bytes = g_conf.filestore_queue_max_bytes;

  if (is_committing()) {
    max_ops += g_conf.filestore_queue_committing_max_ops;
    max_bytes += g_conf.filestore_queue_committing_max_bytes;
  }

  if (logger) {
    logger->set(l_os_oq_max_ops, max_ops);
    logger->set(l_os_oq_max_bytes, max_bytes);
  }

  while ((max_ops && (op_queue_len + 1) > max_ops) ||
	 (max_bytes && op_queue_bytes      // let single large ops through!
	  && (op_queue_bytes + o->bytes) > max_bytes)) {
    dout(2) << caller << " waiting: "
	     << op_queue_len + 1 << " > " << max_ops << " ops || "
	     << op_queue_bytes + o->bytes << " > " << max_bytes << dendl;
    op_tp.wait(op_throttle_cond);
  }

  op_queue_len++;
  op_queue_bytes += o->bytes;
}

void FileStore::_op_queue_release_throttle(Op *o)
{
  // Called with op_tp lock!
  op_queue_len--;
  op_queue_bytes -= o->bytes;
  op_throttle_cond.Signal();
}

void FileStore::_do_op(OpSequencer *osr)
{
  osr->apply_lock.Lock();
  Op *o = osr->peek_queue();

  dout(5) << "_do_op " << o << " " << o->op << " osr " << osr << "/" << osr->parent << " start" << dendl;
  int r = do_transactions(o->tls, o->op);
  op_apply_finish(o->op);
  dout(10) << "_do_op " << o << " " << o->op << " r = " << r
	   << ", finisher " << o->onreadable << " " << o->onreadable_sync << dendl;
  
  /*dout(10) << "op_entry finished " << o->bytes << " bytes, queue now "
	   << op_queue_len << " ops, " << op_queue_bytes << " bytes" << dendl;
  */
}

void FileStore::_finish_op(OpSequencer *osr)
{
  Op *o = osr->dequeue();
  
  dout(10) << "_finish_op on osr " << osr << "/" << osr->parent << dendl;
  osr->apply_lock.Unlock();  // locked in _do_op

  // called with tp lock held
  _op_queue_release_throttle(o);

  if (logger) {
    logger->inc(l_os_readable_ops);
    logger->inc(l_os_readable_bytes, o->bytes);
  }

  if (next_finish == o->op) {
    dout(10) << "_finish_op " << o->op << " next_finish " << next_finish
	     << " queueing " << o->onreadable << " doing " << o->onreadable_sync << dendl;
    next_finish++;
    if (o->onreadable_sync) {
      o->onreadable_sync->finish(0);
      delete o->onreadable_sync;
    }
    op_finisher.queue(o->onreadable);

    while (finish_queue.begin()->first == next_finish) {
      Context *c = finish_queue.begin()->second.first;
      Context *s = finish_queue.begin()->second.second;
      finish_queue.erase(finish_queue.begin());
      dout(10) << "_finish_op " << o->op << " next_finish " << next_finish
	       << " queueing delayed " << c << " doing " << s << dendl;
      if (s) {
	s->finish(0);
	delete s;
      }
      op_finisher.queue(c);
      next_finish++;
    }
  } else {
    dout(10) << "_finish_op " << o->op << " next_finish " << next_finish
	     << ", delaying " << o->onreadable << dendl;
    finish_queue[o->op] = pair<Context*,Context*>(o->onreadable, o->onreadable_sync);
  }

  delete o;
}


struct C_JournaledAhead : public Context {
  FileStore *fs;
  FileStore::OpSequencer *osr;
  FileStore::Op *o;
  Context *ondisk;

  C_JournaledAhead(FileStore *f, FileStore::OpSequencer *os, FileStore::Op *o, Context *ondisk):
    fs(f), osr(os), o(o), ondisk(ondisk) { }
  void finish(int r) {
    fs->_journaled_ahead(osr, o, ondisk);
  }
};

int FileStore::queue_transaction(Sequencer *osr, Transaction *t)
{
  list<Transaction*> tls;
  tls.push_back(t);
  return queue_transactions(osr, tls, new C_DeleteTransaction(t));
}

int FileStore::queue_transactions(Sequencer *posr, list<Transaction*> &tls,
				  Context *onreadable, Context *ondisk,
				  Context *onreadable_sync)
{
  // set up the sequencer
  OpSequencer *osr;
  if (!posr)
    posr = &default_osr;
  if (posr->p) {
    osr = (OpSequencer *)posr->p;
    dout(5) << "queue_transactions existing osr " << osr << "/" << osr->parent << dendl; //<< " w/ q " << osr->q << dendl;
  } else {
    osr = new OpSequencer;
    osr->parent = posr;
    posr->p = osr;
    dout(5) << "queue_transactions new osr " << osr << "/" << osr->parent << dendl;
  }

  if (logger) {
    logger->inc(l_os_in_ops);
    //logger->inc(l_os_in_bytes, 1); 
  }

  if (journal && journal->is_writeable() && !g_conf.filestore_journal_trailing) {
    Op *o = build_op(tls, onreadable, onreadable_sync);
    op_queue_reserve_throttle(o);
    journal->throttle();
    o->op = op_submit_start();
    if (g_conf.filestore_journal_parallel) {
      dout(5) << "queue_transactions (parallel) " << o->op << " " << o->tls << dendl;
      
      _op_journal_transactions(o->tls, o->op, ondisk);
      
      // queue inside journal lock, to preserve ordering
      queue_op(osr, o);
    } else if (g_conf.filestore_journal_writeahead) {
      dout(5) << "queue_transactions (writeahead) " << o->op << " " << o->tls << dendl;
      
      osr->queue_journal(o->op);

      _op_journal_transactions(o->tls, o->op, new C_JournaledAhead(this, osr, o, ondisk));
    } else {
      assert(0);
    }
    op_submit_finish(o->op);
    return 0;
  }

  uint64_t op = op_submit_start();
  dout(5) << "queue_transactions (trailing journal) " << op << " " << tls << dendl;

  _op_apply_start(op);
  int r = do_transactions(tls, op);
    
  if (r >= 0) {
    _op_journal_transactions(tls, op, ondisk);
  } else {
    delete ondisk;
  }

  // start on_readable finisher after we queue journal item, as on_readable callback
  // is allowed to delete the Transaction
  if (onreadable_sync) {
    onreadable_sync->finish(r);
    delete onreadable_sync;
  }
  op_finisher.queue(onreadable, r);

  op_submit_finish(op);
  op_apply_finish(op);

  if (logger) {
    logger->inc(l_os_readable_ops);
    //fixme logger->inc(l_os_readable_bytes, 1);
  }

  return r;
}

void FileStore::_journaled_ahead(OpSequencer *osr, Op *o, Context *ondisk)
{
  dout(5) << "_journaled_ahead " << o->op << " " << o->tls << dendl;

  // this should queue in order because the journal does it's completions in order.
  journal_lock.Lock();
  queue_op(osr, o);
  journal_lock.Unlock();

  osr->dequeue_journal();

  // do ondisk completions async, to prevent any onreadable_sync completions
  // getting blocked behind an ondisk completion.
  if (ondisk) {
    dout(10) << " queueing ondisk " << ondisk << dendl;
    ondisk_finisher.queue(ondisk);
  }
}

int FileStore::do_transactions(list<Transaction*> &tls, uint64_t op_seq)
{
  int r = 0;

  uint64_t bytes = 0, ops = 0;
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       p++) {
    bytes += (*p)->get_num_bytes();
    ops += (*p)->get_num_ops();
  }

  int id = _transaction_start(bytes, ops);
  if (id < 0) {
    return id;
  }
    
  for (list<Transaction*>::iterator p = tls.begin();
       p != tls.end();
       p++) {
    r = _do_transaction(**p);
    if (r < 0)
      break;
  }
  
  _transaction_finish(id);
  return r;
}

unsigned FileStore::apply_transaction(Transaction &t,
				      Context *ondisk)
{
  list<Transaction*> tls;
  tls.push_back(&t);
  return apply_transactions(tls, ondisk);
}

unsigned FileStore::apply_transactions(list<Transaction*> &tls,
				       Context *ondisk)
{
  // use op pool
  Cond my_cond;
  Mutex my_lock("FileStore::apply_transaction::my_lock");
  int r = 0;
  bool done;
  C_SafeCond *onreadable = new C_SafeCond(&my_lock, &my_cond, &done, &r);
  
  dout(10) << "apply queued" << dendl;
  queue_transactions(NULL, tls, onreadable, ondisk);
  
  my_lock.Lock();
  while (!done)
    my_cond.Wait(my_lock);
  my_lock.Unlock();
  dout(10) << "apply done r = " << r << dendl;
  return r;
}


// btrfs transaction start/end interface

int FileStore::_transaction_start(uint64_t bytes, uint64_t ops)
{
#ifdef DARWIN
  return 0;
#else
  if (!btrfs || !btrfs_trans_start_end ||
      !g_conf.filestore_btrfs_trans)
    return 0;

  char buf[80];
  int fd = ::open(basedir.c_str(), O_RDONLY);
  if (fd < 0) {
    dout(0) << "transaction_start got " << strerror_r(errno, buf, sizeof(buf))
 	    << " from btrfs open" << dendl;
    assert(0);
  }

  int r = ::ioctl(fd, BTRFS_IOC_TRANS_START);
  if (r < 0) {
    dout(0) << "transaction_start got " << strerror_r(errno, buf, sizeof(buf))
 	    << " from btrfs ioctl" << dendl;    
    ::close(fd);
    return -errno;
  }
  dout(10) << "transaction_start " << fd << dendl;

  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/current/trans.%d", basedir.c_str(), fd);
  ::mknod(fn, 0644, 0);

  return fd;
#endif /* DARWIN */
}

void FileStore::_transaction_finish(int fd)
{
#ifdef DARWIN
  return;
#else
  if (!btrfs || !btrfs_trans_start_end ||
      !g_conf.filestore_btrfs_trans)
    return;

  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/current/trans.%d", basedir.c_str(), fd);
  ::unlink(fn);
  
  dout(10) << "transaction_finish " << fd << dendl;
  ::ioctl(fd, BTRFS_IOC_TRANS_END);
  ::close(fd);
#endif /* DARWIN */
}

unsigned FileStore::_do_transaction(Transaction& t)
{
  dout(10) << "_do_transaction on " << &t << dendl;

  while (t.have_op()) {
    int op = t.get_op();
    int r = 0;
    switch (op) {
    case Transaction::OP_NOP:
      break;
    case Transaction::OP_TOUCH:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	r = _touch(cid, oid);
      }
      break;
      
    case Transaction::OP_WRITE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	bufferlist bl;
	t.get_bl(bl);
	r = _write(cid, oid, off, len, bl);
      }
      break;
      
    case Transaction::OP_ZERO:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	r = _zero(cid, oid, off, len);
      }
      break;
      
    case Transaction::OP_TRIMCACHE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	trim_from_cache(cid, oid, off, len);
      }
      break;
      
    case Transaction::OP_TRUNCATE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	uint64_t off = t.get_length();
	r = _truncate(cid, oid, off);
      }
      break;
      
    case Transaction::OP_REMOVE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	r = _remove(cid, oid);
      }
      break;
      
    case Transaction::OP_SETATTR:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	string name = t.get_attrname();
	bufferlist bl;
	t.get_bl(bl);
	r = _setattr(cid, oid, name.c_str(), bl.c_str(), bl.length());
	if (r == -ENOSPC)
	  dout(0) << " ENOSPC on setxattr on " << cid << "/" << oid
		  << " name " << name << " size " << bl.length() << dendl;
      }
      break;
      
    case Transaction::OP_SETATTRS:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	map<string, bufferptr> aset;
	t.get_attrset(aset);
	r = _setattrs(cid, oid, aset);
  	if (r == -ENOSPC)
	  dout(0) << " ENOSPC on setxattrs on " << cid << "/" << oid << dendl;
    }
      break;

    case Transaction::OP_RMATTR:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	string name = t.get_attrname();
	r = _rmattr(cid, oid, name.c_str());
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	r = _rmattrs(cid, oid);
      }
      break;
      
    case Transaction::OP_CLONE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	sobject_t noid = t.get_oid();
	r = _clone(cid, oid, noid);
      }
      break;

    case Transaction::OP_CLONERANGE:
      {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	sobject_t noid = t.get_oid();
 	uint64_t off = t.get_length();
	uint64_t len = t.get_length();
	r = _clone_range(cid, oid, noid, off, len);
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	coll_t cid = t.get_cid();
	r = _create_collection(cid);
      }
      break;

    case Transaction::OP_RMCOLL:
      {
	coll_t cid = t.get_cid();
	r = _destroy_collection(cid);
      }
      break;

    case Transaction::OP_COLL_ADD:
      {
	coll_t ocid = t.get_cid();
	coll_t ncid = t.get_cid();
	sobject_t oid = t.get_oid();
	r = _collection_add(ocid, ncid, oid);
      }
      break;

    case Transaction::OP_COLL_REMOVE:
       {
	coll_t cid = t.get_cid();
	sobject_t oid = t.get_oid();
	r = _collection_remove(cid, oid);
       }
      break;

    case Transaction::OP_COLL_SETATTR:
      {
	coll_t cid = t.get_cid();
	string name = t.get_attrname();
	bufferlist bl;
	t.get_bl(bl);
	r = _collection_setattr(cid, name.c_str(), bl.c_str(), bl.length());
      }
      break;

    case Transaction::OP_COLL_RMATTR:
      {
	coll_t cid = t.get_cid();
	string name = t.get_attrname();
	r = _collection_rmattr(cid, name.c_str());
      }
      break;

    case Transaction::OP_STARTSYNC:
      _start_sync();
      break;

    case Transaction::OP_COLL_RENAME:
      {
	coll_t cid(t.get_cid());
	coll_t ncid(t.get_cid());
	r = _collection_rename(cid, ncid);
      }
      break;

    default:
      cerr << "bad op " << op << std::endl;
      assert(0);
    }

    if (r == -ENOTEMPTY) {
      assert(0 == "ENOTEMPTY suggests garbage data in osd data dir");
    }
    if (r == -ENOSPC) {
      // For now, if we hit _any_ ENOSPC, crash, before we do any damage
      // by partially applying transactions.

      // XXX HACK: if it was an setxattr op, silently fail, until we have a better workaround XXX
      if (op == Transaction::OP_SETATTR || op == Transaction::OP_SETATTRS)
	dout(0) << "WARNING: ignoring setattr ENOSPC failure, until we implement a workaround for extN"
		<< " xattr limitations" << dendl;
      else
	assert(0 == "ENOSPC handling not implemented");
    }
    if (r == -EIO) {
      assert(0 == "EIO handling not implemented");
    }
  }
  
  return 0;  // FIXME count errors
}

  /*********************************************/



// --------------------
// objects

bool FileStore::exists(coll_t cid, const sobject_t& oid)
{
  struct stat st;
  if (stat(cid, oid, &st) == 0)
    return true;
  else 
    return false;
}
  
int FileStore::stat(coll_t cid, const sobject_t& oid, struct stat *st)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  int r = lfn_stat(fn, st);
  if (r < 0)
    r = -errno;
  dout(10) << "stat " << fn << " = " << r << " (size " << st->st_size << ")" << dendl;
  return r;
}

int FileStore::read(coll_t cid, const sobject_t& oid, 
                    uint64_t offset, size_t len, bufferlist& bl)
{
  int got;
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));

  dout(15) << "read " << fn << " " << offset << "~" << len << dendl;

  int fd = lfn_open(fn, O_RDONLY);
  if (fd < 0) {
    int err = errno;
    dout(10) << "FileStore::read(" << fn << "): open error "
	     << cpp_strerror(err) << dendl;
    return -err;
  }

  if (len == 0) {
    struct stat st;
    memset(&st, 0, sizeof(struct stat));
    ::fstat(fd, &st);
    len = st.st_size;
  }

  bufferptr bptr(len);  // prealloc space for entire read
  got = safe_pread(fd, bptr.c_str(), len, offset);
  if (got < 0) {
    dout(10) << "FileStore::read(" << fn << "): pread error "
	     << cpp_strerror(got) << dendl;
    TEMP_FAILURE_RETRY(::close(fd));
    return got;
  }
  bptr.set_length(got);   // properly size the buffer
  bl.push_back(bptr);   // put it in the target bufferlist
  TEMP_FAILURE_RETRY(::close(fd));

  dout(10) << "FileStore::read " << fn << " " << offset << "~"
	   << got << "/" << len << dendl;
  return got;
}

int FileStore::fiemap(coll_t cid, const sobject_t& oid,
                    uint64_t offset, size_t len,
                    bufferlist& bl)
{

  if (!ioctl_fiemap) {
    map<off_t, size_t> m;
    m[offset] = len;
    ::encode(m, bl);
    return 0;
  }


  char fn[PATH_MAX];
  struct fiemap *fiemap = NULL;
  map<off_t, size_t> extmap;

  get_coname(cid, oid, fn, sizeof(fn));

  dout(15) << "fiemap " << fn << " " << offset << "~" << len << dendl;

  int r;
  int fd = lfn_open(fn, O_RDONLY);
  if (fd < 0) {
    char buf[80];
    dout(10) << "read couldn't open " << fn << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    r = -errno;
  } else {
    uint64_t i;

    r = do_fiemap(fd, offset, len, &fiemap);
    if (r < 0)
      goto done;

    if (fiemap->fm_mapped_extents == 0)
      goto done;

    struct fiemap_extent *extent = &fiemap->fm_extents[0];

    /* start where we were asked to start */
    if (extent->fe_logical < offset) {
      extent->fe_length -= offset - extent->fe_logical;
      extent->fe_logical = offset;
    }

    i = 0;

    while (i < fiemap->fm_mapped_extents) {
      struct fiemap_extent *next = extent + 1;

      dout(10) << "FileStore::fiemap() fm_mapped_extents=" << fiemap->fm_mapped_extents
	       << " fe_logical=" << extent->fe_logical << " fe_length=" << extent->fe_length << dendl;

      /* try to merge extents */
      while ((i < fiemap->fm_mapped_extents - 1) &&
             (extent->fe_logical + extent->fe_length == next->fe_logical)) {
          next->fe_length += extent->fe_length;
          next->fe_logical = extent->fe_logical;
          extent = next;
          next = extent + 1;
          i++;
      }

      if (extent->fe_logical + extent->fe_length > offset + len)
        extent->fe_length = offset + len - extent->fe_logical;
      extmap[extent->fe_logical] = extent->fe_length;
      i++;
    }
  }

done:
  if (r >= 0)
    ::encode(extmap, bl);

  dout(10) << "fiemap " << fn << " " << offset << "~" << len << " = " << r << " num extents=" << extmap.size() << dendl;
  free(fiemap);
  return r;
}


int FileStore::_remove(coll_t cid, const sobject_t& oid) 
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "remove " << fn << dendl;
  int r = ::lfn_unlink(fn);
  if (r < 0) r = -errno;
  dout(10) << "remove " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_truncate(coll_t cid, const sobject_t& oid, uint64_t size)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "truncate " << fn << " size " << size << dendl;
  int r = lfn_truncate(fn, size);
  dout(10) << "truncate " << fn << " size " << size << " = " << r << dendl;
  return r;
}


int FileStore::_touch(coll_t cid, const sobject_t& oid)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));

  dout(15) << "touch " << fn << dendl;

  int flags = O_WRONLY|O_CREAT;
  int fd = lfn_open(fn, flags, 0644);
  int r;
  if (fd >= 0) {
    ::close(fd);
    r = 0;
  } else
    r = -errno;
  dout(10) << "touch " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_write(coll_t cid, const sobject_t& oid, 
                     uint64_t offset, size_t len,
                     const bufferlist& bl)
{
  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));

  dout(15) << "write " << fn << " " << offset << "~" << len << dendl;
  int r;

  int64_t actual;

  char buf[80];
  int flags = O_WRONLY|O_CREAT;
  int fd = lfn_open(fn, flags, 0644);
  if (fd < 0) {
    dout(0) << "write couldn't open " << fn << " flags " << flags << " errno " << errno << " " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    r = -errno;
    goto out;
  }
    
  // seek
  actual = ::lseek64(fd, offset, SEEK_SET);
  if (actual < 0) {
    dout(0) << "write lseek64 to " << offset << " failed: " << strerror_r(errno, buf, sizeof(buf)) << dendl;
    r = -errno;
    goto out;
  }
  if (actual != (int64_t)offset) {
    dout(0) << "write lseek64 to " << offset << " gave bad offset " << actual << dendl;
    r = -EIO;
    goto out;
  }

  // write
  r = bl.write_fd(fd);
  if (r == 0)
    r = bl.length();

  // flush?
#ifdef HAVE_SYNC_FILE_RANGE
  if (!g_conf.filestore_flusher ||
      !queue_flusher(fd, offset, len)) {
    if (g_conf.filestore_sync_flush)
      ::sync_file_range(fd, offset, len, SYNC_FILE_RANGE_WRITE);
    ::close(fd);
  }
#else
  ::close(fd);
#endif

 out:
  dout(10) << "write " << fn << " " << offset << "~" << len << " = " << r << dendl;
  return r;
}

int FileStore::_zero(coll_t cid, const sobject_t& oid, uint64_t offset, size_t len)
{
  // write zeros.. yuck!
  bufferptr bp(len);
  bufferlist bl;
  bl.push_back(bp);
  return _write(cid, oid, offset, len, bl);
}

int FileStore::_clone(coll_t cid, const sobject_t& oldoid, const sobject_t& newoid)
{
  char ofn[PATH_MAX], nfn[PATH_MAX];
  get_coname(cid, oldoid, ofn, sizeof(ofn));
  get_coname(cid, newoid, nfn, sizeof(nfn));

  dout(15) << "clone " << ofn << " -> " << nfn << dendl;

  int o, n, r;
  o = lfn_open(ofn, O_RDONLY);
  if (o < 0) {
    r = -errno;
    goto out2;
  }
  n = lfn_open(nfn, O_CREAT|O_TRUNC|O_WRONLY, 0644);
  if (n < 0) {
    r = -errno;
    goto out;
  }
  if (btrfs)
#ifndef DARWIN
    r = ::ioctl(n, BTRFS_IOC_CLONE, o);
#else 
  ;
#endif /* DARWIN */
  else {
    struct stat st;
    ::fstat(o, &st);
    dout(10) << "clone " << ofn << " -> " << nfn << " READ+WRITE" << dendl;
    r = _do_clone_range(o, n, 0, st.st_size);
  }
  if (r < 0)
    r = -errno;

  ::close(n);
 out:
  ::close(o);
 out2:
  dout(10) << "clone " << ofn << " -> " << nfn << " = " << r << dendl;
  return 0;
}

int FileStore::_do_clone_range(int from, int to, uint64_t off, uint64_t len)
{
  dout(20) << "_do_clone_range " << off << "~" << len << dendl;
  int r = 0;
  
  if (btrfs_clone_range) {
    btrfs_ioctl_clone_range_args a;
    a.src_fd = from;
    a.src_offset = off;
    a.src_length = len;
    a.dest_offset = off;
    r = ::ioctl(to, BTRFS_IOC_CLONE_RANGE, &a);
    if (r >= 0)
      return r;
    return -errno;
  }

  loff_t pos = off;
  loff_t end = off + len;
  int buflen = 4096*32;
  char buf[buflen];
  while (pos < end) {
    int l = MIN(end-pos, buflen);
    r = ::read(from, buf, l);
    dout(25) << "  read from " << from << "~" << l << " got " << r << dendl;
    if (r < 0) {
      r = -errno;
      derr << "FileStore::_do_clone_range: read error at " << from << "~" << len
	   << ", " << cpp_strerror(r) << dendl;
      break;
    }
    if (r == 0) {
      // hrm, bad source range, wtf.
      r = -ERANGE;
      derr << "FileStore::_do_clone_range got short read result at " << from
	      << " of " << from << "~" << len << dendl;
      break;
    }
    int op = 0;
    while (op < r) {
      int r2 = safe_write(to, buf+op, r-op);
      dout(25) << " write to " << to << "~" << (r-op) << " got " << r2 << dendl;      
      if (r2 < 0) {
	r = r2;
	derr << "FileStore::_do_clone_range: write error at " << to << "~" << r-op
	     << ", " << cpp_strerror(r) << dendl;
	break;
      }
      op += (r-op);
    }
    if (r < 0)
      break;
    pos += r;
  }
  dout(20) << "_do_clone_range " << off << "~" << len << " = " << r << dendl;
  return r;
}

int FileStore::_clone_range(coll_t cid, const sobject_t& oldoid, const sobject_t& newoid, uint64_t off, uint64_t len)
{
  char ofn[PATH_MAX], nfn[PATH_MAX];
  get_coname(cid, oldoid, ofn, sizeof(ofn));
  get_coname(cid, newoid, nfn, sizeof(ofn));

  dout(15) << "clone_range " << ofn << " -> " << nfn << " " << off << "~" << len << dendl;

  int r;
  int o, n;
  o = lfn_open(ofn, O_RDONLY);
  if (o < 0) {
    r = -errno;
    goto out2;
  }
  n = lfn_open(nfn, O_CREAT|O_WRONLY, 0644);
  if (n < 0) {
    r = -errno;
    goto out;
  }
  r = _do_clone_range(o, n, off, len);
  ::close(n);
 out:
  ::close(o);
 out2:
  dout(10) << "clone_range " << ofn << " -> " << nfn << " " << off << "~" << len << " = " << r << dendl;
  return r;
}


bool FileStore::queue_flusher(int fd, uint64_t off, uint64_t len)
{
  bool queued;
  lock.Lock();
  if (flusher_queue_len < g_conf.filestore_flusher_max_fds) {
    flusher_queue.push_back(sync_epoch);
    flusher_queue.push_back(fd);
    flusher_queue.push_back(off);
    flusher_queue.push_back(len);
    flusher_queue_len++;
    flusher_cond.Signal();
    dout(10) << "queue_flusher ep " << sync_epoch << " fd " << fd << " " << off << "~" << len
	     << " qlen " << flusher_queue_len
	     << dendl;
    queued = true;
  } else {
    dout(10) << "queue_flusher ep " << sync_epoch << " fd " << fd << " " << off << "~" << len
	     << " qlen " << flusher_queue_len 
	     << " hit flusher_max_fds " << g_conf.filestore_flusher_max_fds
	     << ", skipping async flush" << dendl;
    queued = false;
  }
  lock.Unlock();
  return queued;
}

void FileStore::flusher_entry()
{
  lock.Lock();
  dout(20) << "flusher_entry start" << dendl;
  while (true) {
    if (!flusher_queue.empty()) {
#ifdef HAVE_SYNC_FILE_RANGE
      list<uint64_t> q;
      q.swap(flusher_queue);

      int num = flusher_queue_len;  // see how many we're taking, here

      lock.Unlock();
      while (!q.empty()) {
	uint64_t ep = q.front();
	q.pop_front();
	int fd = q.front();
	q.pop_front();
	uint64_t off = q.front();
	q.pop_front();
	uint64_t len = q.front();
	q.pop_front();
	if (!stop && ep == sync_epoch) {
	  dout(10) << "flusher_entry flushing+closing " << fd << " ep " << ep << dendl;
	  ::sync_file_range(fd, off, len, SYNC_FILE_RANGE_WRITE);
	} else 
	  dout(10) << "flusher_entry JUST closing " << fd << " (stop=" << stop << ", ep=" << ep
		   << ", sync_epoch=" << sync_epoch << ")" << dendl;
	::close(fd);
      }
      lock.Lock();
      flusher_queue_len -= num;   // they're definitely closed, forget
#endif
    } else {
      if (stop)
	break;
      dout(20) << "flusher_entry sleeping" << dendl;
      flusher_cond.Wait(lock);
      dout(20) << "flusher_entry awoke" << dendl;
    }
  }
  dout(20) << "flusher_entry finish" << dendl;
  lock.Unlock();
}

class SyncEntryTimeout : public Context {
public:
  SyncEntryTimeout() { }

  void finish(int r) {
    BackTrace *bt = new BackTrace(1);
    generic_dout(-1) << "FileStore: sync_entry timed out after "
	   << g_conf.filestore_commit_timeout << " seconds.\n";
    bt->print(*_dout);
    *_dout << dendl;
    delete bt;
    ceph_abort();
  }
};

void FileStore::sync_entry()
{
  lock.Lock();
  while (!stop) {
    utime_t max_interval;
    max_interval.set_from_double(g_conf.filestore_max_sync_interval);
    utime_t min_interval;
    min_interval.set_from_double(g_conf.filestore_min_sync_interval);

    utime_t startwait = g_clock.now();
    if (!force_sync) {
      dout(20) << "sync_entry waiting for max_interval " << max_interval << dendl;
      sync_cond.WaitInterval(lock, max_interval);
    } else {
      dout(20) << "sync_entry not waiting, force_sync set" << dendl;
    }

    if (force_sync) {
      dout(20) << "sync_entry force_sync set" << dendl;
      force_sync = false;
    } else {
      // wait for at least the min interval
      utime_t woke = g_clock.now();
      woke -= startwait;
      dout(20) << "sync_entry woke after " << woke << dendl;
      if (woke < min_interval) {
	utime_t t = min_interval;
	t -= woke;
	dout(20) << "sync_entry waiting for another " << t 
		 << " to reach min interval " << min_interval << dendl;
	sync_cond.WaitInterval(lock, t);
      }
    }

    list<Context*> fin;
  again:
    fin.swap(sync_waiters);
    lock.Unlock();
    
    if (commit_start()) {
      utime_t start = g_clock.now();
      uint64_t cp = committing_seq;

      SyncEntryTimeout *sync_entry_timeo = new SyncEntryTimeout();
      sync_entry_timeo_lock.Lock();
      timer.add_event_after(g_conf.filestore_commit_timeout, sync_entry_timeo);
      sync_entry_timeo_lock.Unlock();

      if (logger)
	logger->set(l_os_committing, 1);

      // make flusher stop flushing previously queued stuff
      sync_epoch++;

      dout(15) << "sync_entry committing " << cp << " sync_epoch " << sync_epoch << dendl;
      write_op_seq(op_fd, cp);

      bool do_snap = btrfs && g_conf.filestore_btrfs_snap;

      if (do_snap) {

	if (btrfs_snap_create_v2) {
	  // be smart!
	  struct btrfs_ioctl_vol_args_v2 async_args;
	  async_args.fd = current_fd;
	  async_args.flags = BTRFS_SUBVOL_CREATE_ASYNC;
	  snprintf(async_args.name, sizeof(async_args.name), COMMIT_SNAP_ITEM,
		   (long long unsigned)cp);

	  dout(10) << "taking async snap '" << async_args.name << "'" << dendl;
	  int r = ::ioctl(basedir_fd, BTRFS_IOC_SNAP_CREATE_V2, &async_args);
	  char buf[100];
	  dout(20) << "async snap create '" << async_args.name
		   << "' transid " << async_args.transid
		   << " got " << r << " " << strerror_r(r < 0 ? errno : 0, buf, sizeof(buf)) << dendl;
	  assert(r == 0);
	  snaps.push_back(cp);

	  commit_started();

	  // wait for commit
	  dout(20) << " waiting for transid " << async_args.transid << " to complete" << dendl;
	  ::ioctl(op_fd, BTRFS_IOC_WAIT_SYNC, &async_args.transid);
	  dout(20) << " done waiting for transid " << async_args.transid << " to complete" << dendl;

	} else {
	  // the synchronous snap create does a sync.
	  struct btrfs_ioctl_vol_args vol_args;
	  vol_args.fd = current_fd;
	  snprintf(vol_args.name, sizeof(vol_args.name), COMMIT_SNAP_ITEM,
		   (long long unsigned)cp);

	  dout(10) << "taking snap '" << vol_args.name << "'" << dendl;
	  int r = ::ioctl(basedir_fd, BTRFS_IOC_SNAP_CREATE, &vol_args);
	  char buf[100];
	  dout(20) << "snap create '" << vol_args.name << "' got " << r
		   << " " << strerror_r(r < 0 ? errno : 0, buf, sizeof(buf)) << dendl;
	  assert(r == 0);
	  snaps.push_back(cp);
	  
	  commit_started();
	}
      } else {
	commit_started();

	if (btrfs) {
	  dout(15) << "sync_entry doing btrfs SYNC" << dendl;
	  // do a full btrfs commit
	  ::ioctl(op_fd, BTRFS_IOC_SYNC);
	} else if (g_conf.filestore_fsync_flushes_journal_data) {
	  dout(15) << "sync_entry doing fsync on " << current_op_seq_fn << dendl;
	  // make the file system's journal commit.
	  //  this works with ext3, but NOT ext4
	  ::fsync(op_fd);  
	} else {
	  dout(15) << "sync_entry doing a full sync (!)" << dendl;
	  sync_filesystem(basedir_fd);
	}
      }
      
      utime_t done = g_clock.now();
      done -= start;
      dout(10) << "sync_entry commit took " << done << dendl;
      commit_finish();

      if (logger)
	logger->set(l_os_committing, 0);

      // remove old snaps?
      if (do_snap) {
	while (snaps.size() > 2) {
	  btrfs_ioctl_vol_args vol_args;
	  vol_args.fd = 0;
	  snprintf(vol_args.name, sizeof(vol_args.name), COMMIT_SNAP_ITEM,
		   (long long unsigned)snaps.front());

	  snaps.pop_front();
	  dout(10) << "removing snap '" << vol_args.name << "'" << dendl;
	  int r = ::ioctl(basedir_fd, BTRFS_IOC_SNAP_DESTROY, &vol_args);
	  if (r) {
	    char buf[100];
	    dout(20) << "unable to destroy snap '" << vol_args.name << "' got " << r
		     << " " << strerror_r(r < 0 ? errno : 0, buf, sizeof(buf)) << dendl;
	  }
	}
      }

      dout(15) << "sync_entry committed to op_seq " << cp << dendl;

      sync_entry_timeo_lock.Lock();
      timer.cancel_event(sync_entry_timeo);
      sync_entry_timeo_lock.Unlock();
    }
    
    lock.Lock();
    finish_contexts(fin, 0);
    fin.clear();
    if (!sync_waiters.empty()) {
      dout(10) << "sync_entry more waiters, committing again" << dendl;
      goto again;
    }
    if (journal && journal->should_commit_now()) {
      dout(10) << "sync_entry journal says we should commit again (probably is/was full)" << dendl;
      goto again;
    }
  }
  lock.Unlock();
}

void FileStore::_start_sync()
{
  if (!journal) {  // don't do a big sync if the journal is on
    dout(10) << "start_sync" << dendl;
    sync_cond.Signal();
  } else {
    dout(10) << "start_sync - NOOP (journal is on)" << dendl;
  }
}

void FileStore::start_sync()
{
  Mutex::Locker l(lock);
  force_sync = true;
  sync_cond.Signal();
}

void FileStore::start_sync(Context *onsafe)
{
  Mutex::Locker l(lock);
  sync_waiters.push_back(onsafe);
  sync_cond.Signal();
  dout(10) << "start_sync" << dendl;
}

void FileStore::sync()
{
  Mutex l("FileStore::sync");
  Cond c;
  bool done;
  C_SafeCond *fin = new C_SafeCond(&l, &c, &done);

  start_sync(fin);

  l.Lock();
  while (!done) {
    dout(10) << "sync waiting" << dendl;
    c.Wait(l);
  }
  l.Unlock();
  dout(10) << "sync done" << dendl;
}

void FileStore::_flush_op_queue()
{
  dout(10) << "_flush_op_queue draining op tp" << dendl;
  op_wq.drain();
  dout(10) << "_flush_op_queue waiting for apply finisher" << dendl;
  op_finisher.wait_for_empty();
}

/*
 * flush - make every queued write readable
 */
void FileStore::flush()
{
  dout(10) << "flush" << dendl;
 
  if (g_conf.filestore_journal_writeahead) {
    if (journal)
      journal->flush();
    dout(10) << "flush draining ondisk finisher" << dendl;
    ondisk_finisher.wait_for_empty();
  }

  _flush_op_queue();
  dout(10) << "flush complete" << dendl;
}

/*
 * sync_and_flush - make every queued write readable AND committed to disk
 */
void FileStore::sync_and_flush()
{
  dout(10) << "sync_and_flush" << dendl;

  if (g_conf.filestore_journal_writeahead) {
    if (journal)
      journal->flush();
    _flush_op_queue();
  } else if (g_conf.filestore_journal_parallel) {
    _flush_op_queue();
    sync();
  } else {
    _flush_op_queue();
    sync();
  }
  dout(10) << "sync_and_flush done" << dendl;
}


// -------------------------------
// attributes

// low-level attr helpers
int FileStore::_getattr(const char *fn, const char *name, bufferptr& bp)
{
  char val[100];
  int l = do_getxattr(fn, name, val, sizeof(val));
  if (l >= 0) {
    bp = buffer::create(l);
    memcpy(bp.c_str(), val, l);
  } else if (l == -ERANGE) {
    l = do_getxattr(fn, name, 0, 0);
    if (l) {
      bp = buffer::create(l);
      l = do_getxattr(fn, name, bp.c_str(), l);
    }
  }
  return l;
}

int FileStore::_getattrs(const char *fn, map<string,bufferptr>& aset, bool user_only) 
{
  // get attr list
  char names1[100];
  int len = do_listxattr(fn, names1, sizeof(names1)-1);
  char *names2 = 0;
  char *name = 0;
  if (len == -ERANGE) {
    len = do_listxattr(fn, 0, 0);
    if (len < 0)
      return len;
    dout(10) << " -ERANGE, len is " << len << dendl;
    names2 = new char[len+1];
    len = do_listxattr(fn, names2, len);
    dout(10) << " -ERANGE, got " << len << dendl;
    if (len < 0)
      return len;
    name = names2;
  } else if (len < 0)
    return len;
  else
    name = names1;
  name[len] = 0;

  char *end = name + len;
  while (name < end) {
    char *attrname = name;
    if (parse_attrname(&name)) {
      char *set_name = name;
      bool can_get = true;
      if (user_only) {
          if (*set_name =='_')
            set_name++;
          else
            can_get = false;
      }
      if (*set_name && can_get) {
        dout(20) << "getattrs " << fn << " getting '" << name << "'" << dendl;
        //dout(0) << "getattrs " << fn << " set_name '" << set_name << "' user_only=" << user_only << dendl;
      
        int r = _getattr(fn, attrname, aset[set_name]);
        if (r < 0) return r;
      }
    }
    name += strlen(name) + 1;
  }

  delete[] names2;
  return 0;
}

// objects

int FileStore::getattr(coll_t cid, const sobject_t& oid, const char *name,
		       void *value, size_t size) 
{
  if (fake_attrs) return attrs.getattr(cid, oid, name, value, size);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "getattr " << fn << " '" << name << "' len " << size << dendl;
  char n[ATTR_MAX_NAME_LEN];
  get_attrname(name, n, ATTR_MAX_NAME_LEN);
  int r = do_getxattr(fn, n, value, size);
  dout(10) << "getattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::getattr(coll_t cid, const sobject_t& oid, const char *name, bufferptr &bp)
{
  if (fake_attrs) return attrs.getattr(cid, oid, name, bp);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "getattr " << fn << " '" << name << "'" << dendl;
  char n[ATTR_MAX_NAME_LEN];
  get_attrname(name, n, ATTR_MAX_NAME_LEN);
  int r = _getattr(fn, n, bp);
  dout(10) << "getattr " << fn << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::getattrs(coll_t cid, const sobject_t& oid, map<string,bufferptr>& aset, bool user_only) 
{
  if (fake_attrs) return attrs.getattrs(cid, oid, aset);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "getattrs " << fn << dendl;
  int r = _getattrs(fn, aset, user_only);
  dout(10) << "getattrs " << fn << " = " << r << dendl;
  return r;
}





int FileStore::_setattr(coll_t cid, const sobject_t& oid, const char *name,
			const void *value, size_t size) 
{
  if (fake_attrs) return attrs.setattr(cid, oid, name, value, size);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "setattr " << fn << " '" << name << "' len " << size << dendl;
  char n[ATTR_MAX_NAME_LEN];
  get_attrname(name, n, ATTR_MAX_NAME_LEN);
  int r = do_setxattr(fn, n, value, size);
  dout(10) << "setattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::_setattrs(coll_t cid, const sobject_t& oid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.setattrs(cid, oid, aset);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "setattrs " << fn << dendl;
  int r = 0;
  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[ATTR_MAX_NAME_LEN];
    get_attrname(p->first.c_str(), n, ATTR_MAX_NAME_LEN);
    const char *val;
    if (p->second.length())
      val = p->second.c_str();
    else
      val = "";
    // ??? Why do we skip setting all the other attrs if one fails?
    r = do_setxattr(fn, n, val, p->second.length());
    if (r < 0) {
      derr << "FileStore::_setattrs: do_setxattr returned " << r << dendl;
      break;
    }
  }
  dout(10) << "setattrs " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_rmattr(coll_t cid, const sobject_t& oid, const char *name) 
{
  if (fake_attrs) return attrs.rmattr(cid, oid, name);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));
  dout(15) << "rmattr " << fn << " '" << name << "'" << dendl;
  char n[ATTR_MAX_NAME_LEN];
  get_attrname(name, n, ATTR_MAX_NAME_LEN);
  int r = do_removexattr(fn, n);
  dout(10) << "rmattr " << fn << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::_rmattrs(coll_t cid, const sobject_t& oid) 
{
  //if (fake_attrs) return attrs.rmattrs(cid, oid);

  char fn[PATH_MAX];
  get_coname(cid, oid, fn, sizeof(fn));

  dout(15) << "rmattrs " << fn << dendl;

  map<string,bufferptr> aset;
  int r = _getattrs(fn, aset);
  if (r >= 0) {
    for (map<string,bufferptr>::iterator p = aset.begin(); p != aset.end(); p++) {
      char n[ATTR_MAX_NAME_LEN];
      get_attrname(p->first.c_str(), n, ATTR_MAX_NAME_LEN);
      r = do_removexattr(fn, n);
      if (r < 0)
	break;
    }
  }
  dout(10) << "rmattrs " << fn << " = " << r << dendl;
  return r;
}



// collections

int FileStore::collection_getattr(coll_t c, const char *name,
				  void *value, size_t size) 
{
  if (fake_attrs) return attrs.collection_getattr(c, name, value, size);

  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_getattr " << fn << " '" << name << "' len " << size << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  int r = do_getxattr(fn, n, value, size);   
  dout(10) << "collection_getattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::collection_getattr(coll_t c, const char *name, bufferlist& bl)
{
  if (fake_attrs) return attrs.collection_getattr(c, name, bl);

  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_getattr " << fn << " '" << name << "'" << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  
  buffer::ptr bp;
  int r = _getattr(fn, n, bp);
  bl.push_back(bp);
  dout(10) << "collection_getattr " << fn << " '" << name << "' = " << r << dendl;
  return r;
}

int FileStore::collection_getattrs(coll_t cid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.collection_getattrs(cid, aset);

  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  dout(10) << "collection_getattrs " << fn << dendl;
  int r = _getattrs(fn, aset);
  dout(10) << "collection_getattrs " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_collection_setattr(coll_t c, const char *name,
				  const void *value, size_t size) 
{
  if (fake_attrs) return attrs.collection_setattr(c, name, value, size);

  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(10) << "collection_setattr " << fn << " '" << name << "' len " << size << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  int r = do_setxattr(fn, n, value, size);
  dout(10) << "collection_setattr " << fn << " '" << name << "' len " << size << " = " << r << dendl;
  return r;
}

int FileStore::_collection_rmattr(coll_t c, const char *name) 
{
  if (fake_attrs) return attrs.collection_rmattr(c, name);

  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_rmattr " << fn << dendl;
  char n[PATH_MAX];
  get_attrname(name, n, PATH_MAX);
  int r = do_removexattr(fn, n);
  dout(10) << "collection_rmattr " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_collection_setattrs(coll_t cid, map<string,bufferptr>& aset) 
{
  if (fake_attrs) return attrs.collection_setattrs(cid, aset);

  char fn[PATH_MAX];
  get_cdir(cid, fn, sizeof(fn));
  dout(15) << "collection_setattrs " << fn << dendl;
  int r = 0;
  for (map<string,bufferptr>::iterator p = aset.begin();
       p != aset.end();
       ++p) {
    char n[PATH_MAX];
    get_attrname(p->first.c_str(), n, PATH_MAX);
    r = do_setxattr(fn, n, p->second.c_str(), p->second.length());
    if (r < 0) break;
  }
  dout(10) << "collection_setattrs " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_collection_rename(const coll_t &cid, const coll_t &ncid)
{
  int ret = 0;
  if (::rename(cid.c_str(), ncid.c_str())) {
    ret = errno;
  }
  dout(10) << "collection_rename '" << cid << "' to '" << ncid << "'"
	   << ": ret = " << ret << dendl;
  return ret;
}

// --------------------------
// collections

int FileStore::list_collections(vector<coll_t>& ls) 
{
  if (fake_collections) return collections.list_collections(ls);

  dout(10) << "list_collections" << dendl;

  char fn[PATH_MAX];
  snprintf(fn, sizeof(fn), "%s/current", basedir.c_str());

  DIR *dir = ::opendir(fn);
  if (!dir)
    return -errno;

  struct dirent sde, *de;
  char new_name[PATH_MAX];
  while (::readdir_r(dir, &sde, &de) == 0) {
    if (!de)
      break;
    if (!S_ISDIR(de->d_type << 12))
      continue;
    if (de->d_name[0] == '.' &&
	(de->d_name[1] == '\0' ||
	 (de->d_name[1] == '.' &&
	  de->d_name[2] == '\0')))
      continue;
    lfn_translate(fn, de->d_name, new_name, sizeof(new_name));
    ls.push_back(coll_t(new_name));
  }
  
  ::closedir(dir);
  return 0;
}

int FileStore::collection_stat(coll_t c, struct stat *st) 
{
  if (fake_collections) return collections.collection_stat(c, st);

  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "collection_stat " << fn << dendl;
  int r = lfn_stat(fn, st);
  if (r < 0) r = -errno;
  dout(10) << "collection_stat " << fn << " = " << r << dendl;
  return r;
}

bool FileStore::collection_exists(coll_t c) 
{
  if (fake_collections) return collections.collection_exists(c);

  struct stat st;
  return collection_stat(c, &st) == 0;
}

bool FileStore::collection_empty(coll_t c) 
{  
  if (fake_collections) return collections.collection_empty(c);

  char buf[PATH_MAX];
  get_cdir(c, buf, sizeof(buf));
  dout(15) << "collection_empty " << buf << dendl;

  DIR *dir = ::opendir(buf);
  if (!dir)
    return -errno;

  bool empty = true;
  struct dirent *de;
  while (::readdir_r(dir, (struct dirent*)buf, &de) == 0) {
    if (!de)
      break;
    // parse
    if (de->d_name[0] == '.') continue;
    //cout << "  got object " << de->d_name << std::endl;
    sobject_t o;
    if (parse_object(de->d_name, o)) {
      empty = false;
      break;
    }
  }
  
  ::closedir(dir);
  dout(10) << "collection_empty " << c << " = " << empty << dendl;
  return empty;
}

int FileStore::collection_list_partial(coll_t c, snapid_t seq, vector<sobject_t>& ls, int max_count,
				       collection_list_handle_t *handle)
{  
  if (fake_collections) return collections.collection_list(c, ls);

  char dir_name[PATH_MAX], buf[PATH_MAX];
  get_cdir(c, dir_name, sizeof(dir_name));

  DIR *dir = NULL;

  struct dirent *de;
  bool end;
  
  dir = ::opendir(dir_name);

  if (!dir) {
    int err = -errno;
    dout(0) << "error opening directory " << dir_name << dendl;
    return err;
  }

  if (handle && *handle) {
    seekdir(dir, *(off_t *)handle);
    *handle = 0;
  }

  char new_name[PATH_MAX];

  int i=0;
  while (i < max_count) {
    errno = 0;
    end = false;
    ::readdir_r(dir, (struct dirent*)buf, &de);
    int err = errno;
    if (!de && err) {
      dout(0) << "error reading directory for " << c << dendl;
      ::closedir(dir);
      return -err;
    }
    if (!de) {
      end = true;
      break;
    }

    // parse
    if (de->d_name[0] == '.') {
      continue;
    }
    //cout << "  got object " << de->d_name << std::endl;
    lfn_translate(dir_name, de->d_name, new_name, sizeof(new_name));
    sobject_t o;
    if (parse_object(new_name, o)) {
      if (o.snap >= seq) {
	ls.push_back(o);
	i++;
      }
    }
  }

  if (handle && !end)
    *handle = (collection_list_handle_t)telldir(dir);

  ::closedir(dir);

  dout(10) << "collection_list " << c << " = 0 (" << ls.size() << " objects)" << dendl;
  return 0;
}


int FileStore::collection_list(coll_t c, vector<sobject_t>& ls) 
{  
  if (fake_collections) return collections.collection_list(c, ls);

  char dir_name[PATH_MAX], buf[PATH_MAX], new_name[PATH_MAX];
  get_cdir(c, dir_name, sizeof(dir_name));
  dout(10) << "collection_list " << dir_name << dendl;

  DIR *dir = ::opendir(dir_name);
  if (!dir)
    return -errno;
  
  // first, build (ino, object) list
  vector< pair<ino_t,sobject_t> > inolist;

  struct dirent *de;
  while (::readdir_r(dir, (struct dirent*)buf, &de) == 0) {
    if (!de)
      break;
    // parse
    if (de->d_name[0] == '.')
      continue;
    //cout << "  got object " << de->d_name << std::endl;
    sobject_t o;
    lfn_translate(dir_name, de->d_name, new_name, sizeof(new_name));
    if (parse_object(new_name, o)) {
      inolist.push_back(pair<ino_t,sobject_t>(de->d_ino, o));
      ls.push_back(o);
    }
  }

  // sort
  dout(10) << "collection_list " << c << " sorting " << inolist.size() << " objects" << dendl;
  sort(inolist.begin(), inolist.end());

  // build final list
  ls.resize(inolist.size());
  int i = 0;
  for (vector< pair<ino_t,sobject_t> >::iterator p = inolist.begin(); p != inolist.end(); p++)
    ls[i++].swap(p->second);
  
  dout(10) << "collection_list " << c << " = 0 (" << ls.size() << " objects)" << dendl;
  ::closedir(dir);
  return 0;
}


int FileStore::_create_collection(coll_t c) 
{
  if (fake_collections) return collections.create_collection(c);
  
  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "create_collection " << fn << dendl;
  int r = ::mkdir(fn, 0755);
  if (r < 0) r = -errno;
  dout(10) << "create_collection " << fn << " = " << r << dendl;
  return r;
}

int FileStore::_destroy_collection(coll_t c) 
{
  if (fake_collections) return collections.destroy_collection(c);

  char fn[PATH_MAX];
  get_cdir(c, fn, sizeof(fn));
  dout(15) << "_destroy_collection " << fn << dendl;
  int r = ::rmdir(fn);
  if (r < 0) r = -errno;
  dout(10) << "_destroy_collection " << fn << " = " << r << dendl;
  return r;
}


int FileStore::_collection_add(coll_t c, coll_t cid, const sobject_t& o) 
{
  if (fake_collections) return collections.collection_add(c, o);

  char cof[PATH_MAX];
  get_coname(c, o, cof, sizeof(cof));
  char of[PATH_MAX];
  get_coname(cid, o, of, sizeof(of));
  dout(15) << "collection_add " << cof << " " << of << dendl;
  int r = lfn_link(of, cof);
  if (r < 0) r = -errno;
  dout(10) << "collection_add " << cof << " " << of << " = " << r << dendl;
  return r;
}

int FileStore::_collection_remove(coll_t c, const sobject_t& o) 
{
  if (fake_collections) return collections.collection_remove(c, o);

  char cof[PATH_MAX];
  get_coname(c, o, cof, sizeof(cof));
  dout(15) << "collection_remove " << cof << dendl;
  int r = lfn_unlink(cof);
  if (r < 0) r = -errno;
  dout(10) << "collection_remove " << cof << " = " << r << dendl;
  return r;
}



// eof.
