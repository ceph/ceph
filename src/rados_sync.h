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

#ifndef CEPH_RADOS_SYNC_H
#define CEPH_RADOS_SYNC_H

#include <stddef.h>
#include "include/atomic.h"
#include "common/WorkQueue.h"

#include <string>
#include <sys/types.h>

namespace librados {
  class IoCtx;
  class Rados;
}

extern const char USER_XATTR_PREFIX[];
extern const char RADOS_SYNC_TMP_SUFFIX[];
#define ERR_PREFIX "[ERROR]        "
#define DEFAULT_NUM_RADOS_WORKER_THREADS 5

/* Linux seems to use ENODATA instead of ENOATTR when an extended attribute
 * is missing */
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

enum {
  CHANGED_XATTRS = 0x1,
  CHANGED_CONTENTS = 0x2,
};

/** Given the name of an extended attribute from a file in the filesystem,
 * returns an empty string if the extended attribute does not represent a rados
 * user extended attribute. Otherwise, returns the name of the rados extended
 * attribute.
 *
 * Rados user xattrs are prefixed with USER_XATTR_PREFIX.
 */
std::string get_user_xattr_name(const char *fs_xattr_name);

/* Returns true if 'suffix' is a suffix of str */
bool is_suffix(const char *str, const char *suffix);

/** Represents a directory in the filesystem that we export rados objects to (or
 * import them from.)
 */
class ExportDir
{
public:
  static ExportDir* create_for_writing(const std::string &path, int version,
					  bool create);
  static ExportDir* from_file_system(const std::string &path);

  /* Given a rados object name, return something which looks kind of like the
   * first part of the name.
   *
   * The actual file name that the backed-up object is stored in is irrelevant
   * to rados_sync. The only reason to make it human-readable at all is to make
   * things easier on sysadmins.  The XATTR_FULLNAME extended attribute has the
   * real, full object name.
  *
   * This function turns unicode into a bunch of 'at' signs. This could be
   * fixed. If you try, be sure to handle all the multibyte characters
   * correctly.
   * I guess a better hash would be nice too.
   */
  std::string get_fs_path(const std::string &rados_name) const;

private:
  ExportDir(int version_, const std::string &path_);

  int version;
  std::string path;
};

/** Smart pointer wrapper for a DIR*
 */
class DirHolder {
public:
  DirHolder();
  ~DirHolder();
  int opendir(const char *dir_name);
  DIR *dp;
};

/** IoCtxDistributor is a singleton that distributes out IoCtx instances to
 * different threads.
 */
class IoCtxDistributor
{
public:
  static IoCtxDistributor* instance();
  int init(librados::Rados &cluster, const char *pool_name, int num_ioctxes);
  void clear();
  librados::IoCtx& get_ioctx();
private:
  static IoCtxDistributor *s_instance;
  IoCtxDistributor();
  ~IoCtxDistributor();

  ceph::atomic_t m_highest_iod_idx;

  /* NB: there might be some false sharing here that we could optimize
   * away in the future */
  std::vector<librados::IoCtx> m_io_ctxes;
};

class RadosSyncWQ : public ThreadPool::WorkQueue<std::string> {
public:
  RadosSyncWQ(IoCtxDistributor *io_ctx_dist, time_t timeout, time_t suicide_timeout, ThreadPool *tp);
protected:
  IoCtxDistributor *m_io_ctx_dist;
private:
  bool _enqueue(std::string *s);
  void _dequeue(std::string *o);
  bool _empty();
  std::string *_dequeue();
  void _process_finish(std::string *s);
  void _clear();
  std::deque<std::string*> m_items;
};

/* Stores a length and a chunk of malloc()ed data */
class Xattr {
public:
  Xattr(char *data_, ssize_t len_);
  ~Xattr();
  bool operator==(const struct Xattr &rhs) const;
  bool operator!=(const struct Xattr &rhs) const;

  char *data;
  ssize_t len;
};

/* Represents an object that we are backing up */
class BackedUpObject
{
public:
  static int from_file(const char *file_name, const char *dir_name,
			    std::auto_ptr<BackedUpObject> &obj);
  static int from_path(const char *path, std::auto_ptr<BackedUpObject> &obj);
  static int from_rados(librados::IoCtx& io_ctx, const char *rados_name_,
			auto_ptr<BackedUpObject> &obj);
  ~BackedUpObject();

  /* Get the mangled name for this rados object. */
  std::string get_fs_path(const ExportDir *export_dir) const;

  /* Convert the xattrs on this BackedUpObject to a kind of JSON-like string.
   * This is only used for debugging.
   * Note that we're assuming we can just treat the xattr data as a
   * null-terminated string, which isn't true. Again, this is just for debugging,
   * so it doesn't matter.
   */
  std::string xattrs_to_str() const;

  /* Diff the extended attributes on this BackedUpObject with those found on a
   * different BackedUpObject
   */
  void xattr_diff(const BackedUpObject *rhs,
		  std::list < std::string > &only_in_a,
		  std::list < std::string > &only_in_b,
		  std::list < std::string > &diff) const;

  void get_xattrs(std::list < std::string > &xattrs_) const;

  const Xattr* get_xattr(const std::string name) const;

  const char *get_rados_name() const;

  uint64_t get_rados_size() const;

  time_t get_mtime() const;

  int download(librados::IoCtx &io_ctx, const char *path);

  int upload(librados::IoCtx &io_ctx, const char *file_name, const char *dir_name);

private:
  BackedUpObject(const char *rados_name_, uint64_t rados_size_, time_t rados_time_);

  int read_xattrs_from_file(int fd);

  int read_xattrs_from_rados(librados::IoCtx &io_ctx);

  // don't allow copying
  BackedUpObject &operator=(const BackedUpObject &rhs);
  BackedUpObject(const BackedUpObject &rhs);

  char *rados_name;
  uint64_t rados_size;
  uint64_t rados_time;
  std::map < std::string, Xattr* > xattrs;
};

extern int do_rados_import(ThreadPool *tp, librados::IoCtx &io_ctx,
    IoCtxDistributor* io_ctx_dist, const char *dir_name,
    bool force, bool delete_after);
extern int do_rados_export(ThreadPool *tp, librados::IoCtx& io_ctx,
    IoCtxDistributor *io_ctx_dist, const char *dir_name, 
    bool create, bool force, bool delete_after);

#endif
