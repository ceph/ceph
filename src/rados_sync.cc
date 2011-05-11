// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/xattr.h>
#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/errno.h"
#include "include/rados/librados.hpp"

using std::auto_ptr;
using namespace librados;

static const char * const XATTR_FULLNAME = "user.rados_full_name";
static const char XATTR_PREFIX[] = "user.rados.";
static const size_t XATTR_PREFIX_LEN =
	sizeof(XATTR_PREFIX)/sizeof(XATTR_PREFIX[0]) - 1;

static const char ERR_PREFIX[] = "[ERROR]        ";

/* Linux seems to use ENODATA instead of ENOATTR when an extended attribute
 * is missing */
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

static int xattr_test(const char *dir_name)
{
  int ret;
  int fd = -1;
  const char file_name[] = "/xattr_test_file";
  char path[strlen(dir_name) + strlen(file_name) + 2];
  snprintf(path, sizeof(path), "%s/%s", dir_name, file_name);

  char buf[] = "12345";
  char buf2[sizeof(buf)];
  memset(buf2, 0, sizeof(buf2));

  fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0700);
  if (fd < 0) {
    ret = errno;
    cerr << ERR_PREFIX << "xattr_test: unable to open '" << path << "': "
	 << cpp_strerror(ret) << std::endl;
    goto done;
  }
  ret = fsetxattr(fd, XATTR_FULLNAME, buf, sizeof(buf), 0);
  if (ret) {
    ret = errno;
    cerr << ERR_PREFIX << "xattr_test: fsetxattr failed with error "
	 << cpp_strerror(ret) << std::endl;
    goto done;
  }
  if (close(fd) < 0) {
    ret = errno;
    fd = -1;
    cerr << ERR_PREFIX << "xattr_test: close failed with error "
	 << cpp_strerror(ret) << std::endl;
    goto done;
  }
  fd = -1;
  ret = getxattr(path, XATTR_FULLNAME, buf2, sizeof(buf2));
  if (ret < 0) {
    ret = errno;
    cerr << ERR_PREFIX << "xattr_test: fgetxattr failed with error "
	 << cpp_strerror(ret) << std::endl;
    goto done;
  }
  if (memcmp(buf, buf2, sizeof(buf))) {
    ret = ENOTSUP;
    cerr << ERR_PREFIX << "xattr_test: failed to read back the same xattr "
         << "value that we set." << std::endl;
    goto done;
  }
  ret = 0;

done:
  if (fd >= 0) {
    close(fd);
    fd = -1;
  }
  unlink(path);
  if (ret) {
    cerr << ERR_PREFIX << "xattr_test: the filesystem at " << dir_name << " does "
         << "not appear to support extended attributes. Please remount your "
	 << "filesystem with extended attributes enabled, or pick a different "
	 << "directory." << std::endl;
  }
  return ret;
}

class DirHolder {
public:
  DirHolder()
    : dp(NULL)
  {
  }
  ~DirHolder() {
    if (!dp)
      return;
    if (closedir(dp)) {
      int err = errno;
      cerr << ERR_PREFIX << "closedir failed: " << cpp_strerror(err) << std::endl;
    }
    dp = NULL;
  }
  int opendir(const char *dir_name) {
    dp = ::opendir(dir_name);
    if (!dp) {
      int err = errno;
      return err;
    }
    return 0;
  }
  DIR *dp;
};

// Stores a length and a chunk of malloc()ed data
class Xattr {
public:
  Xattr(char *data_, ssize_t len_)
    : data(data_), len(len_)
  {
  }
  ~Xattr() {
    free(data);
  }
  bool operator==(const struct Xattr &rhs) const {
    if (len != rhs.len)
      return false;
    return (memcmp(data, rhs.data, len) == 0);
  }
  bool operator!=(const struct Xattr &rhs) const {
    return !((*this) == rhs);
  }
  char *data;
  ssize_t len;
};

// Represents an object that we are backing up
class BackedUpObject
{
public:
  static int from_file(const char *file_name, const char *dir_name,
			    std::auto_ptr<BackedUpObject> &obj)
  {
    char obj_path[strlen(dir_name) + strlen(file_name) + 2];
    snprintf(obj_path, sizeof(obj_path), "%s/%s", dir_name, file_name);
    return BackedUpObject::from_path(obj_path, obj);
  }

  static int from_path(const char *path, std::auto_ptr<BackedUpObject> &obj)
  {
    int ret;
    FILE *fp = fopen(path, "r");
    if (!fp) {
      ret = errno;
      if (ret != ENOENT) {
	cerr << ERR_PREFIX << "BackedUpObject::from_path: error while trying to "
	     << "open '" << path << "': " <<  cpp_strerror(ret) << std::endl;
      }
      return ret;
    }
    int fd = fileno(fp);
    struct stat st_buf;
    memset(&st_buf, 0, sizeof(st_buf));
    ret = fstat(fd, &st_buf);
    if (ret) {
      ret = errno;
      fclose(fp);
      cerr << ERR_PREFIX << "BackedUpObject::from_path: error while trying "
	   << "to stat '" << path << "': " <<  cpp_strerror(ret) << std::endl;
      return ret;
    }

    // get fullname
    ssize_t res = fgetxattr(fd, XATTR_FULLNAME, NULL, 0);
    if (res <= 0) {
      fclose(fp);
      ret = errno;
      if (res == 0) {
	cerr << ERR_PREFIX << "BackedUpObject::from_path: found empty "
	     << XATTR_FULLNAME << " attribute on '" << path
	     << "'" << std::endl;
	ret = ENOATTR;
      } else if (ret == ENOATTR) {
	cerr << ERR_PREFIX << "BackedUpObject::from_path: there was no "
	     << XATTR_FULLNAME << " attribute found on '" << path
	     << "'" << std::endl;
      } else {
	cerr << ERR_PREFIX << "getxattr error: " << cpp_strerror(ret) << std::endl;
      }
      return ret;
    }
    char rados_name_[res + 1];
    memset(rados_name_, 0, sizeof(rados_name_));
    res = fgetxattr(fd, XATTR_FULLNAME, rados_name_, res);
    if (res < 0) {
      ret = errno;
      fclose(fp);
      cerr << ERR_PREFIX << "BackedUpObject::getxattr(" << XATTR_FULLNAME
	   << ") error: " << cpp_strerror(ret) << std::endl;
      return ret;
    }

    BackedUpObject *o = new BackedUpObject(rados_name_,
			       st_buf.st_size, st_buf.st_mtime);
    if (!o) {
      fclose(fp);
      return ENOBUFS;
    }
    ret = o->read_xattrs_from_file(fileno(fp));
    if (ret) {
      fclose(fp);
      cerr << ERR_PREFIX << "BackedUpObject::from_path(path = '"
	   << path << "): read_xattrs_from_file returned " << ret << std::endl;
      delete o;
      return ret;
    }

    fclose(fp);
    obj.reset(o);
    return 0;
  }

  static int from_rados(IoCtx& io_ctx, const char *rados_name_,
			auto_ptr<BackedUpObject> &obj)
  {
    uint64_t rados_size_ = 0;
    time_t rados_time_ = 0;
    int ret = io_ctx.stat(rados_name_, &rados_size_, &rados_time_);
    if (ret == -ENOENT) {
      // don't complain here about ENOENT
      return ret;
    } else if (ret < 0) {
      cerr << ERR_PREFIX << "BackedUpObject::from_rados(rados_name_ = '"
	   << rados_name_ << "'): stat failed with error " << ret << std::endl;
      return ret;
    }
    BackedUpObject *o = new BackedUpObject(rados_name_, rados_size_, rados_time_);
    ret = o->read_xattrs_from_rados(io_ctx);
    if (ret) {
      cerr << ERR_PREFIX << "BackedUpObject::from_rados(rados_name_ = '"
	    << rados_name_ << "'): read_xattrs_from_rados returned "
	    << ret << std::endl;
      delete o;
      return ret;
    }
    obj.reset(o);
    return 0;
  }

  ~BackedUpObject()
  {
    for (std::map < std::string, Xattr* >::iterator x = xattrs.begin();
	   x != xattrs.end(); ++x)
    {
      delete x->second;
      x->second = NULL;
    }
    free(rados_name);
  }

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
  std::string get_fs_path(const char *dir_name) const
  {
    size_t i;
    uint32_t hash = 0;
    size_t sz = strlen(rados_name);
    for (i = 0; i < sz; ++i) {
      hash += (rados_name[i] * 33);
    }
    if (sz > 200)
      sz = 200;
    char fs_path[9 + sz + 1];
    sprintf(fs_path, "%08x_", hash);
    for (i = 0; i < sz; ++i) {
      // Just replace anything that looks funny with an 'at' sign.
      // Unicode also gets turned into 'at' signs.
      char c = rados_name[i];
      if (c < 0x20) // also eliminate bytes with the high bit set
	c = '@';
      else if (c == 0x7f)
	c = '@';
      else if (c == '/')
	c = '@';
      else if (c == '\\')
	c = '@';
      else if (c == ' ')
	c = '_';
      else if (c == '.')
	c = '@';
      else if (c == '\n')
	c = '@';
      else if (c == '\r')
	c = '@';
      fs_path[9 + i] = c;
    }
    fs_path[9 + i] = '\0';
    ostringstream oss;
    oss << dir_name << "/" << fs_path;
    return oss.str();
  }

  /* Convert the xattrs on this BackedUpObject to a kind of JSON-like string.
   * This is only used for debugging.
   * Note that we're assuming we can just treat the xattr data as a
   * null-terminated string, which isn't true. Again, this is just for debugging,
   * so it doesn't matter.
   */
  std::string xattrs_to_str() const
  {
    ostringstream oss;
    std::string prefix;
    for (std::map < std::string, Xattr* >::const_iterator x = xattrs.begin();
	   x != xattrs.end(); ++x)
    {
      char buf[x->second->len + 1];
      memcpy(buf, x->second->data, x->second->len);
      buf[x->second->len] = '\0';
      oss << prefix << "{" << x->first << ":" << buf << "}";
      prefix = ", ";
    }
    return oss.str();
  }

  /* Diff the extended attributes on this BackedUpObject with those found on a
   * different BackedUpObject
   */
  void xattr_diff(const BackedUpObject *rhs,
		  std::list < std::string > &only_in_a,
		  std::list < std::string > &only_in_b,
		  std::list < std::string > &diff) const
  {
    only_in_a.clear();
    only_in_b.clear();
    diff.clear();
    for (std::map < std::string, Xattr* >::const_iterator x = xattrs.begin();
	   x != xattrs.end(); ++x)
    {
      std::map < std::string, Xattr* >::const_iterator r = rhs->xattrs.find(x->first);
      if (r == rhs->xattrs.end()) {
	only_in_a.push_back(x->first);
      }
      else {
	const Xattr &r_obj(*r->second);
	const Xattr &x_obj(*x->second);
	if (r_obj != x_obj)
	  diff.push_back(x->first);
      }
    }
    for (std::map < std::string, Xattr* >::const_iterator r = rhs->xattrs.begin();
	   r != rhs->xattrs.end(); ++r)
    {
      std::map < std::string, Xattr* >::const_iterator x = rhs->xattrs.find(r->first);
      if (x == xattrs.end()) {
	only_in_b.push_back(r->first);
      }
    }
  }

  void get_xattrs(std::list < std::string > &xattrs_) const
  {
    for (std::map < std::string, Xattr* >::const_iterator r = xattrs.begin();
	   r != xattrs.end(); ++r)
    {
      xattrs_.push_back(r->first);
    }
  }

  const Xattr* get_xattr(const std::string name) const
  {
    std::map < std::string, Xattr* >::const_iterator x = xattrs.find(name);
    if (x == xattrs.end())
      return NULL;
    else
      return x->second;
  }

  const char *get_rados_name() const {
    return rados_name;
  }

  uint64_t get_rados_size() const {
    return rados_size;
  }

  time_t get_mtime() const {
    return rados_time;
  }

  int download(IoCtx &io_ctx, const char *file_name)
  {
    FILE *fp = fopen(file_name, "w");
    if (!fp) {
      int err = errno;
      cerr << ERR_PREFIX << "download: error opening '" << file_name << "':"
	   <<  cpp_strerror(err) << std::endl;
      return err;
    }
    int fd = fileno(fp);
    uint64_t off = 0;
    static const int CHUNK_SZ = 32765;
    while (true) {
      bufferlist bl;
      int rlen = io_ctx.read(rados_name, bl, CHUNK_SZ, off);
      if (rlen < 0) {
	cerr << ERR_PREFIX << "download: io_ctx.read(" << rados_name << ") returned "
	     << rlen << std::endl;
	return rlen;
      }
      if (rlen < CHUNK_SZ)
	off = 0;
      else
	off += rlen;
      size_t flen = fwrite(bl.c_str(), 1, rlen, fp);
      if (flen != (size_t)rlen) {
	int err = errno;
	cerr << ERR_PREFIX << "download: fwrite(" << file_name << ") error: "
	     << cpp_strerror(err) << std::endl;
	fclose(fp);
	return err;
      }
      if (off == 0)
	break;
    }
    size_t attr_sz = strlen(rados_name) + 1;
    int res = fsetxattr(fd, XATTR_FULLNAME, rados_name, attr_sz, 0);
    if (res) {
      int err = errno;
      cerr << ERR_PREFIX << "download: fsetxattr(" << file_name << ") error: "
	   << cpp_strerror(err) << std::endl;
      fclose(fp);
      return err;
    }
    if (fclose(fp)) {
      int err = errno;
      cerr << ERR_PREFIX << "download: fclose(" << file_name << ") error: "
	   << cpp_strerror(err) << std::endl;
      return err;
    }
    return 0;
  }

  int upload(IoCtx &io_ctx, const char *file_name, const char *dir_name)
  {
    char path[strlen(file_name) + strlen(dir_name) + 2];
    snprintf(path, sizeof(path), "%s/%s", dir_name, file_name);
    FILE *fp = fopen(path, "r");
    if (!fp) {
      int err = errno;
      cerr << ERR_PREFIX << "upload: error opening '" << path << "': "
	   << cpp_strerror(err) << std::endl;
      return err;
    }
    // Need to truncate RADOS object to size 0, in case there is
    // already something there.
    int ret = io_ctx.trunc(rados_name, 0);
    if (ret) {
      cerr << ERR_PREFIX << "upload: trunc failed with error " << ret << std::endl;
      return ret;
    }
    uint64_t off = 0;
    static const int CHUNK_SZ = 32765;
    while (true) {
      char buf[CHUNK_SZ];
      int flen = fread(buf, CHUNK_SZ, 1, fp);
      if (flen < 0) {
	int err = errno;
	cerr << ERR_PREFIX << "upload: fread(" << file_name << ") error: "
	     << cpp_strerror(err) << std::endl;
	fclose(fp);
	return err;
      }
      if ((flen == 0) && (off != 0)) {
	fclose(fp);
	break;
      }
      // There must be a zero-copy way to do this?
      bufferlist bl;
      bl.append(buf, flen);
      int rlen = io_ctx.write(rados_name, bl, flen, off);
      if (rlen < 0) {
	fclose(fp);
	cerr << ERR_PREFIX << "upload: rados_write error: " << rlen << std::endl;
	return rlen;
      }
      if (rlen != flen) {
	fclose(fp);
	cerr << ERR_PREFIX << "upload: rados_write error: short write" << std::endl;
	return -EIO;
      }
      off += rlen;
      if (flen < CHUNK_SZ) {
	fclose(fp);
	return 0;
      }
    }
    return 0;
  }

private:
  BackedUpObject(const char *rados_name_, uint64_t rados_size_, time_t rados_time_)
    : rados_name(strdup(rados_name_)),
      rados_size(rados_size_),
      rados_time(rados_time_)
  {
  }

  int read_xattrs_from_file(int fd)
  {
    ssize_t blen = flistxattr(fd, NULL, 0);
    if (blen > 0x1000000) {
      cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: unwilling "
	   << "to allocate a buffer of size " << blen << " on the stack for "
	   << "flistxattr." << std::endl;
      return ENOBUFS;
    }
    char buf[blen + 1];
    memset(buf, 0, sizeof(buf));
    ssize_t blen2 = flistxattr(fd, buf, blen);
    if (blen != blen2) {
      cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: xattrs changed while "
	   << "we were trying to "
	   << "list them? First length was " << blen << ", but now it's " << blen2
	   << std::endl;
      return EDOM;
    }
    const char *b = buf;
    while (*b) {
      size_t bs = strlen(b);
      if (strncmp(b, XATTR_PREFIX, XATTR_PREFIX_LEN) == 0) {
	ssize_t attr_len = fgetxattr(fd, b, NULL, 0);
	if (attr_len < 0) {
	  int err = errno;
	  cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: "
	       << "fgetxattr(rados_name = '" << rados_name << "', xattr_name='"
	       << b << "') failed: " << cpp_strerror(err) << std::endl;
	  return EDOM;
	}
	char *attr = (char*)malloc(attr_len);
	if (!attr) {
	  cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: "
	       << "malloc(" << attr_len << ") failed for xattr_name='"
	       << b << "'" << std::endl;
	  return ENOBUFS;
	}
	ssize_t attr_len2 = fgetxattr(fd, b, attr, attr_len);
	if (attr_len2 < 0) {
	  int err = errno;
	  cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: "
	       << "fgetxattr(rados_name = '" << rados_name << "', "
	       << "xattr_name='" << b << "') failed: "
	       << cpp_strerror(err) << std::endl;
	  free(attr);
	  return EDOM;
	}
	if (attr_len2 != attr_len) {
	  cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: xattr "
	       << "changed while we were trying to get it? "
	       << "fgetxattr(rados_name = '"<< rados_name
	       << "', xattr_name='" << b << "') returned a different length "
	       << "than when we first called it! old_len = " << attr_len
	       << "new_len = " << attr_len2 << std::endl;
	  free(attr);
	  return EDOM;
	}
	xattrs[b] = new Xattr(attr, attr_len);
      }
      b += (bs + 1);
    }
    return 0;
  }

  int read_xattrs_from_rados(IoCtx &io_ctx)
  {
    map<std::string, bufferlist> attrset;
    int ret = io_ctx.getxattrs(rados_name, attrset);
    if (ret) {
      cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_rados: "
	   << "getxattrs failed with error code " << ret << std::endl;
      return ret;
    }
    for (map<std::string, bufferlist>::iterator i = attrset.begin();
	 i != attrset.end(); )
    {
      bufferlist& bl(i->second);
      char *data = (char*)malloc(bl.length());
      if (!data)
	return ENOBUFS;
      memcpy(data, bl.c_str(), bl.length());
      Xattr *xattr = new Xattr(data, bl.length());
      if (!xattr) {
	free(data);
	return ENOBUFS;
      }
      xattrs[i->first] = xattr;
      attrset.erase(i++);
    }
    return 0;
  }

  // don't allow copying
  BackedUpObject &operator=(const BackedUpObject &rhs);
  BackedUpObject(const BackedUpObject &rhs);

  char *rados_name;
  uint64_t rados_size;
  uint64_t rados_time;
  std::map < std::string, Xattr* > xattrs;
};

static int do_export(IoCtx& io_ctx, const char *dir_name,
		     bool force, bool delete_after)
{
  int ret;
  librados::ObjectIterator oi = io_ctx.objects_begin();
  librados::ObjectIterator oi_end = io_ctx.objects_end();
  for (; oi != oi_end; ++oi) {
    enum {
      CHANGED_XATTRS = 0x1,
      CHANGED_CONTENTS = 0x2,
    };
    int flags = 0;
    auto_ptr <BackedUpObject> sobj;
    auto_ptr <BackedUpObject> dobj;
    string rados_name(*oi);
    std::list < std::string > only_in_a;
    std::list < std::string > only_in_b;
    std::list < std::string > diff;
    ret = BackedUpObject::from_rados(io_ctx, rados_name.c_str(), sobj);
    if (ret) {
      cerr << ERR_PREFIX << "couldn't get '" << rados_name << "' from rados: error "
	   << ret << std::endl;
      return ret;
    }
    std::string obj_path(sobj->get_fs_path(dir_name));
    if (force) {
      flags |= (CHANGED_CONTENTS | CHANGED_XATTRS);
    }
    else {
      ret = BackedUpObject::from_path(obj_path.c_str(), dobj);
      if (ret == ENOENT) {
	sobj->get_xattrs(only_in_a);
	flags |= CHANGED_CONTENTS;
      }
      else if (ret) {
	cerr << ERR_PREFIX << "BackedUpObject::from_path returned "
	     << ret << std::endl;
	return ret;
      }
      else {
	sobj->xattr_diff(dobj.get(), only_in_a, only_in_b, diff);
	if ((sobj->get_rados_size() == dobj->get_rados_size()) &&
	    (sobj->get_mtime() == dobj->get_mtime())) {
	  flags |= CHANGED_CONTENTS;
	}
      }
    }
    if (flags & CHANGED_CONTENTS) {
      ret = sobj->download(io_ctx, obj_path.c_str());
      if (ret) {
	cerr << ERR_PREFIX << "download error: " << ret << std::endl;
	return ret;
      }
    }
    diff.splice(diff.begin(), only_in_a);
    for (std::list < std::string >::const_iterator x = diff.begin();
	 x != diff.end(); ++x) {
      flags |= CHANGED_XATTRS;
      const Xattr *xattr = sobj->get_xattr(*x);
      if (xattr == NULL) {
	cerr << ERR_PREFIX << "internal error on line: " << __LINE__ << std::endl;
	return -ENOSYS;
      }
      ret = setxattr(obj_path.c_str(), x->c_str(), xattr->data, xattr->len, 0);
      if (ret) {
	ret = errno;
	cerr << ERR_PREFIX << "setxattr error: " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
    for (std::list < std::string >::const_iterator x = only_in_b.begin();
	 x != only_in_b.end(); ++x) {
      flags |= CHANGED_XATTRS;
      ret = removexattr(obj_path.c_str(), x->c_str());
      if (ret) {
	ret = errno;
	cerr << ERR_PREFIX << "removexattr error: " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
    if (force) {
      cout << "[force]        " << rados_name << std::endl;
    }
    else if (flags & CHANGED_CONTENTS) {
      cout << "[exported]     " << rados_name << std::endl;
    }
    else if (flags & CHANGED_XATTRS) {
      cout << "[xattr]        " << rados_name << std::endl;
    }
  }
  if (delete_after) {
    DirHolder dh;
    int err = dh.opendir(dir_name);
    if (err) {
      cerr << ERR_PREFIX << "opendir(" << dir_name << ") error: "
	   << cpp_strerror(err) << std::endl;
      return err;
    }
    while (true) {
      struct dirent *de = readdir(dh.dp);
      if (!de)
	break;
      if ((strcmp(de->d_name, ".") == 0) || (strcmp(de->d_name, "..") == 0))
	continue;
      auto_ptr <BackedUpObject> lobj;
      ret = BackedUpObject::from_file(de->d_name, dir_name, lobj);
      if (ret) {
	cout << ERR_PREFIX << "BackedUpObject::from_file: delete loop: "
	     << "got error " << ret << std::endl;
	return ret;
      }
      auto_ptr <BackedUpObject> robj;
      ret = BackedUpObject::from_rados(io_ctx, lobj->get_rados_name(), robj);
      if (ret == -ENOENT) {
	// The entry doesn't exist on the remote server; delete it locally
	char path[strlen(dir_name) + strlen(de->d_name) + 2];
	snprintf(path, sizeof(path), "%s/%s", dir_name, de->d_name);
	if (unlink(path)) {
	  ret = errno;
	  cerr << ERR_PREFIX << "error unlinking '" << path << "': "
	       << cpp_strerror(ret) << std::endl;
	  return ret;
	}
	cout << "[deleted]      " << "removed '" << de->d_name << "'" << std::endl;
      }
      else if (ret) {
	cerr << ERR_PREFIX << "BackedUpObject::from_rados: delete loop: "
	     << "got error " << ret << std::endl;
	return ret;
      }
    }
  }
  cout << "[done]" << std::endl;
  return 0;
}

static int do_import(IoCtx& io_ctx, const char *dir_name,
		     bool force, bool delete_after)
{
  int ret = mkdir(dir_name, 0700);
  if (ret < 0) {
    int err = errno;
    if (err != EEXIST)
      return err;
  }
  DirHolder dh;
  ret = dh.opendir(dir_name);
  if (ret) {
    cerr << ERR_PREFIX << "opendir(" << dir_name << ") error: "
	 << cpp_strerror(ret) << std::endl;
    return ret;
  }
  while (true) {
    enum {
      CHANGED_XATTRS = 0x1,
      CHANGED_CONTENTS = 0x2,
    };
    auto_ptr <BackedUpObject> sobj;
    auto_ptr <BackedUpObject> dobj;
    std::list < std::string > only_in_a;
    std::list < std::string > only_in_b;
    std::list < std::string > diff;
    int flags = 0;
    struct dirent *de = readdir(dh.dp);
    if (!de)
      break;
    if ((strcmp(de->d_name, ".") == 0) || (strcmp(de->d_name, "..") == 0))
      continue;
    ret = BackedUpObject::from_file(de->d_name, dir_name, sobj);
    if (ret) {
      cerr << ERR_PREFIX << "BackedUpObject::from_file: got error "
	   << ret << std::endl;
      return ret;
    }
    const char *rados_name(sobj->get_rados_name());
    if (force) {
      flags |= (CHANGED_CONTENTS | CHANGED_XATTRS);
    }
    else {
      ret = BackedUpObject::from_rados(io_ctx, rados_name, dobj);
      if (ret == -ENOENT) {
	flags |= CHANGED_CONTENTS;
	sobj->get_xattrs(only_in_a);
      }
      else if (ret) {
	cerr << ERR_PREFIX << "BackedUpObject::from_rados returned "
	     << ret << std::endl;
	return ret;
      }
      else {
	sobj->xattr_diff(dobj.get(), only_in_a, only_in_b, diff);
	if ((sobj->get_rados_size() == dobj->get_rados_size()) &&
	    (sobj->get_mtime() == dobj->get_mtime())) {
	  flags |= CHANGED_CONTENTS;
	}
      }
    }
    if (flags & CHANGED_CONTENTS) {
      ret = sobj->upload(io_ctx, de->d_name, dir_name);
      if (ret) {
	cerr << ERR_PREFIX << "upload error: " << ret << std::endl;
	return ret;
      }
    }
    for (std::list < std::string >::const_iterator x = only_in_a.begin();
	 x != only_in_a.end(); ++x) {
      flags |= CHANGED_XATTRS;
      const Xattr *xattr = sobj->get_xattr(*x);
      if (xattr == NULL) {
	cerr << ERR_PREFIX << "internal error on line: " << __LINE__ << std::endl;
	return -ENOSYS;
      }
      bufferlist bl;
      bl.append(xattr->data, xattr->len);
      ret = io_ctx.setxattr(rados_name, x->c_str(), bl);
      if (ret < 0) {
	ret = errno;
	cerr << ERR_PREFIX << "io_ctx.setxattr(rados_name='" << rados_name
	     << "', xattr_name='" << x->c_str() << "'): " << cpp_strerror(ret)
	     << std::endl;
	return ret;
      }
    }
    for (std::list < std::string >::const_iterator x = diff.begin();
	 x != diff.end(); ++x) {
      flags |= CHANGED_XATTRS;
      const Xattr *xattr = sobj->get_xattr(*x);
      if (xattr == NULL) {
	cerr << ERR_PREFIX << "internal error on line: " << __LINE__ << std::endl;
	return -ENOSYS;
      }
      bufferlist bl;
      bl.append(xattr->data, xattr->len);
      ret = io_ctx.rmxattr(rados_name, x->c_str());
      if (ret < 0) {
	cerr << ERR_PREFIX << "io_ctx.rmxattr error2: " << cpp_strerror(ret)
	     << std::endl;
	return ret;
      }
      ret = io_ctx.setxattr(rados_name, x->c_str(), bl);
      if (ret < 0) {
	ret = errno;
	cerr << ERR_PREFIX << "io_ctx.setxattr(rados_name='" << rados_name
	     << "', xattr='" << x->c_str() << "'): " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
    for (std::list < std::string >::const_iterator x = only_in_b.begin();
	 x != only_in_b.end(); ++x) {
      flags |= CHANGED_XATTRS;
      ret = io_ctx.rmxattr(rados_name, x->c_str());
      if (ret < 0) {
	ret = errno;
	cerr << ERR_PREFIX << "rmxattr error3: " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
    if (force) {
      cout << "[force]        " << rados_name << std::endl;
    }
    else if (flags & CHANGED_CONTENTS) {
      cout << "[imported]     " << rados_name << std::endl;
    }
    else if (flags & CHANGED_XATTRS) {
      cout << "[xattr]        " << rados_name << std::endl;
    }
  }
  if (delete_after) {
    librados::ObjectIterator oi = io_ctx.objects_begin();
    librados::ObjectIterator oi_end = io_ctx.objects_end();
    for (; oi != oi_end; ++oi) {
      string rados_name(*oi);
      auto_ptr <BackedUpObject> robj;
      ret = BackedUpObject::from_rados(io_ctx, rados_name.c_str(), robj);
      if (ret) {
	cerr << ERR_PREFIX << "BackedUpObject::from_rados in delete loop "
	     << "returned " << ret << std::endl;
	return ret;
      }
      std::string obj_path(robj->get_fs_path(dir_name));
      auto_ptr <BackedUpObject> lobj;
      ret = BackedUpObject::from_path(obj_path.c_str(), lobj);
      if (ret == ENOENT) {
	ret = io_ctx.remove(rados_name);
	if (ret && ret != -ENOENT) {
	  cerr << ERR_PREFIX << "io_ctx.remove(" << obj_path << ") failed "
	       << "with error " << ret << std::endl;
	  return ret;
	}
	cout << "[deleted]      " << "removed '" << rados_name << "'" << std::endl;
      }
      else if (ret) {
	cerr << ERR_PREFIX << "BackedUpObject::from_path in delete loop "
	     << "returned " << ret << std::endl;
	return ret;
      }
    }
  }
  cout << "[done]" << std::endl;
  return 0;
}

int rados_tool_sync(const std::map < std::string, std::string > &opts,
                             std::vector<const char*> &args)
{
  int ret;
  bool force = opts.count("force");
  bool delete_after = opts.count("delete-after");
  bool create = opts.count("create");
  std::string action, src, dst;
  std::vector<const char*>::iterator i = args.begin();
  if ((i != args.end()) &&
      ((strcmp(*i, "import") == 0) || (strcmp(*i, "export") == 0))) {
    action = *i;
    ++i;
  }
  else {
    cerr << "rados" << ": You must specify either 'import' or 'export'.\n";
    cerr << "Use --help to show help.\n";
    exit(1);
  }
  if (i != args.end()) {
    src = *i;
    ++i;
  }
  else {
    cerr << "rados" << ": You must give a source.\n";
    cerr << "Use --help to show help.\n";
    exit(1);
  }
  if (i != args.end()) {
    dst = *i;
    ++i;
  }
  else {
    cerr << "rados" << ": You must give a destination.\n";
    cerr << "Use --help to show help.\n";
    exit(1);
  }

  // open rados
  Rados rados;
  if (rados.init_with_config(&g_conf) < 0) {
     cerr << "rados" << ": failed to initialize Rados!" << std::endl;
     exit(1);
  }
  if (rados.connect() < 0) {
     cerr << "rados" << ": failed to connect to Rados cluster!" << std::endl;
     exit(1);
  }
  IoCtx io_ctx;
  std::string pool_name = (action == "import") ? dst : src;
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if ((ret == -ENOENT) && (action == "import")) {
    if (create) {
      ret = rados.pool_create(pool_name.c_str());
      if (ret) {
	cerr << "rados" << ": pool_create failed with error " << ret
	     << std::endl;
	exit(ret);
      }
      ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
    }
    else {
      cerr << "rados" << ": pool '" << pool_name << "' does not exist. Use "
	   << "--create to try to create it." << std::endl;
      exit(ENOENT);
    }
  }
  if (ret < 0) {
    cerr << "rados" << ": error opening pool " << pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    exit(ret);
  }

  std::string dir_name = (action == "import") ? src : dst;

  if (action == "import") {
    if (access(dir_name.c_str(), R_OK)) {
	cerr << "rados" << ": source directory '" << dst
	     << "' appears to be inaccessible." << std::endl;
	exit(ENOENT);
    }
    ret = xattr_test(dir_name.c_str());
    if (ret)
      return ret;
    return do_import(io_ctx, src.c_str(), force, delete_after);
  }
  else {
    if (access(dst.c_str(), W_OK)) {
      if (create) {
	ret = mkdir(dst.c_str(), 0700);
	if (ret < 0) {
	  ret = errno;
	  cerr << "rados" << ": mkdir(" << dst << ") failed with error " << ret
	       << std::endl;
	  exit(ret);
	}
      }
      else {
	cerr << "rados" << ": directory '" << dst << "' is not accessible. Use "
	     << "--create to try to create it.\n";
	exit(ENOENT);
      }
    }
    ret = xattr_test(dir_name.c_str());
    if (ret)
      return ret;
    return do_export(io_ctx, dst.c_str(), force, delete_after);
  }
}
