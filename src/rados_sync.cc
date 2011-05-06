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

using namespace librados;

static const char * const XATTR_FULLNAME = "user.rados_full_name";
static const char XATTR_PREFIX[] = "user.rados.";
static const size_t XATTR_PREFIX_LEN =
	sizeof(XATTR_PREFIX)/sizeof(XATTR_PREFIX[0]);

/* Linux seems to use ENODATA instead of ENOATTR when an extended attribute
 * is missing */
#ifndef ENOATTR
#define ENOATTR ENODATA
#endif

static void usage()
{
  cerr << "usage:\n\
\n\
Importing data from a local directory to a rados pool:\n\
rados_sync [options] import <local-directory> <rados-pool>\n\
\n\
Exporting data from a rados pool to a local directory:\n\
rados_sync [options] export <rados-pool> <local-directory>\n\
\n\
options:\n\
-h or --help            This help message\n\
-c or --create          Create destination pools or directories that don't exist\n\
";
}

static int xattr_test(const char *dir_name)
{
  int ret;
  int fd = -1;
  ostringstream oss;
  oss << dir_name << "/xattr_test_file";

  char buf[] = "12345";
  char buf2[sizeof(buf)];
  memset(buf2, 0, sizeof(buf2));

  fd = open(oss.str().c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0700);
  if (fd < 0) {
    ret = errno;
    cerr << "xattr_test: unable to open '" << oss.str() << "': "
	 << cpp_strerror(ret) << std::endl;
    goto done;
  }
  ret = fsetxattr(fd, XATTR_FULLNAME, buf, sizeof(buf), 0);
  if (ret) {
    ret = errno;
    cerr << "xattr_test: fsetxattr failed with error " << cpp_strerror(ret)
	 << std::endl;
    goto done;
  }
  if (close(fd) < 0) {
    ret = errno;
    fd = -1;
    cerr << "xattr_test: close failed with error " << cpp_strerror(ret)
	 << std::endl;
    goto done;
  }
  fd = -1;
  ret = getxattr(oss.str().c_str(), XATTR_FULLNAME, buf2, sizeof(buf2));
  if (ret < 0) {
    ret = errno;
    cerr << "xattr_test: fgetxattr failed with error " << cpp_strerror(ret)
	 << std::endl;
    goto done;
  }
  if (memcmp(buf, buf2, sizeof(buf))) {
    ret = ENOTSUP;
    cerr << "xattr_test: failed to read back the same xattr value "
         << "that we set." << std::endl;
    goto done;
  }
  ret = 0;

done:
  if (fd >= 0) {
    close(fd);
    fd = -1;
    unlink(oss.str().c_str());
  }
  if (ret) {
    cerr << "xattr_test: the filesystem at " << dir_name << " does not appear to "
         << "support extended attributes. Please remount your filesystem with "
	 << "extended attributes enabled, or pick a different directory."
	 << std::endl;
  }
  return ret;
}

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
			    BackedUpObject **obj)
  {
    int ret;
    char obj_path[strlen(dir_name) + strlen(file_name) + 2];
    snprintf(obj_path, sizeof(obj_path), "%s/%s", dir_name, file_name);
    FILE *fp = fopen(obj_path, "r");
    if (!fp) {
      ret = errno;
      if (ret != ENOENT) {
	cerr << "BackedUpObject::from_file: error while trying to open '"
	     << obj_path << "': " <<  cpp_strerror(ret) << std::endl;
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
      cerr << "BackedUpObject::from_file: error while trying to stat '"
	   << obj_path << "': " <<  cpp_strerror(ret) << std::endl;
      return ret;
    }

    // get fullname
    ssize_t res = fgetxattr(fd, XATTR_FULLNAME, NULL, 0);
    if (res <= 0) {
      ret = (res == 0) ? ENOATTR : errno;
      fclose(fp);
      if (ret == ENOATTR)
	cerr << "no " << XATTR_FULLNAME << " attribute found." << std::endl;
      else
	cerr << "getxattr error: " << cpp_strerror(ret) << std::endl;
      return ret;
    }
    char rados_name_[res + 1];
    memset(rados_name_, 0, sizeof(rados_name_));
    res = fgetxattr(fd, XATTR_FULLNAME, rados_name_, res);
    if (res < 0) {
      ret = errno;
      fclose(fp);
      cerr << "getxattr error: " << cpp_strerror(ret) << std::endl;
      return ret;
    }

    BackedUpObject *o = new BackedUpObject(rados_name_,
			       st_buf.st_size, st_buf.st_mtime);
    if (!o) {
      fclose(fp);
      return ENOBUFS;
    }
    ret = o->read_xattrs(fileno(fp));
    if (ret) {
      fclose(fp);
      cerr << "BackedUpObject::from_file(file_name = '" << file_name
	   << "', dir_name = '" << dir_name << "'): "
	   << "read_xattrs returned " << ret << std::endl;
      delete o;
      return ret;
    }

    fclose(fp);
    *obj = o;
    return 0;
  }

  static int from_rados(IoCtx& io_ctx, const char *rados_name_,
			BackedUpObject **obj)
  {
    uint64_t rados_size_ = 0;
    time_t rados_time_ = 0;
    int ret = io_ctx.stat(rados_name_, &rados_size_, &rados_time_);
    if (ret) {
      cerr << "BackedUpObject::from_rados(rados_name_ = '"
	   << rados_name_ << "'): stat failed with error " << ret << std::endl;
      return ret;
    }
    BackedUpObject *o = new BackedUpObject(rados_name_, rados_size_, rados_time_);
    *obj = o;
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

  /* Given a rados object name, return something which looks kind of like
   * the first part of the name. This is just a convenience for sysadmins,
   * so that they can sort of get an idea of what is stored in the backup
   * files. The extended attribute has the real, full object name.
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

  void xattr_diff(const BackedUpObject &rhs,
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
      std::map < std::string, Xattr* >::const_iterator r = rhs.xattrs.find(x->first);
      if (r == rhs.xattrs.end()) {
	only_in_a.push_back(x->first);
      }
      else {
	const Xattr &r_obj(*r->second);
	const Xattr &x_obj(*x->second);
	if (r_obj != x_obj)
	  diff.push_back(x->first);
      }
    }
    for (std::map < std::string, Xattr* >::const_iterator r = rhs.xattrs.begin();
	   r != rhs.xattrs.end(); ++r)
    {
      std::map < std::string, Xattr* >::const_iterator x = rhs.xattrs.find(r->first);
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
      cerr << "download: error opening '" << file_name << "':"
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
	cerr << "download: io_ctx.read(" << rados_name << ") returned "
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
	cerr << "download: fwrite(" << file_name << ") error: "
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
      cerr << "download: fsetxattr(" << file_name << ") error: "
	   << cpp_strerror(err) << std::endl;
      fclose(fp);
      if (err == ENOENT) {
	cerr << "Please make sure you have xattrs enabled on the filesystem "
	     << "you're trying to back up to!" << std::endl;
      }
      return err;
    }
    if (fclose(fp)) {
      int err = errno;
      cerr << "download: fclose(" << file_name << ") error: "
	   << cpp_strerror(err) << std::endl;
      return err;
    }
    return 0;
  }

  int upload(IoCtx &io_ctx, const char *file_name)
  {
    FILE *fp = fopen(file_name, "r");
    if (!fp) {
      int err = errno;
      cerr << "upload: error opening '" << file_name << "': "
	   << cpp_strerror(err) << std::endl;
      return err;
    }
    // Need to truncate RADOS object to size 0, in case there is
    // already something there.
    int ret = io_ctx.trunc(rados_name, 0);
    if (ret) {
      cerr << "upload: trunc failed with error " << ret << std::endl;
      return ret;
    }
    uint64_t off = 0;
    static const int CHUNK_SZ = 32765;
    while (true) {
      char buf[CHUNK_SZ];
      int flen = fread(buf, CHUNK_SZ, 1, fp);
      if (flen < 0) {
	int err = errno;
	cerr << "upload: fread(" << file_name << ") error: "
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
	cerr << "upload: rados_write error: " << rlen << std::endl;
	return rlen;
      }
      if (rlen != flen) {
	fclose(fp);
	cerr << "upload: rados_write error: short write" << std::endl;
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

  int read_xattrs(int fd)
  {
    ssize_t blen = flistxattr(fd, NULL, 0);
    if (blen > 0x1000000) {
      cerr << "BackedUpObject::read_xattrs: unwilling to allocate a buffer of size "
	   << blen << " on the stack for flistxattr." << std::endl;
      return EDOM;
    }
    char buf[blen + 1];
    memset(buf, 0, sizeof(buf));
    ssize_t blen2 = flistxattr(fd, buf, blen);
    if (blen != blen2) {
      cerr << "BackedUpObject::read_xattrs: xattrs changed while we were trying to "
	   << "list them? First length was " << blen << ", but now it's " << blen2
	   << std::endl;
      return EDOM;
    }
    const char *b = buf;
    while (b) {
      size_t bs = strlen(b);
      if (strncmp(b, XATTR_PREFIX, XATTR_PREFIX_LEN) == 0) {
	ssize_t attr_len = fgetxattr(fd, b, NULL, 0);
	if (attr_len < 0) {
	  int err = errno;
	  cerr << "BackedUpObject::read_xattrs: fgetxattr(rados_name = '"
	       << rados_name << "', xattr_name='" << b << "') failed: "
	       << cpp_strerror(err) << std::endl;
	  return EDOM;
	}
	char *attr = (char*)malloc(attr_len);
	if (!attr) {
	  cerr << "BackedUpObject::read_xattrs: malloc(" << attr_len
	       << ") failed for xattr_name='" << b << "'" << std::endl;
	  return ENOBUFS;
	}
	ssize_t attr_len2 = fgetxattr(fd, b, attr, attr_len);
	if (attr_len2 < 0) {
	  int err = errno;
	  cerr << "BackedUpObject::read_xattrs: fgetxattr(rados_name = '"
	       << rados_name << "', xattr_name='" << b << "') failed: "
	       << cpp_strerror(err) << std::endl;
	  free(attr);
	  return EDOM;
	}
	if (attr_len2 != attr_len) {
	  cerr << "BackedUpObject::read_xattrs: xattr changed whil we were "
	       << "trying to get it? fgetxattr(rados_name = '"<< rados_name
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

  // don't allow copying
  BackedUpObject &operator=(const BackedUpObject &rhs);
  BackedUpObject(const BackedUpObject &rhs);

  char *rados_name;
  uint64_t rados_size;
  uint64_t rados_time;
  std::map < std::string, Xattr* > xattrs;
};

static int do_export(IoCtx& io_ctx, const char *dir_name)
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
    BackedUpObject *sobj = NULL;
    BackedUpObject *dobj = NULL;
    string rados_name(*oi);
    std::list < std::string > only_in_a;
    std::list < std::string > only_in_b;
    std::list < std::string > diff;
    ret = BackedUpObject::from_rados(io_ctx, rados_name.c_str(), &sobj);
    if (ret) {
      cerr << "do_export: error getting BackedUpObject from rados." << std::endl;
      return ret;
    }
    std::string obj_path(sobj->get_fs_path(dir_name));
    ret = BackedUpObject::from_file(obj_path.c_str(), dir_name, &dobj);
    if (ret == ENOENT) {
      sobj->get_xattrs(only_in_a);
      flags |= CHANGED_CONTENTS;
    }
    else if (ret) {
      cerr << "do_export: BackedUpObject::from_file returned "
	   << ret << std::endl;
      return ret;
    }
    else {
      sobj->xattr_diff(*dobj, only_in_a, only_in_b, diff);
      if ((sobj->get_rados_size() == dobj->get_rados_size()) &&
          (sobj->get_mtime() == dobj->get_mtime())) {
	flags |= CHANGED_CONTENTS;
      }
    }
    if (flags & CHANGED_CONTENTS) {
      ret = sobj->download(io_ctx, obj_path.c_str());
      if (ret) {
	cerr << "do_export: download error: " << ret << std::endl;
	return ret;
      }
    }
    diff.splice(diff.begin(), only_in_a);
    for (std::list < std::string >::const_iterator x = diff.begin();
	 x != diff.end(); ++x) {
      flags |= CHANGED_XATTRS;
      const Xattr *xattr = sobj->get_xattr(*x);
      if (xattr == NULL) {
	cerr << "do_export: internal error on line: " << __LINE__ << std::endl;
	return -ENOSYS;
      }
      ret = setxattr(obj_path.c_str(), x->c_str(), xattr->data, xattr->len, 0);
      if (ret) {
	ret = errno;
	cerr << "sexattr error: " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
    for (std::list < std::string >::const_iterator x = only_in_b.begin();
	 x != only_in_b.end(); ++x) {
      flags |= CHANGED_XATTRS;
      ret = removexattr(obj_path.c_str(), x->c_str());
      if (ret) {
	ret = errno;
	cerr << "removexattr error: " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
    if (flags & CHANGED_CONTENTS) {
      cout << "[uploaded]     " << rados_name << std::endl;
    }
    else if (flags & CHANGED_XATTRS) {
      cout << "[xattr]        " << rados_name << std::endl;
    }
  }
  cout << "[done]" << std::endl;
  // TODO: list whole rados bucket, delete non-referenced
  return 0;
}

static int do_import(IoCtx& io_ctx, const char *dir_name)
{
  int ret = mkdir(dir_name, 0700);
  if (ret < 0) {
    int err = errno;
    if (err != EEXIST)
      return err;
  }
  DIR *dp = opendir(dir_name);
  if (!dp) {
    int err = errno;
    cerr << "opendir(" << dir_name << ") error: " << cpp_strerror(err) << std::endl;
    return err;
  }
  while (true) {
    enum {
      CHANGED_XATTRS = 0x1,
      CHANGED_CONTENTS = 0x2,
    };
    BackedUpObject *sobj = NULL;
    BackedUpObject *dobj = NULL;
    std::list < std::string > only_in_a;
    std::list < std::string > only_in_b;
    std::list < std::string > diff;
    int flags = 0;
    struct dirent *de = readdir(dp);
    if (!de)
      break;
    if ((strcmp(de->d_name, ".") == 0) || (strcmp(de->d_name, "..") == 0))
      continue;
    ret = BackedUpObject::from_file(de->d_name, dir_name, &sobj);
    if (ret) {
      cerr << "do_import: BackedUpObject::from_file: got error "
	   << ret << std::endl;
      return ret;
    }
    ret = BackedUpObject::from_rados(io_ctx, de->d_name, &dobj);
    if (ret == ENOENT) {
      flags |= CHANGED_CONTENTS;
      sobj->get_xattrs(only_in_a);
    }
    else if (ret) {
      cerr << "do_import: BackedUpObject::from_rados returned "
	   << ret << std::endl;
      return ret;
    }
    else {
      sobj->xattr_diff(*dobj, only_in_a, only_in_b, diff);
      if ((sobj->get_rados_size() == dobj->get_rados_size()) &&
          (sobj->get_mtime() == dobj->get_mtime())) {
	flags |= CHANGED_CONTENTS;
      }
    }

    if (flags & CHANGED_CONTENTS) {
      ret = sobj->upload(io_ctx, de->d_name);
      if (ret) {
	cerr << "do_import: upload error: " << ret << std::endl;
	return ret;
      }
    }
    diff.splice(diff.begin(), only_in_a);
    for (std::list < std::string >::const_iterator x = diff.begin();
	 x != diff.end(); ++x) {
      flags |= CHANGED_XATTRS;
      const Xattr *xattr = sobj->get_xattr(*x);
      if (xattr == NULL) {
	cerr << "do_import: internal error on line: " << __LINE__ << std::endl;
	return -ENOSYS;
      }
      bufferlist bl;
      bl.append(xattr->data, xattr->len);
      ret = io_ctx.setxattr(sobj->get_rados_name(), x->c_str(), bl);
      if (ret) {
	ret = errno;
	cerr << "io_ctx.sexattr error: " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
    for (std::list < std::string >::const_iterator x = only_in_b.begin();
	 x != only_in_b.end(); ++x) {
      flags |= CHANGED_XATTRS;
      ret = io_ctx.rmxattr(sobj->get_rados_name(), x->c_str());
      if (ret) {
	ret = errno;
	cerr << "rmxattr error: " << cpp_strerror(ret) << std::endl;
	return ret;
      }
    }
    if (flags & CHANGED_CONTENTS) {
      cout << "[downloaded]   " << sobj->get_rados_name() << std::endl;
    }
    else if (flags & CHANGED_XATTRS) {
      cout << "[xattr]        " << sobj->get_rados_name() << std::endl;
    }
  }
  cout << "[done]" << std::endl;
  // TODO: list whole directory, delete non-referenced
  return 0;
}

int main(int argc, const char **argv)
{
  int ret;
  bool create = false;
  vector<const char*> args;
  std::string action, src, dst;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);

  std::vector<const char*>::iterator i;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(1);
    } else if (ceph_argparse_flag(args, i, "-C", "--create", (char*)NULL)) {
      create = true;
    } else {
      // begin positional arguments
      break;
    }
  }
  if ((i != args.end()) &&
      ((strcmp(*i, "import") == 0) || (strcmp(*i, "export") == 0))) {
    action = *i;
    ++i;
  }
  else {
    cerr << argv[0] << ": You must specify either 'import' or 'export'.\n";
    cerr << "Use --help to show help.\n";
    exit(1);
  }
  if (i != args.end()) {
    src = *i;
    ++i;
  }
  else {
    cerr << argv[0] << ": You must give a source.\n";
    cerr << "Use --help to show help.\n";
    exit(1);
  }
  if (i != args.end()) {
    dst = *i;
    ++i;
  }
  else {
    cerr << argv[0] << ": You must give a destination.\n";
    cerr << "Use --help to show help.\n";
    exit(1);
  }

  // open rados
  Rados rados;
  if (rados.init_with_config(&g_conf) < 0) {
     cerr << argv[0] << ": failed to initialize Rados!" << std::endl;
     exit(1);
  }
  if (rados.connect() < 0) {
     cerr << argv[0] << ": failed to connect to Rados cluster!" << std::endl;
     exit(1);
  }
  IoCtx io_ctx;
  std::string pool_name = (action == "import") ? dst : src;
  ret = rados.ioctx_create(pool_name.c_str(), io_ctx);
  if (ret) {
    cerr << argv[0] << ": error opening pool " << pool_name << ": "
	 << cpp_strerror(ret) << std::endl;
    return ret;
  }

  std::string dir_name = (action == "import") ? src : dst;

  if (action == "import") {
    if (rados.pool_lookup(pool_name.c_str()) < 0) {
      if (create) {
	ret = rados.pool_create(pool_name.c_str());
	if (ret) {
	  cerr << argv[0] << ": pool_create failed with error " << ret
	       << std::endl;
	  exit(ret);
	}
      }
      else {
	cerr << argv[0] << ": pool '" << pool_name << "' does not exist. Use "
	     << "--create to try to create it." << std::endl;
	exit(ENOENT);
      }
    }

    ret = xattr_test(dir_name.c_str());
    if (ret)
      return ret;
    return do_import(io_ctx, src.c_str());
  }
  else {
    if (access(dst.c_str(), W_OK)) {
      if (create) {
	ret = mkdir(dst.c_str(), 0700);
	if (ret < 0) {
	  ret = errno;
	  cerr << argv[0] << ": mkdir(" << dst << ") failed with error " << ret
	       << std::endl;
	  exit(ret);
	}
      }
      else {
	cerr << argv[0] << ": directory '" << dst << "' does not exist. Use "
	     << "--create to try to create it.\n";
	exit(ENOENT);
      }
    }
    ret = xattr_test(dir_name.c_str());
    if (ret)
      return ret;
    return do_export(io_ctx, dst.c_str());
  }
}
