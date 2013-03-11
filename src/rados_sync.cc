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

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/rados/librados.hpp"
#include "rados_sync.h"
#include "include/compat.h"

#include "common/xattr.h"

#include <dirent.h>
#include <errno.h>
#include <fstream>
#include <inttypes.h>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdlib.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

using namespace librados;
using std::auto_ptr;

static const char * const XATTR_RADOS_SYNC_VER = "user.rados_sync_ver";
static const char * const XATTR_FULLNAME = "user.rados_full_name";
const char USER_XATTR_PREFIX[] = "user.rados.";
static const size_t USER_XATTR_PREFIX_LEN =
  sizeof(USER_XATTR_PREFIX) / sizeof(USER_XATTR_PREFIX[0]) - 1;
/* It's important that RADOS_SYNC_TMP_SUFFIX contain at least one character
 * that we wouldn't normally alllow in a file name-- in this case, $ */
const char RADOS_SYNC_TMP_SUFFIX[] = "$tmp";
static const size_t RADOS_SYNC_TMP_SUFFIX_LEN =
  sizeof(RADOS_SYNC_TMP_SUFFIX) / sizeof(RADOS_SYNC_TMP_SUFFIX[0]) - 1;

std::string get_user_xattr_name(const char *fs_xattr_name)
{
  if (strncmp(fs_xattr_name, USER_XATTR_PREFIX, USER_XATTR_PREFIX_LEN))
    return "";
  return fs_xattr_name + USER_XATTR_PREFIX_LEN;
}

bool is_suffix(const char *str, const char *suffix)
{
  size_t strlen_str = strlen(str);
  size_t strlen_suffix = strlen(suffix);
  if (strlen_str < strlen_suffix)
    return false;
  return (strcmp(str + (strlen_str - strlen_suffix), suffix) == 0);
}

ExportDir* ExportDir::create_for_writing(const std::string path, int version,
				 bool create)
{
  if (access(path.c_str(), R_OK | W_OK) == 0) {
    return ExportDir::from_file_system(path);
  }
  if (!create) {
    cerr << ERR_PREFIX << "ExportDir: directory '"
	 << path << "' does not exist. Use --create to create it."
	 << std::endl;
    return NULL;
  }
  int ret = mkdir(path.c_str(), 0700);
  if (ret < 0) {
    int err = errno;
    if (err != EEXIST) {
      cerr << ERR_PREFIX << "ExportDir: mkdir error: "
	   << cpp_strerror(err) << std::endl;
      return NULL;
    }
  }
  char buf[32];
  snprintf(buf, sizeof(buf), "%d", version);
  ret = ceph_os_setxattr(path.c_str(), XATTR_RADOS_SYNC_VER, buf, strlen(buf) + 1);
  if (ret < 0) {
    int err = errno;
    cerr << ERR_PREFIX << "ExportDir: setxattr error :"
	 << cpp_strerror(err) << std::endl;
    return NULL;
  }
  return new ExportDir(version, path);
}

ExportDir* ExportDir::from_file_system(const std::string path)
{
  if (access(path.c_str(), R_OK)) {
      cerr << "ExportDir: source directory '" << path
	   << "' appears to be inaccessible." << std::endl;
      return NULL;
  }
  int ret;
  char buf[32];
  memset(buf, 0, sizeof(buf));
  ret = ceph_os_getxattr(path.c_str(), XATTR_RADOS_SYNC_VER, buf, sizeof(buf) - 1);
  if (ret < 0) {
    ret = errno;
    if (ret == ENODATA) {
      cerr << ERR_PREFIX << "ExportDir: directory '" << path
	   << "' does not appear to have been created by a rados "
	   << "export operation." << std::endl;
      return NULL;
    }
    cerr << ERR_PREFIX << "ExportDir: getxattr error :"
	 << cpp_strerror(ret) << std::endl;
    return NULL;
  }
  std::string err;
  ret = strict_strtol(buf, 10, &err);
  if (!err.empty()) {
    cerr << ERR_PREFIX << "ExportDir: invalid value for "
	 << XATTR_RADOS_SYNC_VER << ": " << buf << ". parse error: "
	 << err << std::endl;
    return NULL;
  }
  if (ret != 1) {
    cerr << ERR_PREFIX << "ExportDir: can't handle any naming "
	 << "convention besides version 1. You must upgrade this program to "
	 << "handle the data in the new format." << std::endl;
    return NULL;
  }
  return new ExportDir(ret, path);
}

std::string ExportDir::get_fs_path(const std::string rados_name) const
{
  static int HASH_LENGTH = 17;
  size_t i;
  size_t strlen_rados_name = strlen(rados_name.c_str());
  size_t sz;
  bool need_hash = false;
  if (strlen_rados_name > 200) {
    sz = 200;
    need_hash = true;
  }
  else {
    sz = strlen_rados_name;
  }
  char fs_path[sz + HASH_LENGTH + 1];
  for (i = 0; i < sz; ++i) {
    // Just replace anything that looks funny with an 'at' sign.
    // Unicode also gets turned into 'at' signs.
    signed char c = rados_name[i];
    if (c < 0x20) {
     // Since c is signed, this also eliminates bytes with the high bit set
      c = '@';
      need_hash = true;
    }
    else if (c == 0x7f) {
      c = '@';
      need_hash = true;
    }
    else if (c == '/') {
      c = '@';
      need_hash = true;
    }
    else if (c == '\\') {
      c = '@';
      need_hash = true;
    }
    else if (c == '$') {
      c = '@';
      need_hash = true;
    }
    else if (c == ' ') {
      c = '_';
      need_hash = true;
    }
    else if (c == '\n') {
      c = '@';
      need_hash = true;
    }
    else if (c == '\r') {
      c = '@';
      need_hash = true;
    }
    fs_path[i] = c;
  }

  if (need_hash) {
    uint64_t hash = 17;
    for (i = 0; i < strlen_rados_name; ++i) {
      hash += (rados_name[i] * 33);
    }
    // The extra byte of length is because snprintf always NULL-terminates.
    snprintf(fs_path + i, HASH_LENGTH + 1, "_%016" PRIx64, hash);
  }
  else {
    // NULL-terminate.
    fs_path[i] = '\0';
  }

  ostringstream oss;
  oss << path << "/" << fs_path;
  return oss.str();
}

ExportDir::ExportDir(int version_, const std::string path_)
  : version(version_),
    path(path_)
{
}

DirHolder::DirHolder()
  : dp(NULL)
{
}

DirHolder::~DirHolder() {
  if (!dp)
    return;
  if (closedir(dp)) {
    int err = errno;
    cerr << ERR_PREFIX << "closedir failed: " << cpp_strerror(err) << std::endl;
  }
  dp = NULL;
}

int DirHolder::opendir(const char *dir_name) {
  dp = ::opendir(dir_name);
  if (!dp) {
    int err = errno;
    return err;
  }
  return 0;
}

static __thread int t_iod_idx = -1;

static pthread_mutex_t io_ctx_distributor_lock = PTHREAD_MUTEX_INITIALIZER;

IoCtxDistributor* IoCtxDistributor::instance() {
  IoCtxDistributor *ret;
  pthread_mutex_lock(&io_ctx_distributor_lock);
  if (s_instance == NULL) {
    s_instance = new IoCtxDistributor();
  }
  ret = s_instance;
  pthread_mutex_unlock(&io_ctx_distributor_lock);
  return ret;
}

int IoCtxDistributor::init(Rados &cluster, const char *pool_name,
			   int num_ioctxes) {
  m_io_ctxes.resize(num_ioctxes);
  for (std::vector<IoCtx>::iterator i = m_io_ctxes.begin();
	 i != m_io_ctxes.end(); ++i) {
    IoCtx &io_ctx(*i);
    int ret = cluster.ioctx_create(pool_name, io_ctx);
    if (ret) {
      return ret;
    }
  }
  m_highest_iod_idx.set(0);
  return 0;
}

void IoCtxDistributor::clear() {
  for (std::vector<IoCtx>::iterator i = m_io_ctxes.begin();
	 i != m_io_ctxes.end(); ++i) {
    IoCtx &io_ctx(*i);
    io_ctx.close();
  }
  m_io_ctxes.clear();
  m_highest_iod_idx.set(0);
}

IoCtx& IoCtxDistributor::get_ioctx() {
  if (t_iod_idx == -1) {
    t_iod_idx = m_highest_iod_idx.inc() - 1;
  }
  if (m_io_ctxes.size() <= (unsigned int)t_iod_idx) {
    cerr << ERR_PREFIX << "IoCtxDistributor: logic error on line "
	 << __LINE__ << std::endl;
    _exit(1);
  }
  return m_io_ctxes[t_iod_idx];
}

IoCtxDistributor *IoCtxDistributor::s_instance = NULL;

IoCtxDistributor::IoCtxDistributor() {
  clear();
}

IoCtxDistributor::~IoCtxDistributor() {
  clear();
}

RadosSyncWQ::RadosSyncWQ(IoCtxDistributor *io_ctx_dist, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
  : ThreadPool::WorkQueue<std::string>("FileStore::OpWQ", timeout, suicide_timeout, tp),
    m_io_ctx_dist(io_ctx_dist)
{
}

bool RadosSyncWQ::_enqueue(std::string *s) {
  m_items.push_back(s);
  return true;
}

void RadosSyncWQ::_dequeue(std::string *o) {
  assert(0);
}

bool RadosSyncWQ::_empty() {
  return m_items.empty();
}

std::string *RadosSyncWQ::_dequeue() {
  if (m_items.empty())
    return NULL;
  std::string *ret = m_items.front();
  m_items.pop_front();
  return ret;
}

void RadosSyncWQ::_process_finish(std::string *s) {
  delete s;
}

void RadosSyncWQ::_clear() {
  for (std::deque<std::string*>::iterator i = m_items.begin();
	 i != m_items.end(); ++i) {
    delete *i;
  }
  m_items.clear();
}

Xattr::Xattr(char *data_, ssize_t len_)
    : data(data_), len(len_)
{
}

Xattr::~Xattr() {
  free(data);
}

bool Xattr::operator==(const struct Xattr &rhs) const {
  if (len != rhs.len)
    return false;
  return (memcmp(data, rhs.data, len) == 0);
}

bool Xattr::operator!=(const struct Xattr &rhs) const {
  return !((*this) == rhs);
}

int BackedUpObject::from_file(const char *file_name, const char *dir_name,
			  std::auto_ptr<BackedUpObject> &obj)
{
  char obj_path[strlen(dir_name) + strlen(file_name) + 2];
  snprintf(obj_path, sizeof(obj_path), "%s/%s", dir_name, file_name);
  return BackedUpObject::from_path(obj_path, obj);
}

int BackedUpObject::from_path(const char *path, std::auto_ptr<BackedUpObject> &obj)
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
  ssize_t res = ceph_os_fgetxattr(fd, XATTR_FULLNAME, NULL, 0);
  if (res <= 0) {
    fclose(fp);
    ret = errno;
    if (res == 0) {
      cerr << ERR_PREFIX << "BackedUpObject::from_path: found empty "
	   << XATTR_FULLNAME << " attribute on '" << path
	   << "'" << std::endl;
      ret = ENODATA;
    } else if (ret == ENODATA) {
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
  res = ceph_os_fgetxattr(fd, XATTR_FULLNAME, rados_name_, res);
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

int BackedUpObject::from_rados(IoCtx& io_ctx, const char *rados_name_,
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

BackedUpObject::~BackedUpObject()
{
  for (std::map < std::string, Xattr* >::iterator x = xattrs.begin();
	 x != xattrs.end(); ++x)
  {
    delete x->second;
    x->second = NULL;
  }
  free(rados_name);
}

std::string BackedUpObject::get_fs_path(const ExportDir *export_dir) const
{
  return export_dir->get_fs_path(rados_name);
}

std::string BackedUpObject::xattrs_to_str() const
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

void BackedUpObject::xattr_diff(const BackedUpObject *rhs,
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

void BackedUpObject::get_xattrs(std::list < std::string > &xattrs_) const
{
  for (std::map < std::string, Xattr* >::const_iterator r = xattrs.begin();
	 r != xattrs.end(); ++r)
  {
    xattrs_.push_back(r->first);
  }
}

const Xattr* BackedUpObject::get_xattr(const std::string name) const
{
  std::map < std::string, Xattr* >::const_iterator x = xattrs.find(name);
  if (x == xattrs.end())
    return NULL;
  else
    return x->second;
}

const char *BackedUpObject::get_rados_name() const {
  return rados_name;
}

uint64_t BackedUpObject::get_rados_size() const {
  return rados_size;
}

time_t BackedUpObject::get_mtime() const {
  return rados_time;
}

int BackedUpObject::download(IoCtx &io_ctx, const char *path)
{
  char tmp_path[strlen(path) + RADOS_SYNC_TMP_SUFFIX_LEN + 1];
  snprintf(tmp_path, sizeof(tmp_path), "%s%s", path, RADOS_SYNC_TMP_SUFFIX);
  FILE *fp = fopen(tmp_path, "w");
  if (!fp) {
    int err = errno;
    cerr << ERR_PREFIX << "download: error opening '" << tmp_path << "':"
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
      cerr << ERR_PREFIX << "download: fwrite(" << tmp_path << ") error: "
	   << cpp_strerror(err) << std::endl;
      fclose(fp);
      return err;
    }
    if (off == 0)
      break;
  }
  size_t attr_sz = strlen(rados_name) + 1;
  int res = ceph_os_fsetxattr(fd, XATTR_FULLNAME, rados_name, attr_sz);
  if (res) {
    int err = errno;
    cerr << ERR_PREFIX << "download: fsetxattr(" << tmp_path << ") error: "
	 << cpp_strerror(err) << std::endl;
    fclose(fp);
    return err;
  }
  if (fclose(fp)) {
    int err = errno;
    cerr << ERR_PREFIX << "download: fclose(" << tmp_path << ") error: "
	 << cpp_strerror(err) << std::endl;
    return err;
  }
  if (rename(tmp_path, path)) {
    int err = errno;
    cerr << ERR_PREFIX << "download: rename(" << tmp_path << ", "
	 << path << ") error: " << cpp_strerror(err) << std::endl;
    return err;
  }
  return 0;
}

int BackedUpObject::upload(IoCtx &io_ctx, const char *file_name, const char *dir_name)
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
    fclose(fp);
    return ret;
  }
  uint64_t off = 0;
  static const int CHUNK_SZ = 32765;
  while (true) {
    char buf[CHUNK_SZ];
    int flen = fread(buf, 1, CHUNK_SZ, fp);
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

BackedUpObject::BackedUpObject(const char *rados_name_,
			       uint64_t rados_size_, time_t rados_time_)
  : rados_name(strdup(rados_name_)),
    rados_size(rados_size_),
    rados_time(rados_time_)
{
}

int BackedUpObject::read_xattrs_from_file(int fd)
{
  ssize_t blen = ceph_os_flistxattr(fd, NULL, 0);
  if (blen > 0x1000000) {
    cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: unwilling "
	 << "to allocate a buffer of size " << blen << " on the stack for "
	 << "flistxattr." << std::endl;
    return ENOBUFS;
  }
  char buf[blen + 1];
  memset(buf, 0, sizeof(buf));
  ssize_t blen2 = ceph_os_flistxattr(fd, buf, blen);
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
    std::string xattr_name = get_user_xattr_name(b);
    if (!xattr_name.empty()) {
      ssize_t attr_len = ceph_os_fgetxattr(fd, b, NULL, 0);
      if (attr_len < 0) {
	int err = errno;
	cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: "
	     << "fgetxattr(rados_name = '" << rados_name << "', xattr_name='"
	     << xattr_name << "') failed: " << cpp_strerror(err) << std::endl;
	return EDOM;
      }
      char *attr = (char*)malloc(attr_len);
      if (!attr) {
	cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: "
	     << "malloc(" << attr_len << ") failed for xattr_name='"
	     << xattr_name << "'" << std::endl;
	return ENOBUFS;
      }
      ssize_t attr_len2 = ceph_os_fgetxattr(fd, b, attr, attr_len);
      if (attr_len2 < 0) {
	int err = errno;
	cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: "
	     << "fgetxattr(rados_name = '" << rados_name << "', "
	     << "xattr_name='" << xattr_name << "') failed: "
	     << cpp_strerror(err) << std::endl;
	free(attr);
	return EDOM;
      }
      if (attr_len2 != attr_len) {
	cerr << ERR_PREFIX << "BackedUpObject::read_xattrs_from_file: xattr "
	     << "changed while we were trying to get it? "
	     << "fgetxattr(rados_name = '"<< rados_name
	     << "', xattr_name='" << xattr_name << "') returned a different length "
	     << "than when we first called it! old_len = " << attr_len
	     << "new_len = " << attr_len2 << std::endl;
	free(attr);
	return EDOM;
      }
      xattrs[xattr_name] = new Xattr(attr, attr_len);
    }
    b += (bs + 1);
  }
  return 0;
}

int BackedUpObject::read_xattrs_from_rados(IoCtx &io_ctx)
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

int rados_tool_sync(const std::map < std::string, std::string > &opts,
                             std::vector<const char*> &args)
{
  int ret;
  bool force = opts.count("force");
  bool delete_after = opts.count("delete-after");
  bool create = opts.count("create");

  std::map < std::string, std::string >::const_iterator n = opts.find("workers");
  int num_threads;
  if (n == opts.end()) {
    num_threads = DEFAULT_NUM_RADOS_WORKER_THREADS;
  }
  else {
    std::string err;
    num_threads = strict_strtol(n->second.c_str(), 10, &err);
    if (!err.empty()) {
      cerr << "rados: can't parse number of worker threads given: "
	   << err << std::endl;
      return 1;
    }
    if ((num_threads < 1) || (num_threads > 9000)) {
      cerr << "rados: unreasonable value given for num_threads: "
	   << num_threads << std::endl;
      return 1;
    }
  }


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
  if (rados.init_with_context(g_ceph_context) < 0) {
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

  IoCtxDistributor *io_ctx_dist = IoCtxDistributor::instance();
  ret = io_ctx_dist->init(rados, pool_name.c_str(), num_threads);
  if (ret) {
    cerr << ERR_PREFIX << "failed to initialize Radso io contexts."
	 << std::endl;
    _exit(ret);
  }

  ThreadPool thread_pool(g_ceph_context, "rados_sync_threadpool", num_threads);
  thread_pool.start();

  if (action == "import") {
    ret = do_rados_import(&thread_pool, io_ctx, io_ctx_dist, src.c_str(),
		     force, delete_after);
    thread_pool.stop();
    return ret;
  }
  else {
    ret = do_rados_export(&thread_pool, io_ctx, io_ctx_dist, dst.c_str(),
		     create, force, delete_after);
    thread_pool.stop();
    return ret;
  }
}
