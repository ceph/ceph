// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once
#include "../posix/fsent_common.h"
#include <sys/xattr.h>
#include "common/errno.h"
#include "../posix/bucket_cache.h"
#include "../posix/multipart_cache.h"
#include "fs_strategy.h"

namespace rgw { namespace sal {

class NSFSDriver;
class NSFSBucket;
class NSFSObject;

#define RGW_NSFS_ATTR_BUCKET_INFO "bucket_info"
#define RGW_NSFS_ATTR_MPUPLOAD "mp_upload"
#define RGW_NSFS_ATTR_OBJECT_TYPE "object_type"
#define RGW_NSFS_ATTR_MULTIPART_PART_COUNT "multipart_part_count"
#define RGW_NSFS_ATTR_MULTIPART_PART_SIZES "multipart_part_sizes"
#define RGW_NSFS_ATTR_VERSION_ID "version_id"
#define RGW_NSFS_ATTR_DELETE_MARKER "delete_marker"
#define RGW_NSFS_ATTR_NON_CURRENT_TS "non_current_timestamp"

static const std::string NSFS_XATTR_PREFIX = "user.nsfs.";
static const std::string NSFS_RGW_XATTR_PREFIX = "user.nsfs.rgw.";

static inline std::string make_xattr_name(const std::string& key) {
  if (key.compare(0, RGW_ATTR_PFX.size(), RGW_ATTR_PFX) == 0) {
    return NSFS_RGW_XATTR_PREFIX + key.substr(RGW_ATTR_PFX.size());
  }
  return NSFS_XATTR_PREFIX + key;
}

static inline bool parse_xattr_name(const std::string& xattr, std::string& key) {
  if (xattr.compare(0, NSFS_RGW_XATTR_PREFIX.size(), NSFS_RGW_XATTR_PREFIX) == 0) {
    key = RGW_ATTR_PFX + xattr.substr(NSFS_RGW_XATTR_PREFIX.size());
    return true;
  }
  if (xattr.compare(0, NSFS_XATTR_PREFIX.size(), NSFS_XATTR_PREFIX) == 0) {
    key = xattr.substr(NSFS_XATTR_PREFIX.size());
    return true;
  }
  return false;
}

static inline rgw_obj_key decode_obj_key(const std::string& fname)
{
  rgw_obj_key key;
  rgw_obj_key::parse_raw_oid(fname, &key);
  return key;
}

static inline int decode_acl_owner(Attrs& attrs, ACLOwner& owner)
{
  auto i = attrs.find(RGW_ATTR_ACL);
  if (i == attrs.end()) {
    return -EINVAL;
  }
  RGWAccessControlPolicy policy;
  try {
    auto bp = i->second.cbegin();
    policy.decode(bp);
  } catch (const buffer::error&) {
    return -EIO;
  }
  owner = policy.get_owner();
  return 0;
}

static inline bool is_null_version_fd(int fd)
{
  char buf[256];
  std::string vid_xattr = NSFS_XATTR_PREFIX + RGW_NSFS_ATTR_VERSION_ID;
  ssize_t len = ::fgetxattr(fd, vid_xattr.c_str(), buf, sizeof(buf));
  if (len <= 0) {
    return true;
  }
  return std::string_view(buf, len) == NULL_VERSION_ID;
}

static inline std::string to_base36(uint64_t v)
{
  if (v == 0) return "0";
  const char digits[] = "0123456789abcdefghijklmnopqrstuvwxyz";
  std::string result;
  while (v > 0) {
    result.insert(result.begin(), digits[v % 36]);
    v /= 36;
  }
  return result;
}

static inline bool from_base36(const std::string& s, uint64_t& out)
{
  out = 0;
  for (char c : s) {
    uint64_t d;
    if (c >= '0' && c <= '9') {
      d = c - '0';
    } else if (c >= 'a' && c <= 'z') {
      d = 10 + (c - 'a');
    } else {
      return false;
    }
    out = out * 36 + d;
  }
  return true;
}

static inline uint64_t statx_mtime_ns(const struct statx& stx)
{
  return (uint64_t)stx.stx_mtime.tv_sec * 1000000000ULL
       + stx.stx_mtime.tv_nsec;
}

struct nsfs_version_info {
  uint64_t mtime_ns{0};
  uint64_t ino{0};
};

static inline std::string nsfs_version_id_from_statx(const struct statx& stx)
{
  return "mtime-" + to_base36(statx_mtime_ns(stx))
       + "-ino-" + to_base36(stx.stx_ino);
}

static inline std::string synthesize_etag(const struct statx& stx)
{
  return nsfs_version_id_from_statx(stx);
}

extern int get_x_attrs(optional_yield y, const DoutPrefixProvider* dpp, int fd,
		       Attrs& attrs, const std::string& display);
extern int write_x_attr(const DoutPrefixProvider* dpp, optional_yield y, int fd,
			const std::string& key, bufferlist& value,
			const std::string& display);
extern int remove_x_attr(const DoutPrefixProvider *dpp, optional_yield y,
                         int fd, const std::string &key,
                         const std::string &display);
extern int delete_directory(int parent_fd, const char* dname, bool delete_children,
		     const DoutPrefixProvider* dpp);

namespace nsfs {

static inline bool get_attr(Attrs& attrs, const char* name, bufferlist& bl)
{
  auto iter = attrs.find(name);
  if (iter == attrs.end()) {
    return false;
  }

  bl = iter->second;
  return true;
}

template <typename F>
static bool decode_attr(Attrs &attrs, const char *name, F &f) {
  bufferlist bl;
  if (!get_attr(attrs, name, bl)) {
    return false;
  }
  try {
    auto bufit = bl.cbegin();
    decode(f, bufit);
  } catch (buffer::error &err) {
    return false;
  }

  return true;
}

using BucketCache = file::listing::BucketCache<NSFSDriver, NSFSBucket>;
using MultipartCache = file::listing::MultipartCache<>;

/* integration w/bucket listing cache */
using fill_cache_cb_t = file::listing::fill_cache_cb_t;

struct ObjectType {
  enum Type {
    UNKNOWN = 0,
    FILE = 1,
    DIRECTORY = 2,
    MULTIPART = 4,
  };
  uint32_t type{UNKNOWN};

  ObjectType &operator=(ObjectType::Type &&_t) {
    type = _t;
    return *this;
  };

  ObjectType() {}
  ObjectType(Type _t) : type(_t){}

  bool operator==(const ObjectType &t) const { return (type == t.type); }
  bool operator==(const ObjectType::Type &t) const { return (type == t); }

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(type, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    ceph::decode(type, bl);
    DECODE_FINISH(bl);
  }
  friend inline std::ostream &operator<<(std::ostream &out,
                                         const ObjectType &t) {
    switch (t.type) {
    case UNKNOWN:
      out << "UNKNOWN";
      break;
    case FILE:
      out << "FILE";
      break;
    case DIRECTORY:
      out << "DIRECTORY";
      break;
    case MULTIPART:
      out << "MULTIPART";
      break;
    }
    return out;
  }
};
WRITE_CLASS_ENCODER(ObjectType);

class Directory;

class FSEnt {
protected:
  std::string fname;
  Directory* parent;
  int fd{-1};
  bool need_fsync{false};
  bool exist{false};
  struct statx stx;
  bool stat_done{false};
  CephContext* ctx;
  FSStrategy* fs_strategy;

public:
  static constexpr uint32_t FLAG_NONE =      0x0;
  static constexpr uint32_t FLAG_CURRENT =   0x2;
  static constexpr uint32_t FLAG_LIST_VERSIONS = 0x4;

  FSEnt(std::string _name, Directory* _parent, CephContext* _ctx, FSStrategy* _strat = nullptr);
  FSEnt(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx, FSStrategy* _strat = nullptr);
  FSEnt(const FSEnt& _ent) :
    fname(_ent.fname),
    parent(_ent.parent),
    exist(_ent.exist),
    stx(_ent.stx),
    stat_done(_ent.stat_done),
    ctx(_ent.ctx),
    fs_strategy(_ent.fs_strategy)
  { }

  virtual ~FSEnt() { }

  int get_fd() { return fd; };
  void set_sync_on_close(bool sync) { need_fsync = sync; }
  std::string& get_name() { return fname; }
  Directory* get_parent() { return parent; }
  bool exists() { return exist; }
  struct statx& get_stx() { return stx; }
  virtual ObjectType get_type() { return ObjectType::UNKNOWN; };

  virtual int create(const DoutPrefixProvider *dpp, bool* existed = nullptr, bool temp_file = false) = 0;
  virtual int open(const DoutPrefixProvider *dpp) = 0;
  virtual int close() = 0;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false);
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) = 0;
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) = 0;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) = 0;
  virtual int write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs);
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs);
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name) = 0;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y, std::string target_fname) = 0;
  virtual std::unique_ptr<FSEnt> clone_base() = 0;
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags, const std::string& path_prefix = "");
  virtual std::string get_cur_version() { return ""; };
};

class File : public FSEnt {
protected:
  bool direct_io{false};

public:
  File(std::string _name, Directory* _parent, CephContext* _ctx) : FSEnt(_name, _parent, _ctx)
    {}
  File(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : FSEnt(_name, _parent, _stx, _ctx)
    {}
  File(const File& _f) : FSEnt(_f) {}
  virtual ~File() { close(); }

  virtual uint64_t get_size() { return stx.stx_size; }
  virtual ObjectType get_type() override { return ObjectType::FILE; };


  virtual int create(const DoutPrefixProvider *dpp, bool* existed = nullptr, bool temp_file = false) override;
  virtual int open(const DoutPrefixProvider *dpp) override;
  virtual int close() override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) override;
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name) override;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y, std::string target_fname) override;
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<File>(*this);
  }
  std::unique_ptr<File> clone() {
    return std::make_unique<File>(*this);
  }
};

class Directory : public FSEnt {
protected:

public:
  Directory(std::string _name, Directory* _parent, CephContext* _ctx, FSStrategy* _strat = nullptr) : FSEnt(_name, _parent, _ctx, _strat)
    {}
  Directory(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : FSEnt(_name, _parent, _stx, _ctx)
    {}
  Directory(const Directory& _d) : FSEnt(_d) {}
  virtual ~Directory() { close(); }

  virtual ObjectType get_type() override { return ObjectType::DIRECTORY; };

  virtual bool file_exists(std::string& name);

  virtual int create(const DoutPrefixProvider *dpp, bool* existed = nullptr, bool temp_file = false) override;
  virtual int open(const DoutPrefixProvider *dpp) override;
  virtual int close() override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) override;
  template <typename F>
    int for_each(const DoutPrefixProvider* dpp, const F& func);
  virtual int rename(const DoutPrefixProvider* dpp, optional_yield y, Directory* dst_dir, std::string dst_name);
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<Directory>(*this);
  }
  virtual std::unique_ptr<Directory> clone_dir() {
    return std::make_unique<Directory>(*this);
  }
  std::unique_ptr<Directory> clone() {
    return std::make_unique<Directory>(*this);
  }
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name) override;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y, std::string target_fname) override;
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags, const std::string& path_prefix = "") override;

  int get_ent(const DoutPrefixProvider *dpp, optional_yield y, const std::string& name, const std::string& version, std::unique_ptr<FSEnt>& ent);
};

template <typename F>
int Directory::for_each(const DoutPrefixProvider* dpp, const F& func)
{
  DIR* dir;
  struct dirent* entry;
  int ret;

  ret = open(dpp);
  if (ret < 0) {
    return ret;
  }

  /* fdopendir() takes ownership of its fd and closedir() closes it.
   * Callbacks (get_ent/fill_cache/statx/openat) also use Directory::fd, so
   * iterate on a dup'd descriptor and leave this->fd alone.  Using the same
   * fd for readdir and openat corrupts the directory stream and can skip
   * entries (e.g. multipart parts → InvalidPart on complete). */
  int dir_fd = ::dup(fd);
  if (dir_fd < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not dup dir fd " << get_name() << ": "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  dir = fdopendir(dir_fd);
  if (dir == NULL) {
    ret = errno;
    ::close(dir_fd);
    ldpp_dout(dpp, 0) << "ERROR: could not open dir " << get_name() << " for listing: "
      << cpp_strerror(ret) << dendl;
    return -ret;
  }

  rewinddir(dir);

  ret = 0;
  while ((entry = readdir(dir)) != NULL) {
    std::string_view vname(entry->d_name);

    if (vname == "." || vname == "..")
      continue;

    int r = func(entry->d_name);
    if (r < 0) {
      ret = r;
      break;
    }
  }

  if (ret == -EAGAIN) {
    /* Limit reached */
    ret = 0;
  }

  closedir(dir); /* closes dir_fd only; Directory::fd remains valid */
  return ret;
}

class MPDirectory : public Directory {
  std::string tmpname;
protected:
  std::map<std::string, int64_t> parts;
  std::unique_ptr<FSEnt> cur_read_part;

public:
  MPDirectory(std::string _name, Directory* _parent, CephContext* _ctx) : Directory(_name, _parent, _ctx)
    {}
  MPDirectory(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : Directory(_name, _parent, _stx, _ctx)
    {}
  MPDirectory(const MPDirectory& _d) :
    Directory(_d),
    parts(_d.parts)
    { if (_d.cur_read_part) cur_read_part = _d.cur_read_part->clone_base(); }
  virtual ~MPDirectory() { close(); }

  virtual ObjectType get_type() override { return ObjectType::MULTIPART; };
  virtual int create(const DoutPrefixProvider *dpp, bool* existed = nullptr, bool temp_file = false) override;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y, std::string target_fname) override;
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  std::unique_ptr<File> get_part_file(int partnum);
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<MPDirectory>(*this);
  }
  virtual std::unique_ptr<Directory> clone_dir() override {
    return std::make_unique<MPDirectory>(*this);
  }
  std::unique_ptr<MPDirectory> clone() {
    return std::make_unique<MPDirectory>(*this);
  }
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags, const std::string& path_prefix = "") override;
};

std::string get_key_fname(rgw_obj_key& key, bool use_version);

int resolve_path(const DoutPrefixProvider* dpp,
                 Directory* root,
                 const std::string& key_path,
                 bool create_dirs,
                 CephContext* cct,
                 std::vector<std::unique_ptr<Directory>>& dir_chain,
                 Directory*& leaf_dir,
                 std::string& leaf_name);

} // namespace nsfs


} } // namespace rgw::sal

