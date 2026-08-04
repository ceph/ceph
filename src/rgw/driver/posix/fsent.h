// -*- mode:C++; tab-width:8; c-basic-offset:2; ind
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
#include "rgw_sal.h"
#include "bucket_cache.h"
#include "common/errno.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/xattr.h>
#include <unistd.h>


namespace rgw { namespace sal {

class POSIXDriver;
class POSIXBucket;
class POSIXObject;

using DeleteResult = rgw::sal::Object::DeleteOp::Result;

extern const std::string ATTR_PREFIX;
#define RGW_POSIX_ATTR_BUCKET_INFO "POSIX-Bucket-Info"
#define RGW_POSIX_ATTR_MPUPLOAD "POSIX-Multipart-Upload"
#define RGW_POSIX_ATTR_OBJECT_TYPE "POSIX-Object-Type"
#define RGW_POSIX_ATTR_VERSION "POSIX-version"
#define RGW_POSIX_ATTR_MULTIPART_PART_COUNT "POSIX-Multipart-Part-Count"
#define RGW_POSIX_ATTR_MULTIPART_TOTAL_SIZE "POSIX-Multipart-Total-Size"
extern const std::string mp_ns;
extern const std::string MP_OBJ_PART_PFX;
extern const std::string MP_OBJ_HEAD_NAME;

/* integration w/bucket listing cache */
using fill_cache_cb_t = file::listing::fill_cache_cb_t;

static inline ceph::real_time from_statx_timestamp(const struct statx_timestamp& xts)
{
  struct timespec ts{xts.tv_sec, xts.tv_nsec};
  return ceph::real_clock::from_timespec(ts);
}

static inline std::string gen_rand_instance_name()
{
  enum { OBJ_INSTANCE_LEN = 32 };
  char buf[OBJ_INSTANCE_LEN + 1];

#if 0
  gen_rand_alphanumeric_no_underscore(driver->ctx(), buf, OBJ_INSTANCE_LEN);
#else
  static std::atomic<uint64_t> last_id{UINT64_MAX};
  snprintf(buf, OBJ_INSTANCE_LEN, "%lx", last_id.fetch_sub(1));
#endif

  return buf;
}

namespace posix {

using BucketCache = file::listing::BucketCache<POSIXDriver, POSIXBucket>;

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

static inline rgw_obj_key decode_obj_key(const char* fname)
{
  std::string dname, oname, ns; // XXX ns is unused?
  dname = url_decode(fname);
  rgw_obj_key key;
  rgw_obj_key::parse_raw_oid(dname, &key);
  return key;
}

static inline rgw_obj_key decode_obj_key(const std::string& fname)
{
  return decode_obj_key(fname.c_str());
}

/* Extract object owner from the standard RGW_ATTR_ACL attribute */
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

static inline std::string get_key_fname(rgw_obj_key& key, bool use_version)
{
  std::string oid;
  if (use_version) {
    oid = key.get_oid();
  } else {
    oid = key.get_index_key_name();
  }
  std::string fname = url_encode(oid, true);

  if (!key.get_ns().empty()) {
    /* Namespaced objects are hidden */
    fname.insert(0, 1, '.');
  }

  return fname;
}

static inline int remove_x_attr(const DoutPrefixProvider *dpp, optional_yield y,
                         int fd, const std::string &key,
                         const std::string &display)
{
  int ret;
  std::string attrname{ATTR_PREFIX + key};

  ret = fremovexattr(fd, attrname.c_str());
  if (ret < 0) {
    ret = errno;
    ldpp_dout(dpp, 0) << "ERROR: could not remove attribute " << attrname << " for " << display << ": " << cpp_strerror(ret) << dendl;
    return -ret;
  }

  return 0;
}

struct ObjectType {
  enum Type {
    UNKNOWN = 0,
    FILE = 1,
    DIRECTORY = 2,
    VERSIONED = 3,
    MULTIPART = 4,
    SYMLINK = 5,
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
    case VERSIONED:
      out << "VERSIONED";
      break;
    case MULTIPART:
      out << "MULTIPART";
      break;
    case SYMLINK:
      out << "SYMLINK";
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

public:
  static constexpr uint32_t FLAG_NONE =      0x0;
  static constexpr uint32_t FLAG_CURRENT =   0x2;
  static constexpr uint32_t FLAG_DELETE_MARKER =   0x4;

  FSEnt(std::string _name, Directory* _parent, CephContext* _ctx) : fname(_name), parent(_parent), ctx(_ctx) {}
  FSEnt(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : fname(_name), parent(_parent), exist(true), stx(_stx), stat_done(true), ctx(_ctx) {}
  FSEnt(const FSEnt& _e) :
    fname(_e.fname),
    parent(_e.parent),
    exist(_e.exist),
    stx(_e.stx),
    stat_done(_e.stat_done),
    ctx(_e.ctx)
  { }

  virtual ~FSEnt() { }

  int get_fd() { return fd; };
  std::string& get_name() { return fname; }
  Directory* get_parent() { return parent; }
  bool exists() { return exist; }
  struct statx& get_stx() { return stx; }
  virtual ObjectType get_type() { return ObjectType::UNKNOWN; };

  virtual int create(const DoutPrefixProvider *dpp, bool* existed = nullptr, bool temp_file = false) = 0;
  virtual int open(const DoutPrefixProvider *dpp) = 0;
  virtual int close() = 0;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false);
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children, DeleteResult* result) = 0;
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) = 0;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) = 0;
  virtual int write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs);
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs);
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name) = 0;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y, std::string target_fname) = 0;
  virtual std::unique_ptr<FSEnt> clone_base() = 0;
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags);
  virtual std::string get_cur_version() { return ""; };
};

class File : public FSEnt {
protected:

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
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children, DeleteResult* result) override;
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
  Directory(std::string _name, Directory* _parent, CephContext* _ctx) : FSEnt(_name, _parent, _ctx)
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
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children, DeleteResult* result) override;
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
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags) override;

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

  dir = fdopendir(fd);
  if (dir == NULL) {
    ret = errno;
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

  closedir(dir);
  // closedir() closes the fd, so we need to invalidate it
  fd = -1;
  // closedir() closes fd, but succeeding calls might assume that fd is still valid.
  // so let's reopen it.
  open(dpp);
  return ret;
}

class Symlink: public File {
  std::unique_ptr<FSEnt> target;
public:
  Symlink(std::string _name, Directory* _parent, std::string _tgt, CephContext* _ctx) :
    File(_name, _parent, _ctx)
    { fill_target(nullptr, parent, fname,_tgt, target, _ctx); }
  Symlink(std::string _name, Directory* _parent, CephContext* _ctx) :
    File(_name, _parent, _ctx)
    {}
  Symlink(std::string _name, Directory* _parent, struct statx& _stx, std::string _tgt, CephContext* _ctx) :
    File(_name, _parent, _stx, _ctx)
    { fill_target(nullptr, parent, fname,_tgt, target, _ctx); }
  Symlink(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) :
    File(_name, _parent, _stx, _ctx)
    {}
  Symlink(const Symlink& _s) : File(_s) {}
  virtual ~Symlink() { close(); }

  static int fill_target(const DoutPrefixProvider *dpp, Directory* parent, std::string sname, std::string tname, std::unique_ptr<FSEnt>& ent, CephContext* _ctx);

  virtual ObjectType get_type() override { return ObjectType::SYMLINK; };
  virtual int create(const DoutPrefixProvider *dpp, bool* existed = nullptr, bool temp_file = false) override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs) override;
  FSEnt* get_target() { return target.get(); }
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<Symlink>(*this);
  }
  std::unique_ptr<Symlink> clone() {
    return std::make_unique<Symlink>(*this);
  }
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name) override;
};

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
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children, DeleteResult* result) override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  std::unique_ptr<File> get_part_file(int partnum);
  const std::map<std::string, int64_t>& get_parts() const { return parts; }
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<MPDirectory>(*this);
  }
  virtual std::unique_ptr<Directory> clone_dir() override {
    return std::make_unique<MPDirectory>(*this);
  }
  std::unique_ptr<MPDirectory> clone() {
    return std::make_unique<MPDirectory>(*this);
  }
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags) override;
};

class VersionedDirectory : public Directory {
protected:
  std::string instance_id;
  std::unique_ptr<FSEnt> cur_version;

public:
  VersionedDirectory(std::string _name, Directory* _parent, CephContext* _ctx) : Directory(_name, _parent, _ctx)
    {}
  VersionedDirectory(std::string _name, Directory* _parent, std::string _instance_id, CephContext* _ctx) :
    Directory(_name, _parent, _ctx),
    instance_id(_instance_id)
    {}
  VersionedDirectory(std::string _name, Directory* _parent, std::unique_ptr<FSEnt>&& _cur, CephContext* _ctx) :
    Directory(_name, _parent, _ctx),
    cur_version(std::move(_cur))
    {}
  VersionedDirectory(std::string _name, Directory* _parent, struct statx& _stx, CephContext* _ctx) : Directory(_name, _parent, _stx, _ctx)
    {}
  VersionedDirectory(std::string _name, Directory* _parent, std::string _instance_id, struct statx& _stx, CephContext* _ctx) :
    Directory(_name, _parent, _stx, _ctx),
    instance_id(_instance_id)
    {}
  VersionedDirectory(const VersionedDirectory& _d) :
    Directory(_d),
    instance_id(_d.instance_id),
    cur_version(_d.cur_version ? _d.cur_version->clone_base() : nullptr)
    { }
  VersionedDirectory(const Directory& _d) :
    Directory(_d)
    { }
  virtual ~VersionedDirectory() { close(); }

  virtual ObjectType get_type() override { return ObjectType::VERSIONED; };
  virtual int create(const DoutPrefixProvider *dpp, bool* existed = nullptr, bool temp_file = false) override;
  virtual int open(const DoutPrefixProvider *dpp) override;
  virtual int stat(const DoutPrefixProvider *dpp, bool force = false) override;
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs) override;
  virtual int write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs) override;
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y, std::string target_fname) override;
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children, DeleteResult* result) override;
  virtual std::string get_cur_version() override;
  std::string get_new_instance();
  int remove_symlink(const DoutPrefixProvider *dpp, optional_yield y, std::string match = "");
  int add_file(const DoutPrefixProvider *dpp, std::unique_ptr<FSEnt>&& file, bool* existed = nullptr, bool temp_file = false);
  int add_delete_marker(const DoutPrefixProvider* dpp, optional_yield y, std::unique_ptr<File>& marker, const std::string &name);
  FSEnt* get_cur_version_ent() { return cur_version.get(); };
  int set_cur_version_ent(const DoutPrefixProvider *dpp, FSEnt* file);
  virtual std::unique_ptr<FSEnt> clone_base() override {
    return std::make_unique<VersionedDirectory>(*this);
  }
  virtual std::unique_ptr<Directory> clone_dir() override {
    return std::make_unique<VersionedDirectory>(*this);
  }
  std::unique_ptr<VersionedDirectory> clone() {
    return std::make_unique<VersionedDirectory>(*this);
  }
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name) override;
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb, uint32_t flags) override;
};

std::string get_key_fname(rgw_obj_key& key, bool use_version);

} // namespace posix
} } // namespace rgw::sal
