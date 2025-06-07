// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include "rgw_sal_filter.h"
#include "rgw_sal_store.h"
#include <memory>
#include "common/dout.h"
#include "bucket_cache.h"

namespace rgw { namespace sal {

class POSIXDriver;
class POSIXBucket;
class POSIXObject;

using BucketCache = file::listing::BucketCache<POSIXDriver, POSIXBucket>;

/* integration w/bucket listing cache */
using fill_cache_cb_t = file::listing::fill_cache_cb_t;

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
  bool exist{false};
  struct statx stx;
  bool stat_done{false};
  CephContext* ctx;

public:
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
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) = 0;
  virtual int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) = 0;
  virtual int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y) = 0;
  virtual int write_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs, Attrs* extra_attrs);
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y, Attrs& attrs);
  virtual int copy(const DoutPrefixProvider *dpp, optional_yield y, Directory* dst_dir, const std::string& name) = 0;
  virtual int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y, std::string target_fname) = 0;
  virtual std::unique_ptr<FSEnt> clone_base() = 0;
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb);
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
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb) override;

  int get_ent(const DoutPrefixProvider *dpp, optional_yield y, const std::string& name, const std::string& version, std::unique_ptr<FSEnt>& ent);
};

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
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb) override;
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
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb) override;
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
  virtual int remove(const DoutPrefixProvider* dpp, optional_yield y, bool delete_children) override;
  virtual std::string get_cur_version() override;
  std::string get_new_instance();
  int remove_symlink(const DoutPrefixProvider *dpp, optional_yield y, std::string match = "");
  int add_file(const DoutPrefixProvider *dpp, std::unique_ptr<FSEnt>&& file, bool* existed = nullptr, bool temp_file = false);
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
  virtual int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb) override;
};

std::string get_key_fname(rgw_obj_key& key, bool use_version);

class POSIXDriver : public FilterDriver {
protected:

  std::unique_ptr<BucketCache> bucket_cache;
  std::string base_path;
  std::unique_ptr<Directory> root_dir;
  int root_fd;

public:
  POSIXDriver(Driver* _next) : FilterDriver(_next)
  { }
  virtual ~POSIXDriver() { }
  virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
  virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const
				     std::string& key, optional_yield y,
				     std::unique_ptr<User>* user) override;
  virtual int get_user_by_email(const DoutPrefixProvider* dpp, const
				std::string& email, optional_yield y,
				std::unique_ptr<User>* user) override;
  virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const
				std::string& user_str, optional_yield y,
				std::unique_ptr<User>* user) override;
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  virtual std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i)  override;
  virtual int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                          std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  virtual int list_buckets(const DoutPrefixProvider* dpp,
			   const rgw_owner& owner, const std::string& tenant,
			   const std::string& marker, const std::string& end_marker,
			   uint64_t max, bool need_stats, BucketList& buckets,
			   optional_yield y) override;
  virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;

  virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) override;
  virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* _head_obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;

  virtual void finalize(void) override;
  virtual void register_admin_apis(RGWRESTMgr* mgr) override;

  virtual bool process_expired_objects(const DoutPrefixProvider *dpp,
                                       optional_yield y) override;
  virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj,
				 rgw::sal::Object* src_obj, struct req_state* s,
				 rgw::notify::EventType event_type, optional_yield y,
				 const std::string* object_name=nullptr) override;

  virtual std::unique_ptr<Notification> get_notification(
      const DoutPrefixProvider* dpp,
      rgw::sal::Object* obj,
      rgw::sal::Object* src_obj,
      const rgw::notify::EventTypeList& event_type,
      rgw::sal::Bucket* _bucket,
      std::string& _user_id,
      std::string& _user_tenant,
      std::string& _req_id,
      optional_yield y) override;

  /* Internal APIs */
  int get_root_fd() { return root_dir->get_fd(); }
  Directory* get_root_dir() { return root_dir.get(); }
  const std::string& get_base_path() const { return base_path; }
  BucketCache* get_bucket_cache() { return bucket_cache.get(); }

  /* called by BucketCache layer when a new object is discovered
   * by inotify or similar */
  int mint_listing_entry(
    const std::string& bucket, rgw_bucket_dir_entry& bde /* OUT */);

};

class POSIXUser : public FilterUser {
private:
  POSIXDriver* driver;

public:
  POSIXUser(std::unique_ptr<User> _next, POSIXDriver* _driver) :
    FilterUser(std::move(_next)),
    driver(_driver) {}
  virtual ~POSIXUser() = default;

  virtual Attrs& get_attrs() override { return next->get_attrs(); }
  virtual void set_attrs(Attrs& _attrs) override { next->set_attrs(_attrs); }
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs&
				    new_attrs, optional_yield y) override;
  virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool
			 exclusive, RGWUserInfo* old_info = nullptr) override;
  virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;
};

class POSIXBucket : public StoreBucket {
private:
  POSIXDriver* driver;
  RGWAccessControlPolicy acls;
  std::optional<std::string> ns{std::nullopt};
  std::unique_ptr<Directory> dir;

public:
  POSIXBucket(POSIXDriver *_dr, Directory* _p_dir, const rgw_bucket& _b, std::optional<std::string> _ns = std::nullopt)
    : StoreBucket(_b),
    driver(_dr),
    acls(),
    ns(_ns),
    dir(std::make_unique<Directory>(get_fname(), _p_dir, _dr->ctx()))
    { }

  POSIXBucket(POSIXDriver *_dr, std::unique_ptr<Directory> _this_dir, const rgw_bucket& _b, std::optional<std::string> _ns = std::nullopt)
    : StoreBucket(_b),
    driver(_dr),
    acls(),
    ns(_ns),
    dir(std::move(_this_dir))
    { }

  POSIXBucket(POSIXDriver *_dr, Directory* _p_dir, const RGWBucketInfo& _i)
    : StoreBucket(_i),
    driver(_dr),
    acls(),
    ns(),
    dir(std::make_unique<Directory>(get_fname(), _p_dir, _dr->ctx()))
    { }

  POSIXBucket(const POSIXBucket& _b) :
    StoreBucket(_b),
    driver(_b.driver),
    acls(_b.acls),
    ns(_b.ns)
    {
      dir.reset(static_cast<Directory*>(_b.dir->clone().get()));
    }

  virtual ~POSIXBucket() { }

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
  virtual int list(const DoutPrefixProvider* dpp, ListParams&, int,
		   ListResults&, optional_yield y) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp,
				    Attrs& new_attrs, optional_yield y) override;
  virtual int remove(const DoutPrefixProvider* dpp, bool delete_children,
		     optional_yield y) override;
  virtual int remove_bypass_gc(int concurrent_max,
			       bool keep_index_consistent,
			       optional_yield y,
			       const DoutPrefixProvider *dpp) override;
  virtual int create(const DoutPrefixProvider* dpp,
		     const CreateParams& params,
		     optional_yield y) override;
  virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
  virtual int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl,
		      optional_yield y) override;
  virtual int read_stats(const DoutPrefixProvider *dpp, optional_yield y,
			 const bucket_index_layout_generation& idx_layout,
			 int shard_id, std::string* bucket_ver, std::string* master_ver,
			 std::map<RGWObjCategory, RGWStorageStats>& stats,
			 std::string* max_marker = nullptr,
			 bool* syncstopped = nullptr) override;
  virtual int read_stats_async(const DoutPrefixProvider *dpp,
			       const bucket_index_layout_generation& idx_layout,
			       int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx) override;
  virtual int sync_owner_stats(const DoutPrefixProvider *dpp, optional_yield y,
                               RGWBucketEnt* ent) override;
  virtual int check_bucket_shards(const DoutPrefixProvider* dpp,
                                  uint64_t num_objs, optional_yield y) override;
  virtual int chown(const DoutPrefixProvider* dpp, const rgw_owner& new_owner, optional_yield y) override;
  virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive,
		       ceph::real_time mtime, optional_yield y) override;
  virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
  virtual int try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime, optional_yield y) override;
  virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			 uint64_t end_epoch, uint32_t max_entries, bool* is_truncated,
			 RGWUsageIter& usage_iter, std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) override;
  virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) override;
  virtual int check_index(const DoutPrefixProvider *dpp, optional_yield y,
                          std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
                          std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
  virtual int rebuild_index(const DoutPrefixProvider *dpp, optional_yield y) override;
  virtual int set_tag_timeout(const DoutPrefixProvider *dpp, optional_yield y, uint64_t timeout) override;
  virtual int purge_instance(const DoutPrefixProvider* dpp, optional_yield y) override;

  virtual std::unique_ptr<Bucket> clone() override {
    return std::make_unique<POSIXBucket>(*this);
  }

  virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id=std::nullopt,
				ACLOwner owner={}, ceph::real_time mtime=real_clock::now()) override;
  virtual int list_multiparts(const DoutPrefixProvider *dpp,
			      const std::string& prefix,
			      std::string& marker,
			      const std::string& delim,
			      const int& max_uploads,
			      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
			      std::map<std::string, bool> *common_prefixes,
			      bool *is_truncated, optional_yield y) override;
  virtual int abort_multiparts(const DoutPrefixProvider* dpp,
			       CephContext* cct, optional_yield y) override;

  /* Internal APIs */
  int create(const DoutPrefixProvider *dpp, optional_yield y, bool* existed);
  Directory* get_dir() { return dir.get(); }
  int get_dir_fd(const DoutPrefixProvider *dpp) { dir->open(dpp); return dir->get_fd(); }
  /* TODO dang Escape the bucket name for file use */
  std::string get_fname();
  std::optional<std::string> get_ns() { return ns; }
  int rename(const DoutPrefixProvider* dpp, optional_yield y, Object* target_obj);

  /* enumerate all entries by callback, in any order */
  int fill_cache(const DoutPrefixProvider* dpp, optional_yield y, fill_cache_cb_t& cb);
  
private:
  int write_attrs(const DoutPrefixProvider *dpp, optional_yield y);
}; /* POSIXBucket */

class POSIXObject : public StoreObject {
public:
private:
  POSIXDriver* driver;
  RGWAccessControlPolicy acls;
  std::unique_ptr<FSEnt> ent;
  std::map<std::string, int64_t> parts;

public:
  struct POSIXReadOp : ReadOp {
    POSIXObject* source;

    POSIXReadOp(POSIXObject* _source) :
      source(_source) {}
    virtual ~POSIXReadOp() = default;

    virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int read(int64_t ofs, int64_t left, bufferlist& bl, optional_yield y,
		     const DoutPrefixProvider* dpp) override;
    virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			RGWGetDataCB* cb, optional_yield y) override;
    virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
			 bufferlist& dest, optional_yield y) override;
  };

  struct POSIXDeleteOp : DeleteOp {
    POSIXObject* source;

    POSIXDeleteOp(POSIXObject* _source) :
      source(_source) {}
    virtual ~POSIXDeleteOp() = default;

    virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
  };

  POSIXObject(POSIXDriver *_dr, const rgw_obj_key& _k)
    : StoreObject(_k),
    driver(_dr),
    acls()
    {}

  POSIXObject(POSIXDriver* _driver, const rgw_obj_key& _k, Bucket* _b) :
    StoreObject(_k, _b),
    driver(_driver),
    acls()
    {}

  POSIXObject(const POSIXObject& _o) :
    StoreObject(_o),
    driver(_o.driver)
  {}

  virtual ~POSIXObject() { close(); }

  virtual int delete_object(const DoutPrefixProvider* dpp,
			    optional_yield y,
			    uint32_t flags,
			    std::list<rgw_obj_index_key>* remove_objs,
			    RGWObjVersionTracker* objv) override;
  virtual int copy_object(const ACLOwner& owner,
               const rgw_user& remote_user,
               req_info* info, const rgw_zone_id& source_zone,
               rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
               rgw::sal::Bucket* src_bucket,
               const rgw_placement_rule& dest_placement,
               ceph::real_time* src_mtime, ceph::real_time* mtime,
               const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
               bool high_precision_time,
               const char* if_match, const char* if_nomatch,
               AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
               RGWObjCategory category, uint64_t olh_epoch,
               boost::optional<ceph::real_time> delete_at,
               std::string* version_id, std::string* tag, std::string* etag,
               void (*progress_cb)(off_t, void *), void* progress_data,
               const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
  virtual int set_acl(const RGWAccessControlPolicy& acl) override { acls = acl; return 0; }

  /** If multipart, enumerate (a range [marker..marker+[min(max_parts, parts_count-1)] of) parts of the object */
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int max_parts, int marker, int* next_marker,
			 bool* truncated, list_parts_each_t each_func,
			 optional_yield y) override;

  bool is_sync_completed(const DoutPrefixProvider* dpp, optional_yield y,
                         const ceph::real_time& obj_mtime) override;
  virtual int load_obj_state(const DoutPrefixProvider* dpp, optional_yield y, bool follow_olh = true) override;
  virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
			    Attrs* delattrs, optional_yield y, uint32_t flags) override;
  virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
			    rgw_obj* target_obj = NULL) override;
  virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
			       optional_yield y, const DoutPrefixProvider* dpp,
			       uint32_t flags = rgw::sal::FLAG_LOG_OP) override;
  virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name,
			       optional_yield y) override;
  virtual bool is_expired() override;
  virtual void gen_rand_obj_instance_name() override;
  virtual std::unique_ptr<MPSerializer> get_serializer(const DoutPrefixProvider *dpp,
						       const std::string& lock_name) override;
  virtual int transition(Bucket* bucket,
			 const rgw_placement_rule& placement_rule,
			 const real_time& mtime,
			 uint64_t olh_epoch,
			 const DoutPrefixProvider* dpp,
			 optional_yield y,
                         uint32_t flags) override;
  virtual int transition_to_cloud(Bucket* bucket,
			 rgw::sal::PlacementTier* tier,
			 rgw_bucket_dir_entry& o,
			 std::set<std::string>& cloud_targets,
			 CephContext* cct,
			 bool update_object,
			 const DoutPrefixProvider* dpp,
			 optional_yield y) override;
  virtual int restore_obj_from_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   CephContext* cct,
			   std::optional<uint64_t> days,
		           bool& in_progress,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) override;
  virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
  virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f) override;
  virtual int swift_versioning_restore(const ACLOwner& owner, const rgw_user& remote_user, bool& restored,
				       const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int swift_versioning_copy(const ACLOwner& owner, const rgw_user& remote_user,
				    const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual std::unique_ptr<ReadOp> get_read_op() override;
  virtual std::unique_ptr<DeleteOp> get_delete_op() override;
  virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
				    const std::set<std::string>& keys,
				    Attrs* vals) override;
  virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key,
				  bufferlist& val, bool must_exist, optional_yield y) override;
  virtual int chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual std::unique_ptr<Object> clone() override {
    return std::unique_ptr<Object>(new POSIXObject(*this));
  }

  FSEnt* get_fsent() { return ent.get(); }
  int open(const DoutPrefixProvider *dpp, bool create = false, bool temp_file = false);
  int close();
  int write(int64_t ofs, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y);
  int write_attrs(const DoutPrefixProvider* dpp, optional_yield y);
  int link_temp_file(const DoutPrefixProvider* dpp, optional_yield y);
  std::string gen_temp_fname();
  const std::string get_fname(bool use_version);
  bool check_exists(const DoutPrefixProvider* dpp) { stat(dpp); return state.exists; }
  int get_owner(const DoutPrefixProvider *dpp, optional_yield y, std::unique_ptr<User> *owner);
  int copy(const DoutPrefixProvider *dpp, optional_yield y, POSIXBucket *sb,
           POSIXBucket *db, POSIXObject *dobj);
  int fill_cache(const DoutPrefixProvider *dpp, optional_yield y, fill_cache_cb_t& cb);
  int set_cur_version(const DoutPrefixProvider *dpp);
  int stat(const DoutPrefixProvider *dpp);
  int make_ent(ObjectType type);
  bool versioned() { return bucket->versioned(); }

protected:
  int read(int64_t ofs, int64_t end, bufferlist& bl, const DoutPrefixProvider* dpp, optional_yield y);
  int generate_attrs(const DoutPrefixProvider* dpp, optional_yield y);
private:
  int generate_mp_etag(const DoutPrefixProvider* dpp, optional_yield y);
  int generate_etag(const DoutPrefixProvider* dpp, optional_yield y);
  int get_cur_version(const DoutPrefixProvider *dpp, rgw_obj_key &key);
};

struct POSIXMPObj {
  std::string oid;
  std::string upload_id;
  ACLOwner owner;
  multipart_upload_info upload_info;
  std::string meta;

  POSIXMPObj(POSIXDriver* driver, const std::string& _oid,
	     std::optional<std::string> _upload_id, ACLOwner& _owner) {
    if (_upload_id && !_upload_id->empty()) {
      init(_oid, *_upload_id, _owner);
    } else if (!from_meta(_oid, _owner)) {
      init_gen(driver, _oid, _owner);
    }
  }
  void init(const std::string& _oid, const std::string& _upload_id, ACLOwner& _owner) {
    if (_oid.empty()) {
      clear();
      return;
    }
    oid = _oid;
    upload_id = _upload_id;
    owner = _owner;
    meta = oid;
    if (!upload_id.empty())
      meta += "." + upload_id;
  }
  void init_gen(POSIXDriver* driver, const std::string& _oid, ACLOwner& _owner);
  bool from_meta(const std::string& meta, ACLOwner& _owner) {
    int end_pos = meta.length();
    int mid_pos = meta.rfind('.', end_pos - 1); // <key>.<upload_id>
    if (mid_pos < 0)
      return false;
    oid = meta.substr(0, mid_pos);
    upload_id = meta.substr(mid_pos + 1, end_pos - mid_pos - 1);
    init(oid, upload_id, _owner);
    return true;
  }
  void clear() {
    oid = "";
    meta = "";
    upload_id = "";
  }
  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(oid, bl);
    encode(upload_id, bl);
    encode(owner, bl);
    encode(upload_info, bl);
    encode(meta, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(oid, bl);
    decode(upload_id, bl);
    decode(owner, bl);
    decode(upload_info, bl);
    decode(meta, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(POSIXMPObj)

struct POSIXUploadPartInfo {
  uint32_t num{0};
  std::string etag;
  ceph::real_time mtime;
  std::optional<rgw::cksum::Cksum> cksum;

  void encode(bufferlist& bl) const {
    ENCODE_START(2, 1, bl);
    encode(num, bl);
    encode(etag, bl);
    encode(mtime, bl);
    encode(cksum, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(2, 1, 1, bl);
    decode(num, bl);
    decode(etag, bl);
    decode(mtime, bl);
    if (struct_v > 1) {
      decode(cksum, bl);
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(POSIXUploadPartInfo)

class POSIXMultipartUpload;

class POSIXMultipartPart : public StoreMultipartPart {
protected:
  POSIXUploadPartInfo info;
  POSIXMultipartUpload* upload;
  std::unique_ptr<File> part_file;

public:
  POSIXMultipartPart(POSIXMultipartUpload* _upload) :
    upload(_upload) {}
  virtual ~POSIXMultipartPart() = default;

  virtual uint32_t get_num() { return info.num; }
  virtual uint64_t get_size() { return part_file->get_size(); }
  virtual const std::string& get_etag() { return info.etag; }
  virtual ceph::real_time& get_mtime() { return info.mtime; }
  virtual const std::optional<rgw::cksum::Cksum>& get_cksum() {
    return info.cksum;
  }

  int load(const DoutPrefixProvider* dpp, optional_yield y, POSIXDriver* driver,
	   rgw_obj_key& key);

  friend class POSIXMultipartUpload;
};

class POSIXMultipartUpload : public StoreMultipartUpload {
protected:
  POSIXDriver* driver;
  POSIXMPObj mp_obj;
  ceph::real_time mtime;
  std::unique_ptr<rgw::sal::POSIXBucket> shadow;

public:
  POSIXMultipartUpload(POSIXDriver* _driver, Bucket* _bucket, const std::string& _oid,
		       std::optional<std::string> _upload_id, ACLOwner _owner,
		       ceph::real_time _mtime) :
    StoreMultipartUpload(_bucket), driver(_driver),
    mp_obj(driver, _oid, _upload_id, _owner), mtime(_mtime) {}
  virtual ~POSIXMultipartUpload() = default;

  virtual const std::string& get_meta() const override { return mp_obj.meta; }
  virtual const std::string& get_key() const override { return mp_obj.oid; }
  virtual const std::string& get_upload_id() const override { return mp_obj.upload_id; }
  virtual const ACLOwner& get_owner() const override { return mp_obj.owner; }
  virtual ceph::real_time& get_mtime() override { return mtime; }
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() override;

  virtual int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) override;
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated, optional_yield y,
			 bool assume_unsorted = false) override;
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y) override;
  virtual int complete(const DoutPrefixProvider* dpp,
		       optional_yield y, CephContext* cct,
		       std::map<int, std::string>& part_etags,
		       std::list<rgw_obj_index_key>& remove_objs,
		       uint64_t& accounted_size, bool& compressed,
		       RGWCompressionInfo& cs_info, off_t& ofs,
		       std::string& tag, ACLOwner& owner,
		       uint64_t olh_epoch,
		       rgw::sal::Object* target_obj,
		       prefix_map_t& processed_prefixes) override;
  virtual int cleanup_orphaned_parts(const DoutPrefixProvider *dpp,
                                     CephContext *cct, optional_yield y,
                                     const rgw_obj& obj,
                                     std::list<rgw_obj_index_key>& remove_objs,
                                     prefix_map_t& processed_prefixes) override;
  virtual int get_info(const DoutPrefixProvider *dpp, optional_yield y,
		       rgw_placement_rule** rule, rgw::sal::Attrs* attrs) override;

  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  rgw::sal::Object* _head_obj,
			  const ACLOwner& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) override;

  POSIXBucket* get_shadow() { return shadow.get(); }
private:
  std::string get_fname();
  int load(const DoutPrefixProvider *dpp, bool create=false);
};

class POSIXAtomicWriter : public StoreWriter {
private:
  POSIXDriver* driver;
  const ACLOwner& owner;
  const rgw_placement_rule *ptail_placement_rule;
  uint64_t olh_epoch;
  const std::string& unique_tag;
  POSIXObject* obj;

public:
  POSIXAtomicWriter(const DoutPrefixProvider *dpp,
                    optional_yield y,
		    rgw::sal::Object* _head_obj,
                    POSIXDriver* _driver,
                    const ACLOwner& _owner,
                    const rgw_placement_rule *_ptail_placement_rule,
                    uint64_t _olh_epoch,
                    const std::string& _unique_tag) :
    StoreWriter(dpp, y),
    driver(_driver),
    owner(_owner),
    ptail_placement_rule(_ptail_placement_rule),
    olh_epoch(_olh_epoch),
    unique_tag(_unique_tag),
    obj(static_cast<POSIXObject*>(_head_obj)) {}
  virtual ~POSIXAtomicWriter() = default;

  virtual int prepare(optional_yield y) override;
  virtual int process(bufferlist&& data, uint64_t offset) override;
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
		       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
		       ceph::real_time delete_at,
		       const char *if_match, const char *if_nomatch,
		       const std::string *user_data,
		       rgw_zone_set *zones_trace, bool *canceled,
		       const req_context& rctx,
                       uint32_t flags) override;
};

class POSIXMultipartWriter : public StoreWriter {
private:
  POSIXDriver* driver;
  const ACLOwner& owner;
  const rgw_placement_rule *ptail_placement_rule;
  uint64_t part_num;
  std::unique_ptr<Directory> upload_dir;
  std::unique_ptr<File> part_file;

public:
  POSIXMultipartWriter(const DoutPrefixProvider *dpp,
                    optional_yield y,
		    POSIXBucket* _shadow_bucket,
                    rgw_obj_key& _key,
                    POSIXDriver* _driver,
                    const ACLOwner& _owner,
                    const rgw_placement_rule *_ptail_placement_rule,
                    uint64_t _part_num) :
    StoreWriter(dpp, y),
    driver(_driver),
    owner(_owner),
    ptail_placement_rule(_ptail_placement_rule),
    part_num(_part_num),
    upload_dir(_shadow_bucket->get_dir()->clone()),
    part_file(std::make_unique<File>(get_key_fname(_key, false), upload_dir.get(), _driver->ctx()))
  { upload_dir->open(dpp); }
  virtual ~POSIXMultipartWriter() = default;

  virtual int prepare(optional_yield y) override;
  virtual int process(bufferlist&& data, uint64_t offset) override;
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
		       std::map<std::string, bufferlist>& attrs,
		       const std::optional<rgw::cksum::Cksum>& cksum,
		       ceph::real_time delete_at,
		       const char *if_match, const char *if_nomatch,
		       const std::string *user_data,
		       rgw_zone_set *zones_trace, bool *canceled,
		       const req_context& rctx,
                       uint32_t flags) override;

};

class MPPOSIXSerializer : public StoreMPSerializer {
  POSIXObject* obj;

public:
  MPPOSIXSerializer(const DoutPrefixProvider *dpp, POSIXDriver* driver, POSIXObject* _obj, const std::string& lock_name) : obj(_obj) {}

  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override;
  virtual int unlock() override { return 0; }
};

} } // namespace rgw::sal
