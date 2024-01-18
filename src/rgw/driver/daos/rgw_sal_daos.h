// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * SAL implementation for the CORTX Daos backend
 *
 * Copyright (C) 2022 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <daos.h>
#include <daos_s3.h>
#include <uuid/uuid.h>

#include <map>
#include <set>
#include <string>
#include <vector>

#include "rgw_multi.h"
#include "rgw_notify.h"
#include "rgw_oidc_provider.h"
#include "rgw_putobj_processor.h"
#include "rgw_rados.h"
#include "rgw_role.h"
#include "rgw_sal_store.h"

inline bool IsDebuggerAttached() {
#ifdef DEBUG
  char buf[4096];

  const int status_fd = ::open("/proc/self/status", O_RDONLY);
  if (status_fd == -1) return false;

  const ssize_t num_read = ::read(status_fd, buf, sizeof(buf) - 1);
  ::close(status_fd);

  if (num_read <= 0) return false;

  buf[num_read] = '\0';
  constexpr char tracerPidString[] = "TracerPid:";
  const auto tracer_pid_ptr = ::strstr(buf, tracerPidString);
  if (!tracer_pid_ptr) return false;

  for (const char* characterPtr = tracer_pid_ptr + sizeof(tracerPidString) - 1;
       characterPtr <= buf + num_read; ++characterPtr) {
    if (::isspace(*characterPtr))
      continue;
    else
      return ::isdigit(*characterPtr) != 0 && *characterPtr != '0';
  }
#endif  // DEBUG
  return false;
}

inline void DebugBreak() {
#ifdef DEBUG
  // only break into the debugger if the debugger is attached
  if (IsDebuggerAttached())
    raise(SIGINT);  // breaks into GDB and stops, can be continued
#endif              // DEBUG
}

inline int NotImplementedLog(const DoutPrefixProvider* ldpp,
                             const char* filename, int linenumber,
                             const char* functionname) {
  if (ldpp)
    ldpp_dout(ldpp, 20) << filename << "(" << linenumber << ") " << functionname
                        << ": Not implemented" << dendl;
  return 0;
}

inline int NotImplementedGdbBreak(const DoutPrefixProvider* ldpp,
                                  const char* filename, int linenumber,
                                  const char* functionname) {
  NotImplementedLog(ldpp, filename, linenumber, functionname);
  DebugBreak();
  return 0;
}

#define DAOS_NOT_IMPLEMENTED_GDB_BREAK(ldpp) \
  NotImplementedGdbBreak(ldpp, __FILE__, __LINE__, __FUNCTION__)
#define DAOS_NOT_IMPLEMENTED_LOG(ldpp) \
  NotImplementedLog(ldpp, __FILE__, __LINE__, __FUNCTION__)

namespace rgw::sal {

class DaosStore;
class DaosObject;

#ifdef DEBUG
// Prepends each log entry with the "filename(source_line) function_name". Makes
// it simple to
//  associate log entries with the source that generated the log entry
#undef ldpp_dout
#define ldpp_dout(dpp, v)                                                     \
  if (decltype(auto) pdpp = (dpp);                                            \
      pdpp) /* workaround -Wnonnull-compare for 'this' */                     \
  dout_impl(pdpp->get_cct(), ceph::dout::need_dynamic(pdpp->get_subsys()), v) \
          pdpp->gen_prefix(*_dout)                                            \
      << __FILE__ << "(" << __LINE__ << ") " << __FUNCTION__ << " - "
#endif  // DEBUG

struct DaosUserInfo {
  RGWUserInfo info;
  obj_version user_version;
  rgw::sal::Attrs attrs;

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 3, bl);
    encode(info, bl);
    encode(user_version, bl);
    encode(attrs, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(3, bl);
    decode(info, bl);
    decode(user_version, bl);
    decode(attrs, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(DaosUserInfo);

class DaosNotification : public StoreNotification {
 public:
  DaosNotification(Object* _obj, Object* _src_obj, rgw::notify::EventType _type)
      : StoreNotification(_obj, _src_obj, _type) {}
  ~DaosNotification() = default;

  virtual int publish_reserve(const DoutPrefixProvider* dpp,
                              RGWObjTags* obj_tags = nullptr) override {
    return DAOS_NOT_IMPLEMENTED_LOG(dpp);
  }
  virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
                             const ceph::real_time& mtime,
                             const std::string& etag,
                             const std::string& version) override {
    return DAOS_NOT_IMPLEMENTED_LOG(dpp);
  }
};

class DaosUser : public StoreUser {
 private:
  DaosStore* store;
  std::vector<const char*> access_ids;

 public:
  DaosUser(DaosStore* _st, const rgw_user& _u) : StoreUser(_u), store(_st) {}
  DaosUser(DaosStore* _st, const RGWUserInfo& _i) : StoreUser(_i), store(_st) {}
  DaosUser(DaosStore* _st) : store(_st) {}
  DaosUser(DaosUser& _o) = default;
  DaosUser() {}

  virtual std::unique_ptr<User> clone() override {
    return std::make_unique<DaosUser>(*this);
  }
  int list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
                   const std::string& end_marker, uint64_t max, bool need_stats,
                   BucketList& buckets, optional_yield y) override;
  virtual int create_bucket(
      const DoutPrefixProvider* dpp, const rgw_bucket& b,
      const std::string& zonegroup_id, rgw_placement_rule& placement_rule,
      std::string& swift_ver_location, const RGWQuotaInfo* pquota_info,
      const RGWAccessControlPolicy& policy, Attrs& attrs, RGWBucketInfo& info,
      obj_version& ep_objv, bool exclusive, bool obj_lock_enabled,
      bool* existed, req_info& req_info, std::unique_ptr<Bucket>* bucket,
      optional_yield y) override;
  virtual int read_attrs(const DoutPrefixProvider* dpp,
                         optional_yield y) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp,
                                    Attrs& new_attrs,
                                    optional_yield y) override;
  virtual int read_stats(const DoutPrefixProvider* dpp, optional_yield y,
                         RGWStorageStats* stats,
                         ceph::real_time* last_stats_sync = nullptr,
                         ceph::real_time* last_stats_update = nullptr) override;
  virtual int read_stats_async(const DoutPrefixProvider* dpp,
                               boost::intrusive_ptr<ReadStatsCB> cb) override;
  virtual int complete_flush_stats(const DoutPrefixProvider* dpp,
                                   optional_yield y) override;
  virtual int read_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  virtual int trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch) override;

  virtual int load_user(const DoutPrefixProvider* dpp,
                        optional_yield y) override;
  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y,
                         bool exclusive,
                         RGWUserInfo* old_info = nullptr) override;
  virtual int remove_user(const DoutPrefixProvider* dpp,
                          optional_yield y) override;

  /** Read user info without loading it */
  int read_user(const DoutPrefixProvider* dpp, std::string name,
                DaosUserInfo* duinfo);

  std::unique_ptr<struct ds3_user_info> get_encoded_info(bufferlist& bl,
                                                         obj_version& obj_ver);

  friend class DaosBucket;
};

// RGWBucketInfo and other information that are shown when listing a bucket is
// represented in struct DaosBucketInfo. The structure is encoded and stored
// as the value of the global bucket instance index.
// TODO: compare pros and cons of separating the bucket_attrs (ACLs, tag etc.)
// into a different index.
struct DaosBucketInfo {
  RGWBucketInfo info;

  obj_version bucket_version;
  ceph::real_time mtime;

  rgw::sal::Attrs bucket_attrs;

  void encode(bufferlist& bl) const {
    ENCODE_START(4, 4, bl);
    encode(info, bl);
    encode(bucket_version, bl);
    encode(mtime, bl);
    encode(bucket_attrs, bl);  // rgw_cache.h example for a map
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(4, bl);
    decode(info, bl);
    decode(bucket_version, bl);
    decode(mtime, bl);
    decode(bucket_attrs, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(DaosBucketInfo);

class DaosBucket : public StoreBucket {
 private:
  DaosStore* store;
  RGWAccessControlPolicy acls;

 public:
  /** Container ds3b handle */
  ds3_bucket_t* ds3b = nullptr;

  DaosBucket(DaosStore* _st) : store(_st), acls() {}

  DaosBucket(const DaosBucket& _daos_bucket)
      : store(_daos_bucket.store), acls(), ds3b(nullptr) {
    // TODO: deep copy all objects
  }

  DaosBucket(DaosStore* _st, User* _u) : StoreBucket(_u), store(_st), acls() {}

  DaosBucket(DaosStore* _st, const rgw_bucket& _b)
      : StoreBucket(_b), store(_st), acls() {}

  DaosBucket(DaosStore* _st, const RGWBucketEnt& _e)
      : StoreBucket(_e), store(_st), acls() {}

  DaosBucket(DaosStore* _st, const RGWBucketInfo& _i)
      : StoreBucket(_i), store(_st), acls() {}

  DaosBucket(DaosStore* _st, const rgw_bucket& _b, User* _u)
      : StoreBucket(_b, _u), store(_st), acls() {}

  DaosBucket(DaosStore* _st, const RGWBucketEnt& _e, User* _u)
      : StoreBucket(_e, _u), store(_st), acls() {}

  DaosBucket(DaosStore* _st, const RGWBucketInfo& _i, User* _u)
      : StoreBucket(_i, _u), store(_st), acls() {}

  ~DaosBucket();

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  virtual int list(const DoutPrefixProvider* dpp, ListParams&, int,
                   ListResults&, optional_yield y) override;
  virtual int remove(const DoutPrefixProvider* dpp, bool delete_children,
                     optional_yield y) override;
  virtual int remove_bypass_gc(int concurrent_max,
                               bool keep_index_consistent,
                               optional_yield y,
                               const DoutPrefixProvider* dpp) override;
  virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
  virtual int set_acl(const DoutPrefixProvider* dpp,
                      RGWAccessControlPolicy& acl, optional_yield y) override;
  virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int read_stats(const DoutPrefixProvider* dpp,
                         const bucket_index_layout_generation& idx_layout,
                         int shard_id, std::string* bucket_ver,
                         std::string* master_ver,
                         std::map<RGWObjCategory, RGWStorageStats>& stats,
                         std::string* max_marker = nullptr,
                         bool* syncstopped = nullptr) override;
  virtual int read_stats_async(const DoutPrefixProvider* dpp,
                               const bucket_index_layout_generation& idx_layout,
                               int shard_id,
                               boost::intrusive_ptr<ReadStatsCB> ctx) override;
  virtual int sync_user_stats(const DoutPrefixProvider* dpp,
                              optional_yield y) override;
  virtual int check_bucket_shards(const DoutPrefixProvider* dpp) override;
  virtual int chown(const DoutPrefixProvider* dpp, User& new_user,
                    optional_yield y) override;
  virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive,
                       ceph::real_time mtime) override;
  virtual bool is_owner(User* user) override;
  virtual int check_empty(const DoutPrefixProvider* dpp,
                          optional_yield y) override;
  virtual int check_quota(const DoutPrefixProvider* dpp, RGWQuota& quota,
                          uint64_t obj_size, optional_yield y,
                          bool check_size_only = false) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& attrs,
                                    optional_yield y) override;
  virtual int try_refresh_info(const DoutPrefixProvider* dpp,
                               ceph::real_time* pmtime) override;
  virtual int read_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  virtual int trim_usage(const DoutPrefixProvider* dpp, uint64_t start_epoch,
                         uint64_t end_epoch) override;
  virtual int remove_objs_from_index(
      const DoutPrefixProvider* dpp,
      std::list<rgw_obj_index_key>& objs_to_unlink) override;
  virtual int check_index(
      const DoutPrefixProvider* dpp,
      std::map<RGWObjCategory, RGWStorageStats>& existing_stats,
      std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
  virtual int rebuild_index(const DoutPrefixProvider* dpp) override;
  virtual int set_tag_timeout(const DoutPrefixProvider* dpp,
                              uint64_t timeout) override;
  virtual int purge_instance(const DoutPrefixProvider* dpp) override;
  virtual std::unique_ptr<Bucket> clone() override {
    return std::make_unique<DaosBucket>(*this);
  }
  virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
      const std::string& oid,
      std::optional<std::string> upload_id = std::nullopt, ACLOwner owner = {},
      ceph::real_time mtime = real_clock::now()) override;
  virtual int list_multiparts(
      const DoutPrefixProvider* dpp, const std::string& prefix,
      std::string& marker, const std::string& delim, const int& max_uploads,
      std::vector<std::unique_ptr<MultipartUpload>>& uploads,
      std::map<std::string, bool>* common_prefixes,
      bool* is_truncated) override;
  virtual int abort_multiparts(const DoutPrefixProvider* dpp,
                               CephContext* cct) override;

  int open(const DoutPrefixProvider* dpp);
  int close(const DoutPrefixProvider* dpp);
  bool is_open() { return ds3b != nullptr; }
  std::unique_ptr<struct ds3_bucket_info> get_encoded_info(
      bufferlist& bl, ceph::real_time mtime);

  friend class DaosStore;
};

class DaosPlacementTier : public StorePlacementTier {
  DaosStore* store;
  RGWZoneGroupPlacementTier tier;

 public:
  DaosPlacementTier(DaosStore* _store, const RGWZoneGroupPlacementTier& _tier)
      : store(_store), tier(_tier) {}
  virtual ~DaosPlacementTier() = default;

  virtual const std::string& get_tier_type() { return tier.tier_type; }
  virtual const std::string& get_storage_class() { return tier.storage_class; }
  virtual bool retain_head_object() { return tier.retain_head_object; }
  RGWZoneGroupPlacementTier& get_rt() { return tier; }
};

class DaosZoneGroup : public StoreZoneGroup {
  DaosStore* store;
  const RGWZoneGroup group;
  std::string empty;

 public:
  DaosZoneGroup(DaosStore* _store) : store(_store), group() {}
  DaosZoneGroup(DaosStore* _store, const RGWZoneGroup& _group)
      : store(_store), group(_group) {}
  virtual ~DaosZoneGroup() = default;

  virtual const std::string& get_id() const override { return group.get_id(); };
  virtual const std::string& get_name() const override {
    return group.get_name();
  };
  virtual int equals(const std::string& other_zonegroup) const override {
    return group.equals(other_zonegroup);
  };
  virtual bool placement_target_exists(std::string& target) const override;
  virtual bool is_master_zonegroup() const override {
    return group.is_master_zonegroup();
  };
  virtual const std::string& get_api_name() const override {
    return group.api_name;
  };
  virtual void get_placement_target_names(
      std::set<std::string>& names) const override;
  virtual const std::string& get_default_placement_name() const override {
    return group.default_placement.name;
  };
  virtual int get_hostnames(std::list<std::string>& names) const override {
    names = group.hostnames;
    return 0;
  };
  virtual int get_s3website_hostnames(
      std::list<std::string>& names) const override {
    names = group.hostnames_s3website;
    return 0;
  };
  virtual int get_zone_count() const override { return group.zones.size(); }
  virtual int get_placement_tier(const rgw_placement_rule& rule,
                                 std::unique_ptr<PlacementTier>* tier);
  virtual std::unique_ptr<ZoneGroup> clone() override {
    return std::make_unique<DaosZoneGroup>(store, group);
  }
  const RGWZoneGroup& get_group() { return group; }
};

class DaosZone : public StoreZone {
 protected:
  DaosStore* store;
  RGWRealm* realm{nullptr};
  DaosZoneGroup zonegroup;
  RGWZone* zone_public_config{
      nullptr}; /* external zone params, e.g., entrypoints, log flags, etc. */
  RGWZoneParams* zone_params{
      nullptr}; /* internal zone params, e.g., rados pools */
  RGWPeriod* current_period{nullptr};
  rgw_zone_id cur_zone_id;

 public:
  DaosZone(DaosStore* _store) : store(_store), zonegroup(_store) {
    realm = new RGWRealm();
    zone_public_config = new RGWZone();
    zone_params = new RGWZoneParams();
    current_period = new RGWPeriod();
    cur_zone_id = rgw_zone_id(zone_params->get_id());

    // XXX: only default and STANDARD supported for now
    RGWZonePlacementInfo info;
    RGWZoneStorageClasses sc;
    sc.set_storage_class("STANDARD", nullptr, nullptr);
    info.storage_classes = sc;
    zone_params->placement_pools["default"] = info;
  }
  DaosZone(DaosStore* _store, DaosZoneGroup _zg)
      : store(_store), zonegroup(_zg) {
    realm = new RGWRealm();
    zone_public_config = new RGWZone();
    zone_params = new RGWZoneParams();
    current_period = new RGWPeriod();
    cur_zone_id = rgw_zone_id(zone_params->get_id());

    // XXX: only default and STANDARD supported for now
    RGWZonePlacementInfo info;
    RGWZoneStorageClasses sc;
    sc.set_storage_class("STANDARD", nullptr, nullptr);
    info.storage_classes = sc;
    zone_params->placement_pools["default"] = info;
  }
  ~DaosZone() = default;

  virtual std::unique_ptr<Zone> clone() override {
    return std::make_unique<DaosZone>(store);
  }
  virtual ZoneGroup& get_zonegroup() override;
  virtual int get_zonegroup(const std::string& id,
                            std::unique_ptr<ZoneGroup>* zonegroup) override;
  virtual const rgw_zone_id& get_id() override;
  virtual const std::string& get_name() const override;
  virtual bool is_writeable() override;
  virtual bool get_redirect_endpoint(std::string* endpoint) override;
  virtual bool has_zonegroup_api(const std::string& api) const override;
  virtual const std::string& get_current_period_id() override;
  virtual const RGWAccessKey& get_system_key() {
    return zone_params->system_key;
  }
  virtual const std::string& get_realm_name() { return realm->get_name(); }
  virtual const std::string& get_realm_id() { return realm->get_id(); }
  virtual const std::string_view get_tier_type() { return "rgw"; }

  friend class DaosStore;
};

class DaosLuaManager : public StoreLuaManager {
  DaosStore* store;

 public:
  DaosLuaManager(DaosStore* _s) : store(_s) {}
  virtual ~DaosLuaManager() = default;

  virtual int get_script(const DoutPrefixProvider* dpp, optional_yield y,
                         const std::string& key, std::string& script) override {
    DAOS_NOT_IMPLEMENTED_LOG(dpp);
    return -ENOENT;
  };

  virtual int put_script(const DoutPrefixProvider* dpp, optional_yield y,
                         const std::string& key,
                         const std::string& script) override {
    DAOS_NOT_IMPLEMENTED_LOG(dpp);
    return -ENOENT;
  };

  virtual int del_script(const DoutPrefixProvider* dpp, optional_yield y,
                         const std::string& key) override {
    DAOS_NOT_IMPLEMENTED_LOG(dpp);
    return -ENOENT;
  };

  virtual int add_package(const DoutPrefixProvider* dpp, optional_yield y,
                          const std::string& package_name) override {
    DAOS_NOT_IMPLEMENTED_LOG(dpp);
    return -ENOENT;
  };

  virtual int remove_package(const DoutPrefixProvider* dpp, optional_yield y,
                             const std::string& package_name) override {
    DAOS_NOT_IMPLEMENTED_LOG(dpp);
    return -ENOENT;
  };

  virtual int list_packages(const DoutPrefixProvider* dpp, optional_yield y,
                            rgw::lua::packages_t& packages) override {
    DAOS_NOT_IMPLEMENTED_LOG(dpp);
    return -ENOENT;
  };
};

class DaosObject : public StoreObject {
 private:
  DaosStore* store;
  RGWAccessControlPolicy acls;

 public:
  struct DaosReadOp : public StoreReadOp {
   private:
    DaosObject* source;

   public:
    DaosReadOp(DaosObject* _source);

    virtual int prepare(optional_yield y,
                        const DoutPrefixProvider* dpp) override;

    /*
     * Both `read` and `iterate` read up through index `end`
     * *inclusive*. The number of bytes that could be returned is
     * `end - ofs + 1`.
     */
    virtual int read(int64_t off, int64_t end, bufferlist& bl, optional_yield y,
                     const DoutPrefixProvider* dpp) override;
    virtual int iterate(const DoutPrefixProvider* dpp, int64_t off, int64_t end,
                        RGWGetDataCB* cb, optional_yield y) override;

    virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
                         bufferlist& dest, optional_yield y) override;
  };

  struct DaosDeleteOp : public StoreDeleteOp {
   private:
    DaosObject* source;

   public:
    DaosDeleteOp(DaosObject* _source);

    virtual int delete_obj(const DoutPrefixProvider* dpp,
                           optional_yield y, uint32_t flags) override;
  };

  ds3_obj_t* ds3o = nullptr;

  DaosObject() = default;

  DaosObject(DaosStore* _st, const rgw_obj_key& _k)
      : StoreObject(_k), store(_st), acls() {}
  DaosObject(DaosStore* _st, const rgw_obj_key& _k, Bucket* _b)
      : StoreObject(_k, _b), store(_st), acls() {}

  DaosObject(DaosObject& _o) = default;

  virtual ~DaosObject();

  virtual int delete_object(const DoutPrefixProvider* dpp, optional_yield y,
                            uint32_t flags) override;
  virtual int copy_object(
      User* user, req_info* info, const rgw_zone_id& source_zone,
      rgw::sal::Object* dest_object, rgw::sal::Bucket* dest_bucket,
      rgw::sal::Bucket* src_bucket, const rgw_placement_rule& dest_placement,
      ceph::real_time* src_mtime, ceph::real_time* mtime,
      const ceph::real_time* mod_ptr, const ceph::real_time* unmod_ptr,
      bool high_precision_time, const char* if_match, const char* if_nomatch,
      AttrsMod attrs_mod, bool copy_if_newer, Attrs& attrs,
      RGWObjCategory category, uint64_t olh_epoch,
      boost::optional<ceph::real_time> delete_at, std::string* version_id,
      std::string* tag, std::string* etag, void (*progress_cb)(off_t, void*),
      void* progress_data, const DoutPrefixProvider* dpp,
      optional_yield y) override;
  virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
  virtual int set_acl(const RGWAccessControlPolicy& acl) override {
    acls = acl;
    return 0;
  }

  virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjState** state,
                            optional_yield y, bool follow_olh = true) override;
  virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
                            Attrs* delattrs, optional_yield y) override;
  virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
                            rgw_obj* target_obj = NULL) override;
  virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
                               optional_yield y,
                               const DoutPrefixProvider* dpp) override;
  virtual int delete_obj_attrs(const DoutPrefixProvider* dpp,
                               const char* attr_name,
                               optional_yield y) override;
  virtual bool is_expired() override;
  virtual void gen_rand_obj_instance_name() override;
  virtual std::unique_ptr<Object> clone() override {
    return std::make_unique<DaosObject>(*this);
  }
  virtual std::unique_ptr<MPSerializer> get_serializer(
      const DoutPrefixProvider* dpp, const std::string& lock_name) override;
  virtual int transition(Bucket* bucket,
                         const rgw_placement_rule& placement_rule,
                         const real_time& mtime, uint64_t olh_epoch,
                         const DoutPrefixProvider* dpp,
                         optional_yield y,
                         uint32_t flags) override;
  virtual int transition_to_cloud(Bucket* bucket, rgw::sal::PlacementTier* tier,
                                  rgw_bucket_dir_entry& o,
                                  std::set<std::string>& cloud_targets,
                                  CephContext* cct, bool update_object,
                                  const DoutPrefixProvider* dpp,
                                  optional_yield y) override;
  virtual bool placement_rules_match(rgw_placement_rule& r1,
                                     rgw_placement_rule& r2) override;
  virtual int dump_obj_layout(const DoutPrefixProvider* dpp, optional_yield y,
                              Formatter* f) override;

  /* Swift versioning */
  virtual int swift_versioning_restore(bool& restored,
                                       const DoutPrefixProvider* dpp) override;
  virtual int swift_versioning_copy(const DoutPrefixProvider* dpp,
                                    optional_yield y) override;

  /* OPs */
  virtual std::unique_ptr<ReadOp> get_read_op() override;
  virtual std::unique_ptr<DeleteOp> get_delete_op() override;

  /* OMAP */
  virtual int omap_get_vals_by_keys(const DoutPrefixProvider* dpp,
                                    const std::string& oid,
                                    const std::set<std::string>& keys,
                                    Attrs* vals) override;
  virtual int omap_set_val_by_key(const DoutPrefixProvider* dpp,
                                  const std::string& key, bufferlist& val,
                                  bool must_exist, optional_yield y) override;
  virtual int chown(User& new_user, const DoutPrefixProvider* dpp,
                    optional_yield y) override;

  bool is_open() { return ds3o != nullptr; };
  // Only lookup the object, do not create
  int lookup(const DoutPrefixProvider* dpp);
  // Create the object, truncate if exists
  int create(const DoutPrefixProvider* dpp);
  // Release the daos resources
  int close(const DoutPrefixProvider* dpp);
  // Write to object starting from offset
  int write(const DoutPrefixProvider* dpp, bufferlist&& data, uint64_t offset);
  // Read size bytes from object starting from offset
  int read(const DoutPrefixProvider* dpp, bufferlist& data, uint64_t offset,
           uint64_t& size);
  // Get the object's dirent and attrs
  int get_dir_entry_attrs(const DoutPrefixProvider* dpp,
                          rgw_bucket_dir_entry* ent, Attrs* getattrs = nullptr);
  // Set the object's dirent and attrs
  int set_dir_entry_attrs(const DoutPrefixProvider* dpp,
                          rgw_bucket_dir_entry* ent, Attrs* setattrs = nullptr);
  // Marks this DAOS object as being the latest version and unmarks all other
  // versions as latest
  int mark_as_latest(const DoutPrefixProvider* dpp, ceph::real_time set_mtime);
  // get_bucket casted as DaosBucket*
  DaosBucket* get_daos_bucket() {
    return static_cast<DaosBucket*>(get_bucket());
  }
};

// A placeholder locking class for multipart upload.
class MPDaosSerializer : public StoreMPSerializer {
 public:
  MPDaosSerializer(const DoutPrefixProvider* dpp, DaosStore* store,
                   DaosObject* obj, const std::string& lock_name) {}

  virtual int try_lock(const DoutPrefixProvider* dpp, utime_t dur,
                       optional_yield y) override {
    return DAOS_NOT_IMPLEMENTED_LOG(dpp);
  }
  virtual int unlock() override { return DAOS_NOT_IMPLEMENTED_LOG(nullptr); }
};

class DaosAtomicWriter : public StoreWriter {
 protected:
  rgw::sal::DaosStore* store;
  const rgw_user& owner;
  const rgw_placement_rule* ptail_placement_rule;
  uint64_t olh_epoch;
  const std::string& unique_tag;
  DaosObject obj;
  uint64_t total_data_size = 0;  // for total data being uploaded

 public:
  DaosAtomicWriter(const DoutPrefixProvider* dpp, optional_yield y,
                   rgw::sal::Object* obj,
                   DaosStore* _store, const rgw_user& _owner,
                   const rgw_placement_rule* _ptail_placement_rule,
                   uint64_t _olh_epoch, const std::string& _unique_tag);
  ~DaosAtomicWriter() = default;

  // prepare to start processing object data
  virtual int prepare(optional_yield y) override;

  // Process a bufferlist
  virtual int process(bufferlist&& data, uint64_t offset) override;

  // complete the operation and make its result visible to clients
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time* mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at, const char* if_match,
                       const char* if_nomatch, const std::string* user_data,
                       rgw_zone_set* zones_trace, bool* canceled,
                       const req_context& rctx,
                       uint32_t flags) override;
};

class DaosMultipartWriter : public StoreWriter {
 protected:
  rgw::sal::DaosStore* store;
  MultipartUpload* upload;
  std::string upload_id;

  // Part parameters.
  const uint64_t part_num;
  const std::string part_num_str;
  uint64_t actual_part_size = 0;

  ds3_part_t* ds3p = nullptr;
  bool is_open() { return ds3p != nullptr; };

 public:
  DaosMultipartWriter(const DoutPrefixProvider* dpp, optional_yield y,
                      MultipartUpload* _upload,
                      rgw::sal::Object* obj,
                      DaosStore* _store, const rgw_user& owner,
                      const rgw_placement_rule* ptail_placement_rule,
                      uint64_t _part_num, const std::string& part_num_str)
      : StoreWriter(dpp, y),
        store(_store),
        upload(_upload),
        upload_id(_upload->get_upload_id()),
        part_num(_part_num),
        part_num_str(part_num_str) {}
  virtual ~DaosMultipartWriter();

  // prepare to start processing object data
  virtual int prepare(optional_yield y) override;

  // Process a bufferlist
  virtual int process(bufferlist&& data, uint64_t offset) override;

  // complete the operation and make its result visible to clients
  virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time* mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at, const char* if_match,
                       const char* if_nomatch, const std::string* user_data,
                       rgw_zone_set* zones_trace, bool* canceled,
                       optional_yield y,
                       uint32_t flags) override;

  const std::string& get_bucket_name();
};

class DaosMultipartPart : public StoreMultipartPart {
 protected:
  RGWUploadPartInfo info;

 public:
  DaosMultipartPart() = default;
  virtual ~DaosMultipartPart() = default;

  virtual uint32_t get_num() { return info.num; }
  virtual uint64_t get_size() { return info.accounted_size; }
  virtual const std::string& get_etag() { return info.etag; }
  virtual ceph::real_time& get_mtime() { return info.modified; }

  friend class DaosMultipartUpload;
};

class DaosMultipartUpload : public StoreMultipartUpload {
  DaosStore* store;
  RGWMPObj mp_obj;
  ACLOwner owner;
  ceph::real_time mtime;
  rgw_placement_rule placement;
  RGWObjManifest manifest;

 public:
  DaosMultipartUpload(DaosStore* _store, Bucket* _bucket,
                      const std::string& oid,
                      std::optional<std::string> upload_id, ACLOwner _owner,
                      ceph::real_time _mtime)
      : StoreMultipartUpload(_bucket),
        store(_store),
        mp_obj(oid, upload_id),
        owner(_owner),
        mtime(_mtime) {}
  virtual ~DaosMultipartUpload() = default;

  virtual const std::string& get_meta() const { return mp_obj.get_meta(); }
  virtual const std::string& get_key() const { return mp_obj.get_key(); }
  virtual const std::string& get_upload_id() const {
    return mp_obj.get_upload_id();
  }
  virtual const ACLOwner& get_owner() const override { return owner; }
  virtual ceph::real_time& get_mtime() { return mtime; }
  virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() override;
  virtual int init(const DoutPrefixProvider* dpp, optional_yield y,
                   ACLOwner& owner, rgw_placement_rule& dest_placement,
                   rgw::sal::Attrs& attrs) override;
  virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
                         int num_parts, int marker, int* next_marker,
                         bool* truncated,
                         bool assume_unsorted = false) override;
  virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct, optional_yield y) override;
  virtual int complete(const DoutPrefixProvider* dpp, optional_yield y,
                       CephContext* cct, std::map<int, std::string>& part_etags,
                       std::list<rgw_obj_index_key>& remove_objs,
                       uint64_t& accounted_size, bool& compressed,
                       RGWCompressionInfo& cs_info, off_t& off,
                       std::string& tag, ACLOwner& owner, uint64_t olh_epoch,
                       rgw::sal::Object* target_obj) override;
  virtual int get_info(const DoutPrefixProvider* dpp, optional_yield y,
                       rgw_placement_rule** rule,
                       rgw::sal::Attrs* attrs = nullptr) override;
  virtual std::unique_ptr<Writer> get_writer(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Object* obj, const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule, uint64_t part_num,
      const std::string& part_num_str) override;
  const std::string& get_bucket_name() { return bucket->get_name(); }
};

class DaosStore : public StoreDriver {
 private:
  DaosZone zone;
  RGWSyncModuleInstanceRef sync_module;

 public:
  ds3_t* ds3 = nullptr;

  CephContext* cctx;

  DaosStore(CephContext* c) : zone(this), cctx(c) {}
  ~DaosStore() = default;

  virtual const std::string get_name() const override { return "daos"; }

  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
  virtual std::string get_cluster_id(const DoutPrefixProvider* dpp,
                                     optional_yield y) override;
  virtual int get_user_by_access_key(const DoutPrefixProvider* dpp,
                                     const std::string& key, optional_yield y,
                                     std::unique_ptr<User>* user) override;
  virtual int get_user_by_email(const DoutPrefixProvider* dpp,
                                const std::string& email, optional_yield y,
                                std::unique_ptr<User>* user) override;
  virtual int get_user_by_swift(const DoutPrefixProvider* dpp,
                                const std::string& user_str, optional_yield y,
                                std::unique_ptr<User>* user) override;
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  std::unique_ptr<Bucket> get_bucket(User* u, const RGWBucketInfo& i) override;
  int load_bucket(const DoutPrefixProvider* dpp, User* u,
                  const rgw_bucket& b, std::unique_ptr<Bucket>* bucket,
                  optional_yield y) override;
  virtual bool is_meta_master() override;
  virtual Zone* get_zone() { return &zone; }
  virtual std::string zone_unique_id(uint64_t unique_num) override;
  virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
  virtual int cluster_stat(RGWClusterStat& stats) override;
  virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
  virtual std::unique_ptr<Notification> get_notification(
      rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s,
      rgw::notify::EventType event_type, optional_yield y,
      const std::string* object_name = nullptr) override;
  virtual std::unique_ptr<Notification> get_notification(
      const DoutPrefixProvider* dpp, rgw::sal::Object* obj,
      rgw::sal::Object* src_obj, rgw::notify::EventType event_type,
      rgw::sal::Bucket* _bucket, std::string& _user_id,
      std::string& _user_tenant, std::string& _req_id,
      optional_yield y) override;
  virtual RGWLC* get_rgwlc(void) override { return NULL; }
  virtual RGWCoroutinesManagerRegistry* get_cr_registry() override {
    return NULL;
  }

  virtual int log_usage(
      const DoutPrefixProvider* dpp,
      std::map<rgw_user_bucket, RGWUsageBatch>& usage_info) override;
  virtual int log_op(const DoutPrefixProvider* dpp, std::string& oid,
                     bufferlist& bl) override;
  virtual int register_to_service_map(
      const DoutPrefixProvider* dpp, const std::string& daemon_type,
      const std::map<std::string, std::string>& meta) override;
  virtual void get_quota(RGWQuota& quota) override;
  virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
                             RGWRateLimitInfo& user_ratelimit,
                             RGWRateLimitInfo& anon_ratelimit) override;
  virtual int set_buckets_enabled(const DoutPrefixProvider* dpp,
                                  std::vector<rgw_bucket>& buckets,
                                  bool enabled) override;
  virtual uint64_t get_new_req_id() override {
    return DAOS_NOT_IMPLEMENTED_LOG(nullptr);
  }
  virtual int get_sync_policy_handler(const DoutPrefixProvider* dpp,
                                      std::optional<rgw_zone_id> zone,
                                      std::optional<rgw_bucket> bucket,
                                      RGWBucketSyncPolicyHandlerRef* phandler,
                                      optional_yield y) override;
  virtual RGWDataSyncStatusManager* get_data_sync_manager(
      const rgw_zone_id& source_zone) override;
  virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) override {
    return;
  }
  virtual void wakeup_data_sync_shards(
      const DoutPrefixProvider* dpp, const rgw_zone_id& source_zone,
      boost::container::flat_map<
          int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids)
      override {
    return;
  }
  virtual int clear_usage(const DoutPrefixProvider* dpp) override {
    return DAOS_NOT_IMPLEMENTED_LOG(dpp);
  }
  virtual int read_all_usage(
      const DoutPrefixProvider* dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool* is_truncated, RGWUsageIter& usage_iter,
      std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  virtual int trim_all_usage(const DoutPrefixProvider* dpp,
                             uint64_t start_epoch, uint64_t end_epoch) override;
  virtual int get_config_key_val(std::string name, bufferlist* bl) override;
  virtual int meta_list_keys_init(const DoutPrefixProvider* dpp,
                                  const std::string& section,
                                  const std::string& marker,
                                  void** phandle) override;
  virtual int meta_list_keys_next(const DoutPrefixProvider* dpp, void* handle,
                                  int max, std::list<std::string>& keys,
                                  bool* truncated) override;
  virtual void meta_list_keys_complete(void* handle) override;
  virtual std::string meta_get_marker(void* handle) override;
  virtual int meta_remove(const DoutPrefixProvider* dpp,
                          std::string& metadata_key, optional_yield y) override;

  virtual const RGWSyncModuleInstanceRef& get_sync_module() {
    return sync_module;
  }
  virtual std::string get_host_id() { return ""; }

  std::unique_ptr<LuaManager> get_lua_manager(const DoutPrefixProvider *dpp = nullptr, const std::string& luarocks_path = "") override;
  virtual std::unique_ptr<RGWRole> get_role(
      std::string name, std::string tenant, std::string path = "",
      std::string trust_policy = "", std::string max_session_duration_str = "",
      std::multimap<std::string, std::string> tags = {}) override;
  virtual std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) override;
  virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
  virtual int get_roles(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& path_prefix,
                        const std::string& tenant,
                        std::vector<std::unique_ptr<RGWRole>>& roles) override;
  virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;
  virtual int get_oidc_providers(
      const DoutPrefixProvider* dpp, const std::string& tenant,
      std::vector<std::unique_ptr<RGWOIDCProvider>>& providers) override;
  virtual std::unique_ptr<Writer> get_append_writer(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Object* obj, const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule,
      const std::string& unique_tag, uint64_t position,
      uint64_t* cur_accounted_size) override;
  virtual std::unique_ptr<Writer> get_atomic_writer(
      const DoutPrefixProvider* dpp, optional_yield y,
      rgw::sal::Object* obj, const rgw_user& owner,
      const rgw_placement_rule* ptail_placement_rule, uint64_t olh_epoch,
      const std::string& unique_tag) override;
  virtual const std::string& get_compression_type(
      const rgw_placement_rule& rule) override;
  virtual bool valid_placement(const rgw_placement_rule& rule) override;

  virtual void finalize(void) override;

  virtual CephContext* ctx(void) override { return cctx; }

  virtual int initialize(CephContext* cct,
                         const DoutPrefixProvider* dpp) override;
};

}  // namespace rgw::sal
