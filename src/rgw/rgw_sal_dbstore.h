// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal_store.h"
#include "rgw_oidc_provider.h"
#include "rgw_role.h"
#include "rgw_lc.h"
#include "rgw_multi.h"

#include "driver/dbstore/common/dbstore.h"
#include "driver/dbstore/dbstore_mgr.h"

namespace rgw { namespace sal {

  class DBStore;

class LCDBSerializer : public StoreLCSerializer {

public:
  LCDBSerializer(DBStore* store, const std::string& oid, const std::string& lock_name, const std::string& cookie) {}

  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override { return 0; }
  virtual int unlock() override {
    return 0;
  }
};

class DBLifecycle : public StoreLifecycle {
  DBStore* store;

public:
  DBLifecycle(DBStore* _st) : store(_st) {}

  using StoreLifecycle::get_entry;
  virtual int get_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) override;
  virtual int get_next_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) override;
  virtual int set_entry(const std::string& oid, LCEntry& entry) override;
  virtual int list_entries(const std::string& oid, const std::string& marker,
			   uint32_t max_entries,
			   std::vector<std::unique_ptr<LCEntry>>& entries) override;
  virtual int rm_entry(const std::string& oid, LCEntry& entry) override;
  virtual int get_head(const std::string& oid, std::unique_ptr<LCHead>* head) override;
  virtual int put_head(const std::string& oid, LCHead& head) override;
  virtual std::unique_ptr<LCSerializer> get_serializer(const std::string& lock_name,
						       const std::string& oid,
						       const std::string& cookie) override;
};

class DBNotification : public StoreNotification {
protected:
  public:
  DBNotification(Object* _obj, Object* _src_obj, rgw::notify::EventType _type)
    : StoreNotification(_obj, _src_obj, _type) {}
    ~DBNotification() = default;

    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) override { return 0;}
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const std::string& etag, const std::string& version) override { return 0; }
};

  class DBUser : public StoreUser {
    private:
      DBStore *store;

    public:
      DBUser(DBStore *_st, const rgw_user& _u) : StoreUser(_u), store(_st) { }
      DBUser(DBStore *_st, const RGWUserInfo& _i) : StoreUser(_i), store(_st) { }
      DBUser(DBStore *_st) : store(_st) { }
      DBUser(DBUser& _o) = default;
      DBUser() {}

      virtual std::unique_ptr<User> clone() override {
        return std::unique_ptr<User>(new DBUser(*this));
      }
      int list_buckets(const DoutPrefixProvider *dpp, const std::string& marker, const std::string& end_marker,
          uint64_t max, bool need_stats, BucketList& buckets, optional_yield y) override;
      virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual int read_stats(const DoutPrefixProvider *dpp,
          optional_yield y, RGWStorageStats* stats,
          ceph::real_time *last_stats_sync = nullptr,
          ceph::real_time *last_stats_update = nullptr) override;
      virtual int read_stats_async(const DoutPrefixProvider *dpp, boost::intrusive_ptr<ReadStatsCB> cb) override;
      virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
          bool* is_truncated, RGWUsageIter& usage_iter,
          std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) override;

      /* Placeholders */
      virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) override;
      virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) override;
      virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual int verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider* dpp, optional_yield y) override;

      friend class DBBucket;
  };

  class DBBucket : public StoreBucket {
    private:
      DBStore *store;
      RGWAccessControlPolicy acls;

    public:
      DBBucket(DBStore *_st)
        : store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, const rgw_bucket& _b)
        : StoreBucket(_b),
        store(_st),
        acls() {
        }

      DBBucket(DBStore *_st, const RGWBucketInfo& _i)
        : StoreBucket(_i),
        store(_st),
        acls() {
        }

      ~DBBucket() { }

      virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
      virtual int list(const DoutPrefixProvider *dpp, ListParams&, int, ListResults&, optional_yield y) override;
      virtual int remove(const DoutPrefixProvider *dpp, bool delete_children, optional_yield y) override;
      virtual int remove_bypass_gc(int concurrent_max, bool
				   keep_index_consistent,
				   optional_yield y, const
				   DoutPrefixProvider *dpp) override;
      virtual RGWAccessControlPolicy& get_acl(void) override { return acls; }
      virtual int set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy& acl, optional_yield y) override;
      int create(const DoutPrefixProvider* dpp,
                 const CreateParams& params,
                 optional_yield y) override;
      virtual int load_bucket(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int read_stats(const DoutPrefixProvider *dpp,
			     const bucket_index_layout_generation& idx_layout,
			     int shard_id,
          std::string *bucket_ver, std::string *master_ver,
          std::map<RGWObjCategory, RGWStorageStats>& stats,
          std::string *max_marker = nullptr,
          bool *syncstopped = nullptr) override;
      virtual int read_stats_async(const DoutPrefixProvider *dpp, const bucket_index_layout_generation& idx_layout, int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx) override;
      int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y,
                          RGWBucketEnt* ent) override;
      int check_bucket_shards(const DoutPrefixProvider *dpp,
                              uint64_t num_objs, optional_yield y) override;
      virtual int chown(const DoutPrefixProvider *dpp, const rgw_user& new_owner, optional_yield y) override;
      virtual int put_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time mtime, optional_yield y) override;
      virtual int check_empty(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
      virtual int merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& attrs, optional_yield y) override;
      virtual int try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime, optional_yield y) override;
      virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
          bool *is_truncated, RGWUsageIter& usage_iter,
          std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) override;
      virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) override;
      virtual int check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
      virtual int rebuild_index(const DoutPrefixProvider *dpp) override;
      virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) override;
      virtual int purge_instance(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual std::unique_ptr<Bucket> clone() override {
        return std::make_unique<DBBucket>(*this);
      }
      virtual std::unique_ptr<MultipartUpload> get_multipart_upload(
				const std::string& oid, std::optional<std::string> upload_id,
				ACLOwner owner={}, ceph::real_time mtime=ceph::real_clock::now()) override;
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

      friend class DBStore;
  };

  class DBPlacementTier: public StorePlacementTier {
    DBStore* store;
    RGWZoneGroupPlacementTier tier;
  public:
    DBPlacementTier(DBStore* _store, const RGWZoneGroupPlacementTier& _tier) : store(_store), tier(_tier) {}
    virtual ~DBPlacementTier() = default;

    virtual const std::string& get_tier_type() { return tier.tier_type; }
    virtual const std::string& get_storage_class() { return tier.storage_class; }
    virtual bool retain_head_object() { return tier.retain_head_object; }
    RGWZoneGroupPlacementTier& get_rt() { return tier; }
  };

  class DBZoneGroup : public StoreZoneGroup {
    DBStore* store;
    std::unique_ptr<RGWZoneGroup> group;
    std::string empty;
  public:
    DBZoneGroup(DBStore* _store, std::unique_ptr<RGWZoneGroup> _group) : store(_store), group(std::move(_group)) {}
    virtual ~DBZoneGroup() = default;

    virtual const std::string& get_id() const override { return group->get_id(); };
    virtual const std::string& get_name() const override { return group->get_name(); };
    virtual int equals(const std::string& other_zonegroup) const override {
      return group->equals(other_zonegroup);
    };
    virtual bool placement_target_exists(std::string& target) const override;
    virtual bool is_master_zonegroup() const override {
      return group->is_master_zonegroup();
    };
    virtual const std::string& get_api_name() const override { return group->api_name; };
    virtual void get_placement_target_names(std::set<std::string>& names) const override;
    virtual const std::string& get_default_placement_name() const override {
      return group->default_placement.name; };
    virtual int get_hostnames(std::list<std::string>& names) const override {
      names = group->hostnames;
      return 0;
    };
    virtual int get_s3website_hostnames(std::list<std::string>& names) const override {
      names = group->hostnames_s3website;
      return 0;
    };
    virtual int get_zone_count() const override {
      /* currently only 1 zone supported */
      return 1;
    }
    virtual int get_placement_tier(const rgw_placement_rule& rule,
				   std::unique_ptr<PlacementTier>* tier) {
      return -1;
    }
    virtual int get_zone_by_id(const std::string& id, std::unique_ptr<Zone>* zone) override {
      return -1;
    }
    virtual int get_zone_by_name(const std::string& name, std::unique_ptr<Zone>* zone) override {
      return -1;
    }
    virtual int list_zones(std::list<std::string>& zone_ids) override {
      zone_ids.clear();
      return 0;
    }
    virtual std::unique_ptr<ZoneGroup> clone() override {
      std::unique_ptr<RGWZoneGroup>zg = std::make_unique<RGWZoneGroup>(*group.get());
      return std::make_unique<DBZoneGroup>(store, std::move(zg));
    }
  };

  class DBZone : public StoreZone {
    protected:
      DBStore* store;
      RGWRealm *realm{nullptr};
      DBZoneGroup *zonegroup{nullptr};
      RGWZone *zone_public_config{nullptr}; /* external zone params, e.g., entrypoints, log flags, etc. */
      RGWZoneParams *zone_params{nullptr}; /* internal zone params, e.g., rados pools */
      RGWPeriod *current_period{nullptr};

    public:
      DBZone(DBStore* _store) : store(_store) {
	realm = new RGWRealm();
	std::unique_ptr<RGWZoneGroup> rzg = std::make_unique<RGWZoneGroup>("default", "default");
	rzg->api_name = "default";
	rzg->is_master = true;
        zonegroup = new DBZoneGroup(store, std::move(rzg));
        zone_public_config = new RGWZone();
        zone_params = new RGWZoneParams();
        current_period = new RGWPeriod();

        // XXX: only default and STANDARD supported for now
        RGWZonePlacementInfo info;
        RGWZoneStorageClasses sc;
        sc.set_storage_class("STANDARD", nullptr, nullptr);
        info.storage_classes = sc;
        zone_params->placement_pools["default"] = info;
      }
      ~DBZone() {
	delete realm;
	delete zonegroup;
	delete zone_public_config;
	delete zone_params;
	delete current_period;
      }

      virtual std::unique_ptr<Zone> clone() override {
	return std::make_unique<DBZone>(store);
      }
      virtual ZoneGroup& get_zonegroup() override;
      const RGWZoneParams& get_rgw_params();
      virtual const std::string& get_id() override;
      virtual const std::string& get_name() const override;
      virtual bool is_writeable() override;
      virtual bool get_redirect_endpoint(std::string* endpoint) override;
      virtual bool has_zonegroup_api(const std::string& api) const override;
      virtual const std::string& get_current_period_id() override;
      virtual const RGWAccessKey& get_system_key() override;
      virtual const std::string& get_realm_name() override;
      virtual const std::string& get_realm_id() override;
      virtual const std::string_view get_tier_type() override { return "rgw"; }
      virtual RGWBucketSyncPolicyHandlerRef get_sync_policy_handler() override;
  };

  class DBLuaManager : public StoreLuaManager {
    DBStore* store;

    public:
    DBLuaManager(DBStore* _s) : store(_s)
    {
    }
    virtual ~DBLuaManager() = default;

    /** Get a script named with the given key from the backing store */
    virtual int get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) override;
    /** Put a script named with the given key to the backing store */
    virtual int put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) override;
    /** Delete a script named with the given key from the backing store */
    virtual int del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) override;
    /** Add a lua package */
    virtual int add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name) override;
    /** Remove a lua package */
    virtual int remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name) override;
    /** List lua packages */
    virtual int list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages) override;
    /** Reload lua packages */
    virtual int reload_packages(const DoutPrefixProvider* dpp, optional_yield y) override;
  };

  class DBOIDCProvider : public RGWOIDCProvider {
    DBStore* store;
    public:
    DBOIDCProvider(DBStore* _store) : store(_store) {}
    ~DBOIDCProvider() = default;

    virtual int store_url(const DoutPrefixProvider *dpp, const std::string& url, bool exclusive, optional_yield y) override { return 0; }
    virtual int read_url(const DoutPrefixProvider *dpp, const std::string& url, const std::string& tenant, optional_yield y) override { return 0; }
    virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) override { return 0;}

    void encode(bufferlist& bl) const {
      RGWOIDCProvider::encode(bl);
    }
    void decode(bufferlist::const_iterator& bl) {
      RGWOIDCProvider::decode(bl);
    }
  };

  /*
   * For multipart upload, below is the process flow -
   *
   * MultipartUpload::Init - create head object of meta obj (src_obj_name + "." + upload_id)
   *                     [ Meta object stores all the parts upload info]
   * MultipartWriter::process - create all data/tail objects with obj_name same as
   *                        meta obj (so that they can all be identified & deleted
   *                        during abort)
   * MultipartUpload::Abort - Just delete meta obj .. that will indirectly delete all the
   *                     uploads associated with that upload id / meta obj so far.
   * MultipartUpload::Complete - create head object of the original object (if not exists) &
   *                     rename all data/tail objects to orig object name and update
   *                     metadata of the orig object.
   */
  class DBMultipartPart : public StoreMultipartPart {
  protected:
    RGWUploadPartInfo info; /* XXX: info contains manifest also which is not needed */

  public:
    DBMultipartPart() = default;
    virtual ~DBMultipartPart() = default;

    virtual RGWUploadPartInfo& get_info() { return info; }
    virtual void set_info(const RGWUploadPartInfo& _info) { info = _info; }
    virtual uint32_t get_num() { return info.num; }
    virtual uint64_t get_size() { return info.accounted_size; }
    virtual const std::string& get_etag() { return info.etag; }
    virtual ceph::real_time& get_mtime() { return info.modified; }

  };

  class DBMPObj {
    std::string oid; // object name
    std::string upload_id;
    std::string meta; // multipart meta object = <oid>.<upload_id>
  public:
    DBMPObj() {}
    DBMPObj(const std::string& _oid, const std::string& _upload_id) {
      init(_oid, _upload_id, _upload_id);
    }
    DBMPObj(const std::string& _oid, std::optional<std::string> _upload_id) {
      if (_upload_id) {
        init(_oid, *_upload_id, *_upload_id);
      } else {
        from_meta(_oid);
      }
    }
    void init(const std::string& _oid, const std::string& _upload_id) {
      init(_oid, _upload_id, _upload_id);
    }
    void init(const std::string& _oid, const std::string& _upload_id, const std::string& part_unique_str) {
      if (_oid.empty()) {
        clear();
        return;
      }
      oid = _oid;
      upload_id = _upload_id;
      meta = oid + "." + upload_id;
    }
    const std::string& get_upload_id() const {
      return upload_id;
    }
    const std::string& get_key() const {
      return oid;
    }
    const std::string& get_meta() const { return meta; }
    bool from_meta(const std::string& meta) {
      int end_pos = meta.length();
      int mid_pos = meta.rfind('.', end_pos - 1); // <key>.<upload_id>
      if (mid_pos < 0)
        return false;
      oid = meta.substr(0, mid_pos);
      upload_id = meta.substr(mid_pos + 1, end_pos - mid_pos - 1);
      init(oid, upload_id, upload_id);
      return true;
    }
    void clear() {
      oid = "";
      meta = "";
      upload_id = "";
    }
  };

  class DBMultipartUpload : public StoreMultipartUpload {
    DBStore* store;
    DBMPObj mp_obj;
    ACLOwner owner;
    ceph::real_time mtime;
    rgw_placement_rule placement;

  public:
    DBMultipartUpload(DBStore* _store, Bucket* _bucket, const std::string& oid, std::optional<std::string> upload_id, ACLOwner _owner, ceph::real_time _mtime) : StoreMultipartUpload(_bucket), store(_store), mp_obj(oid, upload_id), owner(_owner), mtime(_mtime) {}
    virtual ~DBMultipartUpload() = default;

    virtual const std::string& get_meta() const { return mp_obj.get_meta(); }
    virtual const std::string& get_key() const { return mp_obj.get_key(); }
    virtual const std::string& get_upload_id() const { return mp_obj.get_upload_id(); }
    virtual const ACLOwner& get_owner() const override { return owner; }
    virtual ceph::real_time& get_mtime() { return mtime; }
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
		       rgw::sal::Object* target_obj) override;
    virtual int get_info(const DoutPrefixProvider *dpp, optional_yield y, rgw_placement_rule** rule, rgw::sal::Attrs* attrs = nullptr) override;
    virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  rgw::sal::Object* obj,
			  const rgw_user& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) override;
  };

  class DBObject : public StoreObject {
    private:
      DBStore* store;
      RGWAccessControlPolicy acls;

    public:
      struct DBReadOp : public ReadOp {
        private:
          DBObject* source;
          RGWObjectCtx* octx;
          DB::Object op_target;
          DB::Object::Read parent_op;

        public:
          DBReadOp(DBObject *_source, RGWObjectCtx *_octx);

          virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;

	  /*
	   * Both `read` and `iterate` read up through index `end`
	   * *inclusive*. The number of bytes that could be returned is
	   * `end - ofs + 1`.
	   */
	  virtual int read(int64_t ofs, int64_t end, bufferlist& bl,
			   optional_yield y,
			   const DoutPrefixProvider* dpp) override;
	  virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs,
			      int64_t end, RGWGetDataCB* cb,
			      optional_yield y) override;

	  virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) override;
      };

      struct DBDeleteOp : public DeleteOp {
        private:
          DBObject* source;
          DB::Object op_target;
          DB::Object::Delete parent_op;

        public:
          DBDeleteOp(DBObject* _source);

          virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
      };

      DBObject() = default;

      DBObject(DBStore *_st, const rgw_obj_key& _k)
        : StoreObject(_k),
        store(_st),
        acls() {}

      DBObject(DBStore *_st, const rgw_obj_key& _k, Bucket* _b)
        : StoreObject(_k, _b),
        store(_st),
        acls() {}

      DBObject(DBObject& _o) = default;

      virtual int delete_object(const DoutPrefixProvider* dpp,
          optional_yield y,
          uint32_t flags) override;
      virtual int copy_object(User* user,
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

      virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state, optional_yield y, bool follow_olh = true) override;
      virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y) override;
      virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) override;
      virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) override;
      virtual int delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y) override;
      virtual bool is_expired() override;
      virtual void gen_rand_obj_instance_name() override;
      virtual std::unique_ptr<Object> clone() override {
        return std::unique_ptr<Object>(new DBObject(*this));
      }
      virtual std::unique_ptr<MPSerializer> get_serializer(const DoutPrefixProvider *dpp,
							   const std::string& lock_name) override;
      virtual int transition(Bucket* bucket,
          const rgw_placement_rule& placement_rule,
          const real_time& mtime,
          uint64_t olh_epoch,
          const DoutPrefixProvider* dpp,
          optional_yield y,
          uint32_t flags) override;
      virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
      virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f) override;

      /* Swift versioning */
      virtual int swift_versioning_restore(bool& restored,
          const DoutPrefixProvider* dpp, optional_yield y) override;
      virtual int swift_versioning_copy(const DoutPrefixProvider* dpp,
          optional_yield y) override;

      /* OPs */
      virtual std::unique_ptr<ReadOp> get_read_op() override;
      virtual std::unique_ptr<DeleteOp> get_delete_op() override;

      /* OMAP */
      virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
          const std::set<std::string>& keys,
          Attrs* vals) override;
      virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
          bool must_exist, optional_yield y) override;
      virtual int chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y) override;
    private:
      int read_attrs(const DoutPrefixProvider* dpp, DB::Object::Read &read_op, optional_yield y, rgw_obj* target_obj = nullptr);
  };

  class MPDBSerializer : public StoreMPSerializer {

  public:
    MPDBSerializer(const DoutPrefixProvider *dpp, DBStore* store, DBObject* obj, const std::string& lock_name) {}

    virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override {return 0; }
    virtual int unlock() override { return 0;}
  };

  class DBAtomicWriter : public StoreWriter {
    protected:
    rgw::sal::DBStore* store;
    const rgw_user& owner;
	const rgw_placement_rule *ptail_placement_rule;
	uint64_t olh_epoch;
	const std::string& unique_tag;
    DBObject obj;
    DB::Object op_target;
    DB::Object::Write parent_op;
    uint64_t total_data_size = 0; /* for total data being uploaded */
    bufferlist head_data;
    bufferlist tail_part_data;
    uint64_t tail_part_offset;
    uint64_t tail_part_size = 0; /* corresponds to each tail part being
                                  written to dbstore */

    public:
    DBAtomicWriter(const DoutPrefixProvider *dpp,
	    	    optional_yield y,
		        rgw::sal::Object* obj,
		        DBStore* _store,
    		    const rgw_user& _owner,
	    	    const rgw_placement_rule *_ptail_placement_rule,
		        uint64_t _olh_epoch,
		        const std::string& _unique_tag);
    ~DBAtomicWriter() = default;

    // prepare to start processing object data
    virtual int prepare(optional_yield y) override;

    // Process a bufferlist
    virtual int process(bufferlist&& data, uint64_t offset) override;

    // complete the operation and make its result visible to clients
    virtual int complete(size_t accounted_size, const std::string& etag,
                         ceph::real_time *mtime, ceph::real_time set_mtime,
                         std::map<std::string, bufferlist>& attrs,
                         ceph::real_time delete_at,
                         const char *if_match, const char *if_nomatch,
                         const std::string *user_data,
                         rgw_zone_set *zones_trace, bool *canceled,
                         const req_context& rctx,
                         uint32_t flags) override;
  };

  class DBMultipartWriter : public StoreWriter {
  protected:
    rgw::sal::DBStore* store;
    const rgw_user& owner;
	const rgw_placement_rule *ptail_placement_rule;
	uint64_t olh_epoch;
    rgw::sal::Object* head_obj;
    std::string upload_id;
    int part_num;
    std::string oid; /* object->name() + "." + "upload_id" + "." + part_num */
    std::unique_ptr<rgw::sal::Object> meta_obj;
    DB::Object op_target;
    DB::Object::Write parent_op;
    std::string part_num_str;
    uint64_t total_data_size = 0; /* for total data being uploaded */
    bufferlist head_data;
    bufferlist tail_part_data;
    uint64_t tail_part_offset;
    uint64_t tail_part_size = 0; /* corresponds to each tail part being
                                  written to dbstore */

public:
    DBMultipartWriter(const DoutPrefixProvider *dpp,
		       optional_yield y, MultipartUpload* upload,
		       rgw::sal::Object* obj,
		       DBStore* _store,
		       const rgw_user& owner,
		       const rgw_placement_rule *ptail_placement_rule,
		       uint64_t part_num, const std::string& part_num_str);
    ~DBMultipartWriter() = default;

    // prepare to start processing object data
    virtual int prepare(optional_yield y) override;

    // Process a bufferlist
    virtual int process(bufferlist&& data, uint64_t offset) override;

    // complete the operation and make its result visible to clients
    virtual int complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx,
                       uint32_t flags) override;
  };

  class DBStore : public StoreDriver {
    private:
      /* DBStoreManager is used in case multiple
       * connections are needed one for each tenant.
       */
      DBStoreManager *dbsm;
      /* default db (single connection). If needed
       * multiple db handles (for eg., one for each tenant),
       * use dbsm->getDB(tenant) */
      DB *db;
      DBZone zone;
      RGWSyncModuleInstanceRef sync_module;
      RGWLC* lc;
      CephContext *cct;
      const DoutPrefixProvider *dpp;
      bool use_lc_thread;

    public:
      DBStore(): dbsm(nullptr), zone(this), cct(nullptr), dpp(nullptr),
                 use_lc_thread(false) {}
      ~DBStore() { delete dbsm; }

      DBStore& set_run_lc_thread(bool _use_lc_thread) {
        use_lc_thread = _use_lc_thread;
        return *this;
      }

      virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;

      virtual const std::string get_name() const override {
        return "dbstore";
      }

      virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
      virtual int get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) override;
      virtual int get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) override;
      virtual int get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) override;
      virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
      virtual std::string get_cluster_id(const DoutPrefixProvider* dpp, optional_yield y);
      std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i) override;
      int load_bucket(const DoutPrefixProvider *dpp, const rgw_bucket& b,
                      std::unique_ptr<Bucket>* bucket, optional_yield y) override;
      virtual bool is_meta_master() override;
      virtual Zone* get_zone() { return &zone; }
      virtual std::string zone_unique_id(uint64_t unique_num) override;
      virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
      virtual int get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup) override;
      virtual int list_all_zones(const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids) override;
      virtual int cluster_stat(RGWClusterStat& stats) override;
      virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;

  virtual std::unique_ptr<Notification> get_notification(
    rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s,
    rgw::notify::EventType event_type, optional_yield y, const std::string* object_name) override;

  virtual std::unique_ptr<Notification> get_notification(
    const DoutPrefixProvider* dpp, rgw::sal::Object* obj,
    rgw::sal::Object* src_obj,
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket,
    std::string& _user_id, std::string& _user_tenant, std::string& _req_id,
    optional_yield y) override;

      virtual RGWLC* get_rgwlc(void) override;
      virtual RGWCoroutinesManagerRegistry* get_cr_registry() override { return NULL; }
      virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info, optional_yield y) override;
      virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) override;
      virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
          const std::map<std::string, std::string>& meta) override;
      virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit) override;
      virtual void get_quota(RGWQuota& quota) override;
    virtual int set_buckets_enabled(const DoutPrefixProvider *dpp, std::vector<rgw_bucket>& buckets, bool enabled, optional_yield y) override;
      virtual int get_sync_policy_handler(const DoutPrefixProvider *dpp,
          std::optional<rgw_zone_id> zone,
          std::optional<rgw_bucket> bucket,
          RGWBucketSyncPolicyHandlerRef *phandler,
          optional_yield y) override;
      virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
      virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) override { return; }
      virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp,
					   const rgw_zone_id& source_zone,
					   boost::container::flat_map<
					     int,
					   boost::container::flat_set<rgw_data_notify_entry>>& shard_ids) override { return; }
      virtual int clear_usage(const DoutPrefixProvider *dpp, optional_yield y) override { return 0; }
      virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
          uint32_t max_entries, bool *is_truncated,
          RGWUsageIter& usage_iter,
          std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y) override;
      virtual int get_config_key_val(std::string name, bufferlist* bl) override;
      virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle) override;
      virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, std::list<std::string>& keys, bool* truncated) override;
      virtual void meta_list_keys_complete(void* handle) override;
      virtual std::string meta_get_marker(void *handle) override;
      virtual int meta_remove(const DoutPrefixProvider *dpp, std::string& metadata_key, optional_yield y) override;

      virtual const RGWSyncModuleInstanceRef& get_sync_module() { return sync_module; }
      virtual std::string get_host_id() { return ""; }

      std::unique_ptr<LuaManager> get_lua_manager(const std::string& luarocks_path) override;
      virtual std::unique_ptr<RGWRole> get_role(std::string name,
          std::string tenant,
          std::string path="",
          std::string trust_policy="",
          std::string max_session_duration_str="",
          std::multimap<std::string,std::string> tags={}) override;
      virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
      virtual std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) override;
      virtual int get_roles(const DoutPrefixProvider *dpp,
          optional_yield y,
          const std::string& path_prefix,
          const std::string& tenant,
          std::vector<std::unique_ptr<RGWRole>>& roles) override;
      virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;
      virtual int get_oidc_providers(const DoutPrefixProvider *dpp,
          const std::string& tenant,
          std::vector<std::unique_ptr<RGWOIDCProvider>>& providers, optional_yield y) override;
      virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) override;
      virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;

      virtual const std::string& get_compression_type(const rgw_placement_rule& rule) override;
      virtual bool valid_placement(const rgw_placement_rule& rule) override;

      virtual void finalize(void) override;

      virtual CephContext *ctx(void) override {
        return db->ctx();
      }

      virtual void register_admin_apis(RGWRESTMgr* mgr) override { };

      /* Unique to DBStore */
      void setDBStoreManager(DBStoreManager *stm) { dbsm = stm; }
      DBStoreManager *getDBStoreManager(void) { return dbsm; }

      void setDB(DB * st) { db = st; }
      DB *getDB(void) { return db; }

      DB *getDB(std::string tenant) { return dbsm->getDB(tenant, false); }
  };

} } // namespace rgw::sal
