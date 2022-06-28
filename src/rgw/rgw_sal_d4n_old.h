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


/*includes need to cover both dbstore and rados? Dan P*/
/*Everywhere that there is a TStore pointer may need to be switched
* to a default store pointer. Dan P */


#pragma once

#include "rgw_sal.h"
#include "rgw_oidc_provider.h"
#include "rgw_role.h"
#include "rgw_multi.h"
#include "rgw_directory.h"

//#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

namespace rgw { namespace sal {

  class D4NFilter;

  /*CURRENTLY UNUSED -Daniel P*/
  class LCTSerializer : public LCSerializer {
  const std::string oid;

  public:
    LCTSerializer(D4NFilter* trace, const std::string& oid, const std::string& lock_name, const std::string& cookie) {}

    virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override { return 0; }
    virtual int unlock() override {
      return 0;
    }
  };

  /*CURRENTLY UNUSED -Daniel P*/
  class TracerLifecycle : public Lifecycle {
  D4NFilter* trace;

  public:
    TracerLifecycle(D4NFilter* _st) : trace(_st) {}

    virtual int get_entry(const std::string& oid, const std::string& marker, std::unique_ptr<LCEntry>* entry) override;
    virtual int get_next_entry(const std::string& oid, std::string& marker, std::unique_ptr<LCEntry>* entry) override;
    virtual int set_entry(const std::string& oid, LCEntry& entry) override;
    virtual int list_entries(const std::string& oid, const std::string& marker,
	  		   uint32_t max_entries,
			   std::vector<std::unique_ptr<LCEntry>>& entries) override;
    virtual int rm_entry(const std::string& oid, LCEntry& entry) override;
    virtual int get_head(const std::string& oid, std::unique_ptr<LCHead>* head) override;
    virtual int put_head(const std::string& oid, LCHead& head) override;
    virtual LCSerializer* get_serializer(const std::string& lock_name, const std::string& oid, const std::string& cookie) override;
    };


  /*CURRENTLY UNUSED -Daniel P*/  
  class TZone : public Zone {
    protected:
      D4NFilter* trace;
      /*Intention is to delete some or all of these as neccessary to remove 'shadow' variables
      *Instead tracer versions of classes should always direct function calls to the real class.
      */
      RGWRealm *realm{nullptr};
      RGWZoneGroup *zonegroup{nullptr};
      RGWZone *zone_public_config{nullptr}; /* external zone params, e.g., entrypoints, log flags, etc. */  
      RGWZoneParams *zone_params{nullptr}; /* internal zone params, e.g., rados pools */
      RGWPeriod *current_period{nullptr};
      rgw_zone_id cur_zone_id;
      Zone* realZone;
      //A lot of these can probably be removed - Dan P

    public:
      TZone(D4NFilter* _tracer) : trace(_tracer) {
        realm = new RGWRealm();
        zonegroup = new RGWZoneGroup();
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
      ~TZone() = default;

      virtual ZoneGroup& get_zonegroup() override;
      virtual int get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup) override;
      virtual const rgw_zone_id& get_id() override;
      virtual const std::string& get_name() const override;
      virtual bool is_writeable() override;
      virtual bool get_redirect_endpoint(std::string* endpoint) override;
      virtual bool has_zonegroup_api(const std::string& api) const override;
      virtual const std::string& get_current_period_id() override;
      virtual const RGWAccessKey& get_system_key() override;
      virtual const std::string& get_realm_name() override;
      virtual const std::string& get_realm_id() override;
  };

  /*CURRENTLY UNUSED -Daniel P*/
  class TNotification : public Notification {
  protected:
  public:
    /*TNotification(Object* _obj, Object* _src_obj, rgw::notify::EventType _type)
    : Notification(_obj, _src_obj, _type) {}*/ // Ignore? -Sam
    ~TNotification() = default;

    virtual int publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags = nullptr) override { return 0;}
    virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			       const ceph::real_time& mtime, const std::string& etag, const std::string& version) override { return 0; }
  };


class D4NUser : public User {
    private:
      D4NFilter *trace;
      std::unique_ptr<User> real_user;

    public:
    //D4NUser(D4NFilter *_st, const rgw_user& _u, std::unique_ptr<User>& _ru) : User(_u), trace(_st), real_user(std::move(_ru)) { } // Ignore? -Sam
    //D4NUser(D4NFilter *_st, const RGWUserInfo& _i, std::unique_ptr<User> _ru) : User(_i), trace(_st), real_user(std::move(_ru)) { } // Ignore? -Sam
    //D4NUser(D4NFilter * _st, const RGWUserInfo& _i, std::unique_ptr<User>* _ru) : User(_i), trace(_st),  real_user(std::move(* _ru)) {} // Ignore? -Sam
    D4NUser(D4NFilter *_st) : trace(_st) { }
    D4NUser(D4NUser& _o, std::unique_ptr<User> _ru) : real_user(std::move(_ru)) {}
    D4NUser() {}

    virtual std::unique_ptr<User> clone() override {
      return std::make_unique<D4NUser>(*this); // Is this the correct implementation? -Sam  
      // return std::unique_ptr<User>(new D4NUser(*this, std::move(this->real_user)));
    } 
      
    bool info_empty()
    {
        return real_user->info_empty();
    }

    std::string& get_display_name() { return real_user->get_display_name(); }  
    /** Get the tenant name for this User */
    const std::string& get_tenant() { return real_user->get_tenant(); } //Changed for Tracer -Daniel P
    /** Set the tenant name for this User */
    void set_tenant(std::string& _t) { real_user->set_tenant(_t); } //Changed for Tracer -Daniel P
    /** Get the namespace for this User */
    const std::string& get_ns() { return real_user->get_ns(); } //Changed for Tracer -Daniel P
    /** Set the namespace for this User */
    void set_ns(std::string& _ns) { real_user->set_ns(_ns); } //Changed for Tracer -Daniel P
    /** Clear the namespace for this User */
    void clear_ns() { real_user->clear_ns(); } //Changed for Tracer -Daniel P
    /** Get the full ID for this User */
    const rgw_user& get_id() const {return real_user->get_id(); } //Changed for Tracer -Daniel P
    /** Get the type of this User */
    uint32_t get_type() const { return real_user->get_type(); } //Changed for Tracer -Daniel P
    /** Get the maximum number of buckets allowed for this User */
    int32_t get_max_buckets() const { return real_user->get_max_buckets(); } //Changed for Tracer -Daniel P
    /** Get the capabilities for this User */
    const RGWUserCaps& get_caps() const { return real_user->get_caps(); } //Changed for Tracer -Daniel P
    /** Get the version tracker for this User */
    RGWObjVersionTracker& get_version_tracker() { return real_user->get_version_tracker(); } 
    /** Get the cached attributes for this User */
    Attrs& get_attrs() { return real_user->get_attrs(); }
    /** Set the cached attributes fro this User */
    void set_attrs(Attrs& _attrs) { real_user->set_attrs(_attrs); }

    RGWUserInfo& get_info() { return real_user->get_info(); } //Changed for Tracer -Daniel P

    int list_buckets(const DoutPrefixProvider *dpp, const std::string& marker, const std::string& end_marker,
        uint64_t max, bool need_stats, BucketList& buckets, optional_yield y) override;
      
    virtual int create_bucket(const DoutPrefixProvider* dpp,
        const rgw_bucket& b,
        const std::string& zonegroup_id,
        rgw_placement_rule& placement_rule,
        std::string& swift_ver_location,
        const RGWQuotaInfo* pquota_info,
        const RGWAccessControlPolicy& policy,
        Attrs& attrs,
        RGWBucketInfo& info,
        obj_version& ep_objv,
        bool exclusive,
        bool obj_lock_enabled,
        bool* existed,
        req_info& req_info,
        std::unique_ptr<Bucket>* bucket,
        optional_yield y) override;

    virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int read_stats(const DoutPrefixProvider *dpp,
        optional_yield y, RGWStorageStats* stats,
        ceph::real_time *last_stats_sync = nullptr,
        ceph::real_time *last_stats_update = nullptr) override;
    virtual int read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb) override;
    virtual int complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
    virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
        bool* is_truncated, RGWUsageIter& usage_iter,
        std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
    virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;

    /* Placeholders */
    virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y) override;
    virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info = nullptr) override;
    virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;

    friend class D4NBucket;
  };

  class D4NBucket : public Bucket {
    private:
      D4NFilter *trace;
      std::unique_ptr<Bucket> real_bucket;

    public:
      /*for get_bucket type 1*/
      D4NBucket(D4NFilter *_st, const rgw_bucket& _b, std::unique_ptr<Bucket> * _rb, User * _u)
      : Bucket(_b, _u),
        trace(_st),
        real_bucket(std::move(*_rb)) {}

      D4NBucket(D4NFilter *_st, const rgw_bucket& _b, User * _u, std::unique_ptr<Bucket>& _rb)
      : Bucket(_b, _u),
        trace(_st),
        real_bucket(std::move(_rb)) {}

      D4NBucket(D4NFilter *_st, const rgw_bucket& _b, User * _u)
      : Bucket(_b, _u),
        trace(_st) {}

      D4NBucket(D4NFilter *_st, const RGWBucketInfo& _i, std::unique_ptr<Bucket> * _rb)
      : Bucket(_i),
        trace(_st),
        real_bucket(std::move(* _rb)) {}

      D4NBucket(D4NFilter *_st, const RGWBucketInfo& _i, User* _u)
      : Bucket(_i, _u),
        trace(_st) {}

      D4NBucket(D4NBucket &_b, std::unique_ptr<Bucket> _rb) 
      : trace(_b.trace),
        real_bucket(std::move(_rb)) {}
      
      D4NBucket(D4NFilter *_st)
      : trace(_st) {}

      D4NBucket(D4NFilter *_st, User* _u)
      : Bucket(_u),
        trace(_st) {}

      D4NBucket(D4NFilter *_st, const rgw_bucket& _b)
      : Bucket(_b),
        trace(_st) {}

      D4NBucket(D4NFilter *_st, const RGWBucketEnt& _e)
      : Bucket(_e),
        trace(_st) {}

      D4NBucket(D4NFilter *_st, const RGWBucketInfo& _i)
      : Bucket(_i),
        trace(_st) {}

      D4NBucket(D4NFilter *_st, const RGWBucketEnt& _e, User* _u)
      : Bucket(_e, _u),
        trace(_st) {}

      ~D4NBucket() { }

      std::unique_ptr<Bucket>* get_real_bucket() { return &real_bucket; }

      /** Check if this instantiation is empty */
      bool empty() const { return real_bucket->empty(); } //changed for D4NFilter -Daniel P
      /** Get the cached name of this bucket */
      const std::string& get_name() const { return real_bucket->get_name(); } //changed for D4NFilter -Daniel P
      /** Get the cached tenant of this bucket */
      const std::string& get_tenant() const { return real_bucket->get_tenant(); } //changed for D4NFilter -Daniel P
      /** Get the cached marker of this bucket */
      const std::string& get_marker() const { return real_bucket->get_marker(); } //changed for D4NFilter -Daniel P
      /** Get the cached ID of this bucket */
      const std::string& get_bucket_id() const { return real_bucket->get_bucket_id(); } //changed for D4NFilter -Daniel P
      /** Get the cached size of this bucket */
      size_t get_size() const { return real_bucket->get_size(); } //changed for D4NFilter -Daniel P
      /** Get the cached rounded size of this bucket */
      size_t get_size_rounded() const { return real_bucket->get_size_rounded(); } //changed for D4NFilter -Daniel P
      /** Get the cached object count of this bucket */
      uint64_t get_count() const { return real_bucket->get_count(); } //changed for D4NFilter -Daniel P
      /** Get the cached placement rule of this bucket */
      rgw_placement_rule& get_placement_rule() { return real_bucket->get_placement_rule(); } //temporary - Daniel P
      /** Get the cached creation time of this bucket */
      ceph::real_time& get_creation_time() { return real_bucket->get_creation_time(); } //changed for D4NFilter -Daniel P
      /** Get the cached modification time of this bucket */
      ceph::real_time& get_modification_time() { return real_bucket->get_modification_time(); } //changed for D4NFilter -Daniel P
      /** Get the cached version of this bucket */
      obj_version& get_version() { return real_bucket->get_version(); } //changed for D4NFilter -Daniel P
      /** Set the cached version of this bucket */
      void set_version(obj_version &ver) { real_bucket->set_version(ver); } //changed for D4NFilter -Daniel P
      /** Check if this bucket is versioned */
      bool versioned() { return real_bucket->versioned(); } //changed for D4NFilter -Daniel P
      /** Check if this bucket has versioning enabled */
      bool versioning_enabled() { return real_bucket->versioning_enabled(); } //changed for D4NFilter -Daniel P

      // XXXX hack
      virtual void set_owner(rgw::sal::User* _owner) { //changed for D4NFilter -Daniel P
        real_bucket->set_owner(_owner);
      }

      bool is_empty() { return real_bucket->is_empty(); }

      /* dang - This is temporary, until the API is completed */
      rgw_bucket& get_key() { return real_bucket->get_key(); } //changed for D4NFilter -Daniel P
      RGWBucketInfo& get_info() { return real_bucket->get_info(); } //changed for D4NFilter -Daniel P

      virtual std::unique_ptr<Bucket> clone() override {
        return std::unique_ptr<Bucket>(new D4NBucket(*this, std::move(this->real_bucket)));
      }
      virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
      virtual int list(const DoutPrefixProvider *dpp, ListParams&, int, ListResults&, optional_yield y) override;
      virtual int remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, bool forward_to_master, req_info* req_info, optional_yield y) override;
      virtual int remove_bucket_bypass_gc(int concurrent_max, bool
					keep_index_consistent,
					optional_yield y, const
					DoutPrefixProvider *dpp) override;
      virtual RGWAccessControlPolicy& get_acl(void) override { return real_bucket->get_acl(); }
      virtual int set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy& acl, optional_yield y) override;
      virtual int load_bucket(const DoutPrefixProvider *dpp, optional_yield y, bool get_stats = false) override;
      virtual int read_stats(const DoutPrefixProvider *dpp,
			   const bucket_index_layout_generation& idx_layout,
			   int shard_id, std::string* bucket_ver, std::string* master_ver,
			   std::map<RGWObjCategory, RGWStorageStats>& stats,
			   std::string* max_marker = nullptr,
			   bool* syncstopped = nullptr) override;
      virtual int read_stats_async(const DoutPrefixProvider *dpp,
				 const bucket_index_layout_generation& idx_layout,
				 int shard_id, RGWGetBucketStats_CB* ctx) override;
      virtual int sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int update_container_stats(const DoutPrefixProvider *dpp) override;
      virtual int check_bucket_shards(const DoutPrefixProvider *dpp) override;
      virtual int chown(const DoutPrefixProvider *dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker = nullptr) override;
      virtual int put_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time mtime) override;
      virtual bool is_owner(User* user) override;
      virtual int check_empty(const DoutPrefixProvider *dpp, optional_yield y) override;
      virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size, optional_yield y, bool check_size_only = false) override;
      virtual int merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& attrs, optional_yield y) override;
      virtual int try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime) override;
      virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
          bool *is_truncated, RGWUsageIter& usage_iter,
          std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
      virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;
      virtual int remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink) override;
      virtual int check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats) override;
      virtual int rebuild_index(const DoutPrefixProvider *dpp) override;
      virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) override;
      virtual int purge_instance(const DoutPrefixProvider *dpp) override;
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
				bool *is_truncated = 0) override;
      virtual int abort_multiparts(const DoutPrefixProvider* dpp,
				   CephContext* cct) override;

      int update_bucket(std::unique_ptr<Bucket>* real_bucket);


      friend class D4NFilter;
  };
  
  /*CURRENTLY UNUSED -Daniel P*/
  class TLuaScriptManager : public LuaScriptManager {
    D4NFilter* trace;

    public:
    TLuaScriptManager(D4NFilter* _s) : trace(_s)
    {
    }
    virtual ~TLuaScriptManager() = default;

    virtual int get(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) override { return -ENOENT; }
    virtual int put(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) override { return -ENOENT; }
    virtual int del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) override { return -ENOENT; }
  };

  class TOIDCProvider : public RGWOIDCProvider {
    D4NFilter* trace;
    public:
    TOIDCProvider(D4NFilter* _store) : trace(_store) {}
    ~TOIDCProvider() = default;

    virtual int store_url(const DoutPrefixProvider *dpp, const std::string& url, bool exclusive, optional_yield y) override { return 0; }
    virtual int read_url(const DoutPrefixProvider *dpp, const std::string& url, const std::string& tenant) override { return 0; }
    virtual int delete_obj(const DoutPrefixProvider *dpp, optional_yield y) override { return 0;}

    
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
  /*CURRENTLY UNUSED -Daniel P*/
  class TMultipartPart : public MultipartPart {
  protected:
    RGWUploadPartInfo info; /* XXX: info contains manifest also which is not needed */

  public:
    TMultipartPart() = default;
    virtual ~TMultipartPart() = default;

    virtual RGWUploadPartInfo& get_info() { return info; }
    virtual void set_info(const RGWUploadPartInfo& _info) { info = _info; }
    virtual uint32_t get_num() { return info.num; }
    virtual uint64_t get_size() { return info.accounted_size; }
    virtual const std::string& get_etag() { return info.etag; }
    virtual ceph::real_time& get_mtime() { return info.modified; }
  };

  /*CURRENTLY UNUSED -Daniel P*/
  class TracerMPObj {
    std::string oid; // object name
    std::string upload_id;
    std::string meta; // multipart meta object = <oid>.<upload_id>
  public:
    TracerMPObj() {}
    TracerMPObj(const std::string& _oid, const std::string& _upload_id) {
      init(_oid, _upload_id, _upload_id);
    }
    TracerMPObj(const std::string& _oid, std::optional<std::string> _upload_id) {
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

  /*CURRENTLY UNUSED -Daniel P*/
  class TracerMultipartUpload : public MultipartUpload {
    D4NFilter* trace;

    /*Intent is to remove all of these and instead have the tracer class always use or 
    * point to the versions of these variables owned by the real multipart upload class object. -Daniel P
    */
    TracerMPObj mp_obj;
    ACLOwner owner;
    ceph::real_time mtime;
    rgw_placement_rule placement;

  public:
    TracerMultipartUpload(D4NFilter* _store, Bucket* _bucket, const std::string& oid, std::optional<std::string> upload_id, ACLOwner _owner, ceph::real_time _mtime) : MultipartUpload(_bucket), trace(_store), mp_obj(oid, upload_id), owner(_owner), mtime(_mtime) {}
    virtual ~TracerMultipartUpload() = default;

    virtual const std::string& get_meta() const { return mp_obj.get_meta(); }
    virtual const std::string& get_key() const { return mp_obj.get_key(); }
    virtual const std::string& get_upload_id() const { return mp_obj.get_upload_id(); }
    virtual const ACLOwner& get_owner() const override { return owner; }
    virtual ceph::real_time& get_mtime() { return mtime; }
    virtual std::unique_ptr<rgw::sal::Object> get_meta_obj() override;
    virtual int init(const DoutPrefixProvider* dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs) override;
    virtual int list_parts(const DoutPrefixProvider* dpp, CephContext* cct,
			 int num_parts, int marker,
			 int* next_marker, bool* truncated,
			 bool assume_unsorted = false) override;
    virtual int abort(const DoutPrefixProvider* dpp, CephContext* cct) override;
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
			  std::unique_ptr<rgw::sal::Object> _head_obj,
			  const rgw_user& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) override;
  };

  /*CURRENTLY UNUSED -Daniel P*/
  class TracerMultipartPart : public MultipartPart {
  protected:
    RGWUploadPartInfo info; /* XXX: info contains manifest also which is not needed */

  public:
    TracerMultipartPart() = default;
    virtual ~TracerMultipartPart() = default;

    virtual RGWUploadPartInfo& get_info() { return info; }
    virtual void set_info(const RGWUploadPartInfo& _info) { info = _info; }
    virtual uint32_t get_num() { return info.num; }
    virtual uint64_t get_size() { return info.accounted_size; }
    virtual const std::string& get_etag() { return info.etag; }
    virtual ceph::real_time& get_mtime() { return info.modified; }

  };

  /*CURRENTLY UNUSED -Daniel P*/
  class D4NObject : public Object {
  private:
    std::unique_ptr<Object> realObject;
    D4NFilter* trace;
    RGWAccessControlPolicy acls;
    /* XXX: to be removed. Till Dan's patch comes, a placeholder
     * for RGWObjState
    */
    RGWObjState state;
    Bucket* bucket; /**< @a Bucket containing this object */
    Attrs attrs; /**< Cache of attributes for this object */
    bool delete_marker{false}; /**< True if this object has a delete marker */

  public:
    struct D4NReadOp : public ReadOp {
      private:
        std::unique_ptr<ReadOp> realReadOp;
        D4NObject* source;

      public:
        D4NReadOp(D4NObject *_source);

        virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
        virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp) override;
        virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y) override;
        virtual int get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y) override; 
    };

    struct D4NDeleteOp : public DeleteOp {
      private:
        std::unique_ptr<DeleteOp> realDeleteOp;
        D4NObject* source;

      public:
        D4NDeleteOp(D4NObject* _source);

        virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y) override;
    };

    D4NObject() = default;

    D4NObject(D4NFilter* _trace, std::unique_ptr<Object> _realObject)
      : realObject(std::move(_realObject)),
      trace(_trace) {}
   
    D4NObject(D4NFilter*_st, const rgw_obj_key& _k)
      : Object(_k),
        trace(_st),
        acls() {}

    D4NObject(D4NFilter*_st, const rgw_obj_key& _k, Bucket* _b)
      : Object(_k, _b),
        trace(_st),
        acls() {}

    D4NObject(D4NObject &_o, std::unique_ptr<Object> _ro)
    : realObject(std::move(_ro)) {}
      
    /*TODO: implement new constructor that takes a real object in - Daniel P*/

    virtual int delete_object(const DoutPrefixProvider* dpp,
        optional_yield y,
        bool prevent_versioning = false) override;
    virtual int delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate, Completions* aio,
        bool keep_index_consistent, optional_yield y) override;
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
    virtual void set_atomic() override;
    virtual void set_prefetch_data() override;
    virtual void set_compressed() override;
    virtual int transition_to_cloud(Bucket* bucket,
          rgw::sal::PlacementTier* tier,
	  rgw_bucket_dir_entry& o,
	  std::set<std::string>& cloud_targets,
	  CephContext* cct,
	  bool update_object,
	  const DoutPrefixProvider* dpp,
	  optional_yield y) override;
    virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state, optional_yield y, bool follow_olh = true) override;
    virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y) override;
    virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj = NULL) override;
    virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int delete_obj_attrs(const DoutPrefixProvider* dpp,  const char* attr_name, optional_yield y) override;
    virtual bool is_expired() override;
    virtual void gen_rand_obj_instance_name() override;
    virtual std::unique_ptr<Object> clone() override {
      return std::make_unique<D4NObject>(*this); // Correct implementation? -Sam
      //return std::unique_ptr<Object>(new D4NObject(*this, std::move(this->realObject)));
    }

    virtual std::unique_ptr<MPSerializer> get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name) override;
    virtual int transition (
          Bucket* bucket,
          const rgw_placement_rule& placement_rule,
          const real_time& mtime,
          uint64_t olh_epoch,
          const DoutPrefixProvider* dpp,
          optional_yield y) override;
    virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
    virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f) override;

    /* Swift versioning */
    virtual int swift_versioning_restore(
          bool& restored,
          const DoutPrefixProvider* dpp) override;
    virtual int swift_versioning_copy(
          const DoutPrefixProvider* dpp,
          optional_yield y) override;

    /* OPs */
    virtual std::unique_ptr<ReadOp> get_read_op() override;
    virtual std::unique_ptr<DeleteOp> get_delete_op() override;

    /* OMAP */
    virtual int omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
          std::map<std::string, bufferlist> *m,
          bool* pmore, optional_yield y) override;
    virtual int omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m,
          optional_yield y) override;
    virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
          const std::set<std::string>& keys,
          Attrs* vals) override;
    virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
        bool must_exist, optional_yield y) override;
  };

  /*CURRENTLY UNUSED -Daniel P*/
  class MPTSerializer : public MPSerializer {

  public:
    MPTSerializer(const DoutPrefixProvider *dpp, D4NFilter* store, D4NObject* obj, const std::string& lock_name) {}

    virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override {return 0; }
    virtual int unlock() override { return 0;}
  };

  // GET operations for directory must be added
  class D4NAtomicWriter : public Writer {
    protected:
    /*goal is to remove as many as these variables as possible (see above classes) -Daniel P*/
    rgw::sal::D4NFilter* trace;
    rgw::sal::Object* head_obj;
    std::unique_ptr<Writer> real_writer;
    const rgw_user& owner;
    const rgw_placement_rule *ptail_placement_rule;
    uint64_t olh_epoch;
    const std::string& unique_tag;
    D4NObject obj;
    uint64_t total_data_size = 0; /* for total data being uploaded */
    bufferlist head_data;
    bufferlist tail_part_data;
    uint64_t tail_part_offset;
    uint64_t tail_part_size = 0; /* corresponds to each tail part being
                                  written to dbstore */

    public:
    D4NAtomicWriter(const DoutPrefixProvider *dpp,
	    	    optional_yield y,
		    std::unique_ptr<rgw::sal::Object> _head_obj,
		    DBStore* _store,
    		    const rgw_user& _owner,
	    	    const rgw_placement_rule *_ptail_placement_rule,
		    uint64_t _olh_epoch,
		    const std::string& _unique_tag);
    D4NAtomicWriter(D4NFilter* _trace,
	            const DoutPrefixProvider *dpp,		// Writer constructor
	    	    optional_yield y,
		    std::unique_ptr<rgw::sal::Object> _head_obj,
		    const rgw_user& _owner,
	    	    const rgw_placement_rule *_ptail_placement_rule,
		    uint64_t part_num,
		    const std::string& part_num_str);
    ~D4NAtomicWriter() = default;

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
                         optional_yield y) override;
  };

  /*CURRENTLY UNUSED -Daniel P*/
  class TracerMultipartWriter : public Writer {
  protected:
    /*goal is to remove as many of these as possible, see above classes -Daniel P*/
    rgw::sal::D4NFilter* trace;
    const rgw_user& owner;
  	const rgw_placement_rule *ptail_placement_rule;
	  uint64_t olh_epoch;
    std::unique_ptr<rgw::sal::Object> head_obj;
    std::string upload_id;
    std::string oid; /* object->name() + "." + "upload_id" + "." + part_num */
    std::unique_ptr<rgw::sal::Object> meta_obj;
    int part_num;
    std::string part_num_str;
    uint64_t total_data_size = 0; /* for total data being uploaded */
    bufferlist head_data;
    bufferlist tail_part_data;
    uint64_t tail_part_offset;
    uint64_t tail_part_size = 0; /* corresponds to each tail part being
                                  written to dbstore */

  public:
    TracerMultipartWriter(const DoutPrefixProvider *dpp,
		       optional_yield y, MultipartUpload* upload,
		       std::unique_ptr<rgw::sal::Object> _head_obj,
		       DBStore* _store,
		       const rgw_user& owner,
		       const rgw_placement_rule *ptail_placement_rule,
		       uint64_t part_num, 
		       const std::string& part_num_str);
    ~TracerMultipartWriter() = default;

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
                       optional_yield y) override;
  };

  class D4NFilter : public Store {
  private:
    Store* real_store; /* The store actually performing work*/
    TZone zone; //May be unneccessary

  public:
    RGWBlockDirectory* blk_dir; // Change to private later -Sam
    cache_block* c_blk;

    D4NFilter() : real_store(nullptr), zone(nullptr) {}
    ~D4NFilter() {
      delete real_store;
      delete blk_dir;
      delete c_blk; 
    }

    Store* get_real_store() { return real_store; }


    virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
    void initialize(Store* inputStore)
    {
        blk_dir = new RGWBlockDirectory("127.0.0.1", 6379); // change so it's not hardcoded - Sam 
        c_blk = new cache_block();
  
        this->real_store = inputStore;
    }
    virtual const std::string get_name() const override 
    {
        return real_store->get_name();
    }
    
    virtual std::unique_ptr<User> get_user(const rgw_user& u)  override;
    virtual int get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user) override;
    virtual int get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user) override;
    virtual int get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user) override;
    virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
    virtual std::string get_cluster_id(const DoutPrefixProvider* dpp, optional_yield y) override;
    virtual int get_bucket(const DoutPrefixProvider *dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
    virtual int get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket) override;
    virtual int get_bucket(const DoutPrefixProvider *dpp, User* u, const std::string& tenant, const std::string&name, std::unique_ptr<Bucket>* bucket, optional_yield y) override;
    virtual bool is_meta_master() override;
    virtual int forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version* objv,
        bufferlist& in_data, JSONParser *jp, req_info& info,
        optional_yield y) override;
    virtual Zone* get_zone() { return real_store->get_zone(); }
    virtual std::string zone_unique_id(uint64_t unique_num) override;
    virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
    virtual int cluster_stat(RGWClusterStat& stats) override;
    virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;
    virtual std::unique_ptr<Completions> get_completions(void) override;

    virtual std::unique_ptr<Notification> get_notification(
        rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s,
        rgw::notify::EventType event_type, const std::string* object_name=nullptr) override;

    virtual std::unique_ptr<Notification> get_notification(
    const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, 
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant,
    std::string& _req_id, optional_yield y) override;
    
    virtual RGWLC* get_rgwlc(void) override;
    virtual RGWCoroutinesManagerRegistry* get_cr_registry() override { return NULL; }
    virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info) override;
    virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl) override;
    virtual int register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
          const std::map<std::string, std::string>& meta) override;
    virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit) override;
    virtual void get_quota(RGWQuota& quota) override;
    virtual int set_buckets_enabled(const DoutPrefixProvider *dpp, std::vector<rgw_bucket>& buckets, bool enabled) override;
    virtual uint64_t get_new_req_id() override { return 0; }
    virtual int get_sync_policy_handler(const DoutPrefixProvider *dpp,
          std::optional<rgw_zone_id> zone,
          std::optional<rgw_bucket> bucket,
          RGWBucketSyncPolicyHandlerRef *phandler,
          optional_yield y) override;
    virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
    virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) override {}; //check this later Dan P
    virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp, const rgw_zone_id& source_zone, boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids) override { return; }
    virtual int clear_usage(const DoutPrefixProvider *dpp) override { return 0; }
    virtual int read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
          uint32_t max_entries, bool *is_truncated,
          RGWUsageIter& usage_iter,
          std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
    virtual int trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch) override;
    virtual int get_config_key_val(std::string name, bufferlist* bl) override;
    virtual int meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle) override;
    virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, std::list<std::string>& keys, bool* truncated) override;
    virtual void meta_list_keys_complete(void* handle) override;
    virtual std::string meta_get_marker(void *handle) override;
    virtual int meta_remove(const DoutPrefixProvider *dpp, std::string& metadata_key, optional_yield y) override;

    virtual const RGWSyncModuleInstanceRef& get_sync_module() { return real_store->get_sync_module(); }
    virtual std::string get_host_id() { return real_store->get_host_id(); }

    virtual std::unique_ptr<LuaScriptManager> get_lua_script_manager() override;
    virtual std::unique_ptr<RGWRole> get_role(std::string name,
          std::string tenant,
          std::string path="",
          std::string trust_policy="",
          std::string max_session_duration_str="",
          std::multimap<std::string,std::string> tags={}) override;
    virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
    virtual int get_roles(const DoutPrefixProvider *dpp,
          optional_yield y,
          const std::string& path_prefix,
          const std::string& tenant,
          std::vector<std::unique_ptr<RGWRole>>& roles) override;
    virtual std::unique_ptr<RGWOIDCProvider> get_oidc_provider() override;
    virtual int get_oidc_providers(const DoutPrefixProvider *dpp,
          const std::string& tenant,
          std::vector<std::unique_ptr<RGWOIDCProvider>>& providers) override;
    virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) override;
    virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;

    virtual const std::string& get_compression_type(const rgw_placement_rule& rule) override;
    virtual bool valid_placement(const rgw_placement_rule& rule) override;
    virtual void finalize(void) override;

    virtual CephContext *ctx(void) override {
        return real_store->ctx();
    }

    virtual const std::string& get_luarocks_path() const override {
        return real_store->get_luarocks_path();
    }

    virtual void set_luarocks_path(const std::string& path) override {
        real_store->set_luarocks_path(path);
      }
  };

}} //namespace rgw::sal
