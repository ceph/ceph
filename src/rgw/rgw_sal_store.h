// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include "rgw_sal.h"

namespace rgw { namespace sal {

class StoreDriver : public Driver {
  public:
    StoreDriver() {}
    virtual ~StoreDriver() = default;

    virtual uint64_t get_new_req_id() override {
      return ceph::util::generate_random_number<uint64_t>();
    }

    int read_topics(const std::string& tenant, rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) override {return -EOPNOTSUPP;}
    int write_topics(const std::string& tenant, const rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
	optional_yield y, const DoutPrefixProvider *dpp) override {return -ENOENT;}
    int remove_topics(const std::string& tenant, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) override {return -ENOENT;}
    int read_topic_v2(const std::string& topic_name,
                      const std::string& tenant,
                      rgw_pubsub_topic& topic,
                      RGWObjVersionTracker* objv_tracker,
                      optional_yield y,
                      const DoutPrefixProvider* dpp) override {
      return -EOPNOTSUPP;
    }
    int write_topic_v2(const rgw_pubsub_topic& topic,
                       RGWObjVersionTracker* objv_tracker,
                       optional_yield y,
                       const DoutPrefixProvider* dpp) override {
      return -EOPNOTSUPP;
    }
    int remove_topic_v2(const std::string& topic_name,
                        const std::string& tenant,
                        RGWObjVersionTracker* objv_tracker,
                        optional_yield y,
                        const DoutPrefixProvider* dpp) override {
      return -EOPNOTSUPP;
    }
    int update_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                    const std::string& bucket_key,
                                    bool add_mapping,
                                    optional_yield y,
                                    const DoutPrefixProvider* dpp) override {
      return -EOPNOTSUPP;
    }
    int remove_bucket_mapping_from_topics(
        const rgw_pubsub_bucket_topics& bucket_topics,
        const std::string& bucket_key,
        optional_yield y,
        const DoutPrefixProvider* dpp) override {
      return -EOPNOTSUPP;
    }
    int get_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                 std::set<std::string>& bucket_keys,
                                 optional_yield y,
                                 const DoutPrefixProvider* dpp) override {
      return -EOPNOTSUPP;
    }
    int delete_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                    optional_yield y,
                                    const DoutPrefixProvider* dpp) override {
      return -EOPNOTSUPP;
    }
};

class StoreUser : public User {
  protected:
    RGWUserInfo info;
    RGWObjVersionTracker objv_tracker;
    Attrs attrs;

  public:
    StoreUser() : info() {}
    StoreUser(const rgw_user& _u) : info() { info.user_id = _u; }
    StoreUser(const RGWUserInfo& _i) : info(_i) {}
    StoreUser(StoreUser& _o) = default;
    virtual ~StoreUser() = default;

    virtual std::string& get_display_name() override { return info.display_name; }
    virtual const std::string& get_tenant() override { return info.user_id.tenant; }
    virtual void set_tenant(std::string& _t) override { info.user_id.tenant = _t; }
    virtual const std::string& get_ns() override { return info.user_id.ns; }
    virtual void set_ns(std::string& _ns) override { info.user_id.ns = _ns; }
    virtual void clear_ns() override { info.user_id.ns.clear(); }
    virtual const rgw_user& get_id() const override { return info.user_id; }
    virtual uint32_t get_type() const override { return info.type; }
    virtual int32_t get_max_buckets() const override { return info.max_buckets; }
    virtual void set_max_buckets(int32_t _max_buckets) override {
      info.max_buckets = _max_buckets;
    }
    virtual const RGWUserCaps& get_caps() const override { return info.caps; }
    virtual RGWObjVersionTracker& get_version_tracker() override { return objv_tracker; }
    virtual Attrs& get_attrs() override { return attrs; }
    virtual void set_attrs(Attrs& _attrs) override { attrs = _attrs; }
    virtual bool empty() const override { return info.user_id.id.empty(); }
    virtual RGWUserInfo& get_info() override { return info; }
    virtual void set_info(RGWQuotaInfo& _quota) override {
      info.quota.user_quota.max_size = _quota.max_size;
      info.quota.user_quota.max_objects = _quota.max_objects;
    }

    virtual void print(std::ostream& out) const override { out << info.user_id; }

    friend class StoreBucket;
};

class StoreBucket : public Bucket {
  protected:
    RGWBucketInfo info;
    Attrs attrs;
    obj_version bucket_version;
    ceph::real_time mtime;

  public:

    StoreBucket() = default;
    StoreBucket(const rgw_bucket& b) { info.bucket = b; }
    StoreBucket(const RGWBucketInfo& i) : info(i) {}
    virtual ~StoreBucket() = default;

    virtual Attrs& get_attrs(void) override { return attrs; }
    virtual int set_attrs(Attrs a) override { attrs = a; return 0; }
    virtual const rgw_user& get_owner() const override { return info.owner; };
    virtual bool empty() const override { return info.bucket.name.empty(); }
    virtual const std::string& get_name() const override { return info.bucket.name; }
    virtual const std::string& get_tenant() const override { return info.bucket.tenant; }
    virtual const std::string& get_marker() const override { return info.bucket.marker; }
    virtual const std::string& get_bucket_id() const override { return info.bucket.bucket_id; }
    virtual rgw_placement_rule& get_placement_rule() override { return info.placement_rule; }
    virtual ceph::real_time& get_creation_time() override { return info.creation_time; }
    virtual ceph::real_time& get_modification_time() override { return mtime; }
    virtual obj_version& get_version() override { return bucket_version; }
    virtual void set_version(obj_version &ver) override { bucket_version = ver; }
    virtual bool versioned() override { return info.versioned(); }
    virtual bool versioning_enabled() override { return info.versioning_enabled(); }
    virtual rgw_bucket& get_key() override { return info.bucket; }
    virtual RGWBucketInfo& get_info() override { return info; }
    virtual void print(std::ostream& out) const override { out << info.bucket; }
    virtual bool operator==(const Bucket& b) const override {
      if (typeid(*this) != typeid(b)) {
	return false;
      }
      const StoreBucket& sb = dynamic_cast<const StoreBucket&>(b);

      return (info.bucket.tenant == sb.info.bucket.tenant) &&
	     (info.bucket.name == sb.info.bucket.name) &&
	     (info.bucket.bucket_id == sb.info.bucket.bucket_id);
    }
    virtual bool operator!=(const Bucket& b) const override {
      if (typeid(*this) != typeid(b)) {
	return false;
      }
      const StoreBucket& sb = dynamic_cast<const StoreBucket&>(b);

      return (info.bucket.tenant != sb.info.bucket.tenant) ||
	     (info.bucket.name != sb.info.bucket.name) ||
	     (info.bucket.bucket_id != sb.info.bucket.bucket_id);
    }

    int read_topics(rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker, 
        optional_yield y, const DoutPrefixProvider *dpp) override {return 0;}
    int write_topics(const rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker, 
        optional_yield y, const DoutPrefixProvider *dpp) override {return 0;}
    int remove_topics(RGWObjVersionTracker* objv_tracker, 
        optional_yield y, const DoutPrefixProvider *dpp) override {return 0;}

    friend class BucketList;
};

class StoreObject : public Object {
  protected:
    RGWObjState state;
    Bucket* bucket = nullptr;
    bool delete_marker{false};

  public:
    StoreObject() = default;
    StoreObject(const rgw_obj_key& _k)
    { state.obj.key = _k; }
    StoreObject(const rgw_obj_key& _k, Bucket* _b)
      : bucket(_b)
    { state.obj.init(_b->get_key(), _k); }
    StoreObject(const StoreObject& _o) = default;

    virtual ~StoreObject() = default;

    virtual void set_atomic() override { state.is_atomic = true; }
    virtual bool is_atomic() override { return state.is_atomic; }
    virtual void set_prefetch_data() override { state.prefetch_data = true; }
    virtual bool is_prefetch_data() override { return state.prefetch_data; }
    virtual void set_compressed() override { state.compressed = true; }
    virtual bool is_compressed() override { return state.compressed; }
    virtual void invalidate() override {
      rgw_obj obj = state.obj;
      bool is_atomic = state.is_atomic;
      bool prefetch_data = state.prefetch_data;
      bool compressed = state.compressed;

      state = RGWObjState();
      state.obj = obj;
      state.is_atomic = is_atomic;
      state.prefetch_data = prefetch_data;
      state.compressed = compressed;
    }

    virtual bool empty() const override { return state.obj.empty(); }
    virtual const std::string &get_name() const override { return state.obj.key.name; }
    virtual void set_obj_state(RGWObjState& _state) override {
      state = _state;
    }
    virtual Attrs& get_attrs(void) override { return state.attrset; }
    virtual const Attrs& get_attrs(void) const override { return state.attrset; }
    virtual int set_attrs(Attrs a) override { state.attrset = a; state.has_attrs = true; return 0; }
    virtual bool has_attrs(void) override { return state.has_attrs; }
    virtual ceph::real_time get_mtime(void) const override { return state.mtime; }
    virtual uint64_t get_obj_size(void) const override { return state.size; }
    virtual Bucket* get_bucket(void) const override { return bucket; }
    virtual void set_bucket(Bucket* b) override { bucket = b; state.obj.bucket = b->get_key(); }
    virtual std::string get_hash_source(void) override { return state.obj.index_hash_source; }
    virtual void set_hash_source(std::string s) override { state.obj.index_hash_source = s; }
    virtual std::string get_oid(void) const override { return state.obj.key.get_oid(); }
    virtual bool get_delete_marker(void) override { return delete_marker; }
    virtual bool get_in_extra_data(void) override { return state.obj.is_in_extra_data(); }
    virtual void set_in_extra_data(bool i) override { state.obj.set_in_extra_data(i); }
    int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end);
    virtual void set_obj_size(uint64_t s) override { state.size = s; }
    virtual void set_name(const std::string& n) override { state.obj.key = n; }
    virtual void set_key(const rgw_obj_key& k) override { state.obj.key = k; }
    virtual rgw_obj get_obj(void) const override { return state.obj; }
    virtual rgw_obj_key& get_key() override { return state.obj.key; }
    virtual void set_instance(const std::string &i) override { state.obj.key.set_instance(i); }
    virtual const std::string &get_instance() const override { return state.obj.key.instance; }
    virtual bool have_instance(void) override { return state.obj.key.have_instance(); }
    virtual void clear_instance() override { state.obj.key.instance.clear(); }
    virtual int transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y) override {
      /* Return failure here, so stores which don't transition to cloud will
       * work with lifecycle */
      return -1;
    }

    virtual int get_torrent_info(const DoutPrefixProvider* dpp,
                                 optional_yield y, bufferlist& bl) override {
      const auto& attrs = get_attrs();
      if (auto i = attrs.find(RGW_ATTR_TORRENT); i != attrs.end()) {
        bl = i->second;
        return 0;
      }
      return -ENOENT;
    }

    virtual void print(std::ostream& out) const override {
      if (bucket)
	out << bucket << ":";
      out << state.obj.key;
    }
};

class StoreMultipartPart : public MultipartPart {
  protected:
    std::string oid;
public:
  StoreMultipartPart() = default;
  virtual ~StoreMultipartPart() = default;
};

class StoreMultipartUpload : public MultipartUpload {
protected:
  Bucket* bucket;
  std::map<uint32_t, std::unique_ptr<MultipartPart>> parts;
  jspan_context trace_ctx{false, false};
public:
  StoreMultipartUpload(Bucket* _bucket) : bucket(_bucket) {}
  virtual ~StoreMultipartUpload() = default;

  virtual std::map<uint32_t, std::unique_ptr<MultipartPart>>& get_parts() override { return parts; }

  virtual const jspan_context& get_trace() override { return trace_ctx; }

  virtual void print(std::ostream& out) const override {
    out << get_meta();
    if (!get_upload_id().empty())
      out << ":" << get_upload_id();
  }
};

class StoreMPSerializer : public MPSerializer {
protected:
  bool locked;
  std::string oid;
public:
  StoreMPSerializer() : locked(false) {}
  StoreMPSerializer(std::string _oid) : locked(false), oid(_oid) {}
  virtual ~StoreMPSerializer() = default;

  virtual void clear_locked() override {
    locked = false;
  }
  virtual bool is_locked() override { return locked; }

  virtual void print(std::ostream& out) const override { out << oid; }
};

class StoreLCSerializer : public LCSerializer {
protected:
  std::string oid;
public:
  StoreLCSerializer() {}
  StoreLCSerializer(std::string _oid) : oid(_oid) {}
  virtual ~StoreLCSerializer() = default;

  virtual void print(std::ostream& out) const override { out << oid; }
};

class StoreLifecycle : public Lifecycle {
public:
  struct StoreLCHead : LCHead {
    time_t start_date{0};
    time_t shard_rollover_date{0};
    std::string marker;

    StoreLCHead() = default;
    StoreLCHead(time_t _start_date, time_t _rollover_date, std::string& _marker) : start_date(_start_date), shard_rollover_date(_rollover_date), marker(_marker) {}

    StoreLCHead& operator=(LCHead& _h) {
      start_date = _h.get_start_date();
      shard_rollover_date = _h.get_shard_rollover_date();
      marker = _h.get_marker();

      return *this;
    }

    virtual time_t& get_start_date() override { return start_date; }
    virtual void set_start_date(time_t _date) override { start_date = _date; }
    virtual std::string& get_marker() override { return marker; }
    virtual void set_marker(const std::string& _marker) override { marker = _marker; }
    virtual time_t& get_shard_rollover_date() override { return shard_rollover_date; }
    virtual void set_shard_rollover_date(time_t _date) override { shard_rollover_date = _date; }
  };

  struct StoreLCEntry : LCEntry {
    std::string bucket;
    std::string oid;
    uint64_t start_time{0};
    uint32_t status{0};

    StoreLCEntry() = default;
    StoreLCEntry(std::string& _bucket, uint64_t _time, uint32_t _status) : bucket(_bucket), start_time(_time), status(_status) {}
    StoreLCEntry(std::string& _bucket, std::string _oid, uint64_t _time, uint32_t _status) : bucket(_bucket), oid(_oid), start_time(_time), status(_status) {}
    StoreLCEntry(const StoreLCEntry& _e) = default;

    StoreLCEntry& operator=(LCEntry& _e) {
      bucket = _e.get_bucket();
      oid = _e.get_oid();
      start_time = _e.get_start_time();
      status = _e.get_status();

      return *this;
    }

    virtual std::string& get_bucket() override { return bucket; }
    virtual void set_bucket(const std::string& _bucket) override { bucket = _bucket; }
    virtual std::string& get_oid() override { return oid; }
    virtual void set_oid(const std::string& _oid) override { oid = _oid; }
    virtual uint64_t get_start_time() override { return start_time; }
    virtual void set_start_time(uint64_t _time) override { start_time = _time; }
    virtual uint32_t get_status() override { return status; }
    virtual void set_status(uint32_t _status) override { status = _status; }
    virtual void print(std::ostream& out) const override {
      out << bucket << ":" << oid << ":" << start_time << ":" << status;
    }
  };

  StoreLifecycle() = default;
  virtual ~StoreLifecycle() = default;

  virtual std::unique_ptr<LCEntry> get_entry() override {
      return std::make_unique<StoreLCEntry>();
  }
  using Lifecycle::get_entry;
};

class StoreNotification : public Notification {
protected:
  Object* obj;
  Object* src_obj;
  rgw::notify::EventType event_type;

  public:
    StoreNotification(Object* _obj, Object* _src_obj, rgw::notify::EventType _type)
      : obj(_obj), src_obj(_src_obj), event_type(_type)
    {}

    virtual ~StoreNotification() = default;
};

class StoreWriter : public Writer {
protected:
  const DoutPrefixProvider* dpp;

public:
  StoreWriter(const DoutPrefixProvider *_dpp, optional_yield y) : dpp(_dpp) {}
  virtual ~StoreWriter() = default;

};

class StorePlacementTier : public PlacementTier {
public:
  virtual ~StorePlacementTier() = default;
};

class StoreZoneGroup : public ZoneGroup {
public:
  virtual ~StoreZoneGroup() = default;
};

class StoreZone : public Zone {
  public:
    virtual ~StoreZone() = default;
};

class StoreLuaManager : public LuaManager {
protected:
  std::string _luarocks_path;
public:
  const std::string& luarocks_path() const override {
    return _luarocks_path;
  }
  void set_luarocks_path(const std::string& path) override {
    _luarocks_path = path;
  }
  StoreLuaManager() = default;
  StoreLuaManager(const std::string& __luarocks_path) :
    _luarocks_path(__luarocks_path) {}
  virtual ~StoreLuaManager() = default;
};

} } // namespace rgw::sal
