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
#include "rgw_role.h"

namespace rgw { namespace sal {

class FilterPlacementTier : public PlacementTier {
protected:
  std::unique_ptr<PlacementTier> next;

public:
  FilterPlacementTier(std::unique_ptr<PlacementTier> _next) : next(std::move(_next)) {}
  virtual ~FilterPlacementTier() = default;

  virtual const std::string& get_tier_type() override { return next->get_tier_type(); }
  virtual const std::string& get_storage_class() override { return next->get_storage_class(); }
  virtual bool retain_head_object() override { return next->retain_head_object(); }

  /* Internal to Filters */
  PlacementTier* get_next() { return next.get(); }
};

class FilterZoneGroup : public ZoneGroup {
protected:
  std::unique_ptr<ZoneGroup> next;

public:
  FilterZoneGroup(std::unique_ptr<ZoneGroup> _next) : next(std::move(_next)) {}
  virtual ~FilterZoneGroup() = default;
  virtual const std::string& get_id() const override
    { return next->get_id(); }
  virtual const std::string& get_name() const override
    { return next->get_name(); }
  virtual int equals(const std::string& other_zonegroup) const override
    { return next->equals(other_zonegroup); }
  virtual bool placement_target_exists(std::string& target) const override
    { return next->placement_target_exists(target); }
  virtual bool is_master_zonegroup() const override
    { return next->is_master_zonegroup(); }
  virtual const std::string& get_api_name() const override
    { return next->get_api_name(); }
  virtual void get_placement_target_names(std::set<std::string>& names) const override
    { next->get_placement_target_names(names); }
  virtual const std::string& get_default_placement_name() const override
    { return next->get_default_placement_name(); }
  virtual int get_hostnames(std::list<std::string>& names) const override
    { return next->get_hostnames(names); }
  virtual int get_s3website_hostnames(std::list<std::string>& names) const override
    { return next->get_s3website_hostnames(names); }
  virtual int get_zone_count() const override
    { return next->get_zone_count(); }
  virtual int get_placement_tier(const rgw_placement_rule& rule, std::unique_ptr<PlacementTier>* tier) override;
  virtual int get_zone_by_id(const std::string& id, std::unique_ptr<Zone>* zone) override;
  virtual int get_zone_by_name(const std::string& name, std::unique_ptr<Zone>* zone) override;
  virtual int list_zones(std::list<std::string>& zone_ids) override
    { return next->list_zones(zone_ids); }
  virtual std::unique_ptr<ZoneGroup> clone() override {
    std::unique_ptr<ZoneGroup> nzg = next->clone();
    return std::make_unique<FilterZoneGroup>(std::move(nzg));
  }
};

class FilterZone : public Zone {
protected:
  std::unique_ptr<Zone> next;
private:
  std::unique_ptr<ZoneGroup> group;

public:
  FilterZone(std::unique_ptr<Zone> _next) : next(std::move(_next))
  {
    group = std::make_unique<FilterZoneGroup>(next->get_zonegroup().clone());
  }
  virtual ~FilterZone() = default;

  virtual std::unique_ptr<Zone> clone() override {
    std::unique_ptr<Zone> nz = next->clone();
    return std::make_unique<FilterZone>(std::move(nz));
  }
  virtual ZoneGroup& get_zonegroup() override {
      return *group.get();
  }
  virtual const std::string& get_id() override {
      return next->get_id();
  }
  virtual const std::string& get_name() const override {
      return next->get_name();
  }
  virtual bool is_writeable() override {
      return next->is_writeable();
  }
  virtual bool get_redirect_endpoint(std::string* endpoint) override {
      return next->get_redirect_endpoint(endpoint);
  }
  virtual bool has_zonegroup_api(const std::string& api) const override {
      return next->has_zonegroup_api(api);
  }
  virtual const std::string& get_current_period_id() override {
      return next->get_current_period_id();
  }
  virtual const RGWAccessKey& get_system_key() override {
      return next->get_system_key();
  }
  virtual const std::string& get_realm_name() override {
      return next->get_realm_name();
  }
  virtual const std::string& get_realm_id() override {
      return next->get_realm_id();
  }
  virtual const std::string_view get_tier_type() override {
      return next->get_tier_type();
  }
  virtual RGWBucketSyncPolicyHandlerRef get_sync_policy_handler() override {
    return next->get_sync_policy_handler();
  }
};

class FilterDriver : public Driver {
protected:
  Driver* next;
private:
  std::unique_ptr<FilterZone> zone;

public:
  FilterDriver(Driver* _next) : next(_next) {}
  virtual ~FilterDriver() = default;

  virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
  virtual const std::string get_name() const override;
  virtual std::string get_cluster_id(const DoutPrefixProvider* dpp,
				     optional_yield y) override;
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

  int load_account_by_id(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view id,
                         RGWAccountInfo& info,
                         Attrs& attrs,
                         RGWObjVersionTracker& objv) override;
  int load_account_by_name(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           std::string_view tenant,
                           std::string_view name,
                           RGWAccountInfo& info,
                           Attrs& attrs,
                           RGWObjVersionTracker& objv) override;
  int load_account_by_email(const DoutPrefixProvider* dpp,
                            optional_yield y,
                            std::string_view email,
                            RGWAccountInfo& info,
                            Attrs& attrs,
                            RGWObjVersionTracker& objv) override;
  int store_account(const DoutPrefixProvider* dpp,
                    optional_yield y, bool exclusive,
                    const RGWAccountInfo& info,
                    const RGWAccountInfo* old_info,
                    const Attrs& attrs,
                    RGWObjVersionTracker& objv) override;
  int delete_account(const DoutPrefixProvider* dpp,
                     optional_yield y,
                     const RGWAccountInfo& info,
                     RGWObjVersionTracker& objv) override;

  int load_stats(const DoutPrefixProvider* dpp,
                 optional_yield y,
                 const rgw_owner& owner,
                 RGWStorageStats& stats,
                 ceph::real_time& last_synced,
                 ceph::real_time& last_updated) override;
  int load_stats_async(const DoutPrefixProvider* dpp,
                       const rgw_owner& owner,
                       boost::intrusive_ptr<ReadStatsCB> cb) override;
  int reset_stats(const DoutPrefixProvider *dpp,
                  optional_yield y,
                  const rgw_owner& owner) override;
  int complete_flush_stats(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           const rgw_owner& owner) override;
  int load_owner_by_email(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          std::string_view email,
                          rgw_owner& owner) override;
  int count_account_roles(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          std::string_view account_id,
                          uint32_t& count) override;
  int list_account_roles(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view account_id,
                         std::string_view path_prefix,
                         std::string_view marker,
                         uint32_t max_items,
                         RoleList& listing) override;
  int load_account_user_by_name(const DoutPrefixProvider* dpp,
                                optional_yield y,
                                std::string_view account_id,
                                std::string_view tenant,
                                std::string_view username,
                                std::unique_ptr<User>* user) override;
  int count_account_users(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          std::string_view account_id,
                          uint32_t& count) override;
  int list_account_users(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view account_id,
                         std::string_view tenant,
                         std::string_view path_prefix,
                         std::string_view marker,
                         uint32_t max_items,
                         UserList& listing) override;

  int load_group_by_id(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       std::string_view id,
                       RGWGroupInfo& info, Attrs& attrs,
                       RGWObjVersionTracker& objv) override;
  int load_group_by_name(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view account_id,
                         std::string_view name,
                         RGWGroupInfo& info, Attrs& attrs,
                         RGWObjVersionTracker& objv) override;
  int store_group(const DoutPrefixProvider* dpp, optional_yield y,
                  const RGWGroupInfo& info, const Attrs& attrs,
                  RGWObjVersionTracker& objv, bool exclusive,
                  const RGWGroupInfo* old_info) override;
  int remove_group(const DoutPrefixProvider* dpp, optional_yield y,
                   const RGWGroupInfo& info,
                   RGWObjVersionTracker& objv) override;
  int list_group_users(const DoutPrefixProvider* dpp,
                       optional_yield y,
                       std::string_view tenant,
                       std::string_view id,
                       std::string_view marker,
                       uint32_t max_items,
                       UserList& listing) override;
  int count_account_groups(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           std::string_view account_id,
                           uint32_t& count) override;
  int list_account_groups(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          std::string_view account_id,
                          std::string_view path_prefix,
                          std::string_view marker,
                          uint32_t max_items,
                          GroupList& listing) override;

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  std::unique_ptr<Bucket> get_bucket(const RGWBucketInfo& i) override;
  int load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                  std::unique_ptr<Bucket>* bucket, optional_yield y) override;
  int list_buckets(const DoutPrefixProvider* dpp,
                   const rgw_owner& owner, const std::string& tenant,
                   const std::string& marker, const std::string& end_marker,
                   uint64_t max, bool need_stats, BucketList& buckets,
                   optional_yield y) override;

  virtual bool is_meta_master() override;
  virtual Zone* get_zone() override { return zone.get(); }
  virtual std::string zone_unique_id(uint64_t unique_num) override;
  virtual std::string zone_unique_trans_id(const uint64_t unique_num) override;
  virtual int get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup) override;
  virtual int list_all_zones(const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids) override {
    return next->list_all_zones(dpp, zone_ids);
  }
  virtual int cluster_stat(RGWClusterStat& stats) override;
  virtual std::unique_ptr<Lifecycle> get_lifecycle(void) override;

  virtual std::unique_ptr<Notification> get_notification(rgw::sal::Object* obj,
				 rgw::sal::Object* src_obj, struct req_state* s,
				 rgw::notify::EventType event_type, optional_yield y,
				 const std::string* object_name=nullptr) override;
  virtual std::unique_ptr<Notification> get_notification(
      const DoutPrefixProvider* dpp,
      rgw::sal::Object* obj,
      rgw::sal::Object* src_obj,
      const rgw::notify::EventTypeList& event_types,
      rgw::sal::Bucket* _bucket,
      std::string& _user_id,
      std::string& _user_tenant,
      std::string& _req_id,
      optional_yield y) override;

  int read_topics(const std::string& tenant, rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
      optional_yield y, const DoutPrefixProvider *dpp) override {
    return next->read_topics(tenant, topics, objv_tracker, y, dpp);
  }
  int stat_topics_v1(const std::string& tenant, optional_yield y, const DoutPrefixProvider *dpp) override {
    return next->stat_topics_v1(tenant, y, dpp);
  }
  int write_topics(const std::string& tenant, const rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
      optional_yield y, const DoutPrefixProvider *dpp) override {
    return next->write_topics(tenant, topics, objv_tracker, y, dpp);
  }
  int remove_topics(const std::string& tenant, RGWObjVersionTracker* objv_tracker, 
      optional_yield y, const DoutPrefixProvider *dpp) override {
    return next->remove_topics(tenant, objv_tracker, y, dpp);
  }
  int read_topic_v2(const std::string& topic_name,
                    const std::string& tenant,
                    rgw_pubsub_topic& topic,
                    RGWObjVersionTracker* objv_tracker,
                    optional_yield y,
                    const DoutPrefixProvider* dpp) override {
    return next->read_topic_v2(topic_name, tenant, topic, objv_tracker, y, dpp);
  }
  int write_topic_v2(const rgw_pubsub_topic& topic, bool exclusive,
                     RGWObjVersionTracker& objv_tracker,
                     optional_yield y,
                     const DoutPrefixProvider* dpp) override {
    return next->write_topic_v2(topic, exclusive, objv_tracker, y, dpp);
  }
  int remove_topic_v2(const std::string& topic_name,
                      const std::string& tenant,
                      RGWObjVersionTracker& objv_tracker,
                      optional_yield y,
                      const DoutPrefixProvider* dpp) override {
    return next->remove_topic_v2(topic_name, tenant, objv_tracker, y, dpp);
  }
  int list_account_topics(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          std::string_view account_id,
                          std::string_view marker,
                          uint32_t max_items,
                          TopicList& listing) override {
    return next->list_account_topics(dpp, y, account_id, marker,
                                     max_items, listing);
  }
  int add_persistent_topic(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           const std::string& topic_queue) override;
  int remove_persistent_topic(const DoutPrefixProvider* dpp,
                              optional_yield y,
                              const std::string& topic_queue) override;
  int update_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                  const std::string& bucket_key,
                                  bool add_mapping,
                                  optional_yield y,
                                  const DoutPrefixProvider* dpp) override {
    return next->update_bucket_topic_mapping(topic, bucket_key, add_mapping, y,
                                             dpp);
  }
  int remove_bucket_mapping_from_topics(
      const rgw_pubsub_bucket_topics& bucket_topics,
      const std::string& bucket_key,
      optional_yield y,
      const DoutPrefixProvider* dpp) override {
    return next->remove_bucket_mapping_from_topics(bucket_topics, bucket_key, y,
                                                   dpp);
  }
  int get_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                               std::set<std::string>& bucket_keys,
                               optional_yield y,
                               const DoutPrefixProvider* dpp) override {
    return next->get_bucket_topic_mapping(topic, bucket_keys, y, dpp);
  }
  virtual RGWLC* get_rgwlc(void) override;
  virtual RGWCoroutinesManagerRegistry* get_cr_registry() override;

  virtual int log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket,
			RGWUsageBatch>& usage_info, optional_yield y) override;
  virtual int log_op(const DoutPrefixProvider *dpp, std::string& oid,
		     bufferlist& bl) override;
  virtual int register_to_service_map(const DoutPrefixProvider *dpp, const
				      std::string& daemon_type,
				      const std::map<std::string,
				      std::string>& meta) override;
  virtual void get_quota(RGWQuota& quota) override;
  virtual void get_ratelimit(RGWRateLimitInfo& bucket_ratelimit,
			     RGWRateLimitInfo& user_ratelimit,
			     RGWRateLimitInfo& anon_ratelimit) override;
  virtual int set_buckets_enabled(const DoutPrefixProvider* dpp,
				  std::vector<rgw_bucket>& buckets,
				  bool enabled, optional_yield y) override;
  virtual uint64_t get_new_req_id() override;
  virtual int get_sync_policy_handler(const DoutPrefixProvider* dpp,
				      std::optional<rgw_zone_id> zone,
				      std::optional<rgw_bucket> bucket,
				      RGWBucketSyncPolicyHandlerRef* phandler,
				      optional_yield y) override;
  virtual RGWDataSyncStatusManager* get_data_sync_manager(const rgw_zone_id& source_zone) override;
  virtual void wakeup_meta_sync_shards(std::set<int>& shard_ids) override;
  virtual void wakeup_data_sync_shards(const DoutPrefixProvider *dpp,
				       const rgw_zone_id& source_zone,
				       boost::container::flat_map<int, boost::container::flat_set<rgw_data_notify_entry>>& shard_ids) override;
  virtual int clear_usage(const DoutPrefixProvider *dpp, optional_yield y) override;
  virtual int read_all_usage(const DoutPrefixProvider *dpp,
			     uint64_t start_epoch, uint64_t end_epoch,
			     uint32_t max_entries, bool* is_truncated,
			     RGWUsageIter& usage_iter,
			     std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  virtual int trim_all_usage(const DoutPrefixProvider *dpp,
			     uint64_t start_epoch, uint64_t end_epoch, optional_yield y) override;
  virtual int get_config_key_val(std::string name, bufferlist* bl) override;
  virtual int meta_list_keys_init(const DoutPrefixProvider *dpp,
				  const std::string& section,
				  const std::string& marker,
				  void** phandle) override;
  virtual int meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle,
				  int max, std::list<std::string>& keys,
				  bool* truncated) override;
  virtual void meta_list_keys_complete(void* handle) override;
  virtual std::string meta_get_marker(void* handle) override;
  virtual int meta_remove(const DoutPrefixProvider* dpp,
			  std::string& metadata_key, optional_yield y) override;
  virtual const RGWSyncModuleInstanceRef& get_sync_module() override;
  virtual std::string get_host_id() override { return next->get_host_id(); }
  virtual std::unique_ptr<LuaManager> get_lua_manager(const std::string& luarocks_path) override;
  virtual std::unique_ptr<RGWRole> get_role(std::string name,
					    std::string tenant,
					    rgw_account_id account_id,
					    std::string path="",
					    std::string trust_policy="",
					    std::string description="",
					    std::string max_session_duration_str="",
                std::multimap<std::string,std::string> tags={}) override;
  virtual std::unique_ptr<RGWRole> get_role(std::string id) override;
  virtual std::unique_ptr<RGWRole> get_role(const RGWRoleInfo& info) override;
  virtual int list_roles(const DoutPrefixProvider *dpp,
			 optional_yield y,
			 const std::string& tenant,
			 const std::string& path_prefix,
			 const std::string& marker,
			 uint32_t max_items,
			 RoleList& listing) override;
  int store_oidc_provider(const DoutPrefixProvider* dpp,
                          optional_yield y,
                          const RGWOIDCProviderInfo& info,
                          bool exclusive) override;
  int load_oidc_provider(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view tenant,
                         std::string_view url,
                         RGWOIDCProviderInfo& info) override;
  int delete_oidc_provider(const DoutPrefixProvider* dpp,
                           optional_yield y,
                           std::string_view tenant,
                           std::string_view url) override;
  int get_oidc_providers(const DoutPrefixProvider* dpp,
                         optional_yield y,
                         std::string_view tenant,
                         std::vector<RGWOIDCProviderInfo>& providers) override;
  virtual std::unique_ptr<Writer> get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule
				  *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) override;
  virtual std::unique_ptr<Writer> get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const ACLOwner& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) override;

  virtual const std::string& get_compression_type(const rgw_placement_rule& rule) override;
  virtual bool valid_placement(const rgw_placement_rule& rule) override;

  virtual void finalize(void) override;

  virtual CephContext* ctx(void) override;

  virtual void register_admin_apis(RGWRESTMgr* mgr) override {
      return next->register_admin_apis(mgr);
  }
};

class FilterUser : public User {
protected:
  std::unique_ptr<User> next;

public:
  FilterUser(std::unique_ptr<User> _next) : next(std::move(_next)) {}
  FilterUser(FilterUser& u) : next(u.next->clone()) {};
  virtual ~FilterUser() = default;

  virtual std::unique_ptr<User> clone() override {
    return std::make_unique<FilterUser>(*this);
  }
  virtual std::string& get_display_name() override { return next->get_display_name(); }
  virtual const std::string& get_tenant() override { return next->get_tenant(); }
  virtual void set_tenant(std::string& _t) override { next->set_tenant(_t); }
  virtual const std::string& get_ns() override { return next->get_ns(); }
  virtual void set_ns(std::string& _ns) override { next->set_ns(_ns); }
  virtual void clear_ns() override { next->clear_ns(); }
  virtual const rgw_user& get_id() const override { return next->get_id(); }
  virtual uint32_t get_type() const override { return next->get_type(); }
  virtual int32_t get_max_buckets() const override { return next->get_max_buckets(); }
  virtual void set_max_buckets(int32_t _max_buckets) override { return next->set_max_buckets(_max_buckets); }
  virtual void set_info(RGWQuotaInfo& _quota) override { return next->set_info(_quota); }
  virtual const RGWUserCaps& get_caps() const override { return next->get_caps(); }
  virtual RGWObjVersionTracker& get_version_tracker() override {
    return next->get_version_tracker();
  }
  virtual Attrs& get_attrs() override { return next->get_attrs(); }
  virtual void set_attrs(Attrs& _attrs) override { next->set_attrs(_attrs); }
  virtual bool empty() const override { return next->empty(); }
  virtual int read_attrs(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs&
				    new_attrs, optional_yield y) override;
  virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			 uint64_t end_epoch, uint32_t max_entries,
			 bool* is_truncated, RGWUsageIter& usage_iter,
			 std::map<rgw_user_bucket, rgw_usage_log_entry>& usage) override;
  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			 uint64_t end_epoch, optional_yield y) override;

  virtual int load_user(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int store_user(const DoutPrefixProvider* dpp, optional_yield y, bool
			 exclusive, RGWUserInfo* old_info = nullptr) override;
  virtual int remove_user(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int verify_mfa(const std::string& mfa_str, bool* verified,
			 const DoutPrefixProvider* dpp, optional_yield y) override;
  int list_groups(const DoutPrefixProvider* dpp, optional_yield y,
                  std::string_view marker, uint32_t max_items,
                  GroupList& listing) override;

  RGWUserInfo& get_info() override { return next->get_info(); }
  virtual void print(std::ostream& out) const override { return next->print(out); }

  /* Internal to Filters */
  User* get_next() { return next.get(); }
};

class FilterBucket : public Bucket {
protected:
  std::unique_ptr<Bucket> next;

public:

  FilterBucket(std::unique_ptr<Bucket> _next) : next(std::move(_next)) {}
  virtual ~FilterBucket() = default;

  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& key) override;
  virtual int list(const DoutPrefixProvider* dpp, ListParams&, int,
		   ListResults&, optional_yield y) override;
  virtual Attrs& get_attrs(void) override { return next->get_attrs(); }
  virtual int set_attrs(Attrs a) override { return next->set_attrs(a); }
  virtual int remove(const DoutPrefixProvider* dpp, bool delete_children,
		     optional_yield y) override;
  virtual int remove_bypass_gc(int concurrent_max, bool
			       keep_index_consistent,
			       optional_yield y, const
			       DoutPrefixProvider *dpp) override;
  virtual RGWAccessControlPolicy& get_acl(void) override { return next->get_acl(); }
  virtual int set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy& acl,
		      optional_yield y) override;

  virtual int create(const DoutPrefixProvider* dpp,
		     const CreateParams& params,
		     optional_yield y) override;
  virtual int load_bucket(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int read_stats(const DoutPrefixProvider *dpp,
			 const bucket_index_layout_generation& idx_layout,
			 int shard_id, std::string* bucket_ver, std::string* master_ver,
			 std::map<RGWObjCategory, RGWStorageStats>& stats,
			 std::string* max_marker = nullptr,
			 bool* syncstopped = nullptr) override;
  virtual int read_stats_async(const DoutPrefixProvider *dpp,
			       const bucket_index_layout_generation& idx_layout,
			       int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx) override;
  int sync_owner_stats(const DoutPrefixProvider *dpp, optional_yield y,
                       RGWBucketEnt* ent) override;
  int check_bucket_shards(const DoutPrefixProvider* dpp,
                          uint64_t num_objs, optional_yield y) override;
  virtual int chown(const DoutPrefixProvider* dpp, const rgw_owner& new_owner,
		    optional_yield y) override;
  virtual int put_info(const DoutPrefixProvider* dpp, bool exclusive,
		       ceph::real_time mtime, optional_yield y) override;
  virtual const rgw_owner& get_owner() const override;
  virtual int check_empty(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual int check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota,
			  uint64_t obj_size, optional_yield y,
			  bool check_size_only = false) override;
  virtual int merge_and_store_attrs(const DoutPrefixProvider* dpp,
				    Attrs& new_attrs, optional_yield y) override;
  virtual int try_refresh_info(const DoutPrefixProvider* dpp,
			       ceph::real_time* pmtime, optional_yield y) override;
  virtual int read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			 uint64_t end_epoch, uint32_t max_entries,
			 bool* is_truncated, RGWUsageIter& usage_iter,
			 std::map<rgw_user_bucket,
			 rgw_usage_log_entry>& usage) override;
  virtual int trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch,
			 uint64_t end_epoch, optional_yield y) override;
  virtual int remove_objs_from_index(const DoutPrefixProvider *dpp,
				     std::list<rgw_obj_index_key>&
				     objs_to_unlink) override;
  virtual int check_index(const DoutPrefixProvider *dpp,
			  std::map<RGWObjCategory, RGWStorageStats>&
			  existing_stats,
			  std::map<RGWObjCategory, RGWStorageStats>&
			  calculated_stats) override;
  virtual int rebuild_index(const DoutPrefixProvider *dpp) override;
  virtual int set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout) override;
  virtual int purge_instance(const DoutPrefixProvider* dpp, optional_yield y) override;
  virtual bool empty() const override { return next->empty(); }
  virtual const std::string& get_name() const override { return next->get_name(); }
  virtual const std::string& get_tenant() const override { return next->get_tenant(); }
  virtual const std::string& get_marker() const override { return next->get_marker(); }
  virtual const std::string& get_bucket_id() const override { return next->get_bucket_id(); }
  virtual rgw_placement_rule& get_placement_rule() override { return next->get_placement_rule(); }
  virtual ceph::real_time& get_creation_time() override { return next->get_creation_time(); }
  virtual ceph::real_time& get_modification_time() override { return next->get_modification_time(); }
  virtual obj_version& get_version() override { return next->get_version(); }
  virtual void set_version(obj_version &ver) override { next->set_version(ver); }
  virtual bool versioned() override { return next->versioned(); }
  virtual bool versioning_enabled() override { return next->versioning_enabled(); }

  virtual std::unique_ptr<Bucket> clone() override {
    return std::make_unique<FilterBucket>(next->clone());
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

  int read_topics(rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* objv_tracker, 
      optional_yield y, const DoutPrefixProvider *dpp) override { 
    return next->read_topics(notifications, objv_tracker, y, dpp); 
  }
  int write_topics(const rgw_pubsub_bucket_topics& notifications, RGWObjVersionTracker* obj_tracker, 
      optional_yield y, const DoutPrefixProvider *dpp) override { 
    return next->write_topics(notifications, obj_tracker, y, dpp); 
  }
  int remove_topics(RGWObjVersionTracker* objv_tracker, 
      optional_yield y, const DoutPrefixProvider *dpp) override {
    return next->remove_topics(objv_tracker, y, dpp);
  }
  int get_logging_object_name(std::string& obj_name, 
      const std::string& prefix, 
      optional_yield y, 
      const DoutPrefixProvider *dpp,
      RGWObjVersionTracker* objv_tracker) override {
    return next->get_logging_object_name(obj_name, prefix, y, dpp, objv_tracker);
  }
  int set_logging_object_name(const std::string& obj_name, 
      const std::string& prefix, 
      optional_yield y, 
      const DoutPrefixProvider *dpp, 
      bool new_obj,
      RGWObjVersionTracker* objv_track) override {
    return next->set_logging_object_name(obj_name, prefix, y, dpp, new_obj, objv_track); 
  }
  int commit_logging_object(const std::string& obj_name, optional_yield y, const DoutPrefixProvider *dpp)override {
    return next->commit_logging_object(obj_name, y, dpp);
  }
  int write_logging_object(const std::string& obj_name, const std::string& record, optional_yield y, const DoutPrefixProvider *dpp, bool async_completion) override {
    return next->write_logging_object(obj_name, record, y, dpp, async_completion);
  }

  virtual rgw_bucket& get_key() override { return next->get_key(); }
  virtual RGWBucketInfo& get_info() override { return next->get_info(); }

  virtual void print(std::ostream& out) const override { return next->print(out); }

  virtual bool operator==(const Bucket& b) const override { return next->operator==(b); }
  virtual bool operator!=(const Bucket& b) const override { return next->operator!=(b); }

  friend class BucketList;

  /* Internal to Filters */
  Bucket* get_next() { return next.get(); }
};

class FilterObject : public Object {
protected:
  std::unique_ptr<Object> next;
private:
  Bucket* bucket{nullptr};

public:

  struct FilterReadOp : ReadOp {
    std::unique_ptr<ReadOp> next;

    FilterReadOp(std::unique_ptr<ReadOp> _next) : next(std::move(_next)) {}
    virtual ~FilterReadOp() = default;

    virtual int prepare(optional_yield y, const DoutPrefixProvider* dpp) override;
    virtual int read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y,
		     const DoutPrefixProvider* dpp) override;
    virtual int iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end,
			RGWGetDataCB* cb, optional_yield y) override;
    virtual int get_attr(const DoutPrefixProvider* dpp, const char* name,
			 bufferlist& dest, optional_yield y) override;
  };

  struct FilterDeleteOp : DeleteOp {
    std::unique_ptr<DeleteOp> next;

    FilterDeleteOp(std::unique_ptr<DeleteOp> _next) : next(std::move(_next)) {}
    virtual ~FilterDeleteOp() = default;

    virtual int delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags) override;
  };

  FilterObject(std::unique_ptr<Object> _next) : next(std::move(_next)) {}
  FilterObject(std::unique_ptr<Object> _next, Bucket* _bucket) :
			next(std::move(_next)), bucket(_bucket) {}
  FilterObject(FilterObject& _o) {
    next = _o.next->clone();
    bucket = _o.bucket;
  }
  virtual ~FilterObject() = default;

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
  virtual RGWAccessControlPolicy& get_acl(void) override;
  virtual int set_acl(const RGWAccessControlPolicy& acl) override { return next->set_acl(acl); }
  virtual void set_atomic() override { return next->set_atomic(); }
  virtual bool is_atomic() override { return next->is_atomic(); }
  virtual void set_prefetch_data() override { return next->set_prefetch_data(); }
  virtual bool is_prefetch_data() override { return next->is_prefetch_data(); }
  virtual void set_compressed() override { return next->set_compressed(); }
  virtual bool is_compressed() override { return next->is_compressed(); }
  virtual bool is_sync_completed(const DoutPrefixProvider* dpp,
    const ceph::real_time& obj_mtime) override { return next->is_sync_completed(dpp, obj_mtime); }
  virtual void invalidate() override { return next->invalidate(); }
  virtual bool empty() const override { return next->empty(); }
  virtual const std::string &get_name() const override { return next->get_name(); }

  virtual int load_obj_state(const DoutPrefixProvider *dpp, optional_yield y,
                             bool follow_olh = true) override;
  virtual int set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs,
			    Attrs* delattrs, optional_yield y, uint32_t flags) override;
  virtual int get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp,
			    rgw_obj* target_obj = NULL) override;
  virtual int modify_obj_attrs(const char* attr_name, bufferlist& attr_val,
			       optional_yield y, const DoutPrefixProvider* dpp) override;
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
			   rgw_placement_rule& placement_rule,
			   rgw_bucket_dir_entry& o,
			   CephContext* cct,
		           RGWObjTier& tier_config,
			   real_time& mtime,
			   uint64_t olh_epoch,
		           std::optional<uint64_t> days,
			   const DoutPrefixProvider* dpp,
			   optional_yield y,
		           uint32_t flags) override;
  virtual bool placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2) override;
  virtual int dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y,
			      Formatter* f) override;

  virtual Attrs& get_attrs(void) override { return next->get_attrs(); };
  virtual const Attrs& get_attrs(void) const override { return next->get_attrs(); };
  virtual int set_attrs(Attrs a) override { return next->set_attrs(a); };
  virtual bool has_attrs(void) override { return next->has_attrs(); };
  virtual bool get_attr(const std::string& name, bufferlist &dest) override { return next->get_attr(name, dest); }
  virtual ceph::real_time get_mtime(void) const override { return next->get_mtime(); };
  virtual void set_mtime(ceph::real_time& mtime) override { return next->set_mtime(mtime); }
  virtual uint64_t get_size(void) const override { return next->get_size(); };
  virtual uint64_t get_accounted_size(void) const override { return next->get_accounted_size(); };
  virtual void set_accounted_size(uint64_t size) override { return next->set_accounted_size(size); }
  virtual uint64_t get_epoch(void) const override { return next->get_epoch(); }
  virtual void set_epoch(uint64_t epoch) override { return next->set_epoch(epoch); }
  virtual uint32_t get_short_zone_id(void) const override { return next->get_short_zone_id(); }
  virtual void set_short_zone_id(uint32_t id) override { return next->set_short_zone_id(id); }
  virtual Bucket* get_bucket(void) const override { return bucket; };
  virtual void set_bucket(Bucket* b) override;
  virtual std::string get_hash_source(void) override { return next->get_hash_source(); };
  virtual void set_hash_source(std::string s) override { return next->set_hash_source(s); };
  virtual std::string get_oid(void) const override { return next->get_oid(); };
  virtual bool get_delete_marker(void) override { return next->get_delete_marker(); };
  virtual bool get_in_extra_data(void) override { return next->get_in_extra_data(); };
  virtual bool exists(void) override { return next->exists(); };
  virtual void set_in_extra_data(bool i) override { return next->set_in_extra_data(i); };
  int range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end) {
    return next->range_to_ofs(obj_size, ofs, end);
  };
  virtual void set_obj_size(uint64_t s) override { return next->set_obj_size(s); };
  virtual void set_name(const std::string& n) override { return next->set_name(n); };
  virtual void set_key(const rgw_obj_key& k) override { return next->set_key(k); };
  virtual rgw_obj get_obj(void) const override { return next->get_obj(); };
  virtual rgw_obj_key& get_key() override { return next->get_key(); }
  virtual void set_instance(const std::string &i) override { return next->set_instance(i); }
  virtual const std::string &get_instance() const override { return next->get_instance(); }
  virtual bool have_instance(void) override { return next->have_instance(); }
  virtual void clear_instance() override { return next->clear_instance(); }

  virtual int swift_versioning_restore(const ACLOwner& owner,
                                       const rgw_user& remote_user,
                                       bool& restored,
                                       const DoutPrefixProvider* dpp,
                                       optional_yield y) override;
  virtual int swift_versioning_copy(const ACLOwner& owner,
                                    const rgw_user& remote_user,
                                    const DoutPrefixProvider* dpp,
                                    optional_yield y) override;

  virtual std::unique_ptr<ReadOp> get_read_op() override;
  virtual std::unique_ptr<DeleteOp> get_delete_op() override;

  virtual int get_torrent_info(const DoutPrefixProvider* dpp,
                               optional_yield y, bufferlist& bl) override;

  virtual RGWObjVersionTracker& get_version_tracker() override { return next->get_version_tracker(); }

  virtual int omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
				    const std::string& oid,
				    const std::set<std::string>& keys,
				    Attrs* vals) override;
  virtual int omap_set_val_by_key(const DoutPrefixProvider *dpp,
				  const std::string& key, bufferlist& val,
				  bool must_exist, optional_yield y) override;
  virtual int chown(User& new_user, const DoutPrefixProvider* dpp,
		    optional_yield y) override;

  virtual std::unique_ptr<Object> clone() override {
    return std::make_unique<FilterObject>(*this);
  }

  virtual jspan_context& get_trace() { return next->get_trace(); }
  virtual void set_trace (jspan_context&& _trace_ctx) { next->set_trace(std::move(_trace_ctx)); }

  virtual void print(std::ostream& out) const override { return next->print(out); }

  /* Internal to Filters */
  Object* get_next() { return next.get(); }
};

class FilterMultipartPart : public MultipartPart {
protected:
  std::unique_ptr<MultipartPart> next;

public:
  FilterMultipartPart(std::unique_ptr<MultipartPart> _next) : next(std::move(_next)) {}
  virtual ~FilterMultipartPart() = default;

  virtual uint32_t get_num() override { return next->get_num(); }
  virtual uint64_t get_size() override { return next->get_size(); }
  virtual const std::string& get_etag() override { return next->get_etag(); }
  virtual ceph::real_time& get_mtime() override { return next->get_mtime(); }
  virtual const std::optional<rgw::cksum::Cksum>& get_cksum() {
    return next->get_cksum();
  }
};

class FilterMultipartUpload : public MultipartUpload {
protected:
  std::unique_ptr<MultipartUpload> next;
  Bucket* bucket;
  std::map<uint32_t, std::unique_ptr<MultipartPart>> parts;

public:
  FilterMultipartUpload(std::unique_ptr<MultipartUpload> _next, Bucket* _b) :
    next(std::move(_next)), bucket(_b) {}
  virtual ~FilterMultipartUpload() = default;

  virtual const std::string& get_meta() const override { return next->get_meta(); }
  virtual const std::string& get_key() const override { return next->get_key(); }
  virtual const std::string& get_upload_id() const override { return next->get_upload_id(); }
  virtual const ACLOwner& get_owner() const override { return next->get_owner(); }
  virtual ceph::real_time& get_mtime() override { return next->get_mtime(); }

  virtual std::map<uint32_t, std::unique_ptr<MultipartPart>>& get_parts() override { return parts; }

  virtual jspan_context& get_trace() override { return next->get_trace(); }

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
		       rgw_placement_rule** rule,
		       rgw::sal::Attrs* attrs = nullptr) override;

  virtual std::unique_ptr<Writer> get_writer(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  rgw::sal::Object* obj,
			  const ACLOwner& owner,
			  const rgw_placement_rule *ptail_placement_rule,
			  uint64_t part_num,
			  const std::string& part_num_str) override;
  virtual void print(std::ostream& out) const override { return next->print(out); }
};

class FilterMPSerializer : public MPSerializer {
protected:
  std::unique_ptr<MPSerializer> next;

public:
  FilterMPSerializer(std::unique_ptr<MPSerializer> _next) : next(std::move(_next)) {}
  virtual ~FilterMPSerializer() = default;

  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override;
  virtual int unlock() override { return next->unlock(); }
  virtual void clear_locked() override { next->clear_locked(); }
  virtual bool is_locked() override { return next->is_locked(); }
  virtual void print(std::ostream& out) const override { return next->print(out); }
};

class FilterLCSerializer : public LCSerializer {
protected:
  std::unique_ptr<LCSerializer> next;

public:
  FilterLCSerializer(std::unique_ptr<LCSerializer> _next) : next(std::move(_next)) {}
  virtual ~FilterLCSerializer() = default;

  virtual int try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y) override;
  virtual int unlock() override { return next->unlock(); }
  virtual void print(std::ostream& out) const override { return next->print(out); }
};

class FilterLifecycle : public Lifecycle {
protected:
  std::unique_ptr<Lifecycle> next;

public:

  FilterLifecycle(std::unique_ptr<Lifecycle> _next) : next(std::move(_next)) {}
  virtual ~FilterLifecycle() = default;

  virtual int get_entry(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& oid, const std::string& marker,
			LCEntry& entry) override;
  virtual int get_next_entry(const DoutPrefixProvider* dpp, optional_yield y,
                             const std::string& oid, const std::string& marker,
			     LCEntry& entry) override;
  virtual int set_entry(const DoutPrefixProvider* dpp, optional_yield y,
                        const std::string& oid, const LCEntry& entry) override;
  virtual int list_entries(const DoutPrefixProvider* dpp, optional_yield y,
                           const std::string& oid, const std::string& marker,
			   uint32_t max_entries,
			   std::vector<LCEntry>& entries) override;
  virtual int rm_entry(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& oid, const LCEntry& entry) override;
  virtual int get_head(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& oid, LCHead& head) override;
  virtual int put_head(const DoutPrefixProvider* dpp, optional_yield y,
                       const std::string& oid, const LCHead& head) override;
  virtual std::unique_ptr<LCSerializer> get_serializer(const std::string& lock_name,
						       const std::string& oid,
						       const std::string& cookie) override;
};

class FilterNotification : public Notification {
protected:
  std::unique_ptr<Notification> next;

public:
  FilterNotification(std::unique_ptr<Notification> _next) : next(std::move(_next)) {}

  virtual ~FilterNotification() = default;

  virtual int publish_reserve(const DoutPrefixProvider *dpp,
			      RGWObjTags* obj_tags = nullptr) override;
  virtual int publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
			     const ceph::real_time& mtime,
			     const std::string& etag,
			     const std::string& version) override;
};

class FilterWriter : public Writer {
protected:
  std::unique_ptr<Writer> next;
  Object* obj;
public:
  FilterWriter(std::unique_ptr<Writer> _next, Object* _obj) :
    next(std::move(_next)), obj(_obj) {}
  virtual ~FilterWriter() = default;

  virtual int prepare(optional_yield y) { return next->prepare(y); }
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

class FilterLuaManager : public LuaManager {
protected:
  std::unique_ptr<LuaManager> next;

public:
  FilterLuaManager(std::unique_ptr<LuaManager> _next) : next(std::move(_next)) {}
  virtual ~FilterLuaManager() = default;

  virtual int get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script) override;
  virtual int put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script) override;
  virtual int del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key) override;
  virtual int add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name) override;
  virtual int remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name) override;
  virtual int list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages) override;
  virtual int reload_packages(const DoutPrefixProvider* dpp, optional_yield y) override;
  const std::string& luarocks_path() const override;
  void set_luarocks_path(const std::string& path) override;

};

} } // namespace rgw::sal
