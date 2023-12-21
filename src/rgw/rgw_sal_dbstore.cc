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

#include <errno.h>
#include <stdlib.h>
#include <system_error>
#include <unistd.h>
#include <sstream>

#include "common/Clock.h"
#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_dbstore.h"
#include "rgw_bucket.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

namespace rgw::sal {

  int DBUser::list_buckets(const DoutPrefixProvider *dpp, const string& marker,
      const string& end_marker, uint64_t max, bool need_stats,
      BucketList &result, optional_yield y)
  {
    RGWUserBuckets ulist;
    bool is_truncated = false;

    int ret = store->getDB()->list_buckets(dpp, "", info.user_id, marker,
        end_marker, max, need_stats, &ulist, &is_truncated);
    if (ret < 0)
      return ret;

    result.buckets.clear();

    for (auto& ent : ulist.get_buckets()) {
      result.buckets.push_back(std::move(ent.second));
    }

    if (is_truncated && !result.buckets.empty()) {
      result.next_marker = result.buckets.back().bucket.name;
    } else {
      result.next_marker.clear();
    }
    return 0;
  }

  int DBBucket::create(const DoutPrefixProvider *dpp,
                       const CreateParams& params,
                       optional_yield y)
  {
    rgw_bucket key = get_key();
    key.marker = params.marker;
    key.bucket_id = params.bucket_id;

    /* XXX: We may not need to send all these params. Cleanup the unused ones */
    return store->getDB()->create_bucket(dpp, params.owner, key,
        params.zonegroup_id, params.placement_rule, params.attrs,
        params.swift_ver_location, params.quota, params.creation_time,
        &bucket_version, info, y);
  }

  int DBUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
  {
    int ret;
    ret = store->getDB()->get_user(dpp, string("user_id"), get_id().id, info, &attrs,
        &objv_tracker);
    return ret;
  }

  int DBUser::read_stats(const DoutPrefixProvider *dpp,
      optional_yield y, RGWStorageStats* stats,
      ceph::real_time *last_stats_sync,
      ceph::real_time *last_stats_update)
  {
    return 0;
  }

  /* stats - Not for first pass */
  int DBUser::read_stats_async(const DoutPrefixProvider *dpp, boost::intrusive_ptr<ReadStatsCB> cb)
  {
    return 0;
  }

  int DBUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
    return 0;
  }

  int DBUser::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
      bool *is_truncated, RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int DBUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
  {
    return 0;
  }

  int DBUser::load_user(const DoutPrefixProvider *dpp, optional_yield y)
  {
    int ret = 0;

    ret = store->getDB()->get_user(dpp, string("user_id"), get_id().id, info, &attrs,
        &objv_tracker);

    return ret;
  }
  int DBUser::merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y)
  {
    for(auto& it : new_attrs) {
  	  attrs[it.first] = it.second;
    }
    return store_user(dpp, y, false);
  }
  int DBUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
  {
    int ret = 0;

    ret = store->getDB()->store_user(dpp, info, exclusive, &attrs, &objv_tracker, old_info);

    return ret;
  }

  int DBUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
  {
    int ret = 0;

    ret = store->getDB()->remove_user(dpp, info, &objv_tracker);

    return ret;
  }

  int DBUser::verify_mfa(const std::string& mfa_str, bool* verified, const DoutPrefixProvider *dpp, optional_yield y)
  {
    *verified = false;
    return 0;
  }

  int DBBucket::remove(const DoutPrefixProvider *dpp, bool delete_children, optional_yield y)
  {
    int ret;

    ret = load_bucket(dpp, y);
    if (ret < 0)
      return ret;

    /* XXX: handle delete_children */

    if (!delete_children) {
      /* Check if there are any objects */
      rgw::sal::Bucket::ListParams params;
      params.list_versions = true;
      params.allow_unordered = true;

      rgw::sal::Bucket::ListResults results;

      results.objs.clear();

      ret = list(dpp, params, 2, results, null_yield);

      if (ret < 0) {
        ldpp_dout(dpp, 20) << __func__ << ": Bucket list objects returned " <<
        ret << dendl;
        return ret;
      }

      if (!results.objs.empty()) {
        ret = -ENOTEMPTY;
        ldpp_dout(dpp, -1) << __func__ << ": Bucket Not Empty.. returning " <<
        ret << dendl;
        return ret;
      }
    }

    ret = store->getDB()->remove_bucket(dpp, info);

    return ret;
  }

  int DBBucket::remove_bypass_gc(int concurrent_max, bool
				 keep_index_consistent,
				 optional_yield y, const
				 DoutPrefixProvider *dpp) {
    return 0;
  }

  int DBBucket::load_bucket(const DoutPrefixProvider *dpp, optional_yield y)
  {
    int ret = 0;

    ret = store->getDB()->get_bucket_info(dpp, string("name"), "", info, &attrs,
        &mtime, &bucket_version);

    return ret;
  }

  /* stats - Not for first pass */
  int DBBucket::read_stats(const DoutPrefixProvider *dpp,
      const bucket_index_layout_generation& idx_layout,
      int shard_id,
      std::string *bucket_ver, std::string *master_ver,
      std::map<RGWObjCategory, RGWStorageStats>& stats,
      std::string *max_marker, bool *syncstopped)
  {
    return 0;
  }

  int DBBucket::read_stats_async(const DoutPrefixProvider *dpp, const bucket_index_layout_generation& idx_layout, int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx)
  {
    return 0;
  }

  int DBBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y,
                                RGWBucketEnt* ent)
  {
    return 0;
  }

  int DBBucket::check_bucket_shards(const DoutPrefixProvider *dpp,
                                    uint64_t num_objs, optional_yield y)
  {
    return 0;
  }

  int DBBucket::chown(const DoutPrefixProvider *dpp, const rgw_user& new_owner, optional_yield y)
  {
    int ret;

    ret = store->getDB()->update_bucket(dpp, "owner", info, false, &new_owner, nullptr, nullptr, nullptr);
    return ret;
  }

  int DBBucket::put_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time _mtime, optional_yield y)
  {
    int ret;

    ret = store->getDB()->update_bucket(dpp, "info", info, exclusive, nullptr, nullptr, &_mtime, &info.objv_tracker);

    return ret;

  }

  int DBBucket::check_empty(const DoutPrefixProvider *dpp, optional_yield y)
  {
    /* XXX: Check if bucket contains any objects */
    return 0;
  }

  int DBBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size,
      optional_yield y, bool check_size_only)
  {
    /* Not Handled in the first pass as stats are also needed */
    return 0;
  }

  int DBBucket::merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& new_attrs, optional_yield y)
  {
    int ret = 0;

    for(auto& it : new_attrs) {
	    attrs[it.first] = it.second;
    }

    /* XXX: handle has_instance_obj like in set_bucket_instance_attrs() */

    ret = store->getDB()->update_bucket(dpp, "attrs", info, false, nullptr, &new_attrs, nullptr, &get_info().objv_tracker);

    return ret;
  }

  int DBBucket::try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime, optional_yield y)
  {
    int ret = 0;

    ret = store->getDB()->get_bucket_info(dpp, string("name"), "", info, &attrs,
        pmtime, &bucket_version);

    return ret;
  }

  /* XXX: usage and stats not supported in the first pass */
  int DBBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
      uint32_t max_entries, bool *is_truncated,
      RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int DBBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
  {
    return 0;
  }

  int DBBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
  {
    /* XXX: CHECK: Unlike RadosStore, there is no separate bucket index table.
     * Delete all the object in the list from the object table of this
     * bucket
     */
    return 0;
  }

  int DBBucket::check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
  {
    /* XXX: stats not supported yet */
    return 0;
  }

  int DBBucket::rebuild_index(const DoutPrefixProvider *dpp)
  {
    /* there is no index table in dbstore. Not applicable */
    return 0;
  }

  int DBBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
  {
    /* XXX: CHECK: set tag timeout for all the bucket objects? */
    return 0;
  }

  int DBBucket::purge_instance(const DoutPrefixProvider *dpp, optional_yield y)
  {
    /* XXX: CHECK: for dbstore only single instance supported.
     * Remove all the objects for that instance? Anything extra needed?
     */
    return 0;
  }

  int DBBucket::set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy &acl, optional_yield y)
  {
    int ret = 0;
    bufferlist aclbl;

    acls = acl;
    acl.encode(aclbl);

    Attrs attrs = get_attrs();
    attrs[RGW_ATTR_ACL] = aclbl;

    ret = store->getDB()->update_bucket(dpp, "attrs", info, false, &acl.get_owner().id, &attrs, nullptr, nullptr);

    return ret;
  }

  std::unique_ptr<Object> DBBucket::get_object(const rgw_obj_key& k)
  {
    return std::make_unique<DBObject>(this->store, k, this);
  }

  int DBBucket::list(const DoutPrefixProvider *dpp, ListParams& params, int max, ListResults& results, optional_yield y)
  {
    int ret = 0;

    results.objs.clear();

    DB::Bucket target(store->getDB(), get_info());
    DB::Bucket::List list_op(&target);

    list_op.params.prefix = params.prefix;
    list_op.params.delim = params.delim;
    list_op.params.marker = params.marker;
    list_op.params.ns = params.ns;
    list_op.params.end_marker = params.end_marker;
    list_op.params.ns = params.ns;
    list_op.params.enforce_ns = params.enforce_ns;
    list_op.params.access_list_filter = params.access_list_filter;
    list_op.params.force_check_filter = params.force_check_filter;
    list_op.params.list_versions = params.list_versions;
    list_op.params.allow_unordered = params.allow_unordered;

    results.objs.clear();
    ret = list_op.list_objects(dpp, max, &results.objs, &results.common_prefixes, &results.is_truncated);
    if (ret >= 0) {
      results.next_marker = list_op.get_next_marker();
      params.marker = results.next_marker;
    }

    return ret;
  }

  std::unique_ptr<MultipartUpload> DBBucket::get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id,
				ACLOwner owner, ceph::real_time mtime) {
    return std::make_unique<DBMultipartUpload>(this->store, this, oid, upload_id,
						std::move(owner), mtime);
  }

  int DBBucket::list_multiparts(const DoutPrefixProvider *dpp,
				const string& prefix,
				string& marker,
				const string& delim,
				const int& max_uploads,
				vector<std::unique_ptr<MultipartUpload>>& uploads,
				map<string, bool> *common_prefixes,
				bool *is_truncated, optional_yield y) {
    return 0;
  }

  int DBBucket::abort_multiparts(const DoutPrefixProvider* dpp,
				 CephContext* cct, optional_yield y) {
    return 0;
  }

  void DBStore::finalize(void)
  {
    if (dbsm)
      dbsm->destroyAllHandles();
  }

  bool DBZoneGroup::placement_target_exists(std::string& target) const {
    return !!group->placement_targets.count(target);
  }

  void DBZoneGroup::get_placement_target_names(std::set<std::string>& names) const {
    for (const auto& target : group->placement_targets) {
      names.emplace(target.second.name);
    }
  }

  ZoneGroup& DBZone::get_zonegroup()
  {
    return *zonegroup;
  }

  const RGWZoneParams& DBZone::get_rgw_params()
  {
    return *zone_params;
  }

  const std::string& DBZone::get_id()
  {
    return zone_params->get_id();
  }


  const std::string& DBZone::get_name() const
  {
    return zone_params->get_name();
  }

  bool DBZone::is_writeable()
  {
    return true;
  }

  bool DBZone::get_redirect_endpoint(std::string* endpoint)
  {
    return false;
  }

  bool DBZone::has_zonegroup_api(const std::string& api) const
  {
    if (api == "default")
      return true;

    return false;
  }

  const std::string& DBZone::get_current_period_id()
  {
    return current_period->get_id();
  }

  const RGWAccessKey& DBZone::get_system_key()
  {
    return zone_params->system_key;
  }

  const std::string& DBZone::get_realm_name()
  {
    return realm->get_name();
  }

  const std::string& DBZone::get_realm_id()
  {
    return realm->get_id();
  }

  RGWBucketSyncPolicyHandlerRef DBZone::get_sync_policy_handler()
  {
    return nullptr;
  }

  std::unique_ptr<LuaManager> DBStore::get_lua_manager(const std::string& luarocks_path)
  {
    return std::make_unique<DBLuaManager>(this);
  }

  int DBObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **pstate, optional_yield y, bool follow_olh)
  {
    RGWObjState* astate;
    DB::Object op_target(store->getDB(), get_bucket()->get_info(), get_obj());
    int ret = op_target.get_obj_state(dpp, get_bucket()->get_info(), get_obj(), follow_olh, &astate);
    if (ret < 0) {
      return ret;
    }

    /* Don't overwrite obj, atomic, or prefetch */
    rgw_obj obj = get_obj();
    bool is_atomic = state.is_atomic;
    bool prefetch_data = state.prefetch_data;

    state = *astate;
    *pstate = &state;

    state.obj = obj;
    state.is_atomic = is_atomic;
    state.prefetch_data = prefetch_data;
    return ret;
  }

  int DBObject::read_attrs(const DoutPrefixProvider* dpp, DB::Object::Read &read_op, optional_yield y, rgw_obj* target_obj)
  {
    read_op.params.attrs = &state.attrset;
    read_op.params.target_obj = target_obj;
    read_op.params.obj_size = &state.size;
    read_op.params.lastmod = &state.mtime;

    return read_op.prepare(dpp);
  }

  int DBObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y)
  {
    Attrs empty;
    DB::Object op_target(store->getDB(),
        get_bucket()->get_info(), get_obj());
    return op_target.set_attrs(dpp, setattrs ? *setattrs : empty, delattrs);
  }

  int DBObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
  {
    DB::Object op_target(store->getDB(), get_bucket()->get_info(), get_obj());
    DB::Object::Read read_op(&op_target);

    return read_attrs(dpp, read_op, y, target_obj);
  }

  int DBObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
  {
    rgw_obj target = get_obj();
    int r = get_obj_attrs(y, dpp, &target);
    if (r < 0) {
      return r;
    }
    set_atomic();
    state.attrset[attr_name] = attr_val;
    return set_obj_attrs(dpp, &state.attrset, nullptr, y);
  }

  int DBObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y)
  {
    Attrs rmattr;
    bufferlist bl;

    set_atomic();
    rmattr[attr_name] = bl;
    return set_obj_attrs(dpp, nullptr, &rmattr, y);
  }

  bool DBObject::is_expired() {
    return false;
  }

  void DBObject::gen_rand_obj_instance_name()
  {
     store->getDB()->gen_rand_obj_instance_name(&state.obj.key);
  }


  int DBObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
      const std::set<std::string>& keys,
      Attrs* vals)
  {
    DB::Object op_target(store->getDB(),
        get_bucket()->get_info(), get_obj());
    return op_target.obj_omap_get_vals_by_keys(dpp, oid, keys, vals);
  }

  int DBObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
      bool must_exist, optional_yield y)
  {
    DB::Object op_target(store->getDB(),
        get_bucket()->get_info(), get_obj());
    return op_target.obj_omap_set_val_by_key(dpp, key, val, must_exist);
  }

  int DBObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y)
  {
    return 0;
  }

  std::unique_ptr<MPSerializer> DBObject::get_serializer(const DoutPrefixProvider *dpp,
							 const std::string& lock_name)
  {
    return std::make_unique<MPDBSerializer>(dpp, store, this, lock_name);
  }

  int DBObject::transition(Bucket* bucket,
      const rgw_placement_rule& placement_rule,
      const real_time& mtime,
      uint64_t olh_epoch,
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
    DB::Object op_target(store->getDB(),
        get_bucket()->get_info(), get_obj());
    return op_target.transition(dpp, placement_rule, mtime, olh_epoch);
  }

  bool DBObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
  {
    /* XXX: support single default zone and zonegroup for now */
    return true;
  }

  int DBObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f)
  {
    return 0;
  }

  std::unique_ptr<Object::ReadOp> DBObject::get_read_op()
  {
    return std::make_unique<DBObject::DBReadOp>(this, nullptr);
  }

  DBObject::DBReadOp::DBReadOp(DBObject *_source, RGWObjectCtx *_octx) :
    source(_source),
    octx(_octx),
    op_target(_source->store->getDB(),
        _source->get_bucket()->get_info(),
        _source->get_obj()),
    parent_op(&op_target)
  { }

  int DBObject::DBReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
  {
    uint64_t obj_size;

    parent_op.conds.mod_ptr = params.mod_ptr;
    parent_op.conds.unmod_ptr = params.unmod_ptr;
    parent_op.conds.high_precision_time = params.high_precision_time;
    parent_op.conds.mod_zone_id = params.mod_zone_id;
    parent_op.conds.mod_pg_ver = params.mod_pg_ver;
    parent_op.conds.if_match = params.if_match;
    parent_op.conds.if_nomatch = params.if_nomatch;
    parent_op.params.lastmod = params.lastmod;
    parent_op.params.target_obj = params.target_obj;
    parent_op.params.obj_size = &obj_size;
    parent_op.params.attrs = &source->get_attrs();

    int ret = parent_op.prepare(dpp);
    if (ret < 0)
      return ret;

    source->set_key(parent_op.state.obj.key);
    source->set_obj_size(obj_size);

    return ret;
  }

  int DBObject::DBReadOp::read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp)
  {
    return parent_op.read(ofs, end, bl, dpp);
  }

  int DBObject::DBReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
  {
    return parent_op.get_attr(dpp, name, dest);
  }

  std::unique_ptr<Object::DeleteOp> DBObject::get_delete_op()
  {
    return std::make_unique<DBObject::DBDeleteOp>(this);
  }

  DBObject::DBDeleteOp::DBDeleteOp(DBObject *_source) :
    source(_source),
    op_target(_source->store->getDB(),
        _source->get_bucket()->get_info(),
        _source->get_obj()),
    parent_op(&op_target)
  { }

  int DBObject::DBDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y)
  {
    parent_op.params.bucket_owner = params.bucket_owner.id;
    parent_op.params.versioning_status = params.versioning_status;
    parent_op.params.obj_owner = params.obj_owner;
    parent_op.params.olh_epoch = params.olh_epoch;
    parent_op.params.marker_version_id = params.marker_version_id;
    parent_op.params.bilog_flags = params.bilog_flags;
    parent_op.params.remove_objs = params.remove_objs;
    parent_op.params.expiration_time = params.expiration_time;
    parent_op.params.unmod_since = params.unmod_since;
    parent_op.params.mtime = params.mtime;
    parent_op.params.high_precision_time = params.high_precision_time;
    parent_op.params.zones_trace = params.zones_trace;
    parent_op.params.abortmp = params.abortmp;
    parent_op.params.parts_accounted_size = params.parts_accounted_size;

    int ret = parent_op.delete_obj(dpp);
    if (ret < 0)
      return ret;

    result.delete_marker = parent_op.result.delete_marker;
    result.version_id = parent_op.result.version_id;

    return ret;
  }

  int DBObject::delete_object(const DoutPrefixProvider* dpp, optional_yield y, bool prevent_versioning)
  {
    DB::Object del_target(store->getDB(), bucket->get_info(), get_obj());
    DB::Object::Delete del_op(&del_target);

    del_op.params.bucket_owner = bucket->get_info().owner;
    del_op.params.versioning_status = bucket->get_info().versioning_status();

    return del_op.delete_obj(dpp);
  }

  int DBObject::copy_object(User* user,
      req_info* info,
      const rgw_zone_id& source_zone,
      rgw::sal::Object* dest_object,
      rgw::sal::Bucket* dest_bucket,
      rgw::sal::Bucket* src_bucket,
      const rgw_placement_rule& dest_placement,
      ceph::real_time* src_mtime,
      ceph::real_time* mtime,
      const ceph::real_time* mod_ptr,
      const ceph::real_time* unmod_ptr,
      bool high_precision_time,
      const char* if_match,
      const char* if_nomatch,
      AttrsMod attrs_mod,
      bool copy_if_newer,
      Attrs& attrs,
      RGWObjCategory category,
      uint64_t olh_epoch,
      boost::optional<ceph::real_time> delete_at,
      std::string* version_id,
      std::string* tag,
      std::string* etag,
      void (*progress_cb)(off_t, void *),
      void* progress_data,
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
        return 0;
  }

  int DBObject::DBReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y)
  {
    return parent_op.iterate(dpp, ofs, end, cb);
  }

  int DBObject::swift_versioning_restore(bool& restored,
      const DoutPrefixProvider* dpp, optional_yield y)
  {
    return 0;
  }

  int DBObject::swift_versioning_copy(const DoutPrefixProvider* dpp,
      optional_yield y)
  {
    return 0;
  }

  int DBMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct, optional_yield y)
  {
    std::unique_ptr<rgw::sal::Object> meta_obj = get_meta_obj();
    meta_obj->set_in_extra_data(true);
    meta_obj->set_hash_source(mp_obj.get_key());
    int ret;

    std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = meta_obj->get_delete_op();
    del_op->params.bucket_owner.id = bucket->get_info().owner;
    del_op->params.versioning_status = 0;

    // Since the data objects are associated with meta obj till
    // MultipartUpload::Complete() is done, removing the metadata obj
    // should remove all the uploads so far.
    ret = del_op->delete_obj(dpp, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << __func__ << ": del_op.delete_obj returned " <<
        ret << dendl;
    }
    return (ret == -ENOENT) ? -ERR_NO_SUCH_UPLOAD : ret;
  }

  static string mp_ns = RGW_OBJ_NS_MULTIPART;

  std::unique_ptr<rgw::sal::Object> DBMultipartUpload::get_meta_obj()
  {
    return bucket->get_object(rgw_obj_key(get_meta(), string(), mp_ns));
  }

  int DBMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs)
  {
    int ret;
    std::string oid = mp_obj.get_key();

    char buf[33];
    std::unique_ptr<rgw::sal::Object> obj; // create meta obj
    gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
    std::string upload_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
    upload_id.append(buf);

    mp_obj.init(oid, upload_id);
    obj = get_meta_obj();

    DB::Object op_target(store->getDB(), obj->get_bucket()->get_info(),
			       obj->get_obj());
    DB::Object::Write obj_op(&op_target);

    /* Create meta object */
    obj_op.meta.owner = owner.id;
    obj_op.meta.category = RGWObjCategory::MultiMeta;
    obj_op.meta.flags = PUT_OBJ_CREATE_EXCL;
    obj_op.meta.mtime = &mtime;

    multipart_upload_info upload_info;
    upload_info.dest_placement = dest_placement;

    bufferlist bl;
    encode(upload_info, bl);
    obj_op.meta.data = &bl; 
    ret = obj_op.prepare(dpp);
    if (ret < 0)
      return ret;
    ret = obj_op.write_meta(dpp, bl.length(), bl.length(), attrs);

    return ret;
  }

  int DBMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				     int num_parts, int marker,
				     int *next_marker, bool *truncated, optional_yield y,
				     bool assume_unsorted)
  {
    std::list<RGWUploadPartInfo> parts_map;

    std::unique_ptr<rgw::sal::Object> obj = get_meta_obj();

    parts.clear();
    int ret;

    DB::Object op_target(store->getDB(),
        obj->get_bucket()->get_info(), obj->get_obj());
    ret = op_target.get_mp_parts_list(dpp, parts_map);
    if (ret < 0) {
      return ret;
    }

    int last_num = 0;

    while (!parts_map.empty()) {
      std::unique_ptr<DBMultipartPart> part = std::make_unique<DBMultipartPart>();
      RGWUploadPartInfo &pinfo = parts_map.front();
      part->set_info(pinfo);
      if ((int)pinfo.num > marker) {
        last_num = pinfo.num;
        parts[pinfo.num] = std::move(part);
      }
      parts_map.pop_front();
    }

    /* rebuild a map with only num_parts entries */
    std::map<uint32_t, std::unique_ptr<MultipartPart>> new_parts;
    std::map<uint32_t, std::unique_ptr<MultipartPart>>::iterator piter;
    int i;
    for (i = 0, piter = parts.begin();
	 i < num_parts && piter != parts.end();
	 ++i, ++piter) {
      last_num = piter->first;
      new_parts[piter->first] = std::move(piter->second);
    }

    if (truncated) {
      *truncated = (piter != parts.end());
    }

    parts.swap(new_parts);

    if (next_marker) {
      *next_marker = last_num;
    }

    return 0;
  }

  int DBMultipartUpload::complete(const DoutPrefixProvider *dpp,
				   optional_yield y, CephContext* cct,
				   map<int, string>& part_etags,
				   list<rgw_obj_index_key>& remove_objs,
				   uint64_t& accounted_size, bool& compressed,
				   RGWCompressionInfo& cs_info, off_t& ofs,
				   std::string& tag, ACLOwner& owner,
				   uint64_t olh_epoch,
				   rgw::sal::Object* target_obj)
  {
    char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
    char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
    std::string etag;
    bufferlist etag_bl;
    MD5 hash;
    bool truncated;
    int ret;

    int total_parts = 0;
    int handled_parts = 0;
    int max_parts = 1000;
    int marker = 0;
    uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
    auto etags_iter = part_etags.begin();
    rgw::sal::Attrs& attrs = target_obj->get_attrs();

    ofs = 0;
    accounted_size = 0;
    do {
      ret = list_parts(dpp, cct, max_parts, marker, &marker, &truncated, y);
      if (ret == -ENOENT) {
        ret = -ERR_NO_SUCH_UPLOAD;
      }
      if (ret < 0)
        return ret;

      total_parts += parts.size();
      if (!truncated && total_parts != (int)part_etags.size()) {
        ldpp_dout(dpp, 0) << "NOTICE: total parts mismatch: have: " << total_parts
		       << " expected: " << part_etags.size() << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      }

      for (auto obj_iter = parts.begin(); etags_iter != part_etags.end() && obj_iter != parts.end(); ++etags_iter, ++obj_iter, ++handled_parts) {
        DBMultipartPart* part = dynamic_cast<rgw::sal::DBMultipartPart*>(obj_iter->second.get());
        uint64_t part_size = part->get_size();
        if (handled_parts < (int)part_etags.size() - 1 &&
            part_size < min_part_size) {
          ret = -ERR_TOO_SMALL;
          return ret;
        }

        char petag[CEPH_CRYPTO_MD5_DIGESTSIZE];
        if (etags_iter->first != (int)obj_iter->first) {
          ldpp_dout(dpp, 0) << "NOTICE: parts num mismatch: next requested: "
		  	 << etags_iter->first << " next uploaded: "
			 << obj_iter->first << dendl;
          ret = -ERR_INVALID_PART;
          return ret;
        }
        string part_etag = rgw_string_unquote(etags_iter->second);
        if (part_etag.compare(part->get_etag()) != 0) {
          ldpp_dout(dpp, 0) << "NOTICE: etag mismatch: part: " << etags_iter->first
			 << " etag: " << etags_iter->second << dendl;
          ret = -ERR_INVALID_PART;
          return ret;
        }

        hex_to_buf(part->get_etag().c_str(), petag,
		  CEPH_CRYPTO_MD5_DIGESTSIZE);
        hash.Update((const unsigned char *)petag, sizeof(petag));

        RGWUploadPartInfo& obj_part = part->get_info();

        ofs += obj_part.size;
        accounted_size += obj_part.accounted_size;
      }
    } while (truncated);
    hash.Final((unsigned char *)final_etag);

    buf_to_hex((unsigned char *)final_etag, sizeof(final_etag), final_etag_str);
    snprintf(&final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2],
	     sizeof(final_etag_str) - CEPH_CRYPTO_MD5_DIGESTSIZE * 2,
           "-%lld", (long long)part_etags.size());
    etag = final_etag_str;
    ldpp_dout(dpp, 10) << "calculated etag: " << etag << dendl;

    etag_bl.append(etag);

    attrs[RGW_ATTR_ETAG] = etag_bl;

    /* XXX: handle compression ? */

    /* Rename all the object data entries with original object name (i.e
     * from 'head_obj.name + "." + upload_id' to head_obj.name) */

    /* Original head object */
    DB::Object op_target(store->getDB(),
			     target_obj->get_bucket()->get_info(),
			     target_obj->get_obj(), get_upload_id());
    DB::Object::Write obj_op(&op_target);
    ret = obj_op.prepare(dpp);

    obj_op.meta.owner = owner.id;
    obj_op.meta.flags = PUT_OBJ_CREATE;
    obj_op.meta.category = RGWObjCategory::Main;
    obj_op.meta.modify_tail = true;
    obj_op.meta.completeMultipart = true;

    ret = obj_op.write_meta(dpp, ofs, accounted_size, attrs);
    if (ret < 0)
      return ret;

    /* No need to delete Meta obj here. It is deleted from sal */
    return ret;
  }

  int DBMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y, rgw_placement_rule** rule, rgw::sal::Attrs* attrs)
  {
    if (!rule && !attrs) {
      return 0;
    }

    if (rule) {
      if (!placement.empty()) {
        *rule = &placement;
        if (!attrs) {
	    /* Don't need attrs, done */
	      return 0;
        }
      } else {
        *rule = nullptr;
      }
    }

    /* We need either attributes or placement, so we need a read */
    std::unique_ptr<rgw::sal::Object> meta_obj;
    meta_obj = get_meta_obj();
    meta_obj->set_in_extra_data(true);

    multipart_upload_info upload_info;
    bufferlist headbl;

    /* Read the obj head which contains the multipart_upload_info */
    std::unique_ptr<rgw::sal::Object::ReadOp> read_op = meta_obj->get_read_op();
    int ret = read_op->prepare(y, dpp);
    if (ret < 0) {
      if (ret == -ENOENT) {
        return -ERR_NO_SUCH_UPLOAD;
      }
      return ret;
    }

    if (attrs) {
      /* Attrs are filled in by prepare */
      *attrs = meta_obj->get_attrs();
      if (!rule || *rule != nullptr) {
        /* placement was cached; don't actually read */
        return 0;
      }
    }

    /* Now read the placement from the head */
    ret = read_op->read(0, store->getDB()->get_max_head_size(), headbl, y, dpp);
    if (ret < 0) {
      if (ret == -ENOENT) {
        return -ERR_NO_SUCH_UPLOAD;
      }
      return ret;
    }

    if (headbl.length() <= 0) {
      return -ERR_NO_SUCH_UPLOAD;
    }

    /* Decode multipart_upload_info */
    auto hiter = headbl.cbegin();
    try {
      decode(upload_info, hiter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode multipart upload info" << dendl;
      return -EIO;
    }
    placement = upload_info.dest_placement;
    *rule = &placement;

    return 0;
  }

  std::unique_ptr<Writer> DBMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
  {
    return std::make_unique<DBMultipartWriter>(dpp, y, this, obj, store, owner,
				 ptail_placement_rule, part_num, part_num_str);
  }

  DBMultipartWriter::DBMultipartWriter(const DoutPrefixProvider *dpp,
	    	    optional_yield y,
                MultipartUpload* upload,
		        rgw::sal::Object* obj,
		        DBStore* _driver,
    		    const rgw_user& _owner,
	    	    const rgw_placement_rule *_ptail_placement_rule,
                uint64_t _part_num, const std::string& _part_num_str):
			StoreWriter(dpp, y),
			store(_driver),
                owner(_owner),
                ptail_placement_rule(_ptail_placement_rule),
                head_obj(obj),
                upload_id(upload->get_upload_id()),
                part_num(_part_num),
                oid(head_obj->get_name() + "." + upload_id +
                    "." + std::to_string(part_num)),
                meta_obj(((DBMultipartUpload*)upload)->get_meta_obj()),
                op_target(_driver->getDB(), head_obj->get_bucket()->get_info(), head_obj->get_obj(), upload_id),
                parent_op(&op_target),
                part_num_str(_part_num_str) {}

  int DBMultipartWriter::prepare(optional_yield y)
  {
    parent_op.prepare(NULL);
    parent_op.set_mp_part_str(upload_id + "." + std::to_string(part_num));
    // XXX: do we need to handle part_num_str??
    return 0;
  }

  int DBMultipartWriter::process(bufferlist&& data, uint64_t offset)
  {
    /* XXX: same as AtomicWriter..consolidate code */
    total_data_size += data.length();

    /* XXX: Optimize all bufferlist copies in this function */

    /* copy head_data into meta. But for multipart we do not
     * need to write head_data */
    uint64_t max_chunk_size = store->getDB()->get_max_chunk_size();
    int excess_size = 0;

    /* Accumulate tail_data till max_chunk_size or flush op */
    bufferlist tail_data;

    if (data.length() != 0) {
        parent_op.meta.data = &head_data; /* Null data ?? */

      /* handle tail )parts.
       * First accumulate and write data into dbstore in its chunk_size
       * parts
       */
      if (!tail_part_size) { /* new tail part */
        tail_part_offset = offset;
      }
      data.begin(0).copy(data.length(), tail_data);
      tail_part_size += tail_data.length();
      tail_part_data.append(tail_data);

      if (tail_part_size < max_chunk_size)  {
        return 0;
      } else {
        int write_ofs = 0;
        while (tail_part_size >= max_chunk_size) {
          excess_size = tail_part_size - max_chunk_size;
          bufferlist tmp;
          tail_part_data.begin(write_ofs).copy(max_chunk_size, tmp);
          /* write tail objects data */
          int ret = parent_op.write_data(dpp, tmp, tail_part_offset);

          if (ret < 0) {
            return ret;
          }

          tail_part_size -= max_chunk_size;
          write_ofs += max_chunk_size;
          tail_part_offset += max_chunk_size;
        }
        /* reset tail parts or update if excess data */
        if (excess_size > 0) { /* wrote max_chunk_size data */
          tail_part_size = excess_size;
          bufferlist tmp;
          tail_part_data.begin(write_ofs).copy(excess_size, tmp);
          tail_part_data = tmp;
        } else {
          tail_part_size = 0;
          tail_part_data.clear();
          tail_part_offset = 0;
        }
      }
    } else {
      if (tail_part_size == 0) {
        return 0; /* nothing more to write */
      }

      /* flush whatever tail data is present */
      int ret = parent_op.write_data(dpp, tail_part_data, tail_part_offset);
      if (ret < 0) {
        return ret;
      }
      tail_part_size = 0;
      tail_part_data.clear();
      tail_part_offset = 0;
    }

    return 0;
  }

  int DBMultipartWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       const req_context& rctx)
  {
    /* XXX: same as AtomicWriter..consolidate code */
    parent_op.meta.mtime = mtime;
    parent_op.meta.delete_at = delete_at;
    parent_op.meta.if_match = if_match;
    parent_op.meta.if_nomatch = if_nomatch;
    parent_op.meta.user_data = user_data;
    parent_op.meta.zones_trace = zones_trace;
    
    /* XXX: handle accounted size */
    accounted_size = total_data_size;

    RGWUploadPartInfo info;
    info.num = part_num;
    info.etag = etag;
    info.size = total_data_size;
    info.accounted_size = accounted_size;
    info.modified = real_clock::now();
    //info.manifest = manifest;

    DB::Object op_target(store->getDB(),
        meta_obj->get_bucket()->get_info(), meta_obj->get_obj());
    auto ret = op_target.add_mp_part(dpp, info);
    if (ret < 0) {
      return ret == -ENOENT ? -ERR_NO_SUCH_UPLOAD : ret;
    }

    return 0;
  }

  DBAtomicWriter::DBAtomicWriter(const DoutPrefixProvider *dpp,
	    	    optional_yield y,
		        rgw::sal::Object* _obj,
		        DBStore* _driver,
    		    const rgw_user& _owner,
	    	    const rgw_placement_rule *_ptail_placement_rule,
		        uint64_t _olh_epoch,
		        const std::string& _unique_tag) :
			StoreWriter(dpp, y),
			store(_driver),
                owner(_owner),
                ptail_placement_rule(_ptail_placement_rule),
                olh_epoch(_olh_epoch),
                unique_tag(_unique_tag),
                obj(_driver, _obj->get_key(), _obj->get_bucket()),
                op_target(_driver->getDB(), obj.get_bucket()->get_info(), obj.get_obj()),
                parent_op(&op_target) {}

  int DBAtomicWriter::prepare(optional_yield y)
  {
    return parent_op.prepare(NULL); /* send dpp */
  }

  int DBAtomicWriter::process(bufferlist&& data, uint64_t offset)
  {
    total_data_size += data.length();

    /* XXX: Optimize all bufferlist copies in this function */

    /* copy head_data into meta. */
    uint64_t head_size = store->getDB()->get_max_head_size();
    unsigned head_len = 0;
    uint64_t max_chunk_size = store->getDB()->get_max_chunk_size();
    int excess_size = 0;

    /* Accumulate tail_data till max_chunk_size or flush op */
    bufferlist tail_data;

    if (data.length() != 0) {
      if (offset < head_size) {
        /* XXX: handle case (if exists) where offset > 0 & < head_size */
        head_len = std::min((uint64_t)data.length(),
                                    head_size - offset);
        bufferlist tmp;
        data.begin(0).copy(head_len, tmp);
        head_data.append(tmp);

        parent_op.meta.data = &head_data;
        if (head_len == data.length()) {
          return 0;
        }

        /* Move offset by copy_len */
        offset = head_len;
      }

      /* handle tail parts.
       * First accumulate and write data into dbstore in its chunk_size
       * parts
       */
      if (!tail_part_size) { /* new tail part */
        tail_part_offset = offset;
      }
      data.begin(head_len).copy(data.length() - head_len, tail_data);
      tail_part_size += tail_data.length();
      tail_part_data.append(tail_data);

      if (tail_part_size < max_chunk_size)  {
        return 0;
      } else {
        int write_ofs = 0;
        while (tail_part_size >= max_chunk_size) {
          excess_size = tail_part_size - max_chunk_size;
          bufferlist tmp;
          tail_part_data.begin(write_ofs).copy(max_chunk_size, tmp);
          /* write tail objects data */
          int ret = parent_op.write_data(dpp, tmp, tail_part_offset);

          if (ret < 0) {
            return ret;
          }

          tail_part_size -= max_chunk_size;
          write_ofs += max_chunk_size;
          tail_part_offset += max_chunk_size;
        }
        /* reset tail parts or update if excess data */
        if (excess_size > 0) { /* wrote max_chunk_size data */
          tail_part_size = excess_size;
          bufferlist tmp;
          tail_part_data.begin(write_ofs).copy(excess_size, tmp);
          tail_part_data = tmp;
        } else {
          tail_part_size = 0;
          tail_part_data.clear();
          tail_part_offset = 0;
        }
      }
    } else {
      if (tail_part_size == 0) {
        return 0; /* nothing more to write */
      }

      /* flush whatever tail data is present */
      int ret = parent_op.write_data(dpp, tail_part_data, tail_part_offset);
      if (ret < 0) {
        return ret;
      }
      tail_part_size = 0;
      tail_part_data.clear();
      tail_part_offset = 0;
    }

    return 0;
  }

  int DBAtomicWriter::complete(size_t accounted_size, const std::string& etag,
                         ceph::real_time *mtime, ceph::real_time set_mtime,
                         std::map<std::string, bufferlist>& attrs,
                         ceph::real_time delete_at,
                         const char *if_match, const char *if_nomatch,
                         const std::string *user_data,
                         rgw_zone_set *zones_trace, bool *canceled,
                         const req_context& rctx)
  {
    parent_op.meta.mtime = mtime;
    parent_op.meta.delete_at = delete_at;
    parent_op.meta.if_match = if_match;
    parent_op.meta.if_nomatch = if_nomatch;
    parent_op.meta.user_data = user_data;
    parent_op.meta.zones_trace = zones_trace;
    parent_op.meta.category = RGWObjCategory::Main;
    
    /* XXX: handle accounted size */
    accounted_size = total_data_size;
    int ret = parent_op.write_meta(dpp, total_data_size, accounted_size, attrs);
    if (canceled) {
      *canceled = parent_op.meta.canceled;
    }

    return ret;

  }

  std::unique_ptr<RGWRole> DBStore::get_role(std::string name,
      std::string tenant,
      std::string path,
      std::string trust_policy,
      std::string max_session_duration_str,
      std::multimap<std::string,std::string> tags)
  {
    RGWRole* p = nullptr;
    return std::unique_ptr<RGWRole>(p);
  }

  std::unique_ptr<RGWRole> DBStore::get_role(std::string id)
  {
    RGWRole* p = nullptr;
    return std::unique_ptr<RGWRole>(p);
  }

  std::unique_ptr<RGWRole> DBStore::get_role(const RGWRoleInfo& info)
  {
    RGWRole* p = nullptr;
    return std::unique_ptr<RGWRole>(p);
  }

  int DBStore::get_roles(const DoutPrefixProvider *dpp,
      optional_yield y,
      const std::string& path_prefix,
      const std::string& tenant,
      vector<std::unique_ptr<RGWRole>>& roles)
  {
    return 0;
  }

  std::unique_ptr<RGWOIDCProvider> DBStore::get_oidc_provider()
  {
    RGWOIDCProvider* p = nullptr;
    return std::unique_ptr<RGWOIDCProvider>(p);
  }

  int DBStore::get_oidc_providers(const DoutPrefixProvider *dpp,
      const std::string& tenant,
      vector<std::unique_ptr<RGWOIDCProvider>>& providers, optional_yield y)
  {
    return 0;
  }

  std::unique_ptr<Writer> DBStore::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) {
    return nullptr;
  }

  std::unique_ptr<Writer> DBStore::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) {
    return std::make_unique<DBAtomicWriter>(dpp, y, obj, this, owner,
                    ptail_placement_rule, olh_epoch, unique_tag);
  }

  const std::string& DBStore::get_compression_type(const rgw_placement_rule& rule) {
    return zone.get_rgw_params().get_compression_type(rule);
  }

  bool DBStore::valid_placement(const rgw_placement_rule& rule)
  {
    // XXX: Till zonegroup, zone and storage-classes can be configured
    // for dbstore return true
    return true; //zone.get_rgw_params().valid_placement(rule);
  }

  std::unique_ptr<User> DBStore::get_user(const rgw_user &u)
  {
    return std::make_unique<DBUser>(this, u);
  }

  int DBStore::get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
  {
    RGWUserInfo uinfo;
    User *u;
    int ret = 0;
    RGWObjVersionTracker objv_tracker;

    ret = getDB()->get_user(dpp, string("access_key"), key, uinfo, nullptr,
        &objv_tracker);

    if (ret < 0)
      return ret;

    u = new DBUser(this, uinfo);

    if (!u)
      return -ENOMEM;

    u->get_version_tracker() = objv_tracker;
    user->reset(u);

    return 0;
  }

  int DBStore::get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
  {
    RGWUserInfo uinfo;
    User *u;
    int ret = 0;
    RGWObjVersionTracker objv_tracker;

    ret = getDB()->get_user(dpp, string("email"), email, uinfo, nullptr,
        &objv_tracker);

    if (ret < 0)
      return ret;

    u = new DBUser(this, uinfo);

    if (!u)
      return -ENOMEM;

    u->get_version_tracker() = objv_tracker;
    user->reset(u);

    return ret;
  }

  int DBStore::get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
  {
    /* Swift keys and subusers are not supported for now */
    return -ENOTSUP;
  }

  std::string DBStore::get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y)
  {
    return "PLACEHOLDER"; // for instance unique identifier
  }

  std::unique_ptr<Object> DBStore::get_object(const rgw_obj_key& k)
  {
    return std::make_unique<DBObject>(this, k);
  }


  std::unique_ptr<Bucket> DBStore::get_bucket(const RGWBucketInfo& i)
  {
    /* Don't need to fetch the bucket info, use the provided one */
    return std::make_unique<DBBucket>(this, i);
  }

  int DBStore::load_bucket(const DoutPrefixProvider *dpp, const rgw_bucket& b,
                           std::unique_ptr<Bucket>* bucket, optional_yield y)
  {
    *bucket = std::make_unique<DBBucket>(this, b);
    return (*bucket)->load_bucket(dpp, y);
  }

  bool DBStore::is_meta_master()
  {
    return true;
  }

  std::string DBStore::zone_unique_id(uint64_t unique_num)
  {
    return "";
  }

  std::string DBStore::zone_unique_trans_id(const uint64_t unique_num)
  {
    return "";
  }

  int DBStore::get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zg)
  {
    /* XXX: for now only one zonegroup supported */
    std::unique_ptr<RGWZoneGroup> rzg =
        std::make_unique<RGWZoneGroup>("default", "default");
    rzg->api_name = "default";
    rzg->is_master = true;
    ZoneGroup* group = new DBZoneGroup(this, std::move(rzg));
    if (!group)
      return -ENOMEM;

    zg->reset(group);
    return 0;
  }

  int DBStore::list_all_zones(const DoutPrefixProvider* dpp,
			      std::list<std::string>& zone_ids)
  {
    zone_ids.push_back(zone.get_id());
    return 0;
  }

  int DBStore::cluster_stat(RGWClusterStat& stats)
  {
    return 0;
  }

  std::unique_ptr<Lifecycle> DBStore::get_lifecycle(void)
  {
    return std::make_unique<DBLifecycle>(this);
  }

  int DBLifecycle::get_entry(const std::string& oid, const std::string& marker,
			      std::unique_ptr<LCEntry>* entry)
  {
    return store->getDB()->get_entry(oid, marker, entry);
  }

  int DBLifecycle::get_next_entry(const std::string& oid, const std::string& marker,
				  std::unique_ptr<LCEntry>* entry)
  {
    return store->getDB()->get_next_entry(oid, marker, entry);
  }

  int DBLifecycle::set_entry(const std::string& oid, LCEntry& entry)
  {
    return store->getDB()->set_entry(oid, entry);
  }

  int DBLifecycle::list_entries(const std::string& oid, const std::string& marker,
  				 uint32_t max_entries, vector<std::unique_ptr<LCEntry>>& entries)
  {
    return store->getDB()->list_entries(oid, marker, max_entries, entries);
  }

  int DBLifecycle::rm_entry(const std::string& oid, LCEntry& entry)
  {
    return store->getDB()->rm_entry(oid, entry);
  }

  int DBLifecycle::get_head(const std::string& oid, std::unique_ptr<LCHead>* head)
  {
    return store->getDB()->get_head(oid, head);
  }

  int DBLifecycle::put_head(const std::string& oid, LCHead& head)
  {
    return store->getDB()->put_head(oid, head);
  }

  std::unique_ptr<LCSerializer> DBLifecycle::get_serializer(const std::string& lock_name,
							    const std::string& oid,
							    const std::string& cookie)
  {
    return std::make_unique<LCDBSerializer>(store, oid, lock_name, cookie);
  }

  std::unique_ptr<Notification> DBStore::get_notification(
    rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s,
    rgw::notify::EventType event_type, optional_yield y,
    const std::string* object_name)
  {
    return std::make_unique<DBNotification>(obj, src_obj, event_type);
  }

  std::unique_ptr<Notification> DBStore::get_notification(
    const DoutPrefixProvider* dpp, rgw::sal::Object* obj,
    rgw::sal::Object* src_obj,
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket,
    std::string& _user_id, std::string& _user_tenant, std::string& _req_id,
    optional_yield y)
  {
    return std::make_unique<DBNotification>(obj, src_obj, event_type);
  }

  RGWLC* DBStore::get_rgwlc(void) {
    return lc;
  }

  int DBStore::log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info, optional_yield y)
  {
    return 0;
  }

  int DBStore::log_op(const DoutPrefixProvider *dpp, string& oid, bufferlist& bl)
  {
    return 0;
  }

  int DBStore::register_to_service_map(const DoutPrefixProvider *dpp, const string& daemon_type,
      const map<string, string>& meta)
  {
    return 0;
  }

  void DBStore::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit)
  {
    return;
  }

  void DBStore::get_quota(RGWQuota& quota)
  {
    // XXX: Not handled for the first pass 
    return;
  }

  int DBStore::set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled, optional_yield y)
  {
    int ret = 0;

    vector<rgw_bucket>::iterator iter;

    for (iter = buckets.begin(); iter != buckets.end(); ++iter) {
      rgw_bucket& bucket = *iter;
      if (enabled) {
        ldpp_dout(dpp, 20) << "enabling bucket name=" << bucket.name << dendl;
      } else {
        ldpp_dout(dpp, 20) << "disabling bucket name=" << bucket.name << dendl;
      }

      RGWBucketInfo info;
      map<string, bufferlist> attrs;
      int r = getDB()->get_bucket_info(dpp, string("name"), "", info, &attrs,
          nullptr, nullptr);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "NOTICE: get_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
        ret = r;
        continue;
      }
      if (enabled) {
        info.flags &= ~BUCKET_SUSPENDED;
      } else {
        info.flags |= BUCKET_SUSPENDED;
      }

      r = getDB()->update_bucket(dpp, "info", info, false, nullptr, &attrs, nullptr, &info.objv_tracker);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "NOTICE: put_bucket_info on bucket=" << bucket.name << " returned err=" << r << ", skipping bucket" << dendl;
        ret = r;
        continue;
      }
    }
    return ret;
  }

  int DBStore::get_sync_policy_handler(const DoutPrefixProvider *dpp,
      std::optional<rgw_zone_id> zone,
      std::optional<rgw_bucket> bucket,
      RGWBucketSyncPolicyHandlerRef *phandler,
      optional_yield y)
  {
    return 0;
  }

  RGWDataSyncStatusManager* DBStore::get_data_sync_manager(const rgw_zone_id& source_zone)
  {
    return 0;
  }

  int DBStore::read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, 
      uint32_t max_entries, bool *is_truncated,
      RGWUsageIter& usage_iter,
      map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    return 0;
  }

  int DBStore::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
  {
    return 0;
  }

  int DBStore::get_config_key_val(string name, bufferlist *bl)
  {
    return -ENOTSUP;
  }

  int DBStore::meta_list_keys_init(const DoutPrefixProvider *dpp, const string& section, const string& marker, void** phandle)
  {
    return 0;
  }

  int DBStore::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, list<string>& keys, bool* truncated)
  {
    return 0;
  }

  void DBStore::meta_list_keys_complete(void* handle)
  {
    return;
  }

  std::string DBStore::meta_get_marker(void* handle)
  {
    return "";
  }

  int DBStore::meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y)
  {
    return 0;
  }

  int DBStore::initialize(CephContext *_cct, const DoutPrefixProvider *_dpp) {
    int ret = 0;
    cct = _cct;
    dpp = _dpp;

    lc = new RGWLC();
    lc->initialize(cct, this);

    if (use_lc_thread) {
      ret = db->createLCTables(dpp);
      lc->start_processor();
    }

    ret = db->createGC(dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) <<"GC thread creation failed: ret = " << ret << dendl;
    }

    return ret;
  }

  int DBLuaManager::get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script)
  {
    return -ENOENT;
  }

  int DBLuaManager::put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script)
  {
    return -ENOENT;
  }

  int DBLuaManager::del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key)
  {
    return -ENOENT;
  }

  int DBLuaManager::add_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name)
  {
    return -ENOENT;
  }

  int DBLuaManager::remove_package(const DoutPrefixProvider* dpp, optional_yield y, const std::string& package_name)
  {
    return -ENOENT;
  }

  int DBLuaManager::list_packages(const DoutPrefixProvider* dpp, optional_yield y, rgw::lua::packages_t& packages)
  {
    return -ENOENT;
  }
  
  int DBLuaManager::reload_packages(const DoutPrefixProvider* dpp, optional_yield y)
  {
    return -ENOENT;
  }
  
} // namespace rgw::sal

extern "C" {

  void *newDBStore(CephContext *cct)
  {
    rgw::sal::DBStore *driver = new rgw::sal::DBStore();
    DBStoreManager *dbsm = new DBStoreManager(cct);

    DB *db = dbsm->getDB();
    if (!db) {
      delete dbsm;
      delete driver;
      return nullptr;
    }

    driver->setDBStoreManager(dbsm);
    driver->setDB(db);
    db->set_driver((rgw::sal::Driver*)driver);
    db->set_context(cct);

    return driver;
  }

}
