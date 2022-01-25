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

 //TODO: fix the indents and tabs - Daniel P


//shared includes
#include <errno.h>
#include "common/errno.h"
#include <stdlib.h>
#include <system_error>
#include <unistd.h>
#include <sstream>
#include "common/dout.h"

#include "common/Clock.h"

#include "rgw_sal.h"

#include "rgw_sal_rados.h"
#ifdef WITH_RADOSGW_DBSTORE
#include "rgw_sal_dbstore.h"
#endif
#include "rgw_bucket.h"

#include "rgw_sal_tracer.h"

#define dout_subsys ceph_subsys_rgw
//for dout?
#define dout_context g_ceph_context

using namespace std;

namespace rgw::sal {

  /*User functions*/
  int TObject::delete_object(const DoutPrefixProvider* dpp, optional_yield y, bool prevent_versioning)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function" << dendl;
    return -1; //implement - Daniel P
  }

  int TObject::delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate,
      Completions* aio, bool keep_index_consistent,
      optional_yield y)
  {
    /* XXX: Make it async */
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function" << dendl;
    return -1; // implement - Daniel P
  }

  int TObject::copy_object(
      User* user,
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
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function" << dendl;
        return -1; // implement - Daniel P
  }

  /* RGWObjectCtx will be moved out of sal */
  /* XXX: Placeholder. Should not be needed later after Dan's patch */
  void TObject::set_atomic()
  {
    dout(20) << "TRACER: Unimplemented function" << dendl;
    return; //implement - Daniel P
  }

  /* RGWObjectCtx will be moved out of sal */
  /* XXX: Placeholder. Should not be needed later after Dan's patch */
  void TObject::set_prefetch_data()
  {
    dout(20) << "TRACER: Unimplemented function" << dendl;
    return; //implement - Daniel P
  }

  /* RGWObjectCtx will be moved out of sal */
  /* XXX: Placeholder. Should not be needed later after Dan's patch */
  int TObject::transition_to_cloud(Bucket* bucket, rgw::sal::PlacementTier* tier, rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets, CephContext* cct, bool update_object, const DoutPrefixProvider* dpp,
			   optional_yield y)
         {
           return realObject->transition_to_cloud(bucket, tier, o, cloud_targets, cct, update_object, dpp, y);
         }


  void TObject::set_compressed()
  {
    dout(20) << "TRACER: Unimplemented function" << dendl;
    return; //implement - Daniel P
  }

  int TObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state, optional_yield y, bool follow_olh)
  {
      ldpp_dout(dpp, 20) << "TRACER: Unimplemented function" << dendl;
      return -1; // implement - Daniel P
  }

  int TObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function" << dendl;
    return -1; // implement - Daniel P
  }

  int TObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function" << dendl;
    return -1; // implement - Daniel P
  }

  int TObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function" << dendl;
    return -1; // implement - Daniel P
  }

  int TObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function" << dendl;
    return -1; // implement - Daniel P
  }

  bool TObject::is_expired() {
    dout(20) << "TRACER: Unimplemented function" << dendl;
    return false; //implement - Daniel P
  }

  void TObject::gen_rand_obj_instance_name()
  {
    dout(20) << "TRACER: pure passthrough function: gen_rand_obj_instance_name " << dendl;
     return realObject->gen_rand_obj_instance_name(); //Implement? - Daniel P
  }

  MPSerializer* TObject::get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: get_serializer" << dendl;
    return new MPTSerializer(dpp, trace, this, lock_name); //Implement - Daniel P
  }

  int TObject::transition(
      Bucket* bucket,
      const rgw_placement_rule& placement_rule,
      const real_time& mtime,
      uint64_t olh_epoch,
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
    //dout(20) << "TRACER: making ReadOp" << dendl;
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: transition" << dendl;
    return realObject->transition(bucket, placement_rule, mtime, olh_epoch, dpp, y);
  }

  bool TObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
  {
    dout(20) << "TRACER: pure passthrough function: placement_rules_match " << dendl;
    /* XXX: support single default zone and zonegroup for now */
    return realObject->placement_rules_match(r1, r2);
  }

  int TObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: dump_obj_layout" << dendl;
    return realObject->dump_obj_layout(dpp, y, f);
  }

  int TObject::swift_versioning_restore(
      bool& restored,
      const DoutPrefixProvider* dpp)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: swift_versioning_restore" << dendl;
    return realObject->swift_versioning_restore( restored, dpp);
  }

  int TObject::swift_versioning_copy(
      const DoutPrefixProvider* dpp,
      optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: swift_versioning_copy" << dendl;
    return realObject->swift_versioning_copy(dpp, y);
  }

  std::unique_ptr<Object::ReadOp> TObject::get_read_op()
  {
    dout(20) << "TRACER: making ReadOp" << dendl;
    return std::make_unique<TObject::TReadOp>(this); // Implement - Daniel P
  }

  TObject::TReadOp::TReadOp(TObject *_source) : //Implement - Daniel P
    source(_source)/*op_target(_source->store->getDB(),
        _source->get_bucket()->get_info(),
        _source->get_obj()),
    parent_op(&op_target)*/ //Implement - Daniel P
  { 
    dout(20) << "TRACER: Unimplemented function: TReadOp" << dendl;
    //ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: transition" << dendl;
  }
  
  std::unique_ptr<Object::DeleteOp> TObject::get_delete_op()
  {
    return NULL; //std::make_unique<TObject::TDeleteOp>(this, ctx); //Implement - Daniel P - This straight up isn't compiling and I don't know why
  }
  
  int TObject::omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
      std::map<std::string, bufferlist> *m,
      bool* pmore, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: omap_get_vals" << dendl;
    return -1; // implement - Daniel P
  }

  int TObject::omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m,
      optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: omap_get_all" << dendl;
    return -1; // implement - Daniel P
  }

  int TObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
      const std::set<std::string>& keys,
      Attrs* vals)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: omap_get_vals_by_keys" << dendl;
    return -1; // implement - Daniel P
  }

  int TObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
      bool must_exist, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: omap_set_val_by_key" << dendl;
    return -1; // implement - Daniel P
  }

  /*Tracer User Functions*/
  int TracerUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER:USER: recieved operation: remove_user" << dendl;
    return real_user->remove_user(dpp, y); //may need to also remove this user - Daniel P
  }
  
  int TracerUser::load_user(const DoutPrefixProvider *dpp, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER:USER: recieved operation: load_user" << dendl;
    return real_user->load_user(dpp, y); //implement - Daniel P
  }
  
  int TracerUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
  {
    ldpp_dout(dpp, 20) << "TRACER:USER: recieved operation: store_user" << dendl;
    return real_user->store_user(dpp, y, exclusive, old_info); //implement - Daniel P
  }

  int TracerUser::create_bucket(const DoutPrefixProvider* dpp, 
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
        optional_yield y)
  {
        
    ldpp_dout(dpp, 20) << "TRACER: USER: recieved operation: create_bucket" << dendl;
    int r;
    r = real_user->create_bucket(dpp, b, zonegroup_id, placement_rule, swift_ver_location, pquota_info, policy,
         attrs, info, ep_objv, exclusive, obj_lock_enabled, existed, req_info, bucket, y);
    ldpp_dout(dpp, 20) << "TRACER: USER: Primary store recieved and carried out operation: create bucket" << dendl;
    return r;
  }

  int TracerUser::list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
	      const std::string& end_marker, uint64_t max, bool need_stats,
	      BucketList &buckets, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: list_buckets" << dendl;
    return real_user->list_buckets(dpp, marker, end_marker, max, need_stats, buckets, y); //implement - Daniel P
  }


  int TracerUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: read_attrs" << dendl;
    return real_user->read_attrs(dpp, y); //implement - Daniel P
  }
    
  int TracerUser::read_stats(const DoutPrefixProvider *dpp,
        optional_yield y, RGWStorageStats* stats,
        ceph::real_time *last_stats_sync,
        ceph::real_time *last_stats_update)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: read_stats " << dendl;
    return real_user->read_stats(dpp, y, stats, last_stats_sync, last_stats_update); //implement - Daniel P
  }

  int TracerUser::read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB *cb)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: read_stats_async " << dendl;
    return real_user->read_stats_async(dpp, cb); //implement - Daniel P
  }

  int TracerUser::merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y)
  {
      ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: merge_and_store_attrs " << dendl;
      return real_user->merge_and_store_attrs(dpp, new_attrs, y); //implement - Daniel P
  }

  int TracerUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
      ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: complete_flush_stats " << dendl;
      return real_user->complete_flush_stats(dpp, y); //implement - Daniel P
  }

  int TracerUser::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, uint32_t max_entries,
        bool *is_truncated, RGWUsageIter& usage_iter,
        std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: read_usage " << dendl;
    return real_user->read_usage(dpp, start_epoch, end_epoch, max_entries, is_truncated, usage_iter, usage); //implement - Daniel P
  }

  int TracerUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: trim_usage " << dendl;
    return real_user->trim_usage(dpp, start_epoch, end_epoch); //implement - Daniel P
  }


  /*ReadOp functions*/
  int TObject::TReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: prepare" << dendl;
    return -1; // implement - Daniel P
  }

  int TObject::TReadOp::read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: read" << dendl;
    return -1; // implement - Daniel P
    //return parent_op.read(ofs, end, bl, dpp);
  }

  int TObject::TReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: get_attr" << dendl;
    return -1; // implement - Daniel P
    //return parent_op.get_attr(dpp, name, dest);
  }

  int TObject::TReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: iterate" << dendl;
    return -1; // implement - Daniel P
    //return parent_op.iterate(dpp, ofs, end, cb);
  }

  /*DeleteOp functions*/
  int TObject::TDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: Unimplemented function: delete_obj" << dendl;
    return -1; // implement - Daniel P
  }


  /*Zonegroup functions */
  const rgw_zone_id& TZone::get_id()
  {
    return realZone->get_id();
  }


  bool TZone::is_writeable()
  {
    return realZone->is_writeable();
  }

  bool TZone::has_zonegroup_api(const std::string& api) const
  {
    return realZone->has_zonegroup_api(api);
  }

  const std::string& TZone::get_current_period_id()
  {
    return realZone->get_current_period_id();
  }

  bool TZone::get_redirect_endpoint(std::string* endpoint)
  {
    return realZone->get_redirect_endpoint(endpoint);
  }

  const std::string& TZone::get_name() const
  {
    return realZone->get_name();
  }

  ZoneGroup& TZone::get_zonegroup()
  {
    return realZone->get_zonegroup();
  }
  
  int TZone::get_zonegroup(const std::string& id, std::unique_ptr<ZoneGroup>* zonegroup)
  {
      return realZone->get_zonegroup(id, zonegroup);
  }

  const RGWAccessKey& TZone::get_system_key()
  {
    return realZone->get_system_key();
  }
  const std::string& TZone::get_realm_name() 
  {
    return realZone->get_realm_name();
  }
  const std::string& TZone::get_realm_id()
  {
    return realZone->get_realm_id();
  }
  
  /*Tracerbucket functions */
  std::unique_ptr<Object> TracerBucket::get_object(const rgw_obj_key& k)
  {
    /* TODO: reimplement when TObjects are complete */
    //return std::make_unique<TObject>(this->trace, k, this);
    
    dout(20) << "TRACER: BUCKET: Intercepted operation: get_object" << dendl;
    return real_bucket->get_object(k);
  }

  int TracerBucket::list(const DoutPrefixProvider *dpp, ListParams& params, int max, ListResults& results, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: list" << dendl;
    int ret;
    ret = real_bucket->list(dpp, params, max, results, y);
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: returned from operation: list" << dendl;
    return ret;
  }

  /*This particular function needs more fleshing out*/
  int TracerBucket::remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, bool forward_to_master, req_info* req_info, optional_yield y)
  {
    int ret;
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: remove_bucket" << dendl;
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

    ret = real_bucket->remove_bucket(dpp, delete_children,forward_to_master, req_info, y);
    ldpp_dout(dpp, 20) << "TRACER:BUCKET: returned from operation: remove_bucket" << dendl;
    return ret;
  }

  int TracerBucket::remove_bucket_bypass_gc(int concurrent_max, bool
					keep_index_consistent,
					optional_yield y, const
					DoutPrefixProvider *dpp) {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: remove_bucket_bypass_gc " << dendl; 
    int ret;       
    ret = real_bucket->remove_bucket_bypass_gc(concurrent_max, keep_index_consistent, y, dpp);
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: returned from operation: remove_bucket_bypass_gc " << dendl;
    return ret;
  }

  int TracerBucket::set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy &acl, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: set_acl " << dendl;

    int ret = real_bucket->set_acl(dpp, acl, y);

    ldpp_dout(dpp, 20) << "TRACER: BUCKET: returned from operation: set_acl "<< dendl;
    return ret;
  }

  int TracerBucket::load_bucket(const DoutPrefixProvider *dpp, optional_yield y, bool get_stats)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: load_bucket "<< dendl;
    int ret = 0;
    ret = real_bucket->load_bucket(dpp, y, get_stats);
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: returned from operation: load_bucket "<< dendl;
    return ret;
  }

  int TracerBucket::read_stats(const DoutPrefixProvider *dpp, int shard_id,
        std::string *bucket_ver, std::string *master_ver,
        std::map<RGWObjCategory, RGWStorageStats>& stats,
        std::string *max_marker, bool *syncstopped)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: read_stats"<< dendl;
    int ret;
    ret = real_bucket->read_stats(dpp, shard_id, bucket_ver, master_ver, stats, max_marker, syncstopped);
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: returned from operation: read_stats"<< dendl;
    return ret;
  }

  int TracerBucket::read_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB *ctx)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: read_stats_async "<< dendl;
    int ret;
    ret = real_bucket->read_stats_async(dpp, shard_id, ctx);
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: returned from operation: read_stats_async "<< dendl;
    return ret;
  }

  int TracerBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: sync_user_stats "<< dendl;
    return real_bucket->sync_user_stats(dpp,y);
  }

  int TracerBucket::update_container_stats(const DoutPrefixProvider *dpp)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: update_container_stats "<< dendl;
    return real_bucket->update_container_stats(dpp);
  }

  int TracerBucket::check_bucket_shards(const DoutPrefixProvider *dpp)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: check_bucket_shards "<< dendl;
    return real_bucket->check_bucket_shards(dpp);
  }

  int TracerBucket::chown(const DoutPrefixProvider *dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: chown "<< dendl;
    return real_bucket->chown(dpp, new_user, old_user, y, marker);
  }

  int TracerBucket::put_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time _mtime)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: put_info "<< dendl;
    return real_bucket->put_info(dpp, exclusive, _mtime);

  }

  /* Make sure to call get_bucket_info() if you need it first */
  bool TracerBucket::is_owner(User* user)
  {
    return real_bucket->is_owner(user);
  }

  int TracerBucket::check_empty(const DoutPrefixProvider *dpp, optional_yield y)
  {
    /* XXX: Check if bucket contains any objects */
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: check_empty" << dendl;
    return real_bucket->check_empty(dpp, y);
  }

  int TracerBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size,
      optional_yield y, bool check_size_only)
  {
    /* Not Handled in the first pass as stats are also needed */
    return real_bucket->check_quota(dpp, quota, obj_size, y, check_size_only);
  }

  int TracerBucket::merge_and_store_attrs(const DoutPrefixProvider *dpp, Attrs& new_attrs, optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: check_empty" << dendl;
    return real_bucket->merge_and_store_attrs(dpp, new_attrs, y);
  }

  int TracerBucket::try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: try_refresh_info" << dendl;
    return real_bucket->try_refresh_info(dpp, pmtime);
  }

  /* XXX: usage and stats not supported in the first pass */
  int TracerBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
        uint32_t max_entries, bool *is_truncated,
        RGWUsageIter& usage_iter,
        std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: read_usage" << dendl;
    return real_bucket->read_usage(dpp, start_epoch, end_epoch, max_entries, is_truncated, usage_iter, usage);
  }

  int TracerBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    ldpp_dout(dpp, 20) << "TRACER: BUCKET: recieved operation: trim_usage" << dendl;
    return real_bucket->trim_usage(dpp, start_epoch, end_epoch);
  }

  int TracerBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
  { 
    /*
     * Delete all the object in the list from the object table of this
     * bucket
     */
    return real_bucket->remove_objs_from_index(dpp, objs_to_unlink);
  }

  int TracerBucket::check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
  {
    /* XXX: stats not supported yet */
    return real_bucket->check_index(dpp, existing_stats,calculated_stats);
  }

  int TracerBucket::rebuild_index(const DoutPrefixProvider *dpp)
  {
    return real_bucket->rebuild_index(dpp);
  }

  int TracerBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
  {
    /* XXX: CHECK: set tag timeout for all the bucket objects? */
    return real_bucket->set_tag_timeout(dpp, timeout);
  }

  int TracerBucket::purge_instance(const DoutPrefixProvider *dpp)
  {
    /* XXX: CHECK: for dbstore only single instance supported.
     * Remove all the objects for that instance? Anything extra needed?
     */
    return real_bucket->purge_instance(dpp);
  }

  std::unique_ptr<MultipartUpload> TracerBucket::get_multipart_upload(
				const std::string& oid,
				std::optional<std::string> upload_id,
				ACLOwner owner, ceph::real_time mtime) {
          /* TODO: reimplement this once TracerMultipartUploads are complete
    return std::make_unique<TracerMultipartUpload>(this->trace, this, oid, upload_id,
						std::move(owner), mtime);
            */
            return real_bucket->get_multipart_upload(oid, upload_id, owner, mtime);
  }

  int TracerBucket::list_multiparts(const DoutPrefixProvider *dpp,
				const std::string& prefix,
				std::string& marker,
				const std::string& delim,
				const int& max_uploads,
				std::vector<std::unique_ptr<MultipartUpload>>& uploads,
				std::map<std::string, bool> *common_prefixes,
				bool *is_truncated) {
    return real_bucket->list_multiparts(dpp, prefix, marker, delim, max_uploads, uploads, common_prefixes, is_truncated);
  }

  int TracerBucket::abort_multiparts(const DoutPrefixProvider* dpp,
  			 CephContext* cct) {
    return real_bucket->abort_multiparts(dpp, cct);
  }

  /*Tracer Driver functions */
  int TracerDriver::initialize(CephContext *_cct, const DoutPrefixProvider *_dpp) 
  {
    return real_store->initialize(_cct, _dpp);
  }

  std::unique_ptr<User> TracerDriver::get_user(const rgw_user &u)
  {
    dout(0) << "TRACER: intercepted operation: get_user" << dendl;
    std::unique_ptr<User> real_user;
    real_user = real_store->get_user(u);

    return make_unique<TracerUser>(this, u, real_user);
  }

  int TracerDriver::get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
  {
    ldpp_dout(dpp,20) << "TRACER: intercepted operation: get_user_by_access_key, key: " << key << dendl;
    User *u;
    RGWObjVersionTracker objv_tracker;
    int ret = 0;

    ret = this->real_store->get_user_by_access_key(dpp, key, y, user);
    if (ret < 0)
    {
      ldpp_dout(dpp, 20) << "TRACER: ret failure: " << ret << dendl;
      return ret;
    }
     
    
    u = new TracerUser(this, user->get()->get_info(), user);
    
    if (!u)
    {
      ldpp_dout(dpp, 20) << "TRACER: -ENOMEM" << dendl;
      return -ENOMEM;
    }

    u->get_version_tracker() = objv_tracker;
    user->reset(u);

    ldpp_dout(dpp,20) << "TRACER: returned operation: get_user_by_access_key" << dendl;

    return 0;
  }

  int TracerDriver::get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: get_user_by_email " << dendl;
    return real_store->get_user_by_email(dpp, email, y, user);
  }

  int TracerDriver::get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
  {
    /* Swift keys and subusers are not supported for now */
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: get_user_by_swift " << dendl;
    return real_store->get_user_by_swift(dpp, user_str, y, user);
  }

  std::string TracerDriver::get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: get_cluster_id " << dendl;
    return real_store->get_cluster_id(dpp, y); // for instance unique identifier
  }

  std::unique_ptr<Object> TracerDriver::get_object(const rgw_obj_key& k)
  {
    dout(20) << "TRACER: recieved operation: get_object" << dendl;
   
    return real_store->get_object(k);
  }

   int TracerDriver::get_bucket(const DoutPrefixProvider *dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
  {
    int ret;
    ldpp_dout(dpp, 20) << "TRACER: intercepting operation: get_bucket type 1, from store: " << this->get_name() << dendl;

    std::unique_ptr<Bucket> real_bucket;
    ret = this->real_store->get_bucket(dpp, u, b, &real_bucket, y);
    if (ret < 0)
      return ret;
    TracerBucket* bp = new TracerBucket(this, b, u, real_bucket);

    if (ret < 0)
    {
      delete bp;
      return ret;
    }

    if (!bp)
      return -ENOMEM;
    
    bucket->reset(bp);
    ldpp_dout(dpp, 20) << "TRACER: Returned from get_bucket type 1" << dendl;
    return ret;
  
  }

  int TracerDriver::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
  {
    dout(20) << "TRACER: intercepting operation: get_bucket type 2, from store: " << this->get_name() << dendl;

    Bucket * bp;
    bp = new TracerBucket(this, i, u);
    bp->get_info().placement_rule = bp->get_placement_rule();
    bucket->reset(bp);

    dout(20) << "TRACER: Returned from get_bucket type 2" << dendl;
    return 0;
  }

  int TracerDriver::get_bucket(const DoutPrefixProvider *dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
  {
    dout(20) << "TRACER: intercepting operation: get_bucket type 3, from store: " << this->get_name() << dendl;

    int ret;

    rgw_bucket b;
    b.tenant = tenant;
    b.name = name;

    ret = get_bucket(dpp, u, b, bucket, y);

    ldpp_dout(dpp,20) << "TRACER: returned operation: get_bucket type 3, from store: " << this->get_name() << dendl;
    return ret;
  }

  bool TracerDriver::is_meta_master()
  {
    return real_store->is_meta_master();
  }

  int TracerDriver::forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version *objv,
       bufferlist& in_data,
        JSONParser *jp, req_info& info,
        optional_yield y)
  {
    ldpp_dout(dpp, 20) << "TRACER: pure passthrough function: forward_request_to)_master " << dendl;
    return real_store->forward_request_to_master(dpp, user, objv, in_data, jp, info, y);
  }

    std::string TracerDriver::zone_unique_id(uint64_t unique_num)
  {
    dout(20) << "TRACER: pure passthrough function: zone_unique_id " << dendl;
    return real_store->zone_unique_id(unique_num);
  }

  std::string TracerDriver::zone_unique_trans_id(const uint64_t unique_num)
  {
    dout(20) << "TRACER: pure passthrough function: zone_unique_trans_id " << dendl;
    return real_store->zone_unique_trans_id(unique_num);
  }

  int TracerDriver::cluster_stat(RGWClusterStat& stats)
  {
    dout(20) << "TRACER: pure passthrough function: read_usage " << dendl;
    return real_store->cluster_stat(stats);
  }

  std::unique_ptr<Lifecycle> TracerDriver::get_lifecycle(void)
  {
    dout(20) << "TRACER: pure passthrough function: get_lifecycle " << dendl;
    return real_store->get_lifecycle();
  }

  std::unique_ptr<Completions> TracerDriver::get_completions(void)
  {
    dout(20) << "TRACER: pure passthrough function: get_completions " << dendl;
    return real_store->get_completions();
  }
  
  std::unique_ptr<Notification> TracerDriver::get_notification(
    rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s,
    rgw::notify::EventType event_type, const std::string* object_name)
  {
    dout(20) << "TRACER: pure passthrough function: read_notification type 1 " << dendl;
    return real_store->get_notification(obj, src_obj, s, event_type, object_name);
  }

  std::unique_ptr<Notification> TracerDriver::get_notification(
    const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, 
    rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant,
    std::string& _req_id, optional_yield y)
  {
    dout(20) << "TRACER: pure passthrough function: get_notification type 2 " << dendl;
    return real_store->get_notification(dpp, obj, src_obj, event_type, _bucket, _user_id, _user_tenant, _req_id, y);
  }

  RGWLC* TracerDriver::get_rgwlc(void) {
    dout(20) << "TRACER: pure passthrough function: get_rgwlc " << dendl;
    return real_store->get_rgwlc();
  }

  int TracerDriver::log_usage(const DoutPrefixProvider *dpp, std::map<rgw_user_bucket, RGWUsageBatch>& usage_info)
  {
    dout(20) << "TRACER: pure passthrough function: log_usage " << dendl;
    return real_store->log_usage(dpp, usage_info);
  }

  int TracerDriver::log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl)
  {
    dout(20) << "TRACER: pure passthrough function: log_op " << dendl;
    return real_store->log_op(dpp, oid, bl);
  }

  int TracerDriver::register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
       const std::map<std::string, std::string>& meta)
  {
    dout(20) << "TRACER: pure passthrough function: register_to_service_map " << dendl;
    return real_store->register_to_service_map(dpp, daemon_type, meta);
  }

  void TracerDriver::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit)
  {
    dout(20) << "TRACER: pure passthrough function: get_ratelimit " << dendl;
    return real_store->get_ratelimit(bucket_ratelimit, user_ratelimit, anon_ratelimit);
  }

  void TracerDriver::get_quota(RGWQuota& quota)
  {
    dout(20) << "TRACER: pure passthrough function: get_quota " << dendl;
    return real_store->get_quota(quota);
  }

  int TracerDriver::set_buckets_enabled(const DoutPrefixProvider *dpp, std::vector<rgw_bucket>& buckets, bool enabled)
  {
    dout(20) << "TRACER: pure passthrough function: set_buckets_enabled " << dendl;
    return real_store->set_buckets_enabled(dpp, buckets, enabled);
  }

  int TracerDriver::get_sync_policy_handler(const DoutPrefixProvider *dpp,
       std::optional<rgw_zone_id> zone,
        std::optional<rgw_bucket> bucket,
       RGWBucketSyncPolicyHandlerRef *phandler,
       optional_yield y)
  {
    dout(20) << "TRACER: pure passthrough function:get_sync_policy_handler " << dendl;
    return real_store->get_sync_policy_handler(dpp, zone, bucket, phandler, y);
  }

  RGWDataSyncStatusManager* TracerDriver::get_data_sync_manager(const rgw_zone_id& source_zone)
  {
    dout(20) << "TRACER: pure passthrough function: get_data_sync_manager " << dendl;
    return real_store->get_data_sync_manager(source_zone);
  }

  int TracerDriver::read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, 
        uint32_t max_entries, bool *is_truncated,
        RGWUsageIter& usage_iter,
        std::map<rgw_user_bucket, rgw_usage_log_entry>& usage)
  {
    dout(20) << "TRACER: pure passthrough function: read_all_usage " << dendl;
    return real_store->read_all_usage(dpp, start_epoch, end_epoch, max_entries, is_truncated, usage_iter, usage);
  }

  int TracerDriver::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
  {
    dout(20) << "TRACER: pure passthrough function: trim_all_usage " << dendl;
    return real_store->trim_all_usage(dpp, start_epoch, end_epoch);
  }

  int TracerDriver::get_config_key_val(string name, bufferlist *bl)
  {
    dout(20) << "TRACER: pure passthrough function: get_confog_key_val " << dendl;
    return real_store->get_config_key_val(name, bl);
  }

  int TracerDriver::meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle)
  {
    dout(20) << "TRACER: pure passthrough function: meta_list_keys_init " << dendl;
    return real_store->meta_list_keys_init(dpp, section, marker, phandle);
  }

  int TracerDriver::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, std::list<std::string>& keys, bool* truncated)
  {
    dout(20) << "TRACER: pure passthrough function: meta_list_keys_next " << dendl;
    return real_store->meta_list_keys_next(dpp, handle, max, keys, truncated);
  }

  void TracerDriver::meta_list_keys_complete(void* handle)
  {
    dout(20) << "TRACER: pure passthrough function: meta_list_keys_complete " << dendl;
    return real_store->meta_list_keys_complete(handle);
  }

  std::string TracerDriver::meta_get_marker(void* handle)
  {
    dout(20) << "TRACER: pure passthrough function: meta_get_marker " << dendl;
    return real_store->meta_get_marker(handle);
  }

  int TracerDriver::meta_remove(const DoutPrefixProvider *dpp, std::string& metadata_key, optional_yield y)
  {
    dout(20) << "TRACER: pure passthrough function: meta_remove " << dendl;
    return real_store->meta_remove(dpp, metadata_key, y);
  }

  std::unique_ptr<LuaScriptManager> TracerDriver::get_lua_script_manager()
  {
    dout(20) << "TRACER: pure passthrough function: get_lua_script_manager " << dendl;
    return real_store->get_lua_script_manager();
  }

  std::unique_ptr<RGWRole> TracerDriver::get_role(std::string name,
        std::string tenant,
        std::string path,
        std::string trust_policy,
        std::string max_session_duration_str,
        std::multimap<std::string,std::string> tags)
  {
    dout(20) << "TRACER: pure passthrough function: get_role type 1" << dendl;
    return real_store->get_role(name, tenant, path, trust_policy, max_session_duration_str, tags);
  }

  std::unique_ptr<RGWRole> TracerDriver::get_role(std::string id)
  {
    dout(20) << "TRACER: pure passthrough function: read_usage type 2" << dendl;
    return real_store->get_role(id);
  }

  int TracerDriver::get_roles(const DoutPrefixProvider *dpp,
        optional_yield y,
        const std::string& path_prefix,
        const std::string& tenant,
        std::vector<std::unique_ptr<RGWRole>>& roles)
  {
    dout(20) << "TRACER: pure passthrough function: get_roles " << dendl;
    return real_store->get_roles(dpp, y, path_prefix, tenant, roles);
  }

  std::unique_ptr<RGWOIDCProvider> TracerDriver::get_oidc_provider()
  {
    dout(20) << "TRACER: pure passthrough function: get_oidc_provider " << dendl;
    return real_store->get_oidc_provider();
  }

    int TracerDriver::get_oidc_providers(const DoutPrefixProvider *dpp,
        const std::string& tenant,
        std::vector<std::unique_ptr<RGWOIDCProvider>>& providers)
  {
    dout(20) << "TRACER: pure passthrough function: get_oidc_providers " << dendl;
    return real_store->get_oidc_providers(dpp, tenant, providers);
  }

  std::unique_ptr<Writer> TracerDriver::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size) 
  {
    dout(20) << "TRACER: pure passthrough function: get_append_writer " << dendl;
    return real_store->get_append_writer(dpp, y, std::move(_head_obj), owner, ptail_placement_rule, unique_tag, position, cur_accounted_size);
  }

  std::unique_ptr<Writer> TracerDriver::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag) 
  {
    dout(20) << "TRACER: pure passthrough function: get_atomic_writer " << dendl;
    return real_store->get_atomic_writer(dpp, y, std::move(_head_obj), owner, ptail_placement_rule, olh_epoch, unique_tag);
  }

  const std::string& TracerDriver::get_compression_type(const rgw_placement_rule& rule)
  {
    return real_store->get_compression_type(rule);
  }

  bool TracerDriver::valid_placement(const rgw_placement_rule& rule) 
  {
    return real_store->valid_placement(rule);
  }
  void TracerDriver::finalize(void)
  {
      if(real_store)
      real_store->finalize(); //May need to implement additional cleanup for this store itself.
  }
  
 } //namespace rgw::sal

 extern "C" {

    void* newTracer(const DoutPrefixProvider *dpp, rgw::sal::Store* inputStore) /*takes in a store and wraps */ //may need to also feed in a string for either rados or dbstore. Daniel P
    {
      
        rgw::sal::TracerDriver *trace = new rgw::sal::TracerDriver(); //TODO: make sure that the constructor is ready. Daniel P
        trace->initialize(inputStore);
        if (trace) {
            ldpp_dout(dpp, 0) << "TracerDriver initialized, intercepting traffic to store name: " << trace->get_name() << dendl; 
            return trace;
        }
        ldpp_dout(dpp, 0) << "ERROR: TracerDriver failed to link to store" << dendl;
        return NULL;
    }

 }