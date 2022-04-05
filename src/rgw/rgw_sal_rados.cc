// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc.
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
#include "rgw_sal_rados.h"
#include "rgw_bucket.h"
#include "rgw_multi.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"
#include "rgw_tracer.h"

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "rgw_service.h"
#include "rgw_lc.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"
#include "services/svc_quota.h"
#include "services/svc_config_key.h"
#include "services/svc_zone_utils.h"
#include "cls/rgw/cls_rgw_client.h"

#include "rgw_pubsub.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static string mp_ns = RGW_OBJ_NS_MULTIPART;

namespace rgw::sal {

// default number of entries to list with each bucket listing call
// (use marker to bridge between calls)
static constexpr size_t listing_max_entries = 1000;

static int decode_policy(CephContext* cct,
                         bufferlist& bl,
                         RGWAccessControlPolicy* policy)
{
  auto iter = bl.cbegin();
  try {
    policy->decode(iter);
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldout(cct, 15) << __func__ << " Read AccessControlPolicy";
    RGWAccessControlPolicy_S3* s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
    s3policy->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}

static int rgw_op_get_bucket_policy_from_attr(const DoutPrefixProvider* dpp,
					      RadosStore* store,
					      User* user,
					      Attrs& bucket_attrs,
					      RGWAccessControlPolicy* policy,
					      optional_yield y)
{
  auto aiter = bucket_attrs.find(RGW_ATTR_ACL);

  if (aiter != bucket_attrs.end()) {
    int ret = decode_policy(store->ctx(), aiter->second, policy);
    if (ret < 0)
      return ret;
  } else {
    ldout(store->ctx(), 0) << "WARNING: couldn't find acl header for bucket, generating default" << dendl;
    /* object exists, but policy is broken */
    int r = user->load_user(dpp, y);
    if (r < 0)
      return r;

    policy->create_default(user->get_id(), user->get_display_name());
  }
  return 0;
}

int RadosCompletions::drain()
{
  int ret = 0;
  while (!handles.empty()) {
    librados::AioCompletion* handle = handles.front();
    handles.pop_front();
    handle->wait_for_complete();
    int r = handle->get_return_value();
    handle->release();
    if (r < 0) {
      ret = r;
    }
  }
  return ret;
}

int RadosUser::list_buckets(const DoutPrefixProvider* dpp, const std::string& marker,
			       const std::string& end_marker, uint64_t max, bool need_stats,
			       BucketList &buckets, optional_yield y)
{
  RGWUserBuckets ulist;
  bool is_truncated = false;
  int ret;

  buckets.clear();
  ret = store->ctl()->user->list_buckets(dpp, info.user_id, marker, end_marker, max,
					 need_stats, &ulist, &is_truncated, y);
  if (ret < 0)
    return ret;

  buckets.set_truncated(is_truncated);
  for (const auto& ent : ulist.get_buckets()) {
    buckets.add(std::unique_ptr<Bucket>(new RadosBucket(this->store, ent.second, this)));
  }

  return 0;
}

int RadosUser::create_bucket(const DoutPrefixProvider* dpp,
				 const rgw_bucket& b,
				 const std::string& zonegroup_id,
				 rgw_placement_rule& placement_rule,
				 std::string& swift_ver_location,
				 const RGWQuotaInfo * pquota_info,
				 const RGWAccessControlPolicy& policy,
				 Attrs& attrs,
				 RGWBucketInfo& info,
				 obj_version& ep_objv,
				 bool exclusive,
				 bool obj_lock_enabled,
				 bool* existed,
				 req_info& req_info,
				 std::unique_ptr<Bucket>* bucket_out,
				 optional_yield y)
{
  int ret;
  bufferlist in_data;
  RGWBucketInfo master_info;
  rgw_bucket* pmaster_bucket;
  uint32_t* pmaster_num_shards;
  real_time creation_time;
  std::unique_ptr<Bucket> bucket;
  obj_version objv,* pobjv = NULL;

  /* If it exists, look it up; otherwise create it */
  ret = store->get_bucket(dpp, this, b, &bucket, y);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  if (ret != -ENOENT) {
    RGWAccessControlPolicy old_policy(store->ctx());
    *existed = true;
    if (swift_ver_location.empty()) {
      swift_ver_location = bucket->get_info().swift_ver_location;
    }
    placement_rule.inherit_from(bucket->get_info().placement_rule);

    // don't allow changes to the acl policy
    int r = rgw_op_get_bucket_policy_from_attr(dpp, store, this, bucket->get_attrs(),
					       &old_policy, y);
    if (r >= 0 && old_policy != policy) {
      bucket_out->swap(bucket);
      return -EEXIST;
    }
  } else {
    bucket = std::unique_ptr<Bucket>(new RadosBucket(store, b, this));
    *existed = false;
    bucket->set_attrs(attrs);
  }

  if (!store->svc()->zone->is_meta_master()) {
    JSONParser jp;
    ret = store->forward_request_to_master(dpp, this, NULL, in_data, &jp, req_info, y);
    if (ret < 0) {
      return ret;
    }

    JSONDecoder::decode_json("entry_point_object_ver", ep_objv, &jp);
    JSONDecoder::decode_json("object_ver", objv, &jp);
    JSONDecoder::decode_json("bucket_info", master_info, &jp);
    ldpp_dout(dpp, 20) << "parsed: objv.tag=" << objv.tag << " objv.ver=" << objv.ver << dendl;
    std::time_t ctime = ceph::real_clock::to_time_t(master_info.creation_time);
    ldpp_dout(dpp, 20) << "got creation time: << " << std::put_time(std::localtime(&ctime), "%F %T") << dendl;
    pmaster_bucket= &master_info.bucket;
    creation_time = master_info.creation_time;
    pmaster_num_shards = &master_info.layout.current_index.layout.normal.num_shards;
    pobjv = &objv;
    if (master_info.obj_lock_enabled()) {
      info.flags = BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
    }
  } else {
    pmaster_bucket = NULL;
    pmaster_num_shards = NULL;
    if (obj_lock_enabled)
      info.flags = BUCKET_VERSIONED | BUCKET_OBJ_LOCK_ENABLED;
  }

  std::string zid = zonegroup_id;
  if (zid.empty()) {
    zid = store->svc()->zone->get_zonegroup().get_id();
  }

  if (*existed) {
    rgw_placement_rule selected_placement_rule;
    ret = store->svc()->zone->select_bucket_placement(dpp, this->get_info(),
					       zid, placement_rule,
					       &selected_placement_rule, nullptr, y);
    if (selected_placement_rule != info.placement_rule) {
      ret = -EEXIST;
      bucket_out->swap(bucket);
      return ret;
    }
  } else {

    ret = store->getRados()->create_bucket(this->get_info(), bucket->get_key(),
				    zid, placement_rule, swift_ver_location, pquota_info,
				    attrs, info, pobjv, &ep_objv, creation_time,
				    pmaster_bucket, pmaster_num_shards, y, dpp,
				    exclusive);
    if (ret == -EEXIST) {
      *existed = true;
      /* bucket already existed, might have raced with another bucket creation,
       * or might be partial bucket creation that never completed. Read existing
       * bucket info, verify that the reported bucket owner is the current user.
       * If all is ok then update the user's list of buckets.  Otherwise inform
       * client about a name conflict.
       */
      if (info.owner.compare(this->get_id()) != 0) {
	return -EEXIST;
      }
      ret = 0;
    } else if (ret != 0) {
      return ret;
    }
  }

  bucket->set_version(ep_objv);
  bucket->get_info() = info;

  RadosBucket* rbucket = static_cast<RadosBucket*>(bucket.get());
  ret = rbucket->link(dpp, this, y, false);
  if (ret && !*existed && ret != -EEXIST) {
    /* if it exists (or previously existed), don't remove it! */
    ret = rbucket->unlink(dpp, this, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: failed to unlink bucket: ret=" << ret
		       << dendl;
    }
  } else if (ret == -EEXIST || (ret == 0 && *existed)) {
    ret = -ERR_BUCKET_EXISTS;
  }

  bucket_out->swap(bucket);

  return ret;
}

int RadosUser::read_attrs(const DoutPrefixProvider* dpp, optional_yield y)
{
  return store->ctl()->user->get_attrs_by_uid(dpp, get_id(), &attrs, y, &objv_tracker);
}

int RadosUser::merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y)
{
  for(auto& it : new_attrs) {
	  attrs[it.first] = it.second;
  }
  return store_user(dpp, y, false);
}

int RadosUser::read_stats(const DoutPrefixProvider *dpp,
                             optional_yield y, RGWStorageStats* stats,
			     ceph::real_time* last_stats_sync,
			     ceph::real_time* last_stats_update)
{
  return store->ctl()->user->read_stats(dpp, get_id(), stats, y, last_stats_sync, last_stats_update);
}

int RadosUser::read_stats_async(const DoutPrefixProvider *dpp, RGWGetUserStats_CB* cb)
{
  return store->ctl()->user->read_stats_async(dpp, get_id(), cb);
}

int RadosUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return store->ctl()->user->complete_flush_stats(dpp, get_id(), y);
}

int RadosUser::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool* is_truncated,
			       RGWUsageIter& usage_iter,
			       map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  std::string bucket_name;
  return store->getRados()->read_usage(dpp, get_id(), bucket_name, start_epoch,
				       end_epoch, max_entries, is_truncated,
				       usage_iter, usage);
}

int RadosUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  std::string bucket_name;

  return store->getRados()->trim_usage(dpp, get_id(), bucket_name, start_epoch, end_epoch);
}

int RadosUser::load_user(const DoutPrefixProvider* dpp, optional_yield y)
{
    return store->ctl()->user->get_info_by_uid(dpp, info.user_id, &info, y, RGWUserCtl::GetParams().set_objv_tracker(&objv_tracker).set_attrs(&attrs));
}

int RadosUser::store_user(const DoutPrefixProvider* dpp, optional_yield y, bool exclusive, RGWUserInfo* old_info)
{
    return store->ctl()->user->store_info(dpp, info, y,
					  RGWUserCtl::PutParams().set_objv_tracker(&objv_tracker)
					  .set_exclusive(exclusive)
					  .set_attrs(&attrs)
					  .set_old_info(old_info));
}

int RadosUser::remove_user(const DoutPrefixProvider* dpp, optional_yield y)
{
    return store->ctl()->user->remove_info(dpp, info, y,
					  RGWUserCtl::RemoveParams().set_objv_tracker(&objv_tracker));
}

RadosBucket::~RadosBucket() {}

int RadosBucket::remove_bucket(const DoutPrefixProvider* dpp,
			       bool delete_children,
			       bool forward_to_master,
			       req_info* req_info,
			       optional_yield y)
{
  int ret;

  // Refresh info
  ret = load_bucket(dpp, y);
  if (ret < 0) {
    return ret;
  }

  ListParams params;
  params.list_versions = true;
  params.allow_unordered = true;

  ListResults results;

  do {
    results.objs.clear();

    ret = list(dpp, params, 1000, results, y);
    if (ret < 0) {
      return ret;
    }

    if (!results.objs.empty() && !delete_children) {
      ldpp_dout(dpp, -1) << "ERROR: could not remove non-empty bucket " << info.bucket.name <<
	dendl;
      return -ENOTEMPTY;
    }

    for (const auto& obj : results.objs) {
      rgw_obj_key key(obj.key);
      /* xxx dang */
      ret = rgw_remove_object(dpp, store, this, key);
      if (ret < 0 && ret != -ENOENT) {
	return ret;
      }
    }
  } while(results.is_truncated);

  ret = abort_multiparts(dpp, store->ctx());
  if (ret < 0) {
    return ret;
  }

  // remove lifecycle config, if any (XXX note could be made generic)
  (void) store->getRados()->get_lc()->remove_bucket_config(
    this, get_attrs());

  ret = store->ctl()->bucket->sync_user_stats(dpp, info.owner, info, y, nullptr);
  if (ret < 0) {
     ldout(store->ctx(), 1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker ot;

  // if we deleted children above we will force delete, as any that
  // remain is detrius from a prior bug
  ret = store->getRados()->delete_bucket(info, ot, y, dpp, !delete_children);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: could not remove bucket " <<
      info.bucket.name << dendl;
    return ret;
  }

  // if bucket has notification definitions associated with it
  // they should be removed (note that any pending notifications on the bucket are still going to be sent)
  RGWPubSub ps(store, info.owner.tenant);
  RGWPubSub::Bucket ps_bucket(&ps, info.bucket);
  const auto ps_ret = ps_bucket.remove_notifications(dpp, y);
  if (ps_ret < 0 && ps_ret != -ENOENT) {
    ldpp_dout(dpp, -1) << "ERROR: unable to remove notifications from bucket. ret=" << ps_ret << dendl;
  }

  ret = store->ctl()->bucket->unlink_bucket(info.owner, info.bucket, y, dpp, false);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: unable to remove user bucket information" << dendl;
  }

  if (forward_to_master) {
    bufferlist in_data;
    ret = store->forward_request_to_master(dpp, owner, &ot.read_version, in_data, nullptr, *req_info, y);
    if (ret < 0) {
      if (ret == -ENOENT) {
	/* adjust error, we want to return with NoSuchBucket and not
	 * NoSuchKey */
	ret = -ERR_NO_SUCH_BUCKET;
      }
      return ret;
    }
  }

  return ret;
}

int RadosBucket::remove_bucket_bypass_gc(int concurrent_max, bool
					 keep_index_consistent,
					 optional_yield y, const
					 DoutPrefixProvider *dpp)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> stats;
  map<string, bool> common_prefixes;
  RGWObjectCtx obj_ctx(store);
  CephContext *cct = store->ctx();

  string bucket_ver, master_ver;

  ret = load_bucket(dpp, null_yield);
  if (ret < 0)
    return ret;

  ret = read_stats(dpp, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, NULL);
  if (ret < 0)
    return ret;

  ret = abort_multiparts(dpp, cct);
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.list_versions = true;
  params.allow_unordered = true;

  std::unique_ptr<rgw::sal::Completions> handles = store->get_completions();

  int max_aio = concurrent_max;
  results.is_truncated = true;

  while (results.is_truncated) {
    ret = list(dpp, params, listing_max_entries, results, null_yield);
    if (ret < 0)
      return ret;

    std::vector<rgw_bucket_dir_entry>::iterator it = results.objs.begin();
    for (; it != results.objs.end(); ++it) {
      RGWObjState *astate = NULL;
      std::unique_ptr<rgw::sal::Object> obj = get_object((*it).key);

      ret = obj->get_obj_state(dpp, &obj_ctx, &astate, y, false);
      if (ret == -ENOENT) {
        ldpp_dout(dpp, 1) << "WARNING: cannot find obj state for obj " << obj << dendl;
        continue;
      }
      if (ret < 0) {
        ldpp_dout(dpp, -1) << "ERROR: get obj state returned with error " << ret << dendl;
        return ret;
      }

      if (astate->manifest) {
        RGWObjManifest& manifest = *astate->manifest;
        RGWObjManifest::obj_iterator miter = manifest.obj_begin(dpp);
	std::unique_ptr<rgw::sal::Object> head_obj = get_object(manifest.get_obj().key);
        rgw_raw_obj raw_head_obj;
	dynamic_cast<RadosObject*>(head_obj.get())->get_raw_obj(&raw_head_obj);

        for (; miter != manifest.obj_end(dpp) && max_aio--; ++miter) {
          if (!max_aio) {
            ret = handles->drain();
            if (ret < 0) {
              ldpp_dout(dpp, -1) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
              return ret;
            }
            max_aio = concurrent_max;
          }

          rgw_raw_obj last_obj = miter.get_location().get_raw_obj(store);
          if (last_obj == raw_head_obj) {
            // have the head obj deleted at the end
            continue;
          }

          ret = store->delete_raw_obj_aio(dpp, last_obj, handles.get());
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR: delete obj aio failed with " << ret << dendl;
            return ret;
          }
        } // for all shadow objs

	ret = head_obj->delete_obj_aio(dpp, astate, handles.get(), keep_index_consistent, null_yield);
        if (ret < 0) {
          ldpp_dout(dpp, -1) << "ERROR: delete obj aio failed with " << ret << dendl;
          return ret;
        }
      }

      if (!max_aio) {
        ret = handles->drain();
        if (ret < 0) {
          ldpp_dout(dpp, -1) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
          return ret;
        }
        max_aio = concurrent_max;
      }
      obj_ctx.invalidate(obj->get_obj());
    } // for all RGW objects in results
  } // while is_truncated

  ret = handles->drain();
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
    return ret;
  }

  sync_user_stats(dpp, y);
  if (ret < 0) {
     ldpp_dout(dpp, 1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker objv_tracker;

  // this function can only be run if caller wanted children to be
  // deleted, so we can ignore the check for children as any that
  // remain are detritus from a prior bug
  ret = remove_bucket(dpp, true, false, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: could not remove bucket " << this << dendl;
    return ret;
  }

  return ret;
}

int RadosBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y, bool get_stats)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  int ret;

  RGWSI_MetaBackend_CtxParams bectx_params = RGWSI_MetaBackend_CtxParams_SObj(&obj_ctx);
  RGWObjVersionTracker ep_ot;
  if (info.bucket.bucket_id.empty()) {
    ret = store->ctl()->bucket->read_bucket_info(info.bucket, &info, y, dpp,
				      RGWBucketCtl::BucketInstance::GetParams()
				      .set_mtime(&mtime)
				      .set_attrs(&attrs)
                                      .set_bectx_params(bectx_params),
				      &ep_ot);
  } else {
    ret  = store->ctl()->bucket->read_bucket_instance_info(info.bucket, &info, y, dpp,
				      RGWBucketCtl::BucketInstance::GetParams()
				      .set_mtime(&mtime)
				      .set_attrs(&attrs)
				      .set_bectx_params(bectx_params));
  }
  if (ret != 0) {
    return ret;
  }

  bucket_version = ep_ot.read_version;

  if (get_stats) {
    ret = store->ctl()->bucket->read_bucket_stats(info.bucket, &ent, y, dpp);
  }

  return ret;
}

int RadosBucket::read_stats(const DoutPrefixProvider *dpp, int shard_id,
				     std::string* bucket_ver, std::string* master_ver,
				     std::map<RGWObjCategory, RGWStorageStats>& stats,
				     std::string* max_marker, bool* syncstopped)
{
  return store->getRados()->get_bucket_stats(dpp, info, shard_id, bucket_ver, master_ver, stats, max_marker, syncstopped);
}

int RadosBucket::read_stats_async(const DoutPrefixProvider *dpp, int shard_id, RGWGetBucketStats_CB* ctx)
{
  return store->getRados()->get_bucket_stats_async(dpp, get_info(), shard_id, ctx);
}

int RadosBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return store->ctl()->bucket->sync_user_stats(dpp, owner->get_id(), info, y, &ent);
}

int RadosBucket::update_container_stats(const DoutPrefixProvider* dpp)
{
  int ret;
  map<std::string, RGWBucketEnt> m;

  m[info.bucket.name] = ent;
  ret = store->getRados()->update_containers_stats(m, dpp);
  if (!ret)
    return -EEXIST;
  if (ret < 0)
    return ret;

  map<std::string, RGWBucketEnt>::iterator iter = m.find(info.bucket.name);
  if (iter == m.end())
    return -EINVAL;

  ent.count = iter->second.count;
  ent.size = iter->second.size;
  ent.size_rounded = iter->second.size_rounded;
  ent.creation_time = iter->second.creation_time;
  ent.placement_rule = std::move(iter->second.placement_rule);

  info.creation_time = ent.creation_time;
  info.placement_rule = ent.placement_rule;

  return 0;
}

int RadosBucket::check_bucket_shards(const DoutPrefixProvider* dpp)
{
      return store->getRados()->check_bucket_shards(info, info.bucket, get_count(), dpp);
}

int RadosBucket::link(const DoutPrefixProvider* dpp, User* new_user, optional_yield y, bool update_entrypoint, RGWObjVersionTracker* objv)
{
  RGWBucketEntryPoint ep;
  ep.bucket = info.bucket;
  ep.owner = new_user->get_id();
  ep.creation_time = get_creation_time();
  ep.linked = true;
  Attrs ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  int r = store->ctl()->bucket->link_bucket(new_user->get_id(), info.bucket,
					    get_creation_time(), y, dpp, update_entrypoint,
					    &ep_data);
  if (r < 0)
    return r;

  if (objv)
    *objv = ep_data.ep_objv;

  return r;
}

int RadosBucket::unlink(const DoutPrefixProvider* dpp, User* new_user, optional_yield y, bool update_entrypoint)
{
  return store->ctl()->bucket->unlink_bucket(new_user->get_id(), info.bucket, y, dpp, update_entrypoint);
}

int RadosBucket::chown(const DoutPrefixProvider* dpp, User* new_user, User* old_user, optional_yield y, const std::string* marker)
{
  std::string obj_marker;

  if (marker == nullptr)
    marker = &obj_marker;

  int r = this->link(dpp, new_user, y);
  if (r < 0) {
    return r;
  }
  if (!old_user) {
    return r;
  }

  return store->ctl()->bucket->chown(store, this, new_user->get_id(),
			   old_user->get_display_name(), *marker, y, dpp);
}

int RadosBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time _mtime)
{
  mtime = _mtime;
  return store->getRados()->put_bucket_instance_info(info, exclusive, mtime, &attrs, dpp);
}

/* Make sure to call get_bucket_info() if you need it first */
bool RadosBucket::is_owner(User* user)
{
  return (info.owner.compare(user->get_id()) == 0);
}

int RadosBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  return store->getRados()->check_bucket_empty(dpp, info, y);
}

int RadosBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
    return store->getRados()->check_quota(dpp, owner->get_id(), get_key(),
					  user_quota, bucket_quota, obj_size, y, check_size_only);
}

int RadosBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y)
{
  for(auto& it : new_attrs) {
	  attrs[it.first] = it.second;
  }
  return store->ctl()->bucket->set_bucket_instance_attrs(get_info(),
				new_attrs, &get_info().objv_tracker, y, dpp);
}

int RadosBucket::try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime)
{
  return store->getRados()->try_refresh_bucket_info(info, pmtime, dpp, &attrs);
}

int RadosBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool* is_truncated,
			       RGWUsageIter& usage_iter,
			       map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return store->getRados()->read_usage(dpp, owner->get_id(), get_name(), start_epoch,
				       end_epoch, max_entries, is_truncated,
				       usage_iter, usage);
}

int RadosBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  return store->getRados()->trim_usage(dpp, owner->get_id(), get_name(), start_epoch, end_epoch);
}

int RadosBucket::remove_objs_from_index(const DoutPrefixProvider *dpp, std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return store->getRados()->remove_objs_from_index(dpp, info, objs_to_unlink);
}

int RadosBucket::check_index(const DoutPrefixProvider *dpp, std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return store->getRados()->bucket_check_index(dpp, info, &existing_stats, &calculated_stats);
}

int RadosBucket::rebuild_index(const DoutPrefixProvider *dpp)
{
  return store->getRados()->bucket_rebuild_index(dpp, info);
}

int RadosBucket::set_tag_timeout(const DoutPrefixProvider *dpp, uint64_t timeout)
{
  return store->getRados()->cls_obj_set_bucket_tag_timeout(dpp, info, timeout);
}

int RadosBucket::purge_instance(const DoutPrefixProvider* dpp)
{
  int max_shards = (info.layout.current_index.layout.normal.num_shards > 0 ? info.layout.current_index.layout.normal.num_shards : 1);
  for (int i = 0; i < max_shards; i++) {
    RGWRados::BucketShard bs(store->getRados());
    int shard_id = (info.layout.current_index.layout.normal.num_shards > 0  ? i : -1);
    int ret = bs.init(info.bucket, shard_id, info.layout.current_index, nullptr, dpp);
    if (ret < 0) {
      cerr << "ERROR: bs.init(bucket=" << info.bucket << ", shard=" << shard_id
           << "): " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    ret = store->getRados()->bi_remove(dpp, bs);
    if (ret < 0) {
      cerr << "ERROR: failed to remove bucket index object: "
           << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  }
  return 0;
}

int RadosBucket::set_acl(const DoutPrefixProvider* dpp, RGWAccessControlPolicy &acl, optional_yield y)
{
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);
  map<string, bufferlist>& attrs = get_attrs();

  attrs[RGW_ATTR_ACL] = aclbl;
  info.owner = acl.get_owner().get_id();

  int r = store->ctl()->bucket->store_bucket_instance_info(info.bucket,
                 info, y, dpp,
                 RGWBucketCtl::BucketInstance::PutParams().set_attrs(&attrs));
  if (r < 0) {
    cerr << "ERROR: failed to set bucket owner: " << cpp_strerror(-r) << std::endl;
    return r;
  }
  
  return 0;
}

std::unique_ptr<Object> RadosBucket::get_object(const rgw_obj_key& k)
{
  return std::make_unique<RadosObject>(this->store, k, this);
}

int RadosBucket::list(const DoutPrefixProvider* dpp, ListParams& params, int max, ListResults& results, optional_yield y)
{
  RGWRados::Bucket target(store->getRados(), get_info());
  if (params.shard_id >= 0) {
    target.set_shard_id(params.shard_id);
  }
  RGWRados::Bucket::List list_op(&target);

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

  int ret = list_op.list_objects(dpp, max, &results.objs, &results.common_prefixes, &results.is_truncated, y);
  if (ret >= 0) {
    results.next_marker = list_op.get_next_marker();
    params.marker = results.next_marker;
  }

  return ret;
}

std::unique_ptr<MultipartUpload> RadosBucket::get_multipart_upload(
				  const std::string& oid,
				  std::optional<std::string> upload_id,
				  ACLOwner owner, ceph::real_time mtime)
{
  return std::make_unique<RadosMultipartUpload>(this->store, this, oid, upload_id,
						std::move(owner), mtime);
}

int RadosBucket::list_multiparts(const DoutPrefixProvider *dpp,
				 const string& prefix,
				 string& marker,
				 const string& delim,
				 const int& max_uploads,
				 vector<std::unique_ptr<MultipartUpload>>& uploads,
				 map<string, bool> *common_prefixes,
				 bool *is_truncated)
{
  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;
  MultipartMetaFilter mp_filter;

  params.prefix = prefix;
  params.delim = delim;
  params.marker = marker;
  params.ns = RGW_OBJ_NS_MULTIPART;
  params.access_list_filter = &mp_filter;

  int ret = list(dpp, params, max_uploads, results, null_yield);

  if (ret < 0)
    return ret;

  if (!results.objs.empty()) {
    for (const rgw_bucket_dir_entry& dentry : results.objs) {
      rgw_obj_key key(dentry.key);
      ACLOwner owner(rgw_user(dentry.meta.owner));
      owner.set_name(dentry.meta.owner_display_name);
      uploads.push_back(this->get_multipart_upload(key.name,
			std::nullopt, std::move(owner)));
    }
  }
  if (common_prefixes) {
    *common_prefixes = std::move(results.common_prefixes);
  }
  *is_truncated = results.is_truncated;
  marker = params.marker.name;

  return 0;
}

int RadosBucket::abort_multiparts(const DoutPrefixProvider* dpp,
				  CephContext* cct)
{
  constexpr int max = 1000;
  int ret, num_deleted = 0;
  vector<std::unique_ptr<MultipartUpload>> uploads;
  RGWObjectCtx obj_ctx(store);
  string marker;
  bool is_truncated;

  const std::string empty_delim;
  const std::string empty_prefix;

  do {
    ret = list_multiparts(dpp, empty_prefix, marker, empty_delim,
			  max, uploads, nullptr, &is_truncated);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ <<
	" ERROR : calling list_bucket_multiparts; ret=" << ret <<
	"; bucket=\"" << this << "\"" << dendl;
      return ret;
    }
    ldpp_dout(dpp, 20) << __func__ <<
      " INFO: aborting and cleaning up multipart upload(s); bucket=\"" <<
      this << "\"; uploads.size()=" << uploads.size() <<
      "; is_truncated=" << is_truncated << dendl;

    if (!uploads.empty()) {
      for (const auto& upload : uploads) {
	ret = upload->abort(dpp, cct, &obj_ctx);
        if (ret < 0) {
	  // we're doing a best-effort; if something cannot be found,
	  // log it and keep moving forward
	  if (ret != -ENOENT && ret != -ERR_NO_SUCH_UPLOAD) {
	    ldpp_dout(dpp, 0) << __func__ <<
	      " ERROR : failed to abort and clean-up multipart upload \"" <<
	      upload->get_meta() << "\"" << dendl;
	    return ret;
	  } else {
	    ldpp_dout(dpp, 10) << __func__ <<
	      " NOTE : unable to find part(s) of "
	      "aborted multipart upload of \"" << upload->get_meta() <<
	      "\" for cleaning up" << dendl;
	  }
        }
        num_deleted++;
      }
      if (num_deleted) {
        ldpp_dout(dpp, 0) << __func__ <<
	  " WARNING : aborted " << num_deleted <<
	  " incomplete multipart uploads" << dendl;
      }
    }
  } while (is_truncated);

  return 0;
}

std::unique_ptr<User> RadosStore::get_user(const rgw_user &u)
{
  return std::make_unique<RadosUser>(this, u);
}

std::string RadosStore::get_cluster_id(const DoutPrefixProvider* dpp,  optional_yield y)
{
  return getRados()->get_cluster_fsid(dpp, y);
}

int RadosStore::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  RGWUserInfo uinfo;
  User* u;
  RGWObjVersionTracker objv_tracker;

  int r = ctl()->user->get_info_by_access_key(dpp, key, &uinfo, y, RGWUserCtl::GetParams().set_objv_tracker(&objv_tracker));
  if (r < 0)
    return r;

  u = new RadosUser(this, uinfo);
  if (!u)
    return -ENOMEM;

  u->get_version_tracker() = objv_tracker;

  user->reset(u);
  return 0;
}

int RadosStore::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  RGWUserInfo uinfo;
  User* u;
  RGWObjVersionTracker objv_tracker;

  int r = ctl()->user->get_info_by_email(dpp, email, &uinfo, y, RGWUserCtl::GetParams().set_objv_tracker(&objv_tracker));
  if (r < 0)
    return r;

  u = new RadosUser(this, uinfo);
  if (!u)
    return -ENOMEM;

  u->get_version_tracker() = objv_tracker;

  user->reset(u);
  return 0;
}

int RadosStore::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  RGWUserInfo uinfo;
  User* u;
  RGWObjVersionTracker objv_tracker;

  int r = ctl()->user->get_info_by_swift(dpp, user_str, &uinfo, y, RGWUserCtl::GetParams().set_objv_tracker(&objv_tracker));
  if (r < 0)
    return r;

  u = new RadosUser(this, uinfo);
  if (!u)
    return -ENOMEM;

  u->get_version_tracker() = objv_tracker;

  user->reset(u);
  return 0;
}

std::unique_ptr<Object> RadosStore::get_object(const rgw_obj_key& k)
{
  return std::make_unique<RadosObject>(this, k);
}

int RadosStore::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  int ret;
  Bucket* bp;

  bp = new RadosBucket(this, b, u);
  ret = bp->load_bucket(dpp, y);
  if (ret < 0) {
    delete bp;
    return ret;
  }

  bucket->reset(bp);
  return 0;
}

int RadosStore::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  Bucket* bp;

  bp = new RadosBucket(this, i, u);
  /* Don't need to fetch the bucket info, use the provided one */

  bucket->reset(bp);
  return 0;
}

int RadosStore::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(dpp, u, b, bucket, y);
}

bool RadosStore::is_meta_master()
{
  return svc()->zone->is_meta_master();
}

int RadosStore::forward_request_to_master(const DoutPrefixProvider *dpp, User* user, obj_version* objv,
					     bufferlist& in_data,
					     JSONParser* jp, req_info& info,
					     optional_yield y)
{
  if (is_meta_master()) {
    /* We're master, don't forward */
    return 0;
  }

  if (!svc()->zone->get_master_conn()) {
    ldpp_dout(dpp, 0) << "rest connection is invalid" << dendl;
    return -EINVAL;
  }
  ldpp_dout(dpp, 0) << "sending request to master zonegroup" << dendl;
  bufferlist response;
  std::string uid_str = user->get_id().to_str();
#define MAX_REST_RESPONSE (128 * 1024) // we expect a very small response
  int ret = svc()->zone->get_master_conn()->forward(dpp, rgw_user(uid_str), info,
                                                    objv, MAX_REST_RESPONSE,
						    &in_data, &response, y);
  if (ret < 0)
    return ret;

  ldpp_dout(dpp, 20) << "response: " << response.c_str() << dendl;
  if (jp && !jp->parse(response.c_str(), response.length())) {
    ldpp_dout(dpp, 0) << "failed parsing response from master zonegroup" << dendl;
    return -EINVAL;
  }

  return 0;
}

std::string RadosStore::zone_unique_id(uint64_t unique_num)
{
  return svc()->zone_utils->unique_id(unique_num);
}

std::string RadosStore::zone_unique_trans_id(const uint64_t unique_num)
{
  return svc()->zone_utils->unique_trans_id(unique_num);
}

int RadosStore::cluster_stat(RGWClusterStat& stats)
{
  rados_cluster_stat_t rados_stats;
  int ret;

  ret = rados->get_rados_handle()->cluster_stat(rados_stats);
  if (ret < 0)
    return ret;

  stats.kb = rados_stats.kb;
  stats.kb_used = rados_stats.kb_used;
  stats.kb_avail = rados_stats.kb_avail;
  stats.num_objects = rados_stats.num_objects;

  return ret;
}

std::unique_ptr<Lifecycle> RadosStore::get_lifecycle(void)
{
  return std::make_unique<RadosLifecycle>(this);
}

std::unique_ptr<Completions> RadosStore::get_completions(void)
{
  return std::make_unique<RadosCompletions>();
}

std::unique_ptr<Notification> RadosStore::get_notification(
  rgw::sal::Object* obj, rgw::sal::Object* src_obj, struct req_state* s, rgw::notify::EventType event_type, const std::string* object_name)
{
  return std::make_unique<RadosNotification>(s, this, obj, src_obj, s, event_type, object_name);
}

std::unique_ptr<Notification> RadosStore::get_notification(const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, RGWObjectCtx* rctx, rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant, std::string& _req_id, optional_yield y)
{
  return std::make_unique<RadosNotification>(dpp, this, obj, src_obj, rctx, event_type, _bucket, _user_id, _user_tenant, _req_id, y);
}

int RadosStore::delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj)
{
  return rados->delete_raw_obj(dpp, obj);
}

int RadosStore::delete_raw_obj_aio(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, Completions* aio)
{
  RadosCompletions* raio = static_cast<RadosCompletions*>(aio);

  return rados->delete_raw_obj_aio(dpp, obj, raio->handles);
}

void RadosStore::get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj)
{
    rados->obj_to_raw(placement_rule, obj, raw_obj);
}

int RadosStore::get_raw_chunk_size(const DoutPrefixProvider* dpp, const rgw_raw_obj& obj, uint64_t* chunk_size)
{
  return rados->get_max_chunk_size(obj.pool, chunk_size, dpp);
}

int RadosStore::log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info)
{
    return rados->log_usage(dpp, usage_info);
}

int RadosStore::log_op(const DoutPrefixProvider *dpp, std::string& oid, bufferlist& bl)
{
  rgw_raw_obj obj(svc()->zone->get_zone_params().log_pool, oid);

  int ret = rados->append_async(dpp, obj, bl.length(), bl);
  if (ret == -ENOENT) {
    ret = rados->create_pool(dpp, svc()->zone->get_zone_params().log_pool);
    if (ret < 0)
      return ret;
    // retry
    ret = rados->append_async(dpp, obj, bl.length(), bl);
  }

  return ret;
}

int RadosStore::register_to_service_map(const DoutPrefixProvider *dpp, const std::string& daemon_type,
					   const map<std::string, std::string>& meta)
{
  return rados->register_to_service_map(dpp, daemon_type, meta);
}

void RadosStore::get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota)
{
    bucket_quota = svc()->quota->get_bucket_quota();
    user_quota = svc()->quota->get_user_quota();
}

void RadosStore::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit)
{
  bucket_ratelimit = svc()->zone->get_current_period().get_config().bucket_ratelimit;
  user_ratelimit = svc()->zone->get_current_period().get_config().user_ratelimit;
  anon_ratelimit = svc()->zone->get_current_period().get_config().anon_ratelimit;
}

int RadosStore::set_buckets_enabled(const DoutPrefixProvider* dpp, vector<rgw_bucket>& buckets, bool enabled)
{
    return rados->set_buckets_enabled(buckets, enabled, dpp);
}

int RadosStore::get_sync_policy_handler(const DoutPrefixProvider* dpp,
					   std::optional<rgw_zone_id> zone,
					   std::optional<rgw_bucket> bucket,
					   RGWBucketSyncPolicyHandlerRef* phandler,
					   optional_yield y)
{
  return ctl()->bucket->get_sync_policy_handler(zone, bucket, phandler, y, dpp);
}

RGWDataSyncStatusManager* RadosStore::get_data_sync_manager(const rgw_zone_id& source_zone)
{
  return rados->get_data_sync_manager(source_zone);
}

int RadosStore::read_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
				  uint32_t max_entries, bool* is_truncated,
				  RGWUsageIter& usage_iter,
				  map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  rgw_user uid;
  std::string bucket_name;

  return rados->read_usage(dpp, uid, bucket_name, start_epoch, end_epoch, max_entries,
			   is_truncated, usage_iter, usage);
}

int RadosStore::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  rgw_user uid;
  std::string bucket_name;

  return rados->trim_usage(dpp, uid, bucket_name, start_epoch, end_epoch);
}

int RadosStore::get_config_key_val(std::string name, bufferlist* bl)
{
  return svc()->config_key->get(name, true, bl);
}

int RadosStore::meta_list_keys_init(const DoutPrefixProvider *dpp, const std::string& section, const std::string& marker, void** phandle)
{
  return ctl()->meta.mgr->list_keys_init(dpp, section, marker, phandle);
}

int RadosStore::meta_list_keys_next(const DoutPrefixProvider *dpp, void* handle, int max, list<std::string>& keys, bool* truncated)
{
  return ctl()->meta.mgr->list_keys_next(dpp, handle, max, keys, truncated);
}

void RadosStore::meta_list_keys_complete(void* handle)
{
  ctl()->meta.mgr->list_keys_complete(handle);
}

std::string RadosStore::meta_get_marker(void* handle)
{
  return ctl()->meta.mgr->get_marker(handle);
}

int RadosStore::meta_remove(const DoutPrefixProvider* dpp, std::string& metadata_key, optional_yield y)
{
  return ctl()->meta.mgr->remove(metadata_key, y, dpp);
}

int RadosStore::list_users(const DoutPrefixProvider *dpp, const std::string& metadata_key,
                        std::string& marker, int max_entries, void *&handle,
                        bool* truncated, std::list<std::string>& users)
{
  int max = 1000;
  uint64_t left;
  uint64_t count = 0;

  if (max_entries > max) {
      max_entries = max;
  }
  int ret = meta_list_keys_init(dpp, metadata_key, marker, &handle);
  if (ret < 0){
    return ret;
  }
  do {
    std::list<std::string> keys;
    left = ((max_entries > 0) ? max_entries - count : max);
    ret = meta_list_keys_next(dpp, handle, left, keys, truncated);
    if (ret < 0 && ret != -ENOENT) {
      return ret;
    }
    count += keys.size();
    users.splice(users.end(), keys);
  } while (*truncated && left > 0);

  marker = meta_get_marker(handle);
  meta_list_keys_complete(handle);
  return ret;
}

void RadosStore::finalize(void)
{
  if (rados)
    rados->finalize();
}

std::unique_ptr<LuaScriptManager> RadosStore::get_lua_script_manager()
{
  return std::make_unique<RadosLuaScriptManager>(this);
}

std::unique_ptr<RGWRole> RadosStore::get_role(std::string name,
					      std::string tenant,
					      std::string path,
					      std::string trust_policy,
					      std::string max_session_duration_str,
                std::multimap<std::string,std::string> tags)
{
  return std::make_unique<RadosRole>(this, name, tenant, path, trust_policy, max_session_duration_str, tags);
}

std::unique_ptr<RGWRole> RadosStore::get_role(std::string id)
{
  return std::make_unique<RadosRole>(this, id);
}

int RadosStore::get_roles(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  const std::string& path_prefix,
			  const std::string& tenant,
			  vector<std::unique_ptr<RGWRole>>& roles)
{
  auto pool = get_zone()->get_params().roles_pool;
  std::string prefix;

  // List all roles if path prefix is empty
  if (! path_prefix.empty()) {
    prefix = tenant + RGWRole::role_path_oid_prefix + path_prefix;
  } else {
    prefix = tenant + RGWRole::role_path_oid_prefix;
  }

  //Get the filtered objects
  list<std::string> result;
  bool is_truncated;
  RGWListRawObjsCtx ctx;
  do {
    list<std::string> oids;
    int r = rados->list_raw_objects(dpp, pool, prefix, 1000, ctx, oids, &is_truncated);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: listing filtered objects failed: "
                  << prefix << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    for (const auto& iter : oids) {
      result.push_back(iter.substr(RGWRole::role_path_oid_prefix.size()));
    }
  } while (is_truncated);

  for (const auto& it : result) {
    //Find the role oid prefix from the end
    size_t pos = it.rfind(RGWRole::role_oid_prefix);
    if (pos == std::string::npos) {
        continue;
    }
    // Split the result into path and info_oid + id
    std::string path = it.substr(0, pos);

    /*Make sure that prefix is part of path (False results could've been returned)
      because of the role info oid + id appended to the path)*/
    if(path_prefix.empty() || path.find(path_prefix) != std::string::npos) {
      //Get id from info oid prefix + id
      std::string id = it.substr(pos + RGWRole::role_oid_prefix.length());

      std::unique_ptr<rgw::sal::RGWRole> role = get_role(id);
      int ret = role->read_info(dpp, y);
      if (ret < 0) {
        return ret;
      }
      roles.push_back(std::move(role));
    }
  }

  return 0;
}

std::unique_ptr<RGWOIDCProvider> RadosStore::get_oidc_provider()
{
  return std::make_unique<RadosOIDCProvider>(this);
}

int RadosStore::get_oidc_providers(const DoutPrefixProvider *dpp,
				   const std::string& tenant,
				   vector<std::unique_ptr<RGWOIDCProvider>>& providers)
{
  std::string prefix = tenant + RGWOIDCProvider::oidc_url_oid_prefix;
  auto pool = zone.get_params().oidc_pool;
  auto obj_ctx = svc()->sysobj->init_obj_ctx();

  //Get the filtered objects
  list<std::string> result;
  bool is_truncated;
  RGWListRawObjsCtx ctx;
  do {
    list<std::string> oids;
    int r = rados->list_raw_objects(dpp, pool, prefix, 1000, ctx, oids, &is_truncated);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: listing filtered objects failed: OIDC pool: "
                  << pool.name << ": " << prefix << ": " << cpp_strerror(-r) << dendl;
      return r;
    }
    for (const auto& iter : oids) {
      std::unique_ptr<rgw::sal::RGWOIDCProvider> provider = get_oidc_provider();
      bufferlist bl;

      r = rgw_get_system_obj(obj_ctx, pool, iter, bl, nullptr, nullptr, null_yield, dpp);
      if (r < 0) {
        return r;
      }

      try {
        using ceph::decode;
        auto iter = bl.cbegin();
        decode(*provider, iter);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, 0) << "ERROR: failed to decode oidc provider info from pool: "
	  << pool.name << ": " << iter << dendl;
        return -EIO;
      }

      providers.push_back(std::move(provider));
    }
  } while (is_truncated);

  return 0;
}

std::unique_ptr<Writer> RadosStore::get_append_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  auto aio = rgw::make_throttle(ctx()->_conf->rgw_put_obj_min_window_size, y);
  return std::make_unique<RadosAppendWriter>(dpp, y,
				 std::move(_head_obj),
				 this, std::move(aio), owner, obj_ctx,
				 ptail_placement_rule,
				 unique_tag, position,
				 cur_accounted_size);
}

std::unique_ptr<Writer> RadosStore::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  auto aio = rgw::make_throttle(ctx()->_conf->rgw_put_obj_min_window_size, y);
  return std::make_unique<RadosAtomicWriter>(dpp, y,
				 std::move(_head_obj),
				 this, std::move(aio), owner, obj_ctx,
				 ptail_placement_rule,
				 olh_epoch, unique_tag);
}

int RadosStore::get_obj_head_ioctx(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx* ioctx)
{
  return rados->get_obj_head_ioctx(dpp, bucket_info, obj, ioctx);
}

RadosObject::~RadosObject() {}

int RadosObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, RGWObjState **state, optional_yield y, bool follow_olh)
{
  return store->getRados()->get_obj_state(dpp, rctx, bucket->get_info(), get_obj(), state, follow_olh, y);
}

int RadosObject::read_attrs(const DoutPrefixProvider* dpp, RGWRados::Object::Read &read_op, optional_yield y, rgw_obj* target_obj)
{
  read_op.params.attrs = &attrs;
  read_op.params.target_obj = target_obj;
  read_op.params.obj_size = &obj_size;
  read_op.params.lastmod = &mtime;

  return read_op.prepare(y, dpp);
}

int RadosObject::set_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, Attrs* setattrs, Attrs* delattrs, optional_yield y, rgw_obj* target_obj)
{
  Attrs empty;
  rgw_obj target = get_obj();

  if (!target_obj)
    target_obj = &target;

  return store->getRados()->set_attrs(dpp, rctx,
			bucket->get_info(),
			*target_obj,
			setattrs ? *setattrs : empty,
			delattrs ? delattrs : nullptr,
			y);
}

int RadosObject::get_obj_attrs(RGWObjectCtx* rctx, optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
{
  RGWRados::Object op_target(store->getRados(), bucket->get_info(), *rctx, get_obj());
  RGWRados::Object::Read read_op(&op_target);

  return read_attrs(dpp, read_op, y, target_obj);
}

int RadosObject::modify_obj_attrs(RGWObjectCtx* rctx, const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
{
  rgw_obj target = get_obj();
  int r = get_obj_attrs(rctx, y, dpp, &target);
  if (r < 0) {
    return r;
  }
  set_atomic(rctx);
  attrs[attr_name] = attr_val;
  return set_obj_attrs(dpp, rctx, &attrs, nullptr, y, &target);
}

int RadosObject::delete_obj_attrs(const DoutPrefixProvider* dpp, RGWObjectCtx* rctx, const char* attr_name, optional_yield y)
{
  Attrs rmattr;
  bufferlist bl;

  set_atomic(rctx);
  rmattr[attr_name] = bl;
  return set_obj_attrs(dpp, rctx, nullptr, &rmattr, y);
}

void RadosObject::set_compressed(RGWObjectCtx* rctx) {
  rgw_obj obj = get_obj();
  store->getRados()->set_compressed(rctx, obj);
}

void RadosObject::set_atomic(RGWObjectCtx* rctx) const
{
  rgw_obj obj = get_obj();
  store->getRados()->set_atomic(rctx, obj);
}

void RadosObject::set_prefetch_data(RGWObjectCtx* rctx)
{
  rgw_obj obj = get_obj();
  store->getRados()->set_prefetch_data(rctx, obj);
}

bool RadosObject::is_expired() {
  auto iter = attrs.find(RGW_ATTR_DELETE_AT);
  if (iter != attrs.end()) {
    utime_t delete_at;
    try {
      auto bufit = iter->second.cbegin();
      decode(delete_at, bufit);
    } catch (buffer::error& err) {
      ldout(store->ctx(), 0) << "ERROR: " << __func__ << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
      return false;
    }

    if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
      return true;
    }
  }

  return false;
}

void RadosObject::gen_rand_obj_instance_name()
{
  store->getRados()->gen_rand_obj_instance_name(&key);
}

void RadosObject::raw_obj_to_obj(const rgw_raw_obj& raw_obj)
{
  rgw_obj tobj = get_obj();
  RGWSI_Tier_RADOS::raw_obj_to_obj(get_bucket()->get_key(), raw_obj, &tobj);
  set_key(tobj.key);
}

void RadosObject::get_raw_obj(rgw_raw_obj* raw_obj)
{
  store->getRados()->obj_to_raw((bucket->get_info()).placement_rule, get_obj(), raw_obj);
}

int RadosObject::omap_get_vals(const DoutPrefixProvider *dpp, const std::string& marker, uint64_t count,
				  std::map<std::string, bufferlist> *m,
				  bool* pmore, optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  rgw_raw_obj raw_obj;
  get_raw_obj(&raw_obj);
  auto sysobj = obj_ctx.get_obj(raw_obj);

  return sysobj.omap().get_vals(dpp, marker, count, m, pmore, y);
}

int RadosObject::omap_get_all(const DoutPrefixProvider *dpp, std::map<std::string, bufferlist> *m,
				 optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  rgw_raw_obj raw_obj;
  get_raw_obj(&raw_obj);
  auto sysobj = obj_ctx.get_obj(raw_obj);

  return sysobj.omap().get_all(dpp, m, y);
}

int RadosObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp, const std::string& oid,
					  const std::set<std::string>& keys,
					  Attrs* vals)
{
  int ret;
  rgw_raw_obj head_obj;
  librados::IoCtx cur_ioctx;
  rgw_obj obj = get_obj();

  store->getRados()->obj_to_raw(bucket->get_placement_rule(), obj, &head_obj);
  ret = store->get_obj_head_ioctx(dpp, bucket->get_info(), obj, &cur_ioctx);
  if (ret < 0) {
    return ret;
  }

  return cur_ioctx.omap_get_vals_by_keys(oid, keys, vals);
}

int RadosObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
					bool must_exist, optional_yield y)
{
  rgw_raw_obj raw_meta_obj;
  rgw_obj obj = get_obj();

  store->getRados()->obj_to_raw(bucket->get_placement_rule(), obj, &raw_meta_obj);

  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(raw_meta_obj);

  return sysobj.omap().set_must_exist(must_exist).set(dpp, key, val, y);
}

MPSerializer* RadosObject::get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name)
{
  return new MPRadosSerializer(dpp, store, this, lock_name);
}

int RadosObject::transition(RGWObjectCtx& rctx,
			       Bucket* bucket,
			       const rgw_placement_rule& placement_rule,
			       const real_time& mtime,
			       uint64_t olh_epoch,
			       const DoutPrefixProvider* dpp,
			       optional_yield y)
{
  return store->getRados()->transition_obj(rctx, bucket, *this, placement_rule, mtime, olh_epoch, dpp, y);
}

int RadosObject::get_max_chunk_size(const DoutPrefixProvider* dpp, rgw_placement_rule placement_rule, uint64_t* max_chunk_size, uint64_t* alignment)
{
  return store->getRados()->get_max_chunk_size(placement_rule, get_obj(), max_chunk_size, dpp, alignment);
}

void RadosObject::get_max_aligned_size(uint64_t size, uint64_t alignment,
				     uint64_t* max_size)
{
  store->getRados()->get_max_aligned_size(size, alignment, max_size);
}

bool RadosObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
{
  rgw_obj obj;
  rgw_pool p1, p2;

  obj = get_obj();

  if (r1 == r2)
    return true;

  if (!store->getRados()->get_obj_data_pool(r1, obj, &p1)) {
    return false;
  }
  if (!store->getRados()->get_obj_data_pool(r2, obj, &p2)) {
    return false;
  }

  return p1 == p2;
}

int RadosObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f, RGWObjectCtx* obj_ctx)
{
  int ret;
  RGWObjManifest *manifest{nullptr};
  rgw_raw_obj head_obj;

  RGWRados::Object op_target(store->getRados(), get_bucket()->get_info(),
			     *obj_ctx, get_obj());
  RGWRados::Object::Read parent_op(&op_target);
  uint64_t obj_size;

  parent_op.params.obj_size = &obj_size;
  parent_op.params.attrs = &get_attrs();

  ret = parent_op.prepare(y, dpp);
  if (ret < 0) {
    return ret;
  }

  head_obj = parent_op.state.head_obj;

  ret = op_target.get_manifest(dpp, &manifest, y);
  if (ret < 0) {
    return ret;
  }

  ::encode_json("head", head_obj, f);
  ::encode_json("manifest", *manifest, f);
  f->open_array_section("data_location");
  for (auto miter = manifest->obj_begin(dpp); miter != manifest->obj_end(dpp); ++miter) {
    f->open_object_section("obj");
    rgw_raw_obj raw_loc = miter.get_location().get_raw_obj(store);
    uint64_t ofs = miter.get_ofs();
    uint64_t left = manifest->get_obj_size() - ofs;
    ::encode_json("ofs", miter.get_ofs(), f);
    ::encode_json("loc", raw_loc, f);
    ::encode_json("loc_ofs", miter.location_ofs(), f);
    uint64_t loc_size = miter.get_stripe_size();
    if (loc_size > left) {
      loc_size = left;
    }
    ::encode_json("loc_size", loc_size, f);
    f->close_section();
  }
  f->close_section();

  return 0;
}

std::unique_ptr<Object::ReadOp> RadosObject::get_read_op(RGWObjectCtx* ctx)
{
  return std::make_unique<RadosObject::RadosReadOp>(this, ctx);
}

RadosObject::RadosReadOp::RadosReadOp(RadosObject *_source, RGWObjectCtx *_rctx) :
	source(_source),
	rctx(_rctx),
	op_target(_source->store->getRados(),
		  _source->get_bucket()->get_info(),
		  *static_cast<RGWObjectCtx *>(rctx),
		  _source->get_obj()),
	parent_op(&op_target)
{ }

int RadosObject::RadosReadOp::prepare(optional_yield y, const DoutPrefixProvider* dpp)
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

  int ret = parent_op.prepare(y, dpp);
  if (ret < 0)
    return ret;

  source->set_key(parent_op.state.obj.key);
  source->set_obj_size(obj_size);

  return ret;
}

int RadosObject::RadosReadOp::read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider* dpp)
{
  return parent_op.read(ofs, end, bl, y, dpp);
}

int RadosObject::RadosReadOp::get_attr(const DoutPrefixProvider* dpp, const char* name, bufferlist& dest, optional_yield y)
{
  return parent_op.get_attr(dpp, name, dest, y);
}

std::unique_ptr<Object::DeleteOp> RadosObject::get_delete_op(RGWObjectCtx* ctx)
{
  return std::make_unique<RadosObject::RadosDeleteOp>(this, ctx);
}

RadosObject::RadosDeleteOp::RadosDeleteOp(RadosObject *_source, RGWObjectCtx *_rctx) :
	source(_source),
	rctx(_rctx),
	op_target(_source->store->getRados(),
		  _source->get_bucket()->get_info(),
		  *static_cast<RGWObjectCtx *>(rctx),
		  _source->get_obj()),
	parent_op(&op_target)
{ }

int RadosObject::RadosDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y)
{
  parent_op.params.bucket_owner = params.bucket_owner.get_id();
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

  int ret = parent_op.delete_obj(y, dpp);
  if (ret < 0)
    return ret;

  result.delete_marker = parent_op.result.delete_marker;
  result.version_id = parent_op.result.version_id;

  return ret;
}

int RadosObject::delete_object(const DoutPrefixProvider* dpp,
			       RGWObjectCtx* obj_ctx,
			       optional_yield y,
			       bool prevent_versioning)
{
  RGWRados::Object del_target(store->getRados(), bucket->get_info(), *obj_ctx, get_obj());
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket->get_info().owner;
  del_op.params.versioning_status = prevent_versioning ? 0 : bucket->get_info().versioning_status();

  return del_op.delete_obj(y, dpp);
}

int RadosObject::delete_obj_aio(const DoutPrefixProvider* dpp, RGWObjState* astate,
				   Completions* aio, bool keep_index_consistent,
				   optional_yield y)
{
  RadosCompletions* raio = static_cast<RadosCompletions*>(aio);

  return store->getRados()->delete_obj_aio(dpp, get_obj(), bucket->get_info(), astate,
					   raio->handles, keep_index_consistent, y);
}

int RadosObject::copy_object(RGWObjectCtx& obj_ctx,
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
  return store->getRados()->copy_obj(obj_ctx,
				     user->get_id(),
				     info,
				     source_zone,
				     dest_object,
				     this,
				     dest_bucket,
				     src_bucket,
				     dest_placement,
				     src_mtime,
				     mtime,
				     mod_ptr,
				     unmod_ptr,
				     high_precision_time,
				     if_match,
				     if_nomatch,
				     static_cast<RGWRados::AttrsMod>(attrs_mod),
				     copy_if_newer,
				     attrs,
				     category,
				     olh_epoch,
				     (delete_at ? *delete_at : real_time()),
				     version_id,
				     tag,
				     etag,
				     progress_cb,
				     progress_data,
				     dpp,
				     y);
}

int RadosObject::RadosReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  return parent_op.iterate(dpp, ofs, end, cb, y);
}

int RadosObject::swift_versioning_restore(RGWObjectCtx* obj_ctx,
					     bool& restored,
					     const DoutPrefixProvider* dpp)
{
  return store->getRados()->swift_versioning_restore(*obj_ctx,
						     bucket->get_owner()->get_id(),
						     bucket,
						     this,
						     restored,
						     dpp);
}

int RadosObject::swift_versioning_copy(RGWObjectCtx* obj_ctx,
					  const DoutPrefixProvider* dpp,
					  optional_yield y)
{
  return store->getRados()->swift_versioning_copy(*obj_ctx,
                                        bucket->get_info().owner,
                                        bucket,
                                        this,
                                        dpp,
                                        y);
}

int RadosMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct,
				RGWObjectCtx *obj_ctx)
{
  std::unique_ptr<rgw::sal::Object> meta_obj = get_meta_obj();
  meta_obj->set_in_extra_data(true);
  meta_obj->set_hash_source(mp_obj.get_key());
  cls_rgw_obj_chain chain;
  list<rgw_obj_index_key> remove_objs;
  bool truncated;
  int marker = 0;
  int ret;
  uint64_t parts_accounted_size = 0;

  do {
    ret = list_parts(dpp, cct, 1000, marker, &marker, &truncated);
    if (ret < 0) {
      ldpp_dout(dpp, 20) << __func__ << ": RadosMultipartUpload::list_parts returned " <<
	ret << dendl;
      return (ret == -ENOENT) ? -ERR_NO_SUCH_UPLOAD : ret;
    }

    for (auto part_it = parts.begin();
	 part_it != parts.end();
	 ++part_it) {
      RadosMultipartPart* obj_part = dynamic_cast<RadosMultipartPart*>(part_it->second.get());
      if (obj_part->info.manifest.empty()) {
	std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(
				    rgw_obj_key(obj_part->oid, std::string(), RGW_OBJ_NS_MULTIPART));
	obj->set_hash_source(mp_obj.get_key());
	ret = obj->delete_object(dpp, obj_ctx, null_yield);
        if (ret < 0 && ret != -ENOENT)
          return ret;
      } else {
	auto target = meta_obj->get_obj();
	store->getRados()->update_gc_chain(dpp, target, obj_part->info.manifest, &chain);
        RGWObjManifest::obj_iterator oiter = obj_part->info.manifest.obj_begin(dpp);
        if (oiter != obj_part->info.manifest.obj_end(dpp)) {
	  std::unique_ptr<rgw::sal::Object> head = bucket->get_object(rgw_obj_key());
          rgw_raw_obj raw_head = oiter.get_location().get_raw_obj(store);
	  dynamic_cast<rgw::sal::RadosObject*>(head.get())->raw_obj_to_obj(raw_head);

          rgw_obj_index_key key;
          head->get_key().get_index_key(&key);
          remove_objs.push_back(key);
        }
      }
      parts_accounted_size += obj_part->info.accounted_size;
    }
  } while (truncated);

  if (store->getRados()->get_gc() == nullptr) {
    //Delete objects inline if gc hasn't been initialised (in case when bypass gc is specified)
    store->getRados()->delete_objs_inline(dpp, chain, mp_obj.get_upload_id());
  } else {
    /* use upload id as tag and do it synchronously */
    ret = store->getRados()->send_chain_to_gc(chain, mp_obj.get_upload_id());
    if (ret < 0) {
      ldpp_dout(dpp, 5) << __func__ << ": gc->send_chain() returned " << ret << dendl;
      if (ret == -ENOENT) {
        return -ERR_NO_SUCH_UPLOAD;
      }
      //Delete objects inline if send chain to gc fails
      store->getRados()->delete_objs_inline(dpp, chain, mp_obj.get_upload_id());
    }
  }

  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = meta_obj->get_delete_op(obj_ctx);
  del_op->params.bucket_owner = bucket->get_acl_owner();
  del_op->params.versioning_status = 0;
  if (!remove_objs.empty()) {
    del_op->params.remove_objs = &remove_objs;
  }
  
  del_op->params.abortmp = true;
  del_op->params.parts_accounted_size = parts_accounted_size;

  // and also remove the metadata obj
  ret = del_op->delete_obj(dpp, null_yield);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << __func__ << ": del_op.delete_obj returned " <<
      ret << dendl;
  }
  return (ret == -ENOENT) ? -ERR_NO_SUCH_UPLOAD : ret;
}

std::unique_ptr<rgw::sal::Object> RadosMultipartUpload::get_meta_obj()
{
  return bucket->get_object(rgw_obj_key(get_meta(), string(), mp_ns));
}

int RadosMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y, RGWObjectCtx* obj_ctx, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs)
{
  int ret;
  std::string oid = mp_obj.get_key();

  do {
    char buf[33];
    string tmp_obj_name;
    std::unique_ptr<rgw::sal::Object> obj;
    gen_rand_alphanumeric(store->ctx(), buf, sizeof(buf) - 1);
    std::string upload_id = MULTIPART_UPLOAD_ID_PREFIX; /* v2 upload id */
    upload_id.append(buf);

    mp_obj.init(oid, upload_id);
    tmp_obj_name = mp_obj.get_meta();

    obj = bucket->get_object(rgw_obj_key(tmp_obj_name, string(), mp_ns));
    // the meta object will be indexed with 0 size, we c
    obj->set_in_extra_data(true);
    obj->set_hash_source(oid);

    RGWRados::Object op_target(store->getRados(),
			       obj->get_bucket()->get_info(),
			       *obj_ctx, obj->get_obj());
    RGWRados::Object::Write obj_op(&op_target);

    op_target.set_versioning_disabled(true); /* no versioning for multipart meta */
    obj_op.meta.owner = owner.get_id();
    obj_op.meta.category = RGWObjCategory::MultiMeta;
    obj_op.meta.flags = PUT_OBJ_CREATE_EXCL;
    obj_op.meta.mtime = &mtime;

    multipart_upload_info upload_info;
    upload_info.dest_placement = dest_placement;

    bufferlist bl;
    encode(upload_info, bl);
    obj_op.meta.data = &bl;

    ret = obj_op.write_meta(dpp, bl.length(), 0, attrs, y);
  } while (ret == -EEXIST);

  return ret;
}

int RadosMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				     int num_parts, int marker,
				     int *next_marker, bool *truncated,
				     bool assume_unsorted)
{
  map<string, bufferlist> parts_map;
  map<string, bufferlist>::iterator iter;

  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(
		      rgw_obj_key(get_meta(), std::string(), RGW_OBJ_NS_MULTIPART));
  obj->set_in_extra_data(true);

  bool sorted_omap = is_v2_upload_id(get_upload_id()) && !assume_unsorted;

  parts.clear();

  int ret;
  if (sorted_omap) {
    string p;
    p = "part.";
    char buf[32];

    snprintf(buf, sizeof(buf), "%08d", marker);
    p.append(buf);

    ret = obj->omap_get_vals(dpp, p, num_parts + 1, &parts_map,
                                 nullptr, null_yield);
  } else {
    ret = obj->omap_get_all(dpp, &parts_map, null_yield);
  }
  if (ret < 0) {
    return ret;
  }

  int i;
  int last_num = 0;

  uint32_t expected_next = marker + 1;

  for (i = 0, iter = parts_map.begin();
       (i < num_parts || !sorted_omap) && iter != parts_map.end();
       ++iter, ++i) {
    bufferlist& bl = iter->second;
    auto bli = bl.cbegin();
    std::unique_ptr<RadosMultipartPart> part = std::make_unique<RadosMultipartPart>();
    try {
      decode(part->info, bli);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: could not part info, caught buffer::error" <<
	dendl;
      return -EIO;
    }
    if (sorted_omap) {
      if (part->info.num != expected_next) {
        /* ouch, we expected a specific part num here, but we got a
         * different one. Either a part is missing, or it could be a
         * case of mixed rgw versions working on the same upload,
         * where one gateway doesn't support correctly sorted omap
         * keys for multipart upload just assume data is unsorted.
         */
        return list_parts(dpp, cct, num_parts, marker, next_marker, truncated, true);
      }
      expected_next++;
    }
    if (sorted_omap ||
      (int)part->info.num > marker) {
      last_num = part->info.num;
      parts[part->info.num] = std::move(part);
    }
  }

  if (sorted_omap) {
    if (truncated) {
      *truncated = (iter != parts_map.end());
    }
  } else {
    /* rebuild a map with only num_parts entries */
    std::map<uint32_t, std::unique_ptr<MultipartPart>> new_parts;
    std::map<uint32_t, std::unique_ptr<MultipartPart>>::iterator piter;
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
  }

  if (next_marker) {
    *next_marker = last_num;
  }

  return 0;
}

int RadosMultipartUpload::complete(const DoutPrefixProvider *dpp,
				   optional_yield y, CephContext* cct,
				   map<int, string>& part_etags,
				   list<rgw_obj_index_key>& remove_objs,
				   uint64_t& accounted_size, bool& compressed,
				   RGWCompressionInfo& cs_info, off_t& ofs,
				   std::string& tag, ACLOwner& owner,
				   uint64_t olh_epoch,
				   rgw::sal::Object* target_obj,
				   RGWObjectCtx* obj_ctx)
{
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE];
  char final_etag_str[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 16];
  std::string etag;
  bufferlist etag_bl;
  MD5 hash;
  // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
  hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
  bool truncated;
  int ret;

  int total_parts = 0;
  int handled_parts = 0;
  int max_parts = 1000;
  int marker = 0;
  uint64_t min_part_size = cct->_conf->rgw_multipart_min_part_size;
  auto etags_iter = part_etags.begin();
  rgw::sal::Attrs attrs = target_obj->get_attrs();

  do {
    ret = list_parts(dpp, cct, max_parts, marker, &marker, &truncated);
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
      RadosMultipartPart* part = dynamic_cast<rgw::sal::RadosMultipartPart*>(obj_iter->second.get());
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

      RGWUploadPartInfo& obj_part = part->info;

      /* update manifest for part */
      string oid = mp_obj.get_part(part->info.num);
      rgw_obj src_obj;
      src_obj.init_ns(bucket->get_key(), oid, mp_ns);

      if (obj_part.manifest.empty()) {
        ldpp_dout(dpp, 0) << "ERROR: empty manifest for object part: obj="
			 << src_obj << dendl;
        ret = -ERR_INVALID_PART;
        return ret;
      } else {
        manifest.append(dpp, obj_part.manifest, store->get_zone());
      }

      bool part_compressed = (obj_part.cs_info.compression_type != "none");
      if ((handled_parts > 0) &&
          ((part_compressed != compressed) ||
            (cs_info.compression_type != obj_part.cs_info.compression_type))) {
          ldpp_dout(dpp, 0) << "ERROR: compression type was changed during multipart upload ("
                           << cs_info.compression_type << ">>" << obj_part.cs_info.compression_type << ")" << dendl;
          ret = -ERR_INVALID_PART;
          return ret; 
      }
      
      if (part_compressed) {
        int64_t new_ofs; // offset in compression data for new part
        if (cs_info.blocks.size() > 0)
          new_ofs = cs_info.blocks.back().new_ofs + cs_info.blocks.back().len;
        else
          new_ofs = 0;
        for (const auto& block : obj_part.cs_info.blocks) {
          compression_block cb;
          cb.old_ofs = block.old_ofs + cs_info.orig_size;
          cb.new_ofs = new_ofs;
          cb.len = block.len;
          cs_info.blocks.push_back(cb);
          new_ofs = cb.new_ofs + cb.len;
        } 
        if (!compressed)
          cs_info.compression_type = obj_part.cs_info.compression_type;
        cs_info.orig_size += obj_part.cs_info.orig_size;
        compressed = true;
      }

      rgw_obj_index_key remove_key;
      src_obj.key.get_index_key(&remove_key);

      remove_objs.push_back(remove_key);

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

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  target_obj->set_atomic(obj_ctx);

  RGWRados::Object op_target(store->getRados(),
			     target_obj->get_bucket()->get_info(),
			     *obj_ctx, target_obj->get_obj());
  RGWRados::Object::Write obj_op(&op_target);

  obj_op.meta.manifest = &manifest;
  obj_op.meta.remove_objs = &remove_objs;

  obj_op.meta.ptag = &tag; /* use req_id as operation tag */
  obj_op.meta.owner = owner.get_id();
  obj_op.meta.flags = PUT_OBJ_CREATE;
  obj_op.meta.modify_tail = true;
  obj_op.meta.completeMultipart = true;
  obj_op.meta.olh_epoch = olh_epoch;

  ret = obj_op.write_meta(dpp, ofs, accounted_size, attrs, y);
  if (ret < 0)
    return ret;

  return ret;
}

int RadosMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y, RGWObjectCtx* obj_ctx, rgw_placement_rule** rule, rgw::sal::Attrs* attrs)
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
  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = meta_obj->get_read_op(obj_ctx);
  meta_obj->set_prefetch_data(obj_ctx);

  int ret = read_op->prepare(y, dpp);
  if (ret < 0) {
    if (ret == -ENOENT) {
      return -ERR_NO_SUCH_UPLOAD;
    }
    return ret;
  }

  extract_span_context(meta_obj->get_attrs(), trace_ctx);

  if (attrs) {
    /* Attrs are filled in by prepare */
    *attrs = meta_obj->get_attrs();
    if (!rule || *rule != nullptr) {
      /* placement was cached; don't actually read */
      return 0;
    }
  }

  /* Now read the placement from the head */
  ret = read_op->read(0, store->ctx()->_conf->rgw_max_chunk_size, headbl, y, dpp);
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

std::unique_ptr<Writer> RadosMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  std::unique_ptr<rgw::sal::Object> _head_obj,
				  const rgw_user& owner, RGWObjectCtx& obj_ctx,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  auto aio = rgw::make_throttle(store->ctx()->_conf->rgw_put_obj_min_window_size, y);
  return std::make_unique<RadosMultipartWriter>(dpp, y, this,
				 std::move(_head_obj), store, std::move(aio), owner,
				 obj_ctx, ptail_placement_rule, part_num, part_num_str);
}

MPRadosSerializer::MPRadosSerializer(const DoutPrefixProvider *dpp, RadosStore* store, RadosObject* obj, const std::string& lock_name) :
  lock(lock_name)
{
  rgw_pool meta_pool;
  rgw_raw_obj raw_obj;

  obj->get_raw_obj(&raw_obj);
  oid = raw_obj.oid;
  store->getRados()->get_obj_data_pool(obj->get_bucket()->get_placement_rule(),
				       obj->get_obj(), &meta_pool);
  store->getRados()->open_pool_ctx(dpp, meta_pool, ioctx, true);
}

int MPRadosSerializer::try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y)
{
  op.assert_exists();
  lock.set_duration(dur);
  lock.lock_exclusive(&op);
  int ret = rgw_rados_operate(dpp, ioctx, oid, &op, y);
  if (! ret) {
    locked = true;
  }
  return ret;
}

LCRadosSerializer::LCRadosSerializer(RadosStore* store, const std::string& _oid, const std::string& lock_name, const std::string& cookie) :
  lock(lock_name), oid(_oid)
{
  ioctx = &store->getRados()->lc_pool_ctx;
  lock.set_cookie(cookie);
}

int LCRadosSerializer::try_lock(const DoutPrefixProvider *dpp, utime_t dur, optional_yield y)
{
  lock.set_duration(dur);
  return lock.lock_exclusive(ioctx, oid);
}

int RadosLifecycle::get_entry(const std::string& oid, const std::string& marker,
			      LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;
  int ret = cls_rgw_lc_get_entry(*store->getRados()->get_lc_pool_ctx(), oid, marker, cls_entry);

  entry.bucket = cls_entry.bucket;
  entry.start_time = cls_entry.start_time;
  entry.status = cls_entry.status;

  return ret;
}

int RadosLifecycle::get_next_entry(const std::string& oid, std::string& marker,
				   LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;
  int ret = cls_rgw_lc_get_next_entry(*store->getRados()->get_lc_pool_ctx(), oid, marker,
				      cls_entry);

  entry.bucket = cls_entry.bucket;
  entry.start_time = cls_entry.start_time;
  entry.status = cls_entry.status;

  return ret;
}

int RadosLifecycle::set_entry(const std::string& oid, const LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;

  cls_entry.bucket = entry.bucket;
  cls_entry.start_time = entry.start_time;
  cls_entry.status = entry.status;

  return cls_rgw_lc_set_entry(*store->getRados()->get_lc_pool_ctx(), oid, cls_entry);
}

int RadosLifecycle::list_entries(const std::string& oid, const std::string& marker,
				 uint32_t max_entries, vector<LCEntry>& entries)
{
  entries.clear();

  vector<cls_rgw_lc_entry> cls_entries;
  int ret = cls_rgw_lc_list(*store->getRados()->get_lc_pool_ctx(), oid, marker, max_entries, cls_entries);

  if (ret < 0)
    return ret;

  for (auto& entry : cls_entries) {
    entries.push_back(LCEntry(entry.bucket, entry.start_time, entry.status));
  }

  return ret;
}

int RadosLifecycle::rm_entry(const std::string& oid, const LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;

  cls_entry.bucket = entry.bucket;
  cls_entry.start_time = entry.start_time;
  cls_entry.status = entry.status;

  return cls_rgw_lc_rm_entry(*store->getRados()->get_lc_pool_ctx(), oid, cls_entry);
}

int RadosLifecycle::get_head(const std::string& oid, LCHead& head)
{
  cls_rgw_lc_obj_head cls_head;
  int ret = cls_rgw_lc_get_head(*store->getRados()->get_lc_pool_ctx(), oid, cls_head);

  head.marker = cls_head.marker;
  head.start_date = cls_head.start_date;

  return ret;
}

int RadosLifecycle::put_head(const std::string& oid, const LCHead& head)
{
  cls_rgw_lc_obj_head cls_head;

  cls_head.marker = head.marker;
  cls_head.start_date = head.start_date;

  return cls_rgw_lc_put_head(*store->getRados()->get_lc_pool_ctx(), oid, cls_head);
}

LCSerializer* RadosLifecycle::get_serializer(const std::string& lock_name, const std::string& oid, const std::string& cookie)
{
  return new LCRadosSerializer(store, oid, lock_name, cookie);
}

int RadosNotification::publish_reserve(const DoutPrefixProvider *dpp, RGWObjTags* obj_tags)
{
  return rgw::notify::publish_reserve(dpp, event_type, res, obj_tags);
}

int RadosNotification::publish_commit(const DoutPrefixProvider* dpp, uint64_t size,
				     const ceph::real_time& mtime, const std::string& etag, const std::string& version)
{
  return rgw::notify::publish_commit(obj, size, mtime, etag, version, event_type, res, dpp);
}

int RadosAtomicWriter::prepare(optional_yield y)
{
  return processor.prepare(y);
}

int RadosAtomicWriter::process(bufferlist&& data, uint64_t offset)
{
  return processor.process(std::move(data), offset);
}

int RadosAtomicWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  return processor.complete(accounted_size, etag, mtime, set_mtime, attrs, delete_at,
			    if_match, if_nomatch, user_data, zones_trace, canceled, y);
}

int RadosAppendWriter::prepare(optional_yield y)
{
  return processor.prepare(y);
}

int RadosAppendWriter::process(bufferlist&& data, uint64_t offset)
{
  return processor.process(std::move(data), offset);
}

int RadosAppendWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  return processor.complete(accounted_size, etag, mtime, set_mtime, attrs, delete_at,
			    if_match, if_nomatch, user_data, zones_trace, canceled, y);
}

int RadosMultipartWriter::prepare(optional_yield y)
{
  return processor.prepare(y);
}

int RadosMultipartWriter::process(bufferlist&& data, uint64_t offset)
{
  return processor.process(std::move(data), offset);
}

int RadosMultipartWriter::complete(size_t accounted_size, const std::string& etag,
                       ceph::real_time *mtime, ceph::real_time set_mtime,
                       std::map<std::string, bufferlist>& attrs,
                       ceph::real_time delete_at,
                       const char *if_match, const char *if_nomatch,
                       const std::string *user_data,
                       rgw_zone_set *zones_trace, bool *canceled,
                       optional_yield y)
{
  return processor.complete(accounted_size, etag, mtime, set_mtime, attrs, delete_at,
			    if_match, if_nomatch, user_data, zones_trace, canceled, y);
}

const RGWZoneGroup& RadosZone::get_zonegroup()
{
  return store->svc()->zone->get_zonegroup();
}

int RadosZone::get_zonegroup(const std::string& id, RGWZoneGroup& zonegroup)
{
  return store->svc()->zone->get_zonegroup(id, zonegroup);
}

const RGWZoneParams& RadosZone::get_params()
{
  return store->svc()->zone->get_zone_params();
}

const rgw_zone_id& RadosZone::get_id()
{
  return store->svc()->zone->zone_id();
}

const RGWRealm& RadosZone::get_realm()
{
  return store->svc()->zone->get_realm();
}

const std::string& RadosZone::get_name() const
{
  return store->svc()->zone->zone_name();
}

bool RadosZone::is_writeable()
{
  return store->svc()->zone->zone_is_writeable();
}

bool RadosZone::get_redirect_endpoint(std::string* endpoint)
{
  return store->svc()->zone->get_redirect_zone_endpoint(endpoint);
}

bool RadosZone::has_zonegroup_api(const std::string& api) const
{
  return store->svc()->zone->has_zonegroup_api(api);
}

const std::string& RadosZone::get_current_period_id()
{
  return store->svc()->zone->get_current_period_id();
}

int RadosLuaScriptManager::get(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  bufferlist bl;

  int r = rgw_get_system_obj(obj_ctx, pool, key, bl, nullptr, nullptr, y, dpp);
  if (r < 0) {
    return r;
  }

  auto iter = bl.cbegin();
  try {
    ceph::decode(script, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

int RadosLuaScriptManager::put(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  bufferlist bl;
  ceph::encode(script, bl);

  int r = rgw_put_system_obj(dpp, obj_ctx, pool, key, bl, false, nullptr, real_time(), y);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RadosLuaScriptManager::del(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key)
{
  int r = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, key, nullptr, y);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  return 0;
}

int RadosOIDCProvider::store_url(const DoutPrefixProvider *dpp, const std::string& url, bool exclusive, optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  std::string oid = tenant + get_url_oid_prefix() + url;

  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);
  return rgw_put_system_obj(dpp, obj_ctx, store->get_zone()->get_params().oidc_pool, oid, bl, exclusive, nullptr, real_time(), y);
}

int RadosOIDCProvider::read_url(const DoutPrefixProvider *dpp, const std::string& url, const std::string& tenant)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  auto& pool = store->get_zone()->get_params().oidc_pool;
  std::string oid = tenant + get_url_oid_prefix() + url;
  bufferlist bl;

  int ret = rgw_get_system_obj(obj_ctx, pool, oid, bl, nullptr, nullptr, null_yield, dpp);
  if (ret < 0) {
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode oidc provider info from pool: " << pool.name <<
                  ": " << url << dendl;
    return -EIO;
  }

  return 0;
}

int RadosOIDCProvider::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto& pool = store->get_zone()->get_params().oidc_pool;

  std::string url, tenant;
  auto ret = get_tenant_url_from_arn(tenant, url);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to parse arn" << dendl;
    return -EINVAL;
  }

  if (this->tenant != tenant) {
    ldpp_dout(dpp, 0) << "ERROR: tenant in arn doesn't match that of user " << this->tenant << ", "
                  << tenant << ": " << dendl;
    return -EINVAL;
  }

  // Delete url
  std::string oid = tenant + get_url_oid_prefix() + url;
  ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting oidc url from pool: " << pool.name << ": "
                  << provider_url << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RadosRole::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  using ceph::encode;
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  std::string oid = get_info_oid_prefix() + id;

  bufferlist bl;
  encode(*this, bl);

  if (!this->tags.empty()) {
    bufferlist bl_tags;
    encode(this->tags, bl_tags);
    map<string, bufferlist> attrs;
    attrs.emplace("tagging", bl_tags);
    return rgw_put_system_obj(dpp, obj_ctx, store->get_zone()->get_params().roles_pool, oid, bl, exclusive, nullptr, real_time(), y, &attrs);
  }

  return rgw_put_system_obj(dpp, obj_ctx, store->get_zone()->get_params().roles_pool, oid, bl, exclusive, nullptr, real_time(), y);
}

int RadosRole::store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  RGWNameToId nameToId;
  nameToId.obj_id = id;

  std::string oid = tenant + get_names_oid_prefix() + name;

  bufferlist bl;
  using ceph::encode;
  encode(nameToId, bl);

  return rgw_put_system_obj(dpp, obj_ctx, store->get_zone()->get_params().roles_pool, oid, bl, exclusive, nullptr, real_time(), y);
}

int RadosRole::store_path(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  std::string oid = tenant + get_path_oid_prefix() + path + get_info_oid_prefix() + id;

  bufferlist bl;

  return rgw_put_system_obj(dpp, obj_ctx, store->get_zone()->get_params().roles_pool, oid, bl, exclusive, nullptr, real_time(), y);
}

int RadosRole::read_id(const DoutPrefixProvider *dpp, const std::string& role_name, const std::string& tenant, std::string& role_id, optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  std::string oid = tenant + get_names_oid_prefix() + role_name;
  bufferlist bl;

  int ret = rgw_get_system_obj(obj_ctx, store->get_zone()->get_params().roles_pool, oid, bl, nullptr, nullptr, null_yield, dpp);
  if (ret < 0) {
    return ret;
  }

  RGWNameToId nameToId;
  try {
    auto iter = bl.cbegin();
    using ceph::decode;
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode role from Role pool: " << role_name << dendl;
    return -EIO;
  }
  role_id = nameToId.obj_id;
  return 0;
}

int RadosRole::read_name(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  std::string oid = tenant + get_names_oid_prefix() + name;
  bufferlist bl;

  int ret = rgw_get_system_obj(obj_ctx, store->get_zone()->get_params().roles_pool, oid, bl, nullptr, nullptr, null_yield, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role name from Role pool: " << name <<
      ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  RGWNameToId nameToId;
  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode role name from Role pool: " << name << dendl;
    return -EIO;
  }
  id = nameToId.obj_id;
  return 0;
}

int RadosRole::read_info(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  std::string oid = get_info_oid_prefix() + id;
  bufferlist bl;

  map<string, bufferlist> attrs;
  int ret = rgw_get_system_obj(obj_ctx, store->get_zone()->get_params().roles_pool, oid, bl, nullptr, nullptr, null_yield, dpp, &attrs, nullptr, boost::none, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role info from Role pool: " << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode role info from Role pool: " << id << dendl;
    return -EIO;
  }

  auto it = attrs.find("tagging");
  if (it != attrs.end()) {
    bufferlist bl_tags = it->second;
    try {
      using ceph::decode;
      auto iter = bl_tags.cbegin();
      decode(tags, iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode attrs" << id << dendl;
      return -EIO;
    }
  }

  return 0;
}

int RadosRole::create(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  int ret;

  if (! validate_input(dpp)) {
    return -EINVAL;
  }

  /* check to see the name is not used */
  ret = read_id(dpp, name, tenant, id, y);
  if (exclusive && ret == 0) {
    ldpp_dout(dpp, 0) << "ERROR: name " << name << " already in use for role id "
                    << id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading role id  " << id << ": "
                  << cpp_strerror(-ret) << dendl;
    return ret;
  }

  /* create unique id */
  uuid_d new_uuid;
  char uuid_str[37];
  new_uuid.generate_random();
  new_uuid.print(uuid_str);
  id = uuid_str;

  //arn
  arn = role_arn_prefix + tenant + ":role" + path + name;

  // Creation time
  real_clock::time_point t = real_clock::now();

  struct timeval tv;
  real_clock::to_timeval(t, tv);

  char buf[30];
  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
  sprintf(buf + strlen(buf),".%dZ",(int)tv.tv_usec/1000);
  creation_date.assign(buf, strlen(buf));

  auto& pool = store->get_zone()->get_params().roles_pool;
  ret = store_info(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing role info in Role pool: "
                  << id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = store_name(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: storing role name in Role pool: "
                  << name << ": " << cpp_strerror(-ret) << dendl;

    //Delete the role info that was stored in the previous call
    std::string oid = get_info_oid_prefix() + id;
    int info_ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
    if (info_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role id from Role pool: "
                  << id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    return ret;
  }

  ret = store_path(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: storing role path in Role pool: "
                  << path << ": " << cpp_strerror(-ret) << dendl;
    //Delete the role info that was stored in the previous call
    std::string oid = get_info_oid_prefix() + id;
    int info_ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
    if (info_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role id from Role pool: "
                  << id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    //Delete role name that was stored in previous call
    oid = tenant + get_names_oid_prefix() + name;
    int name_ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
    if (name_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role name from Role pool: "
                  << name << ": " << cpp_strerror(-name_ret) << dendl;
    }
    return ret;
  }
  return 0;
}

int RadosRole::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto& pool = store->get_zone()->get_params().roles_pool;

  int ret = read_name(dpp, y);
  if (ret < 0) {
    return ret;
  }

  ret = read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  if (! perm_policy_map.empty()) {
    return -ERR_DELETE_CONFLICT;
  }

  // Delete id
  std::string oid = get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role id from Role pool: "
                  << id << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete name
  oid = tenant + get_names_oid_prefix() + name;
  ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role name from Role pool: "
                  << name << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete path
  oid = tenant + get_path_oid_prefix() + path + get_info_oid_prefix() + id;
  ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role path from Role pool: "
                  << path << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}


} // namespace rgw::sal

extern "C" {

void* newStore(void)
{
  rgw::sal::RadosStore* store = new rgw::sal::RadosStore();
  if (store) {
    RGWRados* rados = new RGWRados();

    if (!rados) {
      delete store; store = nullptr;
    } else {
      store->setRados(rados);
      rados->set_store(store);
    }
  }

  return store;
}

}
