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
#include <filesystem>
#include <unistd.h>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/process.hpp>

#include "common/async/blocked_completion.h"

#include "common/Clock.h"
#include "common/errno.h"

#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_bucket.h"
#include "rgw_multi.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_aio.h"
#include "rgw_aio_throttle.h"
#include "rgw_tracer.h"

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "rgw_service.h"
#include "rgw_lc.h"
#include "rgw_lc_tier.h"
#include "rgw_rest_admin.h"
#include "rgw_rest_bucket.h"
#include "rgw_rest_metadata.h"
#include "rgw_rest_log.h"
#include "rgw_rest_config.h"
#include "rgw_rest_ratelimit.h"
#include "rgw_rest_realm.h"
#include "rgw_rest_user.h"
#include "services/svc_sys_obj.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_cls.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"
#include "services/svc_quota.h"
#include "services/svc_config_key.h"
#include "services/svc_zone_utils.h"
#include "services/svc_role_rados.h"
#include "services/svc_user.h"
#include "services/svc_sys_obj_cache.h"
#include "services/svc_topic_rados.h"
#include "cls/rgw/cls_rgw_client.h"

#include "rgw_pubsub.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

static string mp_ns = RGW_OBJ_NS_MULTIPART;

namespace rgw::sal {

// default number of entries to list with each bucket listing call
// (use marker to bridge between calls)
static constexpr size_t listing_max_entries = 1000;
static std::string pubsub_oid_prefix = "pubsub.";

static int drain_aio(std::list<librados::AioCompletion*>& handles)
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
			       BucketList &result, optional_yield y)
{
  RGWUserBuckets ulist;
  bool is_truncated = false;

  int ret = store->ctl()->user->list_buckets(dpp, get_id(), marker, end_marker,
                                             max, need_stats, &ulist,
                                             &is_truncated, y);
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

int RadosBucket::create(const DoutPrefixProvider* dpp,
                        const CreateParams& params,
                        optional_yield y)
{
  rgw_bucket key = get_key();
  key.marker = params.marker;
  key.bucket_id = params.bucket_id;

  int ret = store->getRados()->create_bucket(
      dpp, y, key, params.owner, params.zonegroup_id,
      params.placement_rule, params.zone_placement, params.attrs,
      params.obj_lock_enabled, params.swift_ver_location,
      params.quota, params.creation_time, &bucket_version, info);

  bool existed = false;
  if (ret == -EEXIST) {
    existed = true;
    /* bucket already existed, might have raced with another bucket creation,
     * or might be partial bucket creation that never completed. Read existing
     * bucket info, verify that the reported bucket owner is the current user.
     * If all is ok then update the user's list of buckets.  Otherwise inform
     * client about a name conflict.
     */
    if (info.owner != params.owner) {
      return -ERR_BUCKET_EXISTS;
    }
    ret = 0;
  } else if (ret != 0) {
    return ret;
  }

  ret = link(dpp, params.owner, y, false);
  if (ret && !existed && ret != -EEXIST) {
    /* if it exists (or previously existed), don't remove it! */
    ret = unlink(dpp, params.owner, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: failed to unlink bucket: ret=" << ret
		       << dendl;
    }
  } else if (ret == -EEXIST || (ret == 0 && existed)) {
    ret = -ERR_BUCKET_EXISTS;
  }

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

int RadosUser::read_stats_async(const DoutPrefixProvider *dpp, boost::intrusive_ptr<ReadStatsCB> cb)
{
  return store->svc()->user->read_stats_async(dpp, get_id(), cb);
}

int RadosUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return store->svc()->user->complete_flush_stats(dpp, get_id(), y);
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

int RadosUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  std::string bucket_name;

  return store->getRados()->trim_usage(dpp, get_id(), bucket_name, start_epoch, end_epoch, y);
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

int RadosUser::verify_mfa(const std::string& mfa_str, bool* verified,
			  const DoutPrefixProvider* dpp, optional_yield y)
{
  vector<string> params;
  get_str_vec(mfa_str, " ", params);

  if (params.size() != 2) {
    ldpp_dout(dpp, 5) << "NOTICE: invalid mfa string provided: " << mfa_str << dendl;
    return -EINVAL;
  }

  string& serial = params[0];
  string& pin = params[1];

  auto i = info.mfa_ids.find(serial);
  if (i == info.mfa_ids.end()) {
    ldpp_dout(dpp, 5) << "NOTICE: user does not have mfa device with serial=" << serial << dendl;
    return -EACCES;
  }

  int ret = store->svc()->cls->mfa.check_mfa(dpp, info.user_id, serial, pin, y);
  if (ret < 0) {
    ldpp_dout(dpp, 20) << "NOTICE: failed to check MFA, serial=" << serial << dendl;
    return -EACCES;
  }

  *verified = true;

  return 0;
}

RadosBucket::~RadosBucket() {}

int RadosBucket::remove(const DoutPrefixProvider* dpp,
			bool delete_children,
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
      ret = rgw_remove_object(dpp, store, this, key, y);
      if (ret < 0 && ret != -ENOENT) {
	return ret;
      }
    }
  } while(results.is_truncated);

  ret = abort_multiparts(dpp, store->ctx(), y);
  if (ret < 0) {
    return ret;
  }

  // remove lifecycle config, if any (XXX note could be made generic)
  if (get_attrs().count(RGW_ATTR_LC)) {
    constexpr bool merge_attrs = false; // don't update xattrs, we're deleting
    (void) store->getRados()->get_lc()->remove_bucket_config(
      this, get_attrs(), merge_attrs);
  }

  // remove bucket-topic mapping
  auto iter = get_attrs().find(RGW_ATTR_BUCKET_NOTIFICATION);
  if (iter != get_attrs().end()) {
    rgw_pubsub_bucket_topics bucket_topics;
    try {
      const auto& bl = iter->second;
      auto biter = bl.cbegin();
      bucket_topics.decode(biter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1) << "ERROR: failed to decode bucket topics for bucket: "
                        << get_name() << dendl;
    }
    if (!bucket_topics.topics.empty()) {
      ret = store->remove_bucket_mapping_from_topics(
          bucket_topics, rgw_make_bucket_entry_name(get_tenant(), get_name()),
          y, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 1)
            << "ERROR: unable to remove notifications from bucket "
            << get_name() << ". ret=" << ret << dendl;
      }
    }
  }

  ret = store->ctl()->bucket->sync_user_stats(dpp, info.owner, info, y, nullptr);
  if (ret < 0) {
     ldout(store->ctx(), 1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker ot;

  // if we deleted children above we will force delete, as any that
  // remain is detritus from a prior bug
  ret = store->getRados()->delete_bucket(info, ot, y, dpp, !delete_children);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: could not remove bucket " <<
      info.bucket.name << dendl;
    return ret;
  }

  // if bucket has notification definitions associated with it
  // they should be removed (note that any pending notifications on the bucket are still going to be sent)
  const RGWPubSub ps(store, info.owner.tenant);
  const RGWPubSub::Bucket ps_bucket(ps, this);
  const auto ps_ret = ps_bucket.remove_notifications(dpp, y);
  if (ps_ret < 0 && ps_ret != -ENOENT) {
    ldpp_dout(dpp, -1) << "ERROR: unable to remove notifications from bucket. ret=" << ps_ret << dendl;
  }

  ret = store->ctl()->bucket->unlink_bucket(info.owner, info.bucket, y, dpp, false);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: unable to remove user bucket information" << dendl;
  }

  return ret;
}

int RadosBucket::remove_bypass_gc(int concurrent_max, bool
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

  ret = load_bucket(dpp, y);
  if (ret < 0)
    return ret;

  const auto& index = info.get_current_index();
  ret = read_stats(dpp, index, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, NULL);
  if (ret < 0)
    return ret;

  ret = abort_multiparts(dpp, cct, y);
  if (ret < 0) {
    return ret;
  }

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.list_versions = true;
  params.allow_unordered = true;

  std::list<librados::AioCompletion*> handles;

  int max_aio = concurrent_max;
  results.is_truncated = true;

  while (results.is_truncated) {
    ret = list(dpp, params, listing_max_entries, results, y);
    if (ret < 0)
      return ret;

    std::vector<rgw_bucket_dir_entry>::iterator it = results.objs.begin();
    for (; it != results.objs.end(); ++it) {
      RGWObjState *astate = NULL;
      RGWObjManifest *amanifest = nullptr;
      rgw_obj obj{get_key(), it->key};

      ret = store->getRados()->get_obj_state(dpp, &obj_ctx, get_info(),
					     obj, &astate, &amanifest,
					     false, y);
      if (ret == -ENOENT) {
        ldpp_dout(dpp, 1) << "WARNING: cannot find obj state for obj " << obj << dendl;
        continue;
      }
      if (ret < 0) {
        ldpp_dout(dpp, -1) << "ERROR: get obj state returned with error " << ret << dendl;
        return ret;
      }

      if (amanifest) {
        RGWObjManifest& manifest = *amanifest;
        RGWObjManifest::obj_iterator miter = manifest.obj_begin(dpp);
        const rgw_obj head_obj = manifest.get_obj();
        rgw_raw_obj raw_head_obj;
        store->get_raw_obj(manifest.get_head_placement_rule(), head_obj, &raw_head_obj);

        for (; miter != manifest.obj_end(dpp) && max_aio--; ++miter) {
          if (!max_aio) {
            ret = drain_aio(handles);
            if (ret < 0) {
              ldpp_dout(dpp, -1) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
              return ret;
            }
            max_aio = concurrent_max;
          }

          rgw_raw_obj last_obj = miter.get_location().get_raw_obj(store->getRados());
          if (last_obj == raw_head_obj) {
            // have the head obj deleted at the end
            continue;
          }

          ret = store->getRados()->delete_raw_obj_aio(dpp, last_obj, handles);
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR: delete obj aio failed with " << ret << dendl;
            return ret;
          }
        } // for all shadow objs

        ret = store->getRados()->delete_obj_aio(dpp, head_obj, get_info(), astate,
                                                handles, keep_index_consistent, y);
        if (ret < 0) {
          ldpp_dout(dpp, -1) << "ERROR: delete obj aio failed with " << ret << dendl;
          return ret;
        }
      }

      if (!max_aio) {
        ret = drain_aio(handles);
        if (ret < 0) {
          ldpp_dout(dpp, -1) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
          return ret;
        }
        max_aio = concurrent_max;
      }
      obj_ctx.invalidate(obj);
    } // for all RGW objects in results
  } // while is_truncated

  ret = drain_aio(handles);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
    return ret;
  }

  sync_user_stats(dpp, y, nullptr);
  if (ret < 0) {
     ldpp_dout(dpp, 1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker objv_tracker;

  // this function can only be run if caller wanted children to be
  // deleted, so we can ignore the check for children as any that
  // remain are detritus from a prior bug
  ret = remove(dpp, true, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: could not remove bucket " << this << dendl;
    return ret;
  }

  return ret;
}

int RadosBucket::load_bucket(const DoutPrefixProvider* dpp, optional_yield y)
{
  int ret;

  RGWSI_MetaBackend_CtxParams bectx_params = RGWSI_MetaBackend_CtxParams_SObj();
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

  return ret;
}

int RadosBucket::read_stats(const DoutPrefixProvider *dpp,
			    const bucket_index_layout_generation& idx_layout,
			    int shard_id, std::string* bucket_ver, std::string* master_ver,
			    std::map<RGWObjCategory, RGWStorageStats>& stats,
			    std::string* max_marker, bool* syncstopped)
{
  return store->getRados()->get_bucket_stats(dpp, info, idx_layout, shard_id, bucket_ver, master_ver, stats, max_marker, syncstopped);
}

int RadosBucket::read_stats_async(const DoutPrefixProvider *dpp,
				  const bucket_index_layout_generation& idx_layout,
				  int shard_id, boost::intrusive_ptr<ReadStatsCB> ctx)
{
  return store->getRados()->get_bucket_stats_async(dpp, get_info(), idx_layout, shard_id, ctx);
}

int RadosBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y,
                                 RGWBucketEnt* ent)
{
  return store->ctl()->bucket->sync_user_stats(dpp, info.owner, info, y, ent);
}

int RadosBucket::check_bucket_shards(const DoutPrefixProvider* dpp,
                                     uint64_t num_objs, optional_yield y)
{
  return store->getRados()->check_bucket_shards(info, num_objs, dpp, y);
}

int RadosBucket::link(const DoutPrefixProvider* dpp, const rgw_user& new_user, optional_yield y, bool update_entrypoint, RGWObjVersionTracker* objv)
{
  RGWBucketEntryPoint ep;
  ep.bucket = info.bucket;
  ep.owner = new_user;
  ep.creation_time = get_creation_time();
  ep.linked = true;
  Attrs ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  int r = store->ctl()->bucket->link_bucket(new_user, info.bucket,
					    get_creation_time(), y, dpp, update_entrypoint,
					    &ep_data);
  if (r < 0)
    return r;

  if (objv)
    *objv = ep_data.ep_objv;

  return r;
}

int RadosBucket::unlink(const DoutPrefixProvider* dpp, const rgw_user& owner, optional_yield y, bool update_entrypoint)
{
  return store->ctl()->bucket->unlink_bucket(owner, info.bucket, y, dpp, update_entrypoint);
}

int RadosBucket::chown(const DoutPrefixProvider* dpp, const rgw_user& new_owner, optional_yield y)
{
  std::string obj_marker;
  int r = this->unlink(dpp, info.owner, y);
  if (r < 0) {
    return r;
  }

  return this->link(dpp, new_owner, y);
}

int RadosBucket::put_info(const DoutPrefixProvider* dpp, bool exclusive, ceph::real_time _mtime, optional_yield y)
{
  mtime = _mtime;
  return store->getRados()->put_bucket_instance_info(info, exclusive, mtime, &attrs, dpp, y);
}

int RadosBucket::check_empty(const DoutPrefixProvider* dpp, optional_yield y)
{
  return store->getRados()->check_bucket_empty(dpp, info, y);
}

int RadosBucket::check_quota(const DoutPrefixProvider *dpp, RGWQuota& quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
    return store->getRados()->check_quota(dpp, info.owner, get_key(),
					  quota, obj_size, y, check_size_only);
}

int RadosBucket::merge_and_store_attrs(const DoutPrefixProvider* dpp, Attrs& new_attrs, optional_yield y)
{
  for(auto& it : new_attrs) {
	  attrs[it.first] = it.second;
  }
  return store->ctl()->bucket->set_bucket_instance_attrs(get_info(),
				new_attrs, &get_info().objv_tracker, y, dpp);
}

int RadosBucket::try_refresh_info(const DoutPrefixProvider* dpp, ceph::real_time* pmtime, optional_yield y)
{
  return store->getRados()->try_refresh_bucket_info(info, pmtime, dpp, y, &attrs);
}

int RadosBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool* is_truncated,
			       RGWUsageIter& usage_iter,
			       map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return store->getRados()->read_usage(dpp, info.owner, get_name(), start_epoch,
				       end_epoch, max_entries, is_truncated,
				       usage_iter, usage);
}

int RadosBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  return store->getRados()->trim_usage(dpp, info.owner, get_name(), start_epoch, end_epoch, y);
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

int RadosBucket::purge_instance(const DoutPrefixProvider* dpp, optional_yield y)
{
  int max_shards = (info.layout.current_index.layout.normal.num_shards > 0 ? info.layout.current_index.layout.normal.num_shards : 1);
  for (int i = 0; i < max_shards; i++) {
    RGWRados::BucketShard bs(store->getRados());
    int shard_id = (info.layout.current_index.layout.normal.num_shards > 0  ? i : -1);
    int ret = bs.init(dpp, info, info.layout.current_index, shard_id, y);
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
  info.owner = acl.get_owner().id;

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
				 bool *is_truncated, optional_yield y)
{
  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.prefix = prefix;
  params.delim = delim;
  params.marker = marker;
  params.ns = RGW_OBJ_NS_MULTIPART;
  params.access_list_filter = MultipartMetaFilter;

  int ret = list(dpp, params, max_uploads, results, y);

  if (ret < 0)
    return ret;

  if (!results.objs.empty()) {
    for (const rgw_bucket_dir_entry& dentry : results.objs) {
      rgw_obj_key key(dentry.key);
      const ACLOwner owner{
        .id = rgw_user(dentry.meta.owner),
        .display_name = dentry.meta.owner_display_name
      };
      uploads.push_back(this->get_multipart_upload(key.name,
			std::nullopt, std::move(owner), dentry.meta.mtime));
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
				  CephContext* cct, optional_yield y)
{
  constexpr int max = 1000;
  int ret, num_deleted = 0;
  vector<std::unique_ptr<MultipartUpload>> uploads;
  string marker;
  bool is_truncated;

  const std::string empty_delim;
  const std::string empty_prefix;

  do {
    ret = list_multiparts(dpp, empty_prefix, marker, empty_delim,
			  max, uploads, nullptr, &is_truncated, y);
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
	ret = upload->abort(dpp, cct, y);
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

std::string RadosBucket::topics_oid() const {
  return pubsub_oid_prefix + get_tenant() + ".bucket." + get_name() + "/" + get_marker();
}

int RadosBucket::read_topics(rgw_pubsub_bucket_topics& notifications,
    RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp)
{
  // read from cache
  auto cache = store->getRados()->get_topic_cache();
  const std::string key = store->svc()->zone->get_zone_params().log_pool.to_str() + topics_oid();
  if (auto e = cache->find(key)) {
    notifications = e->info;
    return 0;
  }

  bufferlist bl;
  rgw_cache_entry_info cache_info;
  const int ret = rgw_get_system_obj(store->svc()->sysobj,
                               store->svc()->zone->get_zone_params().log_pool,
                               topics_oid(),
                               bl,
                               objv_tracker, nullptr,
                               y, dpp, nullptr, &cache_info);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(notifications, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 20) << " failed to decode bucket notifications from oid: " << topics_oid() << ". for bucket: "
      << get_name() << ". error: " << err.what() << dendl;
    return -EIO;
  }

  pubsub_bucket_topics_entry e;
  e.info = notifications;
  if (!cache->put(dpp, store->getRados()->svc.cache, key, &e, { &cache_info })) {
    ldpp_dout(dpp, 10) << "couldn't put bucket topics cache entry" << dendl;
  }
  return 0;
}

int RadosBucket::write_topics(const rgw_pubsub_bucket_topics& notifications,
    RGWObjVersionTracker* objv_tracker, optional_yield y, const DoutPrefixProvider *dpp) {
  bufferlist bl;
  encode(notifications, bl);

  return rgw_put_system_obj(dpp, store->svc()->sysobj,
      store->svc()->zone->get_zone_params().log_pool,
      topics_oid(),
      bl, false, objv_tracker, real_time(), y);
}

int RadosBucket::remove_topics(RGWObjVersionTracker* objv_tracker,
    optional_yield y, const DoutPrefixProvider *dpp) {
  return rgw_delete_system_obj(dpp, store->svc()->sysobj,
      store->svc()->zone->get_zone_params().log_pool,
      topics_oid(),
      objv_tracker, y);
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

std::unique_ptr<Bucket> RadosStore::get_bucket(const RGWBucketInfo& i)
{
  /* Don't need to fetch the bucket info, use the provided one */
  return std::make_unique<RadosBucket>(this, i);
}

int RadosStore::load_bucket(const DoutPrefixProvider* dpp, const rgw_bucket& b,
                            std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  *bucket = std::make_unique<RadosBucket>(this, b);
  return (*bucket)->load_bucket(dpp, y);
}

bool RadosStore::is_meta_master()
{
  return svc()->zone->is_meta_master();
}

std::string RadosStore::zone_unique_id(uint64_t unique_num)
{
  return svc()->zone_utils->unique_id(unique_num);
}

std::string RadosStore::zone_unique_trans_id(const uint64_t unique_num)
{
  return svc()->zone_utils->unique_trans_id(unique_num);
}

int RadosStore::get_zonegroup(const std::string& id,
			      std::unique_ptr<ZoneGroup>* zonegroup)
{
  ZoneGroup* zg;
  RGWZoneGroup rzg;
  int r = svc()->zone->get_zonegroup(id, rzg);
  if (r < 0)
    return r;

  zg = new RadosZoneGroup(this, rzg);
  if (!zg)
    return -ENOMEM;

  zonegroup->reset(zg);
  return 0;
}

int RadosStore::list_all_zones(const DoutPrefixProvider* dpp, std::list<std::string>& zone_ids)
{
  return svc()->zone->list_zones(dpp, zone_ids);
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

std::unique_ptr<Notification> RadosStore::get_notification(
  rgw::sal::Object* obj, rgw::sal::Object* src_obj, req_state* s, rgw::notify::EventType event_type, optional_yield y, const std::string* object_name)
{
  return std::make_unique<RadosNotification>(s, this, obj, src_obj, s, event_type, y, object_name);
}

std::unique_ptr<Notification> RadosStore::get_notification(const DoutPrefixProvider* dpp, rgw::sal::Object* obj, rgw::sal::Object* src_obj, rgw::notify::EventType event_type, rgw::sal::Bucket* _bucket, std::string& _user_id, std::string& _user_tenant, std::string& _req_id, optional_yield y)
{
  return std::make_unique<RadosNotification>(dpp, this, obj, src_obj, event_type, _bucket, _user_id, _user_tenant, _req_id, y);
}

std::string RadosStore::topics_oid(const std::string& tenant) const {
  return pubsub_oid_prefix + tenant;
}

int RadosStore::read_topics(const std::string& tenant, rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) {
  bufferlist bl;
  const int ret = rgw_get_system_obj(svc()->sysobj,
                               svc()->zone->get_zone_params().log_pool,
                               topics_oid(tenant),
                               bl,
                               objv_tracker,
                               nullptr, y, dpp, nullptr);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(topics, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 20) << " failed to decode topics from oid: " << topics_oid(tenant) <<
      ". error: " << err.what() << dendl;
    return -EIO;
  }

  return 0;
}

int RadosStore::write_topics(const std::string& tenant, const rgw_pubsub_topics& topics, RGWObjVersionTracker* objv_tracker,
	optional_yield y, const DoutPrefixProvider *dpp) {
  bufferlist bl;
  encode(topics, bl);

  return rgw_put_system_obj(dpp, svc()->sysobj,
      svc()->zone->get_zone_params().log_pool,
      topics_oid(tenant),
      bl, false, objv_tracker, real_time(), y);
}

int RadosStore::remove_topics(const std::string& tenant, RGWObjVersionTracker* objv_tracker,
        optional_yield y, const DoutPrefixProvider *dpp) {
  return rgw_delete_system_obj(dpp, svc()->sysobj,
      svc()->zone->get_zone_params().log_pool,
      topics_oid(tenant),
      objv_tracker, y);
}

int RadosStore::read_topic_v2(const std::string& topic_name,
                              const std::string& tenant,
                              rgw_pubsub_topic& topic,
                              RGWObjVersionTracker* objv_tracker,
                              optional_yield y,
                              const DoutPrefixProvider* dpp) {
  bufferlist bl;
  auto mtime = ceph::real_clock::zero();
  RGWSI_MBSObj_GetParams params(&bl, nullptr, &mtime);
  std::unique_ptr<RGWSI_MetaBackend::Context> ctx(
      svc()->topic->svc.meta_be->alloc_ctx());
  ctx->init(svc()->topic->get_be_handler());
  const int ret = svc()->topic->svc.meta_be->get(
      ctx.get(), get_topic_key(topic_name, tenant), params, objv_tracker, y,
      dpp);
  if (ret < 0) {
    return ret;
  }

  auto iter = bl.cbegin();
  try {
    decode(topic, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 20) << " failed to decode topic: " << topic_name
                       << ". error: " << err.what() << dendl;
    return -EIO;
  }
  return 0;
}

int RadosStore::write_topic_v2(const rgw_pubsub_topic& topic,
                               RGWObjVersionTracker* objv_tracker,
                               optional_yield y,
                               const DoutPrefixProvider* dpp) {
  bufferlist bl;
  encode(topic, bl);
  RGWSI_MBSObj_PutParams params(bl, nullptr, ceph::real_clock::zero(),
                                /*exclusive*/ false);
  std::unique_ptr<RGWSI_MetaBackend::Context> ctx(
      svc()->topic->svc.meta_be->alloc_ctx());
  ctx->init(svc()->topic->get_be_handler());
  return svc()->topic->svc.meta_be->put(
      ctx.get(), get_topic_key(topic.name, topic.user.tenant), params,
      objv_tracker, y, dpp);
}

int RadosStore::remove_topic_v2(const std::string& topic_name,
                                const std::string& tenant,
                                RGWObjVersionTracker* objv_tracker,
                                optional_yield y,
                                const DoutPrefixProvider* dpp) {
  RGWSI_MBSObj_RemoveParams params;
  std::unique_ptr<RGWSI_MetaBackend::Context> ctx(
      svc()->topic->svc.meta_be->alloc_ctx());
  ctx->init(svc()->topic->get_be_handler());
  return svc()->topic->svc.meta_be->remove(ctx.get(),
                                           get_topic_key(topic_name, tenant),
                                           params, objv_tracker, y, dpp);
}

int RadosStore::remove_bucket_mapping_from_topics(
    const rgw_pubsub_bucket_topics& bucket_topics,
    const std::string& bucket_key,
    optional_yield y,
    const DoutPrefixProvider* dpp) {
  // remove the bucket name from  the topic-bucket omap for each topic
  // subscribed.
  std::unordered_set<std::string> topics_mapping_to_remove;
  int ret = 0;
  for (const auto& [_, topic_filter] : bucket_topics.topics) {
    if (!topics_mapping_to_remove.insert(topic_filter.topic.name).second) {
      continue;  // already removed.
    }
    int op_ret = update_bucket_topic_mapping(topic_filter.topic, bucket_key,
                                             /*add_mapping=*/false, y, dpp);
    if (op_ret < 0) {
      ret = op_ret;
    }
  }
  return ret;
}

int RadosStore::update_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                            const std::string& bucket_key,
                                            bool add_mapping,
                                            optional_yield y,
                                            const DoutPrefixProvider* dpp) {
  bufferlist empty_bl;
  librados::ObjectWriteOperation op;
  int ret = 0;
  if (add_mapping) {
    std::map<std::string, bufferlist> mapping{{bucket_key, empty_bl}};
    op.omap_set(mapping);
  } else {
    std::set<std::string> to_rm{{bucket_key}};
    op.omap_rm_keys(to_rm);
  }
  ret = rgw_rados_operate(dpp, rados->get_topics_pool_ctx(),
                          get_bucket_topic_mapping_oid(topic), &op, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to " << (add_mapping ? "add" : "remove")
                      << " topic bucket mapping for bucket: " << bucket_key
                      << " and topic: " << topic.name << " with ret:" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20) << "Successfully " << (add_mapping ? "added" : "removed")
                     << " topic bucket mapping for bucket: " << bucket_key
                     << " and topic: " << topic.name << dendl;
  return ret;
}

int RadosStore::get_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                         std::set<std::string>& bucket_keys,
                                         optional_yield y,
                                         const DoutPrefixProvider* dpp) {
  constexpr auto max_chunk = 1024U;
  std::string start_after;
  bool more = true;
  int rval;
  while (more) {
    librados::ObjectReadOperation op;
    std::set<std::string> curr_keys;
    op.omap_get_keys2(start_after, max_chunk, &curr_keys, &more, &rval);
    const auto ret =
        rgw_rados_operate(dpp, rados->get_topics_pool_ctx(),
                          get_bucket_topic_mapping_oid(topic), &op, nullptr, y);
    if (ret == -ENOENT) {
      // mapping object was not created - nothing to do
      return 0;
    }
    if (ret < 0) {
      // TODO: do we need to check on rval as well as ret?
      ldpp_dout(dpp, 1)
          << "ERROR: failed to read bucket topic mapping object for topic: "
          << topic.name << ", ret= " << ret << dendl;
      return ret;
    }
    if (more) {
      if (curr_keys.empty()) {
        return -EINVAL;  // something wrong.
      }
      start_after = *curr_keys.rbegin();
    }
    bucket_keys.merge(curr_keys);
  }
  return 0;
}

int RadosStore::delete_bucket_topic_mapping(const rgw_pubsub_topic& topic,
                                            optional_yield y,
                                            const DoutPrefixProvider* dpp) {
  librados::ObjectWriteOperation op;
  op.remove();
  const int ret =
      rgw_rados_operate(dpp, rados->get_topics_pool_ctx(),
                        get_bucket_topic_mapping_oid(topic), &op, y);
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 1)
        << "ERROR: failed removing bucket topic mapping omap for topic: "
        << topic.name << ", ret=" << ret << dendl;
    return ret;
  }
  ldpp_dout(dpp, 20)
      << "Successfully deleted topic bucket mapping omap for topic: "
      << topic.name << dendl;
  return 0;
}

int RadosStore::delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, optional_yield y)
{
  return rados->delete_raw_obj(dpp, obj, y);
}

void RadosStore::get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj)
{
    rados->obj_to_raw(placement_rule, obj, raw_obj);
}

int RadosStore::get_raw_chunk_size(const DoutPrefixProvider* dpp, const rgw_raw_obj& obj, uint64_t* chunk_size)
{
  return rados->get_max_chunk_size(obj.pool, chunk_size, dpp);
}

int RadosStore::init_neorados(const DoutPrefixProvider* dpp) {
  if (!neorados) try {
      neorados = neorados::RADOS::make_with_cct(dpp->get_cct(), io_context,
						ceph::async::use_blocked);
    } catch (const boost::system::system_error& e) {
      ldpp_dout(dpp, 0) << "ERROR: creating neorados handle failed: "
			<< e.what() << dendl;
      return ceph::from_error_code(e.code());
    }
  return 0;
}

int RadosStore::initialize(CephContext *cct, const DoutPrefixProvider *dpp)
{
  std::unique_ptr<ZoneGroup> zg =
    std::make_unique<RadosZoneGroup>(this, svc()->zone->get_zonegroup());
  zone = make_unique<RadosZone>(this, std::move(zg));

  return init_neorados(dpp);
}

int RadosStore::log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info, optional_yield y)
{
    return rados->log_usage(dpp, usage_info, y);
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

void RadosStore::get_quota(RGWQuota& quota)
{
    quota.bucket_quota = svc()->quota->get_bucket_quota();
    quota.user_quota = svc()->quota->get_user_quota();
}

void RadosStore::get_ratelimit(RGWRateLimitInfo& bucket_ratelimit, RGWRateLimitInfo& user_ratelimit, RGWRateLimitInfo& anon_ratelimit)
{
  bucket_ratelimit = svc()->zone->get_current_period().get_config().bucket_ratelimit;
  user_ratelimit = svc()->zone->get_current_period().get_config().user_ratelimit;
  anon_ratelimit = svc()->zone->get_current_period().get_config().anon_ratelimit;
}

int RadosStore::set_buckets_enabled(const DoutPrefixProvider* dpp, vector<rgw_bucket>& buckets, bool enabled, optional_yield y)
{
    return rados->set_buckets_enabled(buckets, enabled, dpp, y);
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

int RadosStore::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch, optional_yield y)
{
  rgw_user uid;
  std::string bucket_name;

  return rados->trim_usage(dpp, uid, bucket_name, start_epoch, end_epoch, y);
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

void RadosStore::finalize(void)
{
  if (rados)
    rados->finalize();
}

void RadosStore::register_admin_apis(RGWRESTMgr* mgr)
{
  mgr->register_resource("user", new RGWRESTMgr_User);
  mgr->register_resource("bucket", new RGWRESTMgr_Bucket);
  /*Registering resource for /admin/metadata */
  mgr->register_resource("metadata", new RGWRESTMgr_Metadata);
  mgr->register_resource("log", new RGWRESTMgr_Log);
  /* XXX These may become global when cbodley is done with his zone work */
  mgr->register_resource("config", new RGWRESTMgr_Config);
  mgr->register_resource("realm", new RGWRESTMgr_Realm);
  mgr->register_resource("ratelimit", new RGWRESTMgr_Ratelimit);
}

std::unique_ptr<LuaManager> RadosStore::get_lua_manager(const std::string& luarocks_path)
{
  return std::make_unique<RadosLuaManager>(this, luarocks_path);
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

std::unique_ptr<RGWRole> RadosStore::get_role(const RGWRoleInfo& info)
{
  return std::make_unique<RadosRole>(this, info);
}

int RadosStore::get_roles(const DoutPrefixProvider *dpp,
			  optional_yield y,
			  const std::string& path_prefix,
			  const std::string& tenant,
			  vector<std::unique_ptr<RGWRole>>& roles)
{
  auto pool = svc()->zone->get_zone_params().roles_pool;
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
				   vector<std::unique_ptr<RGWOIDCProvider>>& providers, optional_yield y)
{
  std::string prefix = tenant + RGWOIDCProvider::oidc_url_oid_prefix;
  auto pool = svc()->zone->get_zone_params().oidc_pool;

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

      r = rgw_get_system_obj(svc()->sysobj, pool, iter, bl, nullptr, nullptr, y, dpp);
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
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  const std::string& unique_tag,
				  uint64_t position,
				  uint64_t *cur_accounted_size)
{
  RGWBucketInfo& bucket_info = obj->get_bucket()->get_info();
  RGWObjectCtx& obj_ctx = static_cast<RadosObject*>(obj)->get_ctx();
  auto aio = rgw::make_throttle(ctx()->_conf->rgw_put_obj_min_window_size, y);
  return std::make_unique<RadosAppendWriter>(dpp, y,
				 bucket_info, obj_ctx, obj->get_obj(),
				 this, std::move(aio), owner,
				 ptail_placement_rule,
				 unique_tag, position,
				 cur_accounted_size, obj->get_trace());
}

std::unique_ptr<Writer> RadosStore::get_atomic_writer(const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t olh_epoch,
				  const std::string& unique_tag)
{
  RGWBucketInfo& bucket_info = obj->get_bucket()->get_info();
  RGWObjectCtx& obj_ctx = static_cast<RadosObject*>(obj)->get_ctx();
  auto aio = rgw::make_throttle(ctx()->_conf->rgw_put_obj_min_window_size, y);
  return std::make_unique<RadosAtomicWriter>(dpp, y,
				 bucket_info, obj_ctx, obj->get_obj(),
				 this, std::move(aio), owner,
				 ptail_placement_rule,
				 olh_epoch, unique_tag, obj->get_trace());
}

const std::string& RadosStore::get_compression_type(const rgw_placement_rule& rule)
{
      return svc()->zone->get_zone_params().get_compression_type(rule);
}

bool RadosStore::valid_placement(const rgw_placement_rule& rule)
{
  return svc()->zone->get_zone_params().valid_placement(rule);
}

int RadosStore::get_obj_head_ioctx(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx* ioctx)
{
  return rados->get_obj_head_ioctx(dpp, bucket_info, obj, ioctx);
}

RadosObject::~RadosObject()
{
  if (rados_ctx_owned)
    delete rados_ctx;
}

int RadosObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **pstate, optional_yield y, bool follow_olh)
{
  int ret = store->getRados()->get_obj_state(dpp, rados_ctx, bucket->get_info(), get_obj(), pstate, &manifest, follow_olh, y);
  if (ret < 0) {
    return ret;
  }

  /* Don't overwrite obj, atomic, or prefetch */
  rgw_obj obj = get_obj();
  bool is_atomic = state.is_atomic;
  bool prefetch_data = state.prefetch_data;

  state = **pstate;

  state.obj = obj;
  state.is_atomic = is_atomic;
  state.prefetch_data = prefetch_data;
  return ret;
}

int RadosObject::read_attrs(const DoutPrefixProvider* dpp, RGWRados::Object::Read &read_op, optional_yield y, rgw_obj* target_obj)
{
  read_op.params.attrs = &state.attrset;
  read_op.params.target_obj = target_obj;
  read_op.params.obj_size = &state.size;
  read_op.params.lastmod = &state.mtime;

  return read_op.prepare(y, dpp);
}

int RadosObject::set_obj_attrs(const DoutPrefixProvider* dpp, Attrs* setattrs, Attrs* delattrs, optional_yield y)
{
  Attrs empty;
  return store->getRados()->set_attrs(dpp, rados_ctx,
			bucket->get_info(),
			get_obj(),
			setattrs ? *setattrs : empty,
			delattrs ? delattrs : nullptr,
			y);
}

int RadosObject::get_obj_attrs(optional_yield y, const DoutPrefixProvider* dpp, rgw_obj* target_obj)
{
  RGWRados::Object op_target(store->getRados(), bucket->get_info(), *rados_ctx, get_obj());
  RGWRados::Object::Read read_op(&op_target);

  return read_attrs(dpp, read_op, y, target_obj);
}

int RadosObject::modify_obj_attrs(const char* attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider* dpp)
{
  rgw_obj target = get_obj();
  rgw_obj save = get_obj();
  int r = get_obj_attrs(y, dpp, &target);
  if (r < 0) {
    return r;
  }

  /* Temporarily set target */
  state.obj = target;
  set_atomic();
  state.attrset[attr_name] = attr_val;
  r = set_obj_attrs(dpp, &state.attrset, nullptr, y);
  /* Restore target */
  state.obj = save;

  return r;
}

int RadosObject::delete_obj_attrs(const DoutPrefixProvider* dpp, const char* attr_name, optional_yield y)
{
  Attrs rmattr;
  bufferlist bl;

  set_atomic();
  rmattr[attr_name] = bl;
  return set_obj_attrs(dpp, nullptr, &rmattr, y);
}

bool RadosObject::is_expired() {
  auto iter = state.attrset.find(RGW_ATTR_DELETE_AT);
  if (iter == state.attrset.end()) {
    return false;
  }
  utime_t delete_at;
  try {
    auto bufit = iter->second.cbegin();
    decode(delete_at, bufit);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: " << __func__ << ": failed to decode " RGW_ATTR_DELETE_AT " attr" << dendl;
    return false;
  }

  return delete_at <= ceph_clock_now() && !delete_at.is_zero();
}

void RadosObject::gen_rand_obj_instance_name()
{
  store->getRados()->gen_rand_obj_instance_name(&state.obj.key);
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

int RadosObject::get_torrent_info(const DoutPrefixProvider* dpp,
                                  optional_yield y, bufferlist& bl)
{
  // try to read torrent info from attr
  int ret = StoreObject::get_torrent_info(dpp, y, bl);
  if (ret >= 0) {
    return ret;
  }

  // try falling back to old torrent info stored in omap
  rgw_raw_obj raw_obj;
  get_raw_obj(&raw_obj);

  rgw_rados_ref ref;
  ret = store->getRados()->get_raw_obj_ref(dpp, raw_obj, &ref);
  if (ret < 0) {
    return ret;
  }

  const std::set<std::string> keys = {"rgw.torrent"};
  std::map<std::string, bufferlist> result;

  librados::ObjectReadOperation op;
  op.omap_get_vals_by_keys(keys, &result, nullptr);

  ret = rgw_rados_operate(dpp, ref.ioctx, ref.obj.oid, &op, nullptr, y);
  if (ret < 0) {
    return ret;
  }
  if (result.empty()) { // omap key not found
    return -ENOENT;
  }
  bl = std::move(result.begin()->second);
  return 0;
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

  auto sysobj = store->svc()->sysobj->get_obj(raw_meta_obj);

  return sysobj.omap().set_must_exist(must_exist).set(dpp, key, val, y);
}

int RadosObject::chown(User& new_user, const DoutPrefixProvider* dpp, optional_yield y)
{
  int r = get_obj_attrs(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to read object attrs " << get_name() << cpp_strerror(-r) << dendl;
    return r;
  }

  const auto& aiter = get_attrs().find(RGW_ATTR_ACL);
  if (aiter == get_attrs().end()) {
    ldpp_dout(dpp, 0) << "ERROR: no acls found for object " << get_name() << dendl;
    return -EINVAL;
  }

  bufferlist& bl = aiter->second;
  RGWAccessControlPolicy policy;
  ACLOwner owner;
  auto bliter = bl.cbegin();
  try {
    policy.decode(bliter);
    owner = policy.get_owner();
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: decode policy failed" << err.what()
      << dendl;
    return -EIO;
  }

  //Get the ACL from the policy
  RGWAccessControlList& acl = policy.get_acl();

  //Remove grant that is set to old owner
  acl.remove_canon_user_grant(owner.id);

  //Create a grant and add grant
  ACLGrant grant;
  grant.set_canon(new_user.get_id(), new_user.get_display_name(), RGW_PERM_FULL_CONTROL);
  acl.add_grant(grant);

  //Update the ACL owner to the new user
  owner.id = new_user.get_id();
  owner.display_name = new_user.get_display_name();
  policy.set_owner(owner);

  bl.clear();
  encode(policy, bl);

  set_atomic();
  map<string, bufferlist> attrs;
  attrs[RGW_ATTR_ACL] = bl;
  r = set_obj_attrs(dpp, &attrs, nullptr, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: modify attr failed " << cpp_strerror(-r) << dendl;
    return r;
  }

  return 0;
}

std::unique_ptr<MPSerializer> RadosObject::get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name)
{
  return std::make_unique<MPRadosSerializer>(dpp, store, this, lock_name);
}

int RadosObject::transition(Bucket* bucket,
			    const rgw_placement_rule& placement_rule,
			    const real_time& mtime,
			    uint64_t olh_epoch,
			    const DoutPrefixProvider* dpp,
			    optional_yield y,
                            uint32_t flags)
{
  return store->getRados()->transition_obj(*rados_ctx, bucket->get_info(), get_obj(), placement_rule,
                                           mtime, olh_epoch, dpp, y, flags & FLAG_LOG_OP);
}

int RadosObject::transition_to_cloud(Bucket* bucket,
			   rgw::sal::PlacementTier* tier,
			   rgw_bucket_dir_entry& o,
			   std::set<std::string>& cloud_targets,
			   CephContext* cct,
			   bool update_object,
			   const DoutPrefixProvider* dpp,
			   optional_yield y)
{
  /* init */
  rgw::sal::RadosPlacementTier* rtier = static_cast<rgw::sal::RadosPlacementTier*>(tier);
  string id = "cloudid";
  string endpoint = rtier->get_rt().t.s3.endpoint;
  RGWAccessKey key = rtier->get_rt().t.s3.key;
  string region = rtier->get_rt().t.s3.region;
  HostStyle host_style = rtier->get_rt().t.s3.host_style;
  string bucket_name = rtier->get_rt().t.s3.target_path;
  const rgw::sal::ZoneGroup& zonegroup = store->get_zone()->get_zonegroup();

  if (bucket_name.empty()) {
    bucket_name = "rgwx-" + zonegroup.get_name() + "-" + tier->get_storage_class() +
                    "-cloud-bucket";
    boost::algorithm::to_lower(bucket_name);
  }

  /* Create RGW REST connection */
  S3RESTConn conn(cct, id, { endpoint }, key, zonegroup.get_id(), region, host_style);

  RGWLCCloudTierCtx tier_ctx(cct, dpp, o, store, bucket->get_info(),
			     this, conn, bucket_name,
			     rtier->get_rt().t.s3.target_storage_class);
  tier_ctx.acl_mappings = rtier->get_rt().t.s3.acl_mappings;
  tier_ctx.multipart_min_part_size = rtier->get_rt().t.s3.multipart_min_part_size;
  tier_ctx.multipart_sync_threshold = rtier->get_rt().t.s3.multipart_sync_threshold;
  tier_ctx.storage_class = tier->get_storage_class();

  ldpp_dout(dpp, 0) << "Transitioning object(" << o.key << ") to the cloud endpoint(" << endpoint << ")" << dendl;

  /* Transition object to cloud end point */
  int ret = rgw_cloud_tier_transfer_object(tier_ctx, cloud_targets);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to transfer object(" << o.key << ") to the cloud endpoint(" << endpoint << ") ret=" << ret << dendl;
    return ret;
  }

  if (update_object) {
    real_time read_mtime;

    std::unique_ptr<rgw::sal::Object::ReadOp> read_op(get_read_op());
    read_op->params.lastmod = &read_mtime;

    ret = read_op->prepare(y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: Updating tier object(" << o.key << ") failed ret=" << ret << dendl;
      return ret;
    }

    if (read_mtime != tier_ctx.o.meta.mtime) {
      /* raced */
      ldpp_dout(dpp, 0) << "ERROR: Updating tier object(" << o.key << ") failed ret=" << -ECANCELED << dendl;
      return -ECANCELED;
    }

    rgw_placement_rule target_placement;
    target_placement.inherit_from(tier_ctx.bucket_info.placement_rule);
    target_placement.storage_class = tier->get_storage_class();

    ret = write_cloud_tier(dpp, y, tier_ctx.o.versioned_epoch,
			   tier, tier_ctx.is_multipart_upload,
			   target_placement, tier_ctx.obj);

  }

  return ret;
}

int RadosObject::write_cloud_tier(const DoutPrefixProvider* dpp,
				  optional_yield y,
				  uint64_t olh_epoch,
				  PlacementTier* tier,
				  bool is_multipart_upload,
				  rgw_placement_rule& target_placement,
				  Object* head_obj)
{
  rgw::sal::RadosPlacementTier* rtier = static_cast<rgw::sal::RadosPlacementTier*>(tier);
  map<string, bufferlist> attrs = get_attrs();
  RGWRados::Object op_target(store->getRados(), bucket->get_info(), *rados_ctx, get_obj());
  RGWRados::Object::Write obj_op(&op_target);

  obj_op.meta.modify_tail = true;
  obj_op.meta.flags = PUT_OBJ_CREATE;
  obj_op.meta.category = RGWObjCategory::CloudTiered;
  obj_op.meta.delete_at = real_time();
  bufferlist blo;
  obj_op.meta.data = &blo;
  obj_op.meta.if_match = NULL;
  obj_op.meta.if_nomatch = NULL;
  obj_op.meta.user_data = NULL;
  obj_op.meta.zones_trace = NULL;
  obj_op.meta.delete_at = real_time();
  obj_op.meta.olh_epoch = olh_epoch;

  RGWObjManifest *pmanifest;
  RGWObjManifest manifest;

  pmanifest = &manifest;
  RGWObjTier tier_config;
  tier_config.name = tier->get_storage_class();
  tier_config.tier_placement = rtier->get_rt();
  tier_config.is_multipart_upload = is_multipart_upload;

  pmanifest->set_tier_type("cloud-s3");
  pmanifest->set_tier_config(tier_config);

  /* check if its necessary */
  pmanifest->set_head(target_placement, head_obj->get_obj(), 0);
  pmanifest->set_tail_placement(target_placement, head_obj->get_obj().bucket);
  pmanifest->set_obj_size(0);
  obj_op.meta.manifest = pmanifest;

  /* update storage class */
  bufferlist bl;
  bl.append(tier->get_storage_class());
  attrs[RGW_ATTR_STORAGE_CLASS] = bl;

  attrs.erase(RGW_ATTR_ID_TAG);
  attrs.erase(RGW_ATTR_TAIL_TAG);

  const req_context rctx{dpp, y, nullptr};
  return obj_op.write_meta(0, 0, attrs, rctx, head_obj->get_trace());
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

int RadosObject::dump_obj_layout(const DoutPrefixProvider *dpp, optional_yield y, Formatter* f)
{
  int ret;
  RGWObjManifest *amanifest{nullptr};
  rgw_raw_obj head_obj;

  RGWRados::Object op_target(store->getRados(), bucket->get_info(), *rados_ctx, get_obj());
  RGWRados::Object::Read parent_op(&op_target);
  uint64_t obj_size;

  parent_op.params.obj_size = &obj_size;
  parent_op.params.attrs = &get_attrs();

  ret = parent_op.prepare(y, dpp);
  if (ret < 0) {
    return ret;
  }

  head_obj = parent_op.state.head_obj;

  ret = op_target.get_manifest(dpp, &amanifest, y);
  if (ret < 0) {
    return ret;
  }

  ::encode_json("head", head_obj, f);
  ::encode_json("manifest", *amanifest, f);
  f->open_array_section("data_location");
  for (auto miter = amanifest->obj_begin(dpp); miter != amanifest->obj_end(dpp); ++miter) {
    f->open_object_section("obj");
    rgw_raw_obj raw_loc = miter.get_location().get_raw_obj(store->getRados());
    uint64_t ofs = miter.get_ofs();
    uint64_t left = amanifest->get_obj_size() - ofs;
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

std::unique_ptr<Object::ReadOp> RadosObject::get_read_op()
{
  return std::make_unique<RadosObject::RadosReadOp>(this, rados_ctx);
}

RadosObject::RadosReadOp::RadosReadOp(RadosObject *_source, RGWObjectCtx *_octx) :
	source(_source),
	octx(_octx),
	op_target(_source->store->getRados(),
		  _source->get_bucket()->get_info(),
		  *static_cast<RGWObjectCtx *>(octx),
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
  parent_op.params.part_num = params.part_num;
  parent_op.params.obj_size = &obj_size;
  parent_op.params.attrs = &source->get_attrs();

  int ret = parent_op.prepare(y, dpp);
  if (ret < 0)
    return ret;

  source->set_instance(parent_op.state.obj.key.instance);
  source->set_obj_size(obj_size);
  params.parts_count = parent_op.params.parts_count;

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

std::unique_ptr<Object::DeleteOp> RadosObject::get_delete_op()
{
  return std::make_unique<RadosObject::RadosDeleteOp>(this);
}

RadosObject::RadosDeleteOp::RadosDeleteOp(RadosObject *_source) :
	source(_source),
	op_target(_source->store->getRados(),
		  _source->get_bucket()->get_info(),
		  _source->get_ctx(),
		  _source->get_obj()),
	parent_op(&op_target)
{ }

int RadosObject::RadosDeleteOp::delete_obj(const DoutPrefixProvider* dpp, optional_yield y, uint32_t flags)
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

  int ret = parent_op.delete_obj(y, dpp, flags & FLAG_LOG_OP);
  if (ret < 0)
    return ret;

  result.delete_marker = parent_op.result.delete_marker;
  result.version_id = parent_op.result.version_id;

  return ret;
}

int RadosObject::delete_object(const DoutPrefixProvider* dpp,
			       optional_yield y,
			       uint32_t flags)
{
  RGWRados::Object del_target(store->getRados(), bucket->get_info(), *rados_ctx, get_obj());
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket->get_info().owner;
  del_op.params.versioning_status = (flags & FLAG_PREVENT_VERSIONING)
                                    ? 0 : bucket->get_info().versioning_status();

  return del_op.delete_obj(y, dpp, flags & FLAG_LOG_OP);
}

int RadosObject::copy_object(User* user,
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
  return store->getRados()->copy_obj(*rados_ctx,
				     user->get_id(),
				     info,
				     source_zone,
				     dest_object->get_obj(),
				     get_obj(),
				     dest_bucket->get_info(),
				     src_bucket->get_info(),
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
				     y,
                                     dest_object->get_trace());
}

int RadosObject::RadosReadOp::iterate(const DoutPrefixProvider* dpp, int64_t ofs, int64_t end, RGWGetDataCB* cb, optional_yield y)
{
  return parent_op.iterate(dpp, ofs, end, cb, y);
}

int RadosObject::swift_versioning_restore(bool& restored,
					  const DoutPrefixProvider* dpp, optional_yield y)
{
  rgw_obj obj = get_obj();
  return store->getRados()->swift_versioning_restore(*rados_ctx,
						     bucket->get_owner(),
						     bucket->get_info(),
						     obj,
						     restored,
						     dpp, y);
}

int RadosObject::swift_versioning_copy(const DoutPrefixProvider* dpp, optional_yield y)
{
  return store->getRados()->swift_versioning_copy(*rados_ctx,
                                        bucket->get_info().owner,
                                        bucket->get_info(),
                                        get_obj(),
                                        dpp,
                                        y);
}

int RadosMultipartUpload::cleanup_part_history(const DoutPrefixProvider* dpp,
                                               optional_yield y,
                                               RadosMultipartPart *part,
                                               list<rgw_obj_index_key>& remove_objs)
{
  cls_rgw_obj_chain chain;
  for (auto& ppfx : part->get_past_prefixes()) {
    rgw_obj past_obj;
    past_obj.init_ns(bucket->get_key(), ppfx + "." + std::to_string(part->info.num), mp_ns);
    rgw_obj_index_key past_key;
    past_obj.key.get_index_key(&past_key);
    // Remove past upload part objects from index, too.
    remove_objs.push_back(past_key);

    RGWObjManifest manifest = part->get_manifest();
    manifest.set_prefix(ppfx);
    RGWObjManifest::obj_iterator miter = manifest.obj_begin(dpp);
    for (; miter != manifest.obj_end(dpp); ++miter) {
      rgw_raw_obj raw_part_obj = miter.get_location().get_raw_obj(store->getRados());
      cls_rgw_obj_key part_key(raw_part_obj.oid);
      chain.push_obj(raw_part_obj.pool.to_str(), part_key, raw_part_obj.loc);
    }
  }
  if (store->getRados()->get_gc() == nullptr) {
    // Delete objects inline if gc hasn't been initialised (in case when bypass gc is specified)
    store->getRados()->delete_objs_inline(dpp, chain, mp_obj.get_upload_id());
  } else {
    // use upload id as tag and do it synchronously
    auto [ret, leftover_chain] = store->getRados()->send_chain_to_gc(chain, mp_obj.get_upload_id(), y);
    if (ret < 0 && leftover_chain) {
      ldpp_dout(dpp, 5) << __func__ << ": gc->send_chain() returned " << ret << dendl;
      if (ret == -ENOENT) {
        return -ERR_NO_SUCH_UPLOAD;
      }
      // Delete objects inline if send chain to gc fails
      store->getRados()->delete_objs_inline(dpp, *leftover_chain, mp_obj.get_upload_id());
    }
  }
  return 0;
}


int RadosMultipartUpload::abort(const DoutPrefixProvider *dpp, CephContext *cct, optional_yield y)
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
    ret = list_parts(dpp, cct, 1000, marker, &marker, &truncated, y);
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
	ret = obj->delete_object(dpp, y, 0);
        if (ret < 0 && ret != -ENOENT)
          return ret;
      } else {
	auto target = meta_obj->get_obj();
	store->getRados()->update_gc_chain(dpp, target, obj_part->info.manifest, &chain);
        RGWObjManifest::obj_iterator oiter = obj_part->info.manifest.obj_begin(dpp);
        if (oiter != obj_part->info.manifest.obj_end(dpp)) {
	  std::unique_ptr<rgw::sal::Object> head = bucket->get_object(rgw_obj_key());
          rgw_raw_obj raw_head = oiter.get_location().get_raw_obj(store->getRados());
	  dynamic_cast<rgw::sal::RadosObject*>(head.get())->raw_obj_to_obj(raw_head);

          rgw_obj_index_key key;
          head->get_key().get_index_key(&key);
          remove_objs.push_back(key);

          cleanup_part_history(dpp, null_yield, obj_part, remove_objs);
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
    auto [ret, leftover_chain] = store->getRados()->send_chain_to_gc(chain, mp_obj.get_upload_id(), y);
    if (ret < 0 && leftover_chain) {
      ldpp_dout(dpp, 5) << __func__ << ": gc->send_chain() returned " << ret << dendl;
      if (ret == -ENOENT) {
        return -ERR_NO_SUCH_UPLOAD;
      }
      //Delete objects inline if send chain to gc fails
      store->getRados()->delete_objs_inline(dpp, *leftover_chain, mp_obj.get_upload_id());
    }
  }

  std::unique_ptr<rgw::sal::Object::DeleteOp> del_op = meta_obj->get_delete_op();
  del_op->params.bucket_owner.id = bucket->get_info().owner;
  del_op->params.versioning_status = 0;
  if (!remove_objs.empty()) {
    del_op->params.remove_objs = &remove_objs;
  }

  del_op->params.abortmp = true;
  del_op->params.parts_accounted_size = parts_accounted_size;

  // and also remove the metadata obj
  ret = del_op->delete_obj(dpp, y, 0);
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

int RadosMultipartUpload::init(const DoutPrefixProvider *dpp, optional_yield y, ACLOwner& owner, rgw_placement_rule& dest_placement, rgw::sal::Attrs& attrs)
{
  int ret;
  std::string oid = mp_obj.get_key();
  RGWObjectCtx obj_ctx(store);
  const req_context rctx{dpp, y, nullptr};

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
			       obj_ctx, obj->get_obj());
    RGWRados::Object::Write obj_op(&op_target);

    op_target.set_versioning_disabled(true); /* no versioning for multipart meta */
    obj_op.meta.owner = owner.id;
    obj_op.meta.category = RGWObjCategory::MultiMeta;
    obj_op.meta.flags = PUT_OBJ_CREATE_EXCL;
    obj_op.meta.mtime = &mtime;

    multipart_upload_info upload_info;
    upload_info.dest_placement = dest_placement;
    
    if (obj_legal_hold) {
      upload_info.obj_legal_hold_exist = true;
      upload_info.obj_legal_hold = (*obj_legal_hold);
    }
    if (obj_retention) {
      upload_info.obj_retention_exist = true;
      upload_info.obj_retention = (*obj_retention);
    }

    bufferlist bl;
    encode(upload_info, bl);
    obj_op.meta.data = &bl;

    ret = obj_op.write_meta(bl.length(), 0, attrs, rctx, get_trace(), false);
  } while (ret == -EEXIST);

  return ret;
}

int RadosMultipartUpload::list_parts(const DoutPrefixProvider *dpp, CephContext *cct,
				     int num_parts, int marker,
				     int *next_marker, bool *truncated, optional_yield y,
				     bool assume_unsorted)
{
  map<string, bufferlist> parts_map;
  map<string, bufferlist>::iterator iter;

  rgw_obj_key key(get_meta(), std::string(), RGW_OBJ_NS_MULTIPART);
  rgw_obj obj(bucket->get_key(), key);
  obj.in_extra_data = true;

  rgw_raw_obj raw_obj;
  store->getRados()->obj_to_raw(bucket->get_placement_rule(), obj, &raw_obj);
  auto sysobj = store->svc()->sysobj->get_obj(raw_obj);

  bool sorted_omap = is_v2_upload_id(get_upload_id()) && !assume_unsorted;

  parts.clear();

  int ret;
  if (sorted_omap) {
    string p;
    p = "part.";
    char buf[32];

    snprintf(buf, sizeof(buf), "%08d", marker);
    p.append(buf);

    ret = sysobj.omap().get_vals(dpp, p, num_parts + 1, &parts_map,
                                 nullptr, y);
  } else {
    ret = sysobj.omap().get_all(dpp, &parts_map, y);
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
        return list_parts(dpp, cct, num_parts, marker, next_marker, truncated, y, true);
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
				   rgw::sal::Object* target_obj)
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
  rgw::sal::Attrs& attrs = target_obj->get_attrs();

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
        manifest.append(dpp, obj_part.manifest, store->svc()->zone->get_zonegroup(), store->svc()->zone->get_zone_params());
        auto manifest_prefix = part->info.manifest.get_prefix();
        if (not manifest_prefix.empty()) {
          // It has an explicit prefix. Override the default one.
          src_obj.init_ns(bucket->get_key(), manifest_prefix + "." + std::to_string(part->info.num), mp_ns);
        }
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

      cleanup_part_history(dpp, y, part, remove_objs);

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

  rgw_placement_rule* ru;
  ru = &placement;
  rgw::sal::Attrs mpu_attrs; // don't overwrite the target object attrs we are updating
  ret = RadosMultipartUpload::get_info(dpp, y, &ru, &mpu_attrs);

  if (upload_information.obj_retention_exist) {
    bufferlist obj_retention_bl;
    upload_information.obj_retention.encode(obj_retention_bl);
    attrs[RGW_ATTR_OBJECT_RETENTION] = std::move(obj_retention_bl);
  }
  if (upload_information.obj_legal_hold_exist) {
    bufferlist obj_legal_hold_bl;
    upload_information.obj_legal_hold.encode(obj_legal_hold_bl);
    attrs[RGW_ATTR_OBJECT_LEGAL_HOLD] = std::move(obj_legal_hold_bl);
  }

  if (compressed) {
    // write compression attribute to full object
    bufferlist tmp;
    encode(cs_info, tmp);
    attrs[RGW_ATTR_COMPRESSION] = tmp;
  }

  target_obj->set_atomic();

  RGWRados::Object op_target(store->getRados(),
			     target_obj->get_bucket()->get_info(),
			     dynamic_cast<RadosObject*>(target_obj)->get_ctx(),
			     target_obj->get_obj());
  RGWRados::Object::Write obj_op(&op_target);

  obj_op.meta.manifest = &manifest;
  obj_op.meta.remove_objs = &remove_objs;

  obj_op.meta.ptag = &tag; /* use req_id as operation tag */
  obj_op.meta.owner = owner.id;
  obj_op.meta.flags = PUT_OBJ_CREATE;
  obj_op.meta.modify_tail = true;
  obj_op.meta.completeMultipart = true;
  obj_op.meta.olh_epoch = olh_epoch;

  const req_context rctx{dpp, y, nullptr};
  ret = obj_op.write_meta(ofs, accounted_size, attrs, rctx, get_trace());
  if (ret < 0)
    return ret;

  return ret;
}

int RadosMultipartUpload::get_info(const DoutPrefixProvider *dpp, optional_yield y, rgw_placement_rule** rule, rgw::sal::Attrs* attrs)
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
  meta_obj->set_prefetch_data();

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
  upload_information = upload_info;
  *rule = &placement;

  return 0;
}

std::unique_ptr<Writer> RadosMultipartUpload::get_writer(
				  const DoutPrefixProvider *dpp,
				  optional_yield y,
				  rgw::sal::Object* obj,
				  const rgw_user& owner,
				  const rgw_placement_rule *ptail_placement_rule,
				  uint64_t part_num,
				  const std::string& part_num_str)
{
  RGWBucketInfo& bucket_info = obj->get_bucket()->get_info();
  RGWObjectCtx& obj_ctx = static_cast<RadosObject*>(obj)->get_ctx();
  auto aio = rgw::make_throttle(store->ctx()->_conf->rgw_put_obj_min_window_size, y);
  return std::make_unique<RadosMultipartWriter>(dpp, y, get_upload_id(),
				 bucket_info, obj_ctx,
				 obj->get_obj(), store, std::move(aio), owner,
				 ptail_placement_rule, part_num, part_num_str, obj->get_trace());
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
  store->getRados()->open_pool_ctx(dpp, meta_pool, ioctx, true, true);
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
  StoreLCSerializer(_oid),
  lock(lock_name)
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
			      std::unique_ptr<LCEntry>* entry)
{
  cls_rgw_lc_entry cls_entry;
  int ret = cls_rgw_lc_get_entry(*store->getRados()->get_lc_pool_ctx(), oid, marker, cls_entry);
  if (ret)
    return ret;

  LCEntry* e;
  e = new StoreLCEntry(cls_entry.bucket, cls_entry.start_time, cls_entry.status);
  if (!e)
    return -ENOMEM;

  entry->reset(e);
  return 0;
}

int RadosLifecycle::get_next_entry(const std::string& oid, const std::string& marker,
				   std::unique_ptr<LCEntry>* entry)
{
  cls_rgw_lc_entry cls_entry;
  int ret = cls_rgw_lc_get_next_entry(*store->getRados()->get_lc_pool_ctx(), oid, marker,
				      cls_entry);

  if (ret)
    return ret;

  LCEntry* e;
  e = new StoreLCEntry(cls_entry.bucket, cls_entry.start_time, cls_entry.status);
  if (!e)
    return -ENOMEM;

  entry->reset(e);
  return 0;
}

int RadosLifecycle::set_entry(const std::string& oid, LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;

  cls_entry.bucket = entry.get_bucket();
  cls_entry.start_time = entry.get_start_time();
  cls_entry.status = entry.get_status();

  return cls_rgw_lc_set_entry(*store->getRados()->get_lc_pool_ctx(), oid, cls_entry);
}

int RadosLifecycle::list_entries(const std::string& oid, const std::string& marker,
				 uint32_t max_entries, std::vector<std::unique_ptr<LCEntry>>& entries)
{
  entries.clear();

  vector<cls_rgw_lc_entry> cls_entries;
  int ret = cls_rgw_lc_list(*store->getRados()->get_lc_pool_ctx(), oid, marker, max_entries, cls_entries);

  if (ret < 0)
    return ret;

  for (auto& entry : cls_entries) {
    entries.push_back(std::make_unique<StoreLCEntry>(entry.bucket, oid,
				entry.start_time, entry.status));
  }

  return ret;
}

int RadosLifecycle::rm_entry(const std::string& oid, LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;

  cls_entry.bucket = entry.get_bucket();
  cls_entry.start_time = entry.get_start_time();
  cls_entry.status = entry.get_status();

  return cls_rgw_lc_rm_entry(*store->getRados()->get_lc_pool_ctx(), oid, cls_entry);
}

int RadosLifecycle::get_head(const std::string& oid, std::unique_ptr<LCHead>* head)
{
  cls_rgw_lc_obj_head cls_head;
  int ret = cls_rgw_lc_get_head(*store->getRados()->get_lc_pool_ctx(), oid, cls_head);
  if (ret)
    return ret;

  LCHead* h;
  h = new StoreLCHead(cls_head.start_date, cls_head.shard_rollover_date, cls_head.marker);
  if (!h)
    return -ENOMEM;

  head->reset(h);
  return 0;
}

int RadosLifecycle::put_head(const std::string& oid, LCHead& head)
{
  cls_rgw_lc_obj_head cls_head;

  cls_head.marker = head.get_marker();
  cls_head.start_date = head.get_start_date();
  cls_head.shard_rollover_date = head.get_shard_rollover_date();

  return cls_rgw_lc_put_head(*store->getRados()->get_lc_pool_ctx(), oid, cls_head);
}

std::unique_ptr<LCSerializer> RadosLifecycle::get_serializer(const std::string& lock_name,
							     const std::string& oid,
							     const std::string& cookie)
{
  return std::make_unique<LCRadosSerializer>(store, oid, lock_name, cookie);
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
                       const req_context& rctx,
                       uint32_t flags)
{
  return processor.complete(accounted_size, etag, mtime, set_mtime, attrs, delete_at,
			    if_match, if_nomatch, user_data, zones_trace, canceled, rctx, flags);
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
                       const req_context& rctx,
                       uint32_t flags)
{
  return processor.complete(accounted_size, etag, mtime, set_mtime, attrs, delete_at,
			    if_match, if_nomatch, user_data, zones_trace, canceled, rctx, flags);
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
                       const req_context& rctx,
                       uint32_t flags)
{
  return processor.complete(accounted_size, etag, mtime, set_mtime, attrs, delete_at,
                            if_match, if_nomatch, user_data, zones_trace, canceled, rctx, flags);
}

bool RadosZoneGroup::placement_target_exists(std::string& target) const
{
  return !!group.placement_targets.count(target);
}

void RadosZoneGroup::get_placement_target_names(std::set<std::string>& names) const
{
  for (const auto& target : group.placement_targets) {
    names.emplace(target.second.name);
  }
}

int RadosZoneGroup::get_placement_tier(const rgw_placement_rule& rule,
				       std::unique_ptr<PlacementTier>* tier)
{
  std::map<std::string, RGWZoneGroupPlacementTarget>::const_iterator titer;
  titer = group.placement_targets.find(rule.name);
  if (titer == group.placement_targets.end()) {
    return -ENOENT;
  }

  const auto& target_rule = titer->second;
  std::map<std::string, RGWZoneGroupPlacementTier>::const_iterator ttier;
  ttier = target_rule.tier_targets.find(rule.storage_class);
  if (ttier == target_rule.tier_targets.end()) {
    // not found
    return -ENOENT;
  }

  PlacementTier* t;
  t = new RadosPlacementTier(store, ttier->second);
  if (!t)
    return -ENOMEM;

  tier->reset(t);
  return 0;
}

int RadosZoneGroup::get_zone_by_id(const std::string& id, std::unique_ptr<Zone>* zone)
{
  RGWZone* rz = store->svc()->zone->find_zone(id);
  if (!rz)
    return -ENOENT;

  Zone* z = new RadosZone(store, clone(), *rz);
  zone->reset(z);
  return 0;
}

int RadosZoneGroup::get_zone_by_name(const std::string& name, std::unique_ptr<Zone>* zone)
{
  rgw_zone_id id;
  int ret = store->svc()->zone->find_zone_id_by_name(name, &id);
  if (ret < 0)
    return ret;

  RGWZone* rz = store->svc()->zone->find_zone(id.id);
  if (!rz)
    return -ENOENT;

  Zone* z = new RadosZone(store, clone(), *rz);
  zone->reset(z);
  return 0;
}

int RadosZoneGroup::list_zones(std::list<std::string>& zone_ids)
{
  for (const auto& entry : group.zones)
    {
      zone_ids.push_back(entry.second.id);
    }
  return 0;
}

std::unique_ptr<Zone> RadosZone::clone()
{
  if (local_zone)
    return std::make_unique<RadosZone>(store, group->clone());

  return std::make_unique<RadosZone>(store, group->clone(), rgw_zone);
}

const std::string& RadosZone::get_id()
{
  if (local_zone)
    return store->svc()->zone->zone_id().id;

  return rgw_zone.id;
}

const std::string& RadosZone::get_name() const
{
  if (local_zone)
    return store->svc()->zone->zone_name();

  return rgw_zone.name;
}

bool RadosZone::is_writeable()
{
  if (local_zone)
    return store->svc()->zone->zone_is_writeable();

  return !rgw_zone.read_only;
}

bool RadosZone::get_redirect_endpoint(std::string* endpoint)
{
  if (local_zone)
    return store->svc()->zone->get_redirect_zone_endpoint(endpoint);

  endpoint = &rgw_zone.redirect_zone;
  return true;
}

bool RadosZone::has_zonegroup_api(const std::string& api) const
{
  return store->svc()->zone->has_zonegroup_api(api);
}

const std::string& RadosZone::get_current_period_id()
{
  return store->svc()->zone->get_current_period_id();
}

const RGWAccessKey& RadosZone::get_system_key()
{
  return store->svc()->zone->get_zone_params().system_key;
}

const std::string& RadosZone::get_realm_name()
{
  return store->svc()->zone->get_realm().get_name();
}

const std::string& RadosZone::get_realm_id()
{
  return store->svc()->zone->get_realm().get_id();
}

const std::string_view RadosZone::get_tier_type()
{
  if (local_zone)
    return store->svc()->zone->get_zone().tier_type;

  return rgw_zone.tier_type;
}

RGWBucketSyncPolicyHandlerRef RadosZone::get_sync_policy_handler()
{
  return store->svc()->zone->get_sync_policy_handler(get_id());
}

RadosLuaManager::RadosLuaManager(RadosStore* _s, const std::string& _luarocks_path) :
  StoreLuaManager(_luarocks_path),
  store(_s),
  pool((store->svc() && store->svc()->zone) ? store->svc()->zone->get_zone_params().log_pool : rgw_pool()),
  ioctx(*store->getRados()->get_lc_pool_ctx()),
  packages_watcher(this)
{ }

int RadosLuaManager::get_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, std::string& script)
{
  if (pool.empty()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when reading Lua script " << dendl;
    return 0;
  }
  bufferlist bl;

  int r = rgw_get_system_obj(store->svc()->sysobj, pool, key, bl, nullptr, nullptr, y, dpp);
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

int RadosLuaManager::put_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key, const std::string& script)
{
  if (pool.empty()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when writing Lua script " << dendl;
    return 0;
  }
  bufferlist bl;
  ceph::encode(script, bl);

  int r = rgw_put_system_obj(dpp, store->svc()->sysobj, pool, key, bl, false, nullptr, real_time(), y);
  if (r < 0) {
    return r;
  }

  return 0;
}

int RadosLuaManager::del_script(const DoutPrefixProvider* dpp, optional_yield y, const std::string& key)
{
  if (pool.empty()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when deleting Lua script " << dendl;
    return 0;
  }
  int r = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, key, nullptr, y);
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  return 0;
}

const std::string PACKAGE_LIST_OBJECT_NAME = "lua_package_allowlist";

int RadosLuaManager::add_package(const DoutPrefixProvider *dpp, optional_yield y, const std::string& package_name)
{
  if (!ioctx.is_valid()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when adding Lua package" << dendl;
    return 0;
  }
  // add package to list
  const bufferlist empty_bl;
  std::map<std::string, bufferlist> new_package{{package_name, empty_bl}};
  librados::ObjectWriteOperation op;
  op.omap_set(new_package);
  return rgw_rados_operate(dpp, ioctx,
      PACKAGE_LIST_OBJECT_NAME, &op, y);
}

int RadosLuaManager::remove_package(const DoutPrefixProvider *dpp, optional_yield y, const std::string& package_name)
{
  if (!ioctx.is_valid()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when removing Lua package" << dendl;
    return -ENOENT;
  }
  librados::ObjectWriteOperation op;
  size_t pos = package_name.find(" ");
  if (pos != package_name.npos) {
    // remove specific version of the the package
    op.omap_rm_keys(std::set<std::string>({package_name}));
    auto ret = rgw_rados_operate(dpp, ioctx,
        PACKAGE_LIST_OBJECT_NAME, &op, y);
    if (ret < 0) {
        return ret;
    }
    return 0;
  }
  // otherwise, remove any existing versions of the package
  rgw::lua::packages_t packages;
  auto ret = list_packages(dpp, y, packages);
  if (ret < 0 && ret != -ENOENT) {
    return ret;
  }
  for(const auto& package : packages) {
    const std::string package_no_version = package.substr(0, package.find(" "));
    if (package_no_version.compare(package_name) == 0) {
        op.omap_rm_keys(std::set<std::string>({package}));
        ret = rgw_rados_operate(dpp, ioctx,
            PACKAGE_LIST_OBJECT_NAME, &op, y);
        if (ret < 0) {
            return ret;
        }
    }
  }
  return 0;
}

int RadosLuaManager::list_packages(const DoutPrefixProvider *dpp, optional_yield y, rgw::lua::packages_t& packages)
{
  if (!ioctx.is_valid()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when listing Lua packages" << dendl;
    return -ENOENT;
  }
  constexpr auto max_chunk = 1024U;
  std::string start_after;
  bool more = true;
  int rval;
  while (more) {
    librados::ObjectReadOperation op;
    rgw::lua::packages_t packages_chunk;
    op.omap_get_keys2(start_after, max_chunk, &packages_chunk, &more, &rval);
    const auto ret = rgw_rados_operate(dpp, ioctx,
      PACKAGE_LIST_OBJECT_NAME, &op, nullptr, y);

    if (ret < 0) {
      return ret;
    }

    packages.merge(packages_chunk);
  }

  return 0;
}

int RadosLuaManager::watch_reload(const DoutPrefixProvider* dpp)
{
  if (!ioctx.is_valid()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when watching reloads of Lua packages" << dendl;
    return -ENOENT;
  }
  // create the object to watch (object may already exist)
  librados::ObjectWriteOperation op;
  op.create(false);
  auto r = rgw_rados_operate(dpp, ioctx,
      PACKAGE_LIST_OBJECT_NAME, &op, null_yield);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to watch " << PACKAGE_LIST_OBJECT_NAME
        << ". cannot create object. error: " << cpp_strerror(r) << dendl;
    return r;
  }
  r = ioctx.watch2(PACKAGE_LIST_OBJECT_NAME, &watch_handle, &packages_watcher);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to watch " << PACKAGE_LIST_OBJECT_NAME
        << ". error: " << cpp_strerror(r) << dendl;
    return r;
  }
  ldpp_dout(dpp, 20) << "Started watching for reloads of  " << PACKAGE_LIST_OBJECT_NAME
    << " with handle: " << watch_handle << dendl;

  return 0;
}

int RadosLuaManager::unwatch_reload(const DoutPrefixProvider* dpp)
{
  if (watch_handle == 0) {
    // nothing to unwatch
    return 0;
  }

  if (!ioctx.is_valid()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when unwatching reloads of Lua packages" << dendl;
    return -ENOENT;
  }
  const auto r = ioctx.unwatch2(watch_handle);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to unwatch " << PACKAGE_LIST_OBJECT_NAME
        << ". error: " << cpp_strerror(r) << dendl;
    return r;
  }
  ldpp_dout(dpp, 20) << "Stopped watching for reloads of " << PACKAGE_LIST_OBJECT_NAME
    << " with handle: " << watch_handle << dendl;

  return 0;
}

void RadosLuaManager::ack_reload(const DoutPrefixProvider* dpp, uint64_t notify_id, uint64_t cookie, int reload_status) {
  if (!ioctx.is_valid()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool when acking reload of Lua packages" << dendl;
    return;
  }
  bufferlist reply;
  ceph::encode(reload_status, reply);
  ioctx.notify_ack(PACKAGE_LIST_OBJECT_NAME, notify_id, cookie, reply);
}

void RadosLuaManager::handle_reload_notify(const DoutPrefixProvider* dpp, optional_yield y, uint64_t notify_id, uint64_t cookie) {
  if (cookie != watch_handle) {
    return;
  }

#ifdef WITH_RADOSGW_LUA_PACKAGES
  rgw::lua::packages_t failed_packages;
  std::string install_dir;
  auto r = rgw::lua::install_packages(dpp, store, 
      y, store->ctx()->_conf.get_val<std::string>("rgw_luarocks_location"), 
      failed_packages, install_dir);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "WARNING: failed to install Lua packages from allowlist. error code: " << r
            << dendl;
  }
  set_luarocks_path(install_dir);
  for (const auto &p : failed_packages) {
    ldpp_dout(dpp, 5) << "WARNING: failed to install Lua package: " << p
            << " from allowlist" << dendl;
  }
#else 
  const int r = 0;
#endif  
  ack_reload(dpp, notify_id, cookie, r);
}

int RadosLuaManager::reload_packages(const DoutPrefixProvider *dpp, optional_yield y)
{
  if (!ioctx.is_valid()) {
    ldpp_dout(dpp, 10) << "WARNING: missing pool trying to notify reload of Lua packages" << dendl;
    return -ENOENT;
  }
  bufferlist empty_bl;
  bufferlist reply_bl;
  const uint64_t timeout_ms = 0;
  auto r = rgw_rados_notify(dpp,
      ioctx,
      PACKAGE_LIST_OBJECT_NAME,
      empty_bl, timeout_ms, &reply_bl, y);
  if (r < 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify reload on " << PACKAGE_LIST_OBJECT_NAME
        << ". error: " << cpp_strerror(r) << dendl;
    return r;
  }
 
  std::vector<librados::notify_ack_t> acks;
  std::vector<librados::notify_timeout_t> timeouts;
  ioctx.decode_notify_response(reply_bl, &acks, &timeouts);
  if (timeouts.size() > 0) {
    ldpp_dout(dpp, 1) << "ERROR: failed to notify reload on " << PACKAGE_LIST_OBJECT_NAME
      << ". error: timeout" << dendl;
    return -EAGAIN;
  }
  for (auto& ack : acks) {
    try {
      auto iter = ack.payload_bl.cbegin();
      ceph::decode(r, iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 1) << "ERROR: couldn't decode Lua packages reload status. error: " << 
        err.what() << dendl;
      return -EINVAL;
    }
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

void RadosLuaManager::PackagesWatcher::handle_notify(uint64_t notify_id, uint64_t cookie, uint64_t notifier_id, bufferlist &bl)
{
  parent->handle_reload_notify(this, null_yield, notify_id, cookie);
}

void RadosLuaManager::PackagesWatcher::handle_error(uint64_t cookie, int err)
{
  if (parent->watch_handle != cookie) {
    return;
  }
  ldpp_dout(this, 5) << "WARNING: restarting reload watch handler. error: " << err << dendl;

  parent->unwatch_reload(this);
  parent->watch_reload(this);
}

CephContext* RadosLuaManager::PackagesWatcher::get_cct() const {
  return parent->store->ctx();
}

unsigned RadosLuaManager::PackagesWatcher::get_subsys() const {
  return dout_subsys;
}

std::ostream& RadosLuaManager::PackagesWatcher::gen_prefix(std::ostream& out) const {
  return out << "rgw lua package reloader: ";
}

int RadosOIDCProvider::store_url(const DoutPrefixProvider *dpp, const std::string& url, bool exclusive, optional_yield y)
{
  auto sysobj = store->svc()->sysobj;
  std::string oid = tenant + get_url_oid_prefix() + url;

  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);
  return rgw_put_system_obj(dpp, sysobj, store->svc()->zone->get_zone_params().oidc_pool, oid, bl, exclusive, nullptr, real_time(), y);
}

int RadosOIDCProvider::read_url(const DoutPrefixProvider *dpp, const std::string& url, const std::string& tenant, optional_yield y)
{
  auto sysobj = store->svc()->sysobj;
  auto& pool = store->svc()->zone->get_zone_params().oidc_pool;
  std::string oid = tenant + get_url_oid_prefix() + url;
  bufferlist bl;

  int ret = rgw_get_system_obj(sysobj, pool, oid, bl, nullptr, nullptr, y, dpp);
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
  auto& pool = store->svc()->zone->get_zone_params().oidc_pool;

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
  std::string oid;

  oid = info.id;

  bufferlist bl;
  encode(this->info, bl);

  if (!this->info.tags.empty()) {
    bufferlist bl_tags;
    encode(this->info.tags, bl_tags);
    map<string, bufferlist> attrs;
    attrs.emplace("tagging", bl_tags);

    RGWSI_MBSObj_PutParams params(bl, &attrs, info.mtime, exclusive);
    std::unique_ptr<RGWSI_MetaBackend::Context> ctx(store->svc()->role->svc.meta_be->alloc_ctx());
    ctx->init(store->svc()->role->get_be_handler());
    return store->svc()->role->svc.meta_be->put(ctx.get(), oid, params, &info.objv_tracker, y, dpp);
  } else {
    RGWSI_MBSObj_PutParams params(bl, nullptr, info.mtime, exclusive);
    std::unique_ptr<RGWSI_MetaBackend::Context> ctx(store->svc()->role->svc.meta_be->alloc_ctx());
    ctx->init(store->svc()->role->get_be_handler());
    return store->svc()->role->svc.meta_be->put(ctx.get(), oid, params, &info.objv_tracker, y, dpp);
  }
}

int RadosRole::store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  auto sysobj = store->svc()->sysobj;
  RGWNameToId nameToId;
  nameToId.obj_id = info.id;

  std::string oid = info.tenant + get_names_oid_prefix() + info.name;

  bufferlist bl;
  using ceph::encode;
  encode(nameToId, bl);

  return rgw_put_system_obj(dpp, sysobj, store->svc()->zone->get_zone_params().roles_pool, oid, bl, exclusive, &info.objv_tracker, real_time(), y);
}

int RadosRole::store_path(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  auto sysobj = store->svc()->sysobj;
  std::string oid = info.tenant + get_path_oid_prefix() + info.path + get_info_oid_prefix() + info.id;

  bufferlist bl;

  return rgw_put_system_obj(dpp, sysobj, store->svc()->zone->get_zone_params().roles_pool, oid, bl, exclusive, &info.objv_tracker, real_time(), y);
}

int RadosRole::read_id(const DoutPrefixProvider *dpp, const std::string& role_name, const std::string& tenant, std::string& role_id, optional_yield y)
{
  auto sysobj = store->svc()->sysobj;
  std::string oid = info.tenant + get_names_oid_prefix() + role_name;
  bufferlist bl;

  int ret = rgw_get_system_obj(sysobj, store->svc()->zone->get_zone_params().roles_pool, oid, bl, nullptr, nullptr, y, dpp);
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
  auto sysobj = store->svc()->sysobj;
  std::string oid = info.tenant + get_names_oid_prefix() + info.name;
  bufferlist bl;

  int ret = rgw_get_system_obj(sysobj, store->svc()->zone->get_zone_params().roles_pool, oid, bl, nullptr, nullptr, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role name from Role pool: " << info.name <<
      ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  RGWNameToId nameToId;
  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode role name from Role pool: " << info.name << dendl;
    return -EIO;
  }
  info.id = nameToId.obj_id;
  return 0;
}

int RadosRole::read_info(const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string oid;

  oid = info.id;
  ldpp_dout(dpp, 20) << "INFO: oid in read_info is: " << oid << dendl;

  bufferlist bl;

  RGWSI_MBSObj_GetParams params(&bl, &info.attrs, &info.mtime);
  std::unique_ptr<RGWSI_MetaBackend::Context> ctx(store->svc()->role->svc.meta_be->alloc_ctx());
  ctx->init(store->svc()->role->get_be_handler());
  int ret = store->svc()->role->svc.meta_be->get(ctx.get(), oid, params, &info.objv_tracker, y, dpp, true);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed reading role info from Role pool: " << info.id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(this->info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode role info from Role pool: " << info.id << dendl;
    return -EIO;
  }

  auto it = info.attrs.find("tagging");
  if (it != info.attrs.end()) {
    bufferlist bl_tags = it->second;
    try {
      using ceph::decode;
      auto iter = bl_tags.cbegin();
      decode(info.tags, iter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) << "ERROR: failed to decode attrs" << info.id << dendl;
      return -EIO;
    }
  }

  return 0;
}

int RadosRole::create(const DoutPrefixProvider *dpp, bool exclusive, const std::string& role_id, optional_yield y)
{
  int ret;

  if (! validate_input(dpp)) {
    return -EINVAL;
  }

  if (!role_id.empty()) {
    info.id = role_id;
  }

  /* check to see the name is not used */
  ret = read_id(dpp, info.name, info.tenant, info.id, y);
  if (exclusive && ret == 0) {
    ldpp_dout(dpp, 0) << "ERROR: name " << info.name << " already in use for role id "
                    << info.id << dendl;
    return -EEXIST;
  } else if ( ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "failed reading role id  " << info.id << ": "
                  << cpp_strerror(-ret) << dendl;
    return ret;
  }

  if (info.id.empty()) {
    /* create unique id */
    uuid_d new_uuid;
    char uuid_str[37];
    new_uuid.generate_random();
    new_uuid.print(uuid_str);
    info.id = uuid_str;
  }

  //arn
  info.arn = role_arn_prefix + info.tenant + ":role" + info.path + info.name;

  // Creation time
  real_clock::time_point t = real_clock::now();

  struct timeval tv;
  real_clock::to_timeval(t, tv);

  char buf[30];
  struct tm result;
  gmtime_r(&tv.tv_sec, &result);
  strftime(buf,30,"%Y-%m-%dT%H:%M:%S", &result);
  sprintf(buf + strlen(buf),".%dZ",(int)tv.tv_usec/1000);
  info.creation_date.assign(buf, strlen(buf));

  auto& pool = store->svc()->zone->get_zone_params().roles_pool;
  ret = store_info(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR:  storing role info in Role pool: "
                  << info.id << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ret = store_name(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: storing role name in Role pool: "
                  << info.name << ": " << cpp_strerror(-ret) << dendl;

    //Delete the role info that was stored in the previous call
    std::string oid = get_info_oid_prefix() + info.id;
    int info_ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
    if (info_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role id from Role pool: "
                  << info.id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    return ret;
  }

  ret = store_path(dpp, exclusive, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: storing role path in Role pool: "
                  << info.path << ": " << cpp_strerror(-ret) << dendl;
    //Delete the role info that was stored in the previous call
    std::string oid = get_info_oid_prefix() + info.id;
    int info_ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
    if (info_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role id from Role pool: "
                  << info.id << ": " << cpp_strerror(-info_ret) << dendl;
    }
    //Delete role name that was stored in previous call
    oid = info.tenant + get_names_oid_prefix() + info.name;
    int name_ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
    if (name_ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: cleanup of role name from Role pool: "
                  << info.name << ": " << cpp_strerror(-name_ret) << dendl;
    }
    return ret;
  }
  return 0;
}

int RadosRole::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  auto& pool = store->svc()->zone->get_zone_params().roles_pool;

  int ret = read_name(dpp, y);
  if (ret < 0) {
    return ret;
  }

  ret = read_info(dpp, y);
  if (ret < 0) {
    return ret;
  }

  if (! info.perm_policy_map.empty()) {
    return -ERR_DELETE_CONFLICT;
  }

  // Delete id & insert MD Log
  RGWSI_MBSObj_RemoveParams params;
  std::unique_ptr<RGWSI_MetaBackend::Context> ctx(store->svc()->role->svc.meta_be->alloc_ctx());
  ctx->init(store->svc()->role->get_be_handler());
  ret = store->svc()->role->svc.meta_be->remove(ctx.get(), info.id, params, &info.objv_tracker, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role id: " << info.id << " failed with code: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  // Delete name
  std::string oid = info.tenant + get_names_oid_prefix() + info.name;
  ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role name from Role pool: "
                  << info.name << ": " << cpp_strerror(-ret) << dendl;
  }

  // Delete path
  oid = info.tenant + get_path_oid_prefix() + info.path + get_info_oid_prefix() + info.id;
  ret = rgw_delete_system_obj(dpp, store->svc()->sysobj, pool, oid, nullptr, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: deleting role path from Role pool: "
                  << info.path << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

} // namespace rgw::sal

extern "C" {

void* newRadosStore(void* io_context)
{
  rgw::sal::RadosStore* store = new rgw::sal::RadosStore(
    *static_cast<boost::asio::io_context*>(io_context));
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
