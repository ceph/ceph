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

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "rgw_service.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"
#include "services/svc_quota.h"
#include "services/svc_config_key.h"
#include "services/svc_zone_utils.h"
#include "cls/rgw/cls_rgw_client.h"

#include "rgw_pubsub.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

static int decode_policy(CephContext *cct,
                         bufferlist& bl,
                         RGWAccessControlPolicy *policy)
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
    RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
    s3policy->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}

static int rgw_op_get_bucket_policy_from_attr(const DoutPrefixProvider *dpp,
					      RGWRadosStore *store,
					      RGWUser& user,
					      RGWAttrs& bucket_attrs,
					      RGWAccessControlPolicy *policy,
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
    int r = user.load_by_id(dpp, y);
    if (r < 0)
      return r;

    policy->create_default(user.get_id(), user.get_display_name());
  }
  return 0;
}

static int process_completed(const AioResultList& completed, RawObjSet *written)
{
  std::optional<int> error;
  for (auto& r : completed) {
    if (r.result >= 0) {
      written->insert(r.obj.get_ref().obj);
    } else if (!error) { // record first error code
      error = r.result;
    }
  }
  return error.value_or(0);
}

int RadosCompletions::drain()
{
  int ret = 0;
  while (!handles.empty()) {
    librados::AioCompletion *handle = handles.front();
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

int RGWRadosUser::list_buckets(const DoutPrefixProvider *dpp, const string& marker,
			       const string& end_marker, uint64_t max, bool need_stats,
			       RGWBucketList &buckets, optional_yield y)
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
    buckets.add(std::unique_ptr<RGWBucket>(new RGWRadosBucket(this->store, ent.second, this)));
  }

  return 0;
}

RGWBucket* RGWRadosUser::create_bucket(rgw_bucket& bucket,
				       ceph::real_time creation_time)
{
  return NULL;
}

int RGWRadosUser::read_attrs(const DoutPrefixProvider *dpp, optional_yield y, RGWAttrs* uattrs, RGWObjVersionTracker* tracker)
{
  return store->ctl()->user->get_attrs_by_uid(dpp, get_id(), uattrs, y, tracker);
}

int RGWRadosUser::read_stats(const DoutPrefixProvider *dpp, 
                             optional_yield y, RGWStorageStats* stats,
			     ceph::real_time *last_stats_sync,
			     ceph::real_time *last_stats_update)
{
  return store->ctl()->user->read_stats(dpp, get_id(), stats, y, last_stats_sync, last_stats_update);
}

int RGWRadosUser::read_stats_async(RGWGetUserStats_CB *cb)
{
  return store->ctl()->user->read_stats_async(get_id(), cb);
}

int RGWRadosUser::complete_flush_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return store->ctl()->user->complete_flush_stats(dpp, get_id(), y);
}

int RGWRadosUser::read_usage(uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool *is_truncated,
			       RGWUsageIter& usage_iter,
			       map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  std::string bucket_name;
  return store->getRados()->read_usage(get_id(), bucket_name, start_epoch,
				       end_epoch, max_entries, is_truncated,
				       usage_iter, usage);
}

int RGWRadosUser::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  std::string bucket_name;

  return store->getRados()->trim_usage(dpp, get_id(), bucket_name, start_epoch, end_epoch);
}

int RGWRadosUser::load_by_id(const DoutPrefixProvider *dpp, optional_yield y)
{
    return store->ctl()->user->get_info_by_uid(dpp, info.user_id, &info, y, RGWUserCtl::GetParams().set_objv_tracker(&objv_tracker));
}

int RGWRadosUser::store_info(const DoutPrefixProvider *dpp, optional_yield y, const RGWUserCtl::PutParams& params)
{
    return store->ctl()->user->store_info(dpp, info, y, params);
}

int RGWRadosUser::remove_info(const DoutPrefixProvider *dpp, optional_yield y, const RGWUserCtl::RemoveParams& params)
{
    return store->ctl()->user->remove_info(dpp, info, y, params);
}

/* Placeholder */
RGWObject *RGWRadosBucket::create_object(const rgw_obj_key &key)
{
  return nullptr;
}

int RGWRadosBucket::remove_bucket(const DoutPrefixProvider *dpp, bool delete_children, std::string prefix, std::string delimiter, bool forward_to_master, req_info* req_info, optional_yield y)
{
  int ret;

  // Refresh info
  ret = get_bucket_info(dpp, y);
  if (ret < 0)
    return ret;

  ListParams params;
  params.list_versions = true;
  params.allow_unordered = true;

  ListResults results;

  bool is_truncated = false;
  do {
    results.objs.clear();

      ret = list(dpp, params, 1000, results, y);
      if (ret < 0)
	return ret;

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
  } while(is_truncated);

  /* If there's a prefix, then we are aborting multiparts as well */
  if (!prefix.empty()) {
    ret = abort_bucket_multiparts(dpp, store, store->ctx(), this, prefix, delimiter);
    if (ret < 0) {
      return ret;
    }
  }

  ret = store->ctl()->bucket->sync_user_stats(dpp, info.owner, info, y);
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
    lderr(store->ctx()) << "ERROR: unable to remove notifications from bucket. ret=" << ps_ret << dendl;
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

int RGWRadosBucket::get_bucket_info(const DoutPrefixProvider *dpp, optional_yield y)
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
  if (ret == 0) {
    bucket_version = ep_ot.read_version;
    ent.placement_rule = info.placement_rule;
  }
  return ret;
}

int RGWRadosBucket::get_bucket_stats(int shard_id,
				     std::string *bucket_ver, std::string *master_ver,
				     std::map<RGWObjCategory, RGWStorageStats>& stats,
				     std::string *max_marker, bool *syncstopped)
{
  return store->getRados()->get_bucket_stats(info, shard_id, bucket_ver, master_ver, stats, max_marker, syncstopped);
}

int RGWRadosBucket::get_bucket_stats_async(int shard_id, RGWGetBucketStats_CB *ctx)
{
  return store->getRados()->get_bucket_stats_async(get_info(), shard_id, ctx);
}

int RGWRadosBucket::read_bucket_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
      int ret = store->ctl()->bucket->read_bucket_stats(info.bucket, &ent, y, dpp);
      info.placement_rule = ent.placement_rule;
      return ret;
}

int RGWRadosBucket::sync_user_stats(const DoutPrefixProvider *dpp, optional_yield y)
{
  return store->ctl()->bucket->sync_user_stats(dpp, owner->get_id(), info, y);
}

int RGWRadosBucket::update_container_stats(const DoutPrefixProvider *dpp)
{
  int ret;
  map<std::string, RGWBucketEnt> m;

  m[info.bucket.name] = ent;
  ret = store->getRados()->update_containers_stats(m, dpp);
  if (!ret)
    return -EEXIST;
  if (ret < 0)
    return ret;

  map<string, RGWBucketEnt>::iterator iter = m.find(info.bucket.name);
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

int RGWRadosBucket::check_bucket_shards(const DoutPrefixProvider *dpp)
{
      return store->getRados()->check_bucket_shards(info, info.bucket, get_count(), dpp);
}

int RGWRadosBucket::link(const DoutPrefixProvider *dpp, RGWUser* new_user, optional_yield y, bool update_entrypoint, RGWObjVersionTracker* objv)
{
  RGWBucketEntryPoint ep;
  ep.bucket = info.bucket;
  ep.owner = new_user->get_id();
  ep.creation_time = get_creation_time();
  ep.linked = true;
  RGWAttrs ep_attrs;
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

int RGWRadosBucket::unlink(const DoutPrefixProvider *dpp, RGWUser* new_user, optional_yield y, bool update_entrypoint)
{
  return store->ctl()->bucket->unlink_bucket(new_user->get_id(), info.bucket, y, dpp, update_entrypoint);
}

int RGWRadosBucket::chown(const DoutPrefixProvider *dpp, RGWUser* new_user, RGWUser* old_user, optional_yield y, const std::string* marker)
{
  string obj_marker;

  if (marker == nullptr)
    marker = &obj_marker;

  return store->ctl()->bucket->chown(store, this, new_user->get_id(),
			   old_user->get_display_name(), *marker, y, dpp);
}

int RGWRadosBucket::put_instance_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time _mtime)
{
  mtime = _mtime;
  return store->getRados()->put_bucket_instance_info(info, exclusive, mtime, &attrs, dpp);
}

int RGWRadosBucket::remove_entrypoint(const DoutPrefixProvider *dpp, RGWObjVersionTracker* objv, optional_yield y)
{
  return store->ctl()->bucket->remove_bucket_entrypoint_info(get_key(), y, dpp,
                                           RGWBucketCtl::Bucket::RemoveParams()
                                           .set_objv_tracker(objv));
}

int RGWRadosBucket::remove_instance_info(const DoutPrefixProvider *dpp, RGWObjVersionTracker* objv, optional_yield y)
{
  return store->ctl()->bucket->remove_bucket_instance_info(get_key(), info, y, dpp,
                                           RGWBucketCtl::BucketInstance::RemoveParams()
                                           .set_objv_tracker(objv));
}

/* Make sure to call get_bucket_info() if you need it first */
bool RGWRadosBucket::is_owner(RGWUser* user)
{
  return (info.owner.compare(user->get_id()) == 0);
}

int RGWRadosBucket::check_empty(const DoutPrefixProvider *dpp, optional_yield y)
{
  return store->getRados()->check_bucket_empty(dpp, info, y);
}

int RGWRadosBucket::check_quota(RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
    return store->getRados()->check_quota(owner->get_id(), get_key(),
					  user_quota, bucket_quota, obj_size, y, check_size_only);
}

int RGWRadosBucket::set_instance_attrs(const DoutPrefixProvider *dpp, RGWAttrs& attrs, optional_yield y)
{
    return store->ctl()->bucket->set_bucket_instance_attrs(get_info(),
				attrs, &get_info().objv_tracker, y, dpp);
}

int RGWRadosBucket::try_refresh_info(const DoutPrefixProvider *dpp, ceph::real_time *pmtime)
{
  return store->getRados()->try_refresh_bucket_info(info, pmtime, dpp, &attrs);
}

int RGWRadosBucket::read_usage(uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool *is_truncated,
			       RGWUsageIter& usage_iter,
			       map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  return store->getRados()->read_usage(owner->get_id(), get_name(), start_epoch,
				       end_epoch, max_entries, is_truncated,
				       usage_iter, usage);
}

int RGWRadosBucket::trim_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  return store->getRados()->trim_usage(dpp, owner->get_id(), get_name(), start_epoch, end_epoch);
}

int RGWRadosBucket::remove_objs_from_index(std::list<rgw_obj_index_key>& objs_to_unlink)
{
  return store->getRados()->remove_objs_from_index(info, objs_to_unlink);
}

int RGWRadosBucket::check_index(std::map<RGWObjCategory, RGWStorageStats>& existing_stats, std::map<RGWObjCategory, RGWStorageStats>& calculated_stats)
{
  return store->getRados()->bucket_check_index(info, &existing_stats, &calculated_stats);
}

int RGWRadosBucket::rebuild_index()
{
  return store->getRados()->bucket_rebuild_index(info);
}

int RGWRadosBucket::set_tag_timeout(uint64_t timeout)
{
  return store->getRados()->cls_obj_set_bucket_tag_timeout(info, timeout);
}

int RGWRadosBucket::purge_instance(const DoutPrefixProvider *dpp)
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
    ret = store->getRados()->bi_remove(bs);
    if (ret < 0) {
      cerr << "ERROR: failed to remove bucket index object: "
           << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  }
  return 0;
}

int RGWRadosBucket::set_acl(const DoutPrefixProvider *dpp, RGWAccessControlPolicy &acl, optional_yield y)
{
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  return store->ctl()->bucket->set_acl(acl.get_owner(), info.bucket, info, aclbl, y, dpp);
}

std::unique_ptr<RGWObject> RGWRadosBucket::get_object(const rgw_obj_key& k)
{
  return std::unique_ptr<RGWObject>(new RGWRadosObject(this->store, k, this));
}

int RGWRadosBucket::list(const DoutPrefixProvider *dpp, ListParams& params, int max, ListResults& results, optional_yield y)
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
  list_op.params.filter = params.filter;
  list_op.params.list_versions = params.list_versions;
  list_op.params.allow_unordered = params.allow_unordered;

  int ret = list_op.list_objects(dpp, max, &results.objs, &results.common_prefixes, &results.is_truncated, y);
  if (ret >= 0) {
    results.next_marker = list_op.get_next_marker();
  }

  return ret;
}

std::unique_ptr<RGWUser> RGWRadosStore::get_user(const rgw_user &u)
{
  return std::unique_ptr<RGWUser>(new RGWRadosUser(this, u));
}

int RGWRadosStore::get_user_by_access_key(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y, std::unique_ptr<RGWUser>* user)
{
  RGWUserInfo uinfo;
  RGWUser *u;
  RGWObjVersionTracker objv_tracker;

  int r = ctl()->user->get_info_by_access_key(dpp, key, &uinfo, y, RGWUserCtl::GetParams().set_objv_tracker(&objv_tracker));
  if (r < 0)
    return r;

  u = new RGWRadosUser(this, uinfo);
  if (!u)
    return -ENOMEM;

  u->get_version_tracker() = objv_tracker;

  user->reset(u);
  return 0;
}

int RGWRadosStore::get_user_by_email(const DoutPrefixProvider *dpp, const std::string& email, optional_yield y, std::unique_ptr<RGWUser>* user)
{
  RGWUserInfo uinfo;
  RGWUser *u;
  RGWObjVersionTracker objv_tracker;

  int r = ctl()->user->get_info_by_email(dpp, email, &uinfo, y, RGWUserCtl::GetParams().set_objv_tracker(&objv_tracker));
  if (r < 0)
    return r;

  u = new RGWRadosUser(this, uinfo);
  if (!u)
    return -ENOMEM;

  u->get_version_tracker() = objv_tracker;

  user->reset(u);
  return 0;
}

int RGWRadosStore::get_user_by_swift(const DoutPrefixProvider *dpp, const std::string& user_str, optional_yield y, std::unique_ptr<RGWUser>* user)
{
  RGWUserInfo uinfo;
  RGWUser *u;
  RGWObjVersionTracker objv_tracker;

  int r = ctl()->user->get_info_by_swift(dpp, user_str, &uinfo, y, RGWUserCtl::GetParams().set_objv_tracker(&objv_tracker));
  if (r < 0)
    return r;

  u = new RGWRadosUser(this, uinfo);
  if (!u)
    return -ENOMEM;

  u->get_version_tracker() = objv_tracker;

  user->reset(u);
  return 0;
}

std::unique_ptr<RGWObject> RGWRadosStore::get_object(const rgw_obj_key& k)
{
  return std::unique_ptr<RGWObject>(new RGWRadosObject(this, k));
}

int RGWRadosStore::get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const rgw_bucket& b, std::unique_ptr<RGWBucket>* bucket, optional_yield y)
{
  int ret;
  RGWBucket* bp;

  bp = new RGWRadosBucket(this, b, u);
  ret = bp->get_bucket_info(dpp, y);
  if (ret < 0) {
    delete bp;
    return ret;
  }

  bucket->reset(bp);
  return 0;
}

int RGWRadosStore::get_bucket(RGWUser* u, const RGWBucketInfo& i, std::unique_ptr<RGWBucket>* bucket)
{
  RGWBucket* bp;

  bp = new RGWRadosBucket(this, i, u);
  /* Don't need to fetch the bucket info, use the provided one */

  bucket->reset(bp);
  return 0;
}

int RGWRadosStore::get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const std::string& tenant, const std::string& name, std::unique_ptr<RGWBucket>* bucket, optional_yield y)
{
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(dpp, u, b, bucket, y);
}

int RGWRadosStore::create_bucket(const DoutPrefixProvider *dpp,
				 RGWUser& u, const rgw_bucket& b,
				 const string& zonegroup_id,
				 rgw_placement_rule& placement_rule,
				 string& swift_ver_location,
				 const RGWQuotaInfo * pquota_info,
				 const RGWAccessControlPolicy& policy,
				 RGWAttrs& attrs,
				 RGWBucketInfo& info,
				 obj_version& ep_objv,
				 bool exclusive,
				 bool obj_lock_enabled,
				 bool *existed,
				 req_info& req_info,
				 std::unique_ptr<RGWBucket>* bucket_out,
				 optional_yield y)
{
  int ret;
  bufferlist in_data;
  RGWBucketInfo master_info;
  rgw_bucket *pmaster_bucket;
  uint32_t *pmaster_num_shards;
  real_time creation_time;
  std::unique_ptr<RGWBucket> bucket;
  obj_version objv, *pobjv = NULL;

  /* If it exists, look it up; otherwise create it */
  ret = get_bucket(dpp, &u, b, &bucket, y);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  if (ret != -ENOENT) {
    RGWAccessControlPolicy old_policy(ctx());
    *existed = true;
    if (swift_ver_location.empty()) {
      swift_ver_location = bucket->get_info().swift_ver_location;
    }
    placement_rule.inherit_from(bucket->get_info().placement_rule);

    // don't allow changes to the acl policy
    int r = rgw_op_get_bucket_policy_from_attr(dpp, this, u, bucket->get_attrs(),
					       &old_policy, y);
    if (r >= 0 && old_policy != policy) {
      bucket_out->swap(bucket);
      return -EEXIST;
    }
  } else {
    bucket = std::unique_ptr<RGWBucket>(new RGWRadosBucket(this, b, &u));
    *existed = false;
    bucket->set_attrs(attrs);
  }

  if (!svc()->zone->is_meta_master()) {
    JSONParser jp;
    ret = forward_request_to_master(dpp, &u, NULL, in_data, &jp, req_info, y);
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
    zid = svc()->zone->get_zonegroup().get_id();
  }

  if (*existed) {
    rgw_placement_rule selected_placement_rule;
    ret = svc()->zone->select_bucket_placement(dpp, u.get_info(),
					       zid, placement_rule,
					       &selected_placement_rule, nullptr, y);
    if (selected_placement_rule != info.placement_rule) {
      ret = -EEXIST;
      bucket_out->swap(bucket);
      return ret;
    }
  } else {

    ret = getRados()->create_bucket(u.get_info(), bucket->get_key(),
				    zid, placement_rule, swift_ver_location, pquota_info,
				    attrs, info, pobjv, &ep_objv, creation_time,
				    pmaster_bucket, pmaster_num_shards, y, dpp,
				    exclusive);
    if (ret == -EEXIST) {
      *existed = true;
      ret = 0;
    } else if (ret != 0) {
      return ret;
    }
  }

  bucket->set_version(ep_objv);
  bucket->get_info() = info;

  bucket_out->swap(bucket);

  return ret;
}

bool RGWRadosStore::is_meta_master()
{
  return svc()->zone->is_meta_master();
}

int RGWRadosStore::forward_request_to_master(const DoutPrefixProvider *dpp, RGWUser* user, obj_version *objv,
					     bufferlist& in_data,
					     JSONParser *jp, req_info& info,
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
  string uid_str = user->get_id().to_str();
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

int RGWRadosStore::defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, RGWBucket* bucket, RGWObject* obj, optional_yield y)
{
  return rados->defer_gc(dpp, rctx, bucket->get_info(), obj->get_obj(), y);
}

std::string RGWRadosStore::zone_unique_id(uint64_t unique_num)
{
  return svc()->zone_utils->unique_id(unique_num);
}

std::string RGWRadosStore::zone_unique_trans_id(const uint64_t unique_num)
{
  return svc()->zone_utils->unique_trans_id(unique_num);
}

int RGWRadosStore::cluster_stat(RGWClusterStat& stats)
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

std::unique_ptr<Lifecycle> RGWRadosStore::get_lifecycle(void)
{
  return std::unique_ptr<Lifecycle>(new RadosLifecycle(this));
}

std::unique_ptr<Completions> RGWRadosStore::get_completions(void)
{
  return std::unique_ptr<Completions>(new RadosCompletions());
}

std::unique_ptr<Notification> RGWRadosStore::get_notification(rgw::sal::RGWObject* obj,
							    struct req_state* s,
							    rgw::notify::EventType event_type)
{
  return std::unique_ptr<Notification>(new RadosNotification(this, obj, s, event_type));
}

std::unique_ptr<GCChain> RGWRadosStore::get_gc_chain(rgw::sal::RGWObject* obj)
{
  return std::unique_ptr<GCChain>(new RadosGCChain(this, obj));
}

std::unique_ptr<Writer> RGWRadosStore::get_writer(Aio *aio, rgw::sal::RGWBucket* bucket,
              RGWObjectCtx& obj_ctx, std::unique_ptr<rgw::sal::RGWObject> _head_obj,
              const DoutPrefixProvider *dpp, optional_yield y)
{
  return std::unique_ptr<Writer>(new RadosWriter(aio, this, bucket, obj_ctx, std::move(_head_obj), dpp, y));
}

int RGWRadosStore::delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj)
{
  return rados->delete_raw_obj(dpp, obj);
}

int RGWRadosStore::delete_raw_obj_aio(const rgw_raw_obj& obj, Completions* aio)
{
  RadosCompletions *raio = static_cast<RadosCompletions*>(aio);

  return rados->delete_raw_obj_aio(obj, raio->handles);
}

void RGWRadosStore::get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj)
{
    rados->obj_to_raw(placement_rule, obj, raw_obj);
}

int RGWRadosStore::get_raw_chunk_size(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, uint64_t* chunk_size)
{
  return rados->get_max_chunk_size(obj.pool, chunk_size, dpp);
}

int RGWRadosStore::log_usage(const DoutPrefixProvider *dpp, map<rgw_user_bucket, RGWUsageBatch>& usage_info)
{
    return rados->log_usage(dpp, usage_info);
}

int RGWRadosStore::log_op(string& oid, bufferlist& bl)
{
  rgw_raw_obj obj(svc()->zone->get_zone_params().log_pool, oid);

  int ret = rados->append_async(obj, bl.length(), bl);
  if (ret == -ENOENT) {
    ret = rados->create_pool(svc()->zone->get_zone_params().log_pool);
    if (ret < 0)
      return ret;
    // retry
    ret = rados->append_async(obj, bl.length(), bl);
  }

  return ret;
}

int RGWRadosStore::register_to_service_map(const string& daemon_type,
					   const map<string, string>& meta)
{
  return rados->register_to_service_map(daemon_type, meta);
}

void RGWRadosStore::get_quota(RGWQuotaInfo& bucket_quota, RGWQuotaInfo& user_quota)
{
    bucket_quota = svc()->quota->get_bucket_quota();
    user_quota = svc()->quota->get_user_quota();
}

int RGWRadosStore::list_raw_objects(const rgw_pool& pool, const string& prefix_filter,
				    int max, RGWListRawObjsCtx& ctx, list<string>& oids,
				    bool *is_truncated)
{
    return rados->list_raw_objects(pool, prefix_filter, max, ctx, oids, is_truncated);
}

int RGWRadosStore::set_buckets_enabled(const DoutPrefixProvider *dpp, vector<rgw_bucket>& buckets, bool enabled)
{
    return rados->set_buckets_enabled(buckets, enabled, dpp);
}

int RGWRadosStore::get_sync_policy_handler(const DoutPrefixProvider *dpp,
					   std::optional<rgw_zone_id> zone,
					   std::optional<rgw_bucket> bucket,
					   RGWBucketSyncPolicyHandlerRef *phandler,
					   optional_yield y)
{
  return ctl()->bucket->get_sync_policy_handler(zone, bucket, phandler, y, dpp);
}

RGWDataSyncStatusManager* RGWRadosStore::get_data_sync_manager(const rgw_zone_id& source_zone)
{
  return rados->get_data_sync_manager(source_zone);
}

int RGWRadosStore::read_all_usage(uint64_t start_epoch, uint64_t end_epoch,
				  uint32_t max_entries, bool *is_truncated,
				  RGWUsageIter& usage_iter,
				  map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
  rgw_user uid;
  std::string bucket_name;

  return rados->read_usage(uid, bucket_name, start_epoch, end_epoch, max_entries,
			   is_truncated, usage_iter, usage);
}

int RGWRadosStore::trim_all_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch)
{
  rgw_user uid;
  std::string bucket_name;

  return rados->trim_usage(dpp, uid, bucket_name, start_epoch, end_epoch);
}

int RGWRadosStore::get_config_key_val(string name, bufferlist *bl)
{
  return svc()->config_key->get(name, true, bl);
}

int RGWRadosStore::put_system_obj(const DoutPrefixProvider *dpp, const rgw_pool& pool, const string& oid,
				  bufferlist& data, bool exclusive,
				  RGWObjVersionTracker *objv_tracker, real_time set_mtime,
				  optional_yield y, map<string, bufferlist> *pattrs)
{
  auto obj_ctx = svc()->sysobj->init_obj_ctx();
  return rgw_put_system_obj(dpp, obj_ctx, pool, oid, data, exclusive, objv_tracker, set_mtime, y, pattrs);
}

int RGWRadosStore::get_system_obj(const DoutPrefixProvider *dpp,
				  const rgw_pool& pool, const string& key,
				  bufferlist& bl,
				  RGWObjVersionTracker *objv_tracker, real_time *pmtime,
				  optional_yield y, map<string, bufferlist> *pattrs,
				  rgw_cache_entry_info *cache_info,
				  boost::optional<obj_version> refresh_version)
{
  auto obj_ctx = svc()->sysobj->init_obj_ctx();
  return rgw_get_system_obj(obj_ctx, pool, key, bl, objv_tracker, pmtime, y, dpp, pattrs, cache_info, refresh_version);
}

int RGWRadosStore::delete_system_obj(const DoutPrefixProvider *dpp, const rgw_pool& pool, const string& oid,
				     RGWObjVersionTracker *objv_tracker, optional_yield y)
{
    return rgw_delete_system_obj(dpp, svc()->sysobj, pool, oid, objv_tracker, y);
}

int RGWRadosStore::meta_list_keys_init(const string& section, const string& marker, void** phandle)
{
  return ctl()->meta.mgr->list_keys_init(section, marker, phandle);
}

int RGWRadosStore::meta_list_keys_next(void* handle, int max, list<string>& keys, bool* truncated)
{
  return ctl()->meta.mgr->list_keys_next(handle, max, keys, truncated);
}

void RGWRadosStore::meta_list_keys_complete(void* handle)
{
  ctl()->meta.mgr->list_keys_complete(handle);
}

std::string RGWRadosStore::meta_get_marker(void* handle)
{
  return ctl()->meta.mgr->get_marker(handle);
}

int RGWRadosStore::meta_remove(const DoutPrefixProvider *dpp, string& metadata_key, optional_yield y)
{
  return ctl()->meta.mgr->remove(metadata_key, y, dpp);
}

void RGWRadosStore::finalize(void)
{
  if (rados)
    rados->finalize();
}

int RGWRadosStore::get_obj_head_ioctx(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx *ioctx)
{
  return rados->get_obj_head_ioctx(bucket_info, obj, ioctx);
}

int RGWObject::range_to_ofs(uint64_t obj_size, int64_t &ofs, int64_t &end)
{
  if (ofs < 0) {
    ofs += obj_size;
    if (ofs < 0)
      ofs = 0;
    end = obj_size - 1;
  } else if (end < 0) {
    end = obj_size - 1;
  }

  if (obj_size > 0) {
    if (ofs >= (off_t)obj_size) {
      return -ERANGE;
    }
    if (end >= (off_t)obj_size) {
      end = obj_size - 1;
    }
  }
  return 0;
}

int RGWRadosObject::get_obj_state(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, RGWObjState **state, optional_yield y, bool follow_olh)
{
  return store->getRados()->get_obj_state(dpp, rctx, bucket->get_info(), get_obj(), state, follow_olh, y);
}

int RGWRadosObject::read_attrs(const DoutPrefixProvider *dpp, RGWRados::Object::Read &read_op, optional_yield y, rgw_obj *target_obj)
{
  read_op.params.attrs = &attrs;
  read_op.params.target_obj = target_obj;
  read_op.params.obj_size = &obj_size;
  read_op.params.lastmod = &mtime;

  return read_op.prepare(y, dpp);
}

int RGWRadosObject::set_obj_attrs(const DoutPrefixProvider *dpp, RGWObjectCtx* rctx, RGWAttrs* setattrs, RGWAttrs* delattrs, optional_yield y, rgw_obj* target_obj)
{
  RGWAttrs empty;
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

int RGWRadosObject::get_obj_attrs(RGWObjectCtx *rctx, optional_yield y, const DoutPrefixProvider *dpp, rgw_obj* target_obj)
{
  RGWRados::Object op_target(store->getRados(), bucket->get_info(), *rctx, get_obj());
  RGWRados::Object::Read read_op(&op_target);

  return read_attrs(dpp, read_op, y, target_obj);
}

int RGWRadosObject::modify_obj_attrs(RGWObjectCtx *rctx, const char *attr_name, bufferlist& attr_val, optional_yield y, const DoutPrefixProvider *dpp)
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

int RGWRadosObject::delete_obj_attrs(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, const char *attr_name, optional_yield y)
{
  RGWAttrs rmattr;
  bufferlist bl;

  set_atomic(rctx);
  rmattr[attr_name] = bl;
  return set_obj_attrs(dpp, rctx, nullptr, &rmattr, y);
}

int RGWRadosObject::copy_obj_data(RGWObjectCtx& rctx, RGWBucket* dest_bucket,
				  RGWObject* dest_obj,
				  uint16_t olh_epoch,
				  std::string* petag,
				  const DoutPrefixProvider *dpp,
				  optional_yield y)
{
  RGWAttrs attrset;
  RGWRados::Object op_target(store->getRados(), dest_bucket->get_info(), rctx, get_obj());
  RGWRados::Object::Read read_op(&op_target);

  int ret = read_attrs(dpp, read_op, y);
  if (ret < 0)
    return ret;

  attrset = attrs;

  attrset.erase(RGW_ATTR_ID_TAG);
  attrset.erase(RGW_ATTR_TAIL_TAG);

  return store->getRados()->copy_obj_data(rctx, dest_bucket,
					  dest_bucket->get_info().placement_rule, read_op,
					  obj_size - 1, dest_obj, NULL, mtime, attrset, 0,
					  real_time(), NULL, dpp, y);
}

void RGWRadosObject::set_atomic(RGWObjectCtx *rctx) const
{
  rgw_obj obj = get_obj();
  store->getRados()->set_atomic(rctx, obj);
}

void RGWRadosObject::set_prefetch_data(RGWObjectCtx *rctx)
{
  rgw_obj obj = get_obj();
  store->getRados()->set_prefetch_data(rctx, obj);
}

bool RGWRadosObject::is_expired() {
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

void RGWRadosObject::gen_rand_obj_instance_name()
{
  store->getRados()->gen_rand_obj_instance_name(&key);
}

void RGWRadosObject::raw_obj_to_obj(const rgw_raw_obj& raw_obj)
{
  rgw_obj tobj = get_obj();
  RGWSI_Tier_RADOS::raw_obj_to_obj(get_bucket()->get_key(), raw_obj, &tobj);
  set_key(tobj.key);
}

void RGWRadosObject::get_raw_obj(rgw_raw_obj* raw_obj)
{
  store->getRados()->obj_to_raw((bucket->get_info()).placement_rule, get_obj(), raw_obj);
}

int RGWRadosObject::omap_get_vals(const DoutPrefixProvider *dpp, const string& marker, uint64_t count,
				  std::map<string, bufferlist> *m,
				  bool *pmore, optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  rgw_raw_obj raw_obj;
  get_raw_obj(&raw_obj);
  auto sysobj = obj_ctx.get_obj(raw_obj);

  return sysobj.omap().get_vals(dpp, marker, count, m, pmore, y);
}

int RGWRadosObject::omap_get_all(const DoutPrefixProvider *dpp, std::map<string, bufferlist> *m,
				 optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  rgw_raw_obj raw_obj;
  get_raw_obj(&raw_obj);
  auto sysobj = obj_ctx.get_obj(raw_obj);

  return sysobj.omap().get_all(dpp, m, y);
}

int RGWRadosObject::omap_get_vals_by_keys(const std::string& oid,
					  const std::set<std::string>& keys,
					  RGWAttrs *vals)
{
  int ret;
  rgw_raw_obj head_obj;
  librados::IoCtx cur_ioctx;
  rgw_obj obj = get_obj();

  store->getRados()->obj_to_raw(bucket->get_placement_rule(), obj, &head_obj);
  ret = store->get_obj_head_ioctx(bucket->get_info(), obj, &cur_ioctx);
  if (ret < 0) {
    return ret;
  }

  return cur_ioctx.omap_get_vals_by_keys(oid, keys, vals);
}

int RGWRadosObject::omap_set_val_by_key(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& val,
					bool must_exist, optional_yield y)
{
  rgw_raw_obj raw_meta_obj;
  rgw_obj obj = get_obj();

  store->getRados()->obj_to_raw(bucket->get_placement_rule(), obj, &raw_meta_obj);

  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(raw_meta_obj);

  return sysobj.omap().set_must_exist(must_exist).set(dpp, key, val, y);
}

MPSerializer* RGWRadosObject::get_serializer(const std::string& lock_name)
{
  return new MPRadosSerializer(store, this, lock_name);
}

int RGWRadosObject::transition(RGWObjectCtx& rctx,
			       RGWBucket* bucket,
			       const rgw_placement_rule& placement_rule,
			       const real_time& mtime,
			       uint64_t olh_epoch,
			       const DoutPrefixProvider *dpp,
			       optional_yield y)
{
  return store->getRados()->transition_obj(rctx, bucket, *this, placement_rule, mtime, olh_epoch, dpp, y);
}

int RGWRadosObject::get_max_chunk_size(const DoutPrefixProvider *dpp, rgw_placement_rule placement_rule, uint64_t *max_chunk_size, uint64_t *alignment)
{
  return store->getRados()->get_max_chunk_size(placement_rule, get_obj(), max_chunk_size, dpp, alignment);
}

void RGWRadosObject::get_max_aligned_size(uint64_t size, uint64_t alignment,
				     uint64_t *max_size)
{
  store->getRados()->get_max_aligned_size(size, alignment, max_size);
}

bool RGWRadosObject::placement_rules_match(rgw_placement_rule& r1, rgw_placement_rule& r2)
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

std::unique_ptr<RGWObject::ReadOp> RGWRadosObject::get_read_op(RGWObjectCtx *ctx)
{
  return std::unique_ptr<RGWObject::ReadOp>(new RGWRadosObject::RadosReadOp(this, ctx));
}

RGWRadosObject::RadosReadOp::RadosReadOp(RGWRadosObject *_source, RGWObjectCtx *_rctx) :
	source(_source),
	rctx(_rctx),
	op_target(_source->store->getRados(),
		  _source->get_bucket()->get_info(),
		  *static_cast<RGWObjectCtx *>(rctx),
		  _source->get_obj()),
	parent_op(&op_target)
{ }

int RGWRadosObject::RadosReadOp::prepare(optional_yield y, const DoutPrefixProvider *dpp)
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
  result.head_obj = parent_op.state.head_obj;

  return ret;
}

int RGWRadosObject::RadosReadOp::read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y, const DoutPrefixProvider *dpp)
{
  return parent_op.read(ofs, end, bl, y, dpp);
}

int RGWRadosObject::RadosReadOp::get_manifest(const DoutPrefixProvider *dpp, RGWObjManifest **pmanifest,
					      optional_yield y)
{
  return op_target.get_manifest(dpp, pmanifest, y);
}

int RGWRadosObject::RadosReadOp::get_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist& dest, optional_yield y)
{
  return parent_op.get_attr(dpp, name, dest, y);
}

std::unique_ptr<RGWObject::DeleteOp> RGWRadosObject::get_delete_op(RGWObjectCtx *ctx)
{
  return std::unique_ptr<RGWObject::DeleteOp>(new RGWRadosObject::RadosDeleteOp(this, ctx));
}

RGWRadosObject::RadosDeleteOp::RadosDeleteOp(RGWRadosObject *_source, RGWObjectCtx *_rctx) :
	source(_source),
	rctx(_rctx),
	op_target(_source->store->getRados(),
		  _source->get_bucket()->get_info(),
		  *static_cast<RGWObjectCtx *>(rctx),
		  _source->get_obj()),
	parent_op(&op_target)
{ }

int RGWRadosObject::RadosDeleteOp::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
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

int RGWRadosObject::delete_object(const DoutPrefixProvider *dpp, RGWObjectCtx* obj_ctx, optional_yield y)
{
  RGWRados::Object del_target(store->getRados(), bucket->get_info(), *obj_ctx, get_obj());
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.bucket_owner = bucket->get_info().owner;
  del_op.params.versioning_status = bucket->get_info().versioning_status();

  return del_op.delete_obj(y, dpp);
}

int RGWRadosObject::delete_obj_aio(const DoutPrefixProvider *dpp, RGWObjState *astate,
				   Completions* aio, bool keep_index_consistent,
				   optional_yield y)
{
  RadosCompletions* raio = static_cast<RadosCompletions*>(aio);

  return store->getRados()->delete_obj_aio(dpp, get_obj(), bucket->get_info(), astate,
					   raio->handles, keep_index_consistent, y);
}

std::unique_ptr<RGWObject::StatOp> RGWRadosObject::get_stat_op(RGWObjectCtx *ctx)
{
  return std::unique_ptr<RGWObject::StatOp>(new RGWRadosObject::RadosStatOp(this, ctx));
}

RGWRadosObject::RadosStatOp::RadosStatOp(RGWRadosObject *_source, RGWObjectCtx *_rctx) :
	source(_source),
	rctx(_rctx),
	op_target(_source->store->getRados(),
		  _source->get_bucket()->get_info(),
		  *static_cast<RGWObjectCtx *>(rctx),
		  _source->get_obj()),
	parent_op(&op_target)
{ }

int RGWRadosObject::RadosStatOp::stat_async()
{
  return parent_op.stat_async();
}

int RGWRadosObject::RadosStatOp::wait()
{
  result.obj = source;
  int ret =  parent_op.wait();
  if (ret < 0)
    return ret;

  source->obj_size = parent_op.result.size;
  source->mtime = ceph::real_clock::from_timespec(parent_op.result.mtime);
  source->attrs = parent_op.result.attrs;
  source->key = parent_op.result.obj.key;
  source->in_extra_data = parent_op.result.obj.in_extra_data;
  source->index_hash_source = parent_op.result.obj.index_hash_source;
  if (parent_op.result.manifest)
    result.manifest = &(*parent_op.result.manifest);
  else
    result.manifest = nullptr;

  return ret;
}

int RGWRadosObject::copy_object(RGWObjectCtx& obj_ctx,
				RGWUser* user,
				req_info *info,
				const rgw_zone_id& source_zone,
				rgw::sal::RGWObject* dest_object,
				rgw::sal::RGWBucket* dest_bucket,
				rgw::sal::RGWBucket* src_bucket,
				const rgw_placement_rule& dest_placement,
				ceph::real_time *src_mtime,
				ceph::real_time *mtime,
				const ceph::real_time *mod_ptr,
				const ceph::real_time *unmod_ptr,
				bool high_precision_time,
				const char *if_match,
				const char *if_nomatch,
				AttrsMod attrs_mod,
				bool copy_if_newer,
				RGWAttrs& attrs,
				RGWObjCategory category,
				uint64_t olh_epoch,
				boost::optional<ceph::real_time> delete_at,
				string *version_id,
				string *tag,
				string *etag,
				void (*progress_cb)(off_t, void *),
				void *progress_data,
				const DoutPrefixProvider *dpp,
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

int RGWRadosObject::RadosReadOp::iterate(const DoutPrefixProvider *dpp, int64_t ofs, int64_t end, RGWGetDataCB *cb, optional_yield y)
{
  return parent_op.iterate(dpp, ofs, end, cb, y);
}

std::unique_ptr<RGWObject::WriteOp> RGWRadosObject::get_write_op(RGWObjectCtx* ctx)
{
  return std::unique_ptr<RGWObject::WriteOp>(new RGWRadosObject::RadosWriteOp(this, ctx));
}

RGWRadosObject::RadosWriteOp::RadosWriteOp(RGWRadosObject* _source, RGWObjectCtx* _rctx) :
	source(_source),
	rctx(_rctx),
	op_target(_source->store->getRados(),
		  _source->get_bucket()->get_info(),
		  *static_cast<RGWObjectCtx *>(rctx),
		  _source->get_obj()),
	parent_op(&op_target)
{ }

int RGWRadosObject::RadosWriteOp::prepare(optional_yield y)
{
  op_target.set_versioning_disabled(params.versioning_disabled);
  parent_op.meta.mtime = params.mtime;
  parent_op.meta.rmattrs = params.rmattrs;
  parent_op.meta.data = params.data;
  parent_op.meta.manifest = params.manifest;
  parent_op.meta.ptag = params.ptag;
  parent_op.meta.remove_objs = params.remove_objs;
  parent_op.meta.set_mtime = params.set_mtime;
  parent_op.meta.owner = params.owner.get_id();
  parent_op.meta.category = params.category;
  parent_op.meta.flags = params.flags;
  parent_op.meta.if_match = params.if_match;
  parent_op.meta.if_nomatch = params.if_nomatch;
  parent_op.meta.olh_epoch = params.olh_epoch;
  parent_op.meta.delete_at = params.delete_at;
  parent_op.meta.canceled = params.canceled;
  parent_op.meta.user_data = params.user_data;
  parent_op.meta.zones_trace = params.zones_trace;
  parent_op.meta.modify_tail = params.modify_tail;
  parent_op.meta.completeMultipart = params.completeMultipart;
  parent_op.meta.appendable = params.appendable;

  return 0;
}

int RGWRadosObject::RadosWriteOp::write_meta(const DoutPrefixProvider *dpp, uint64_t size, uint64_t accounted_size, optional_yield y)
{
  int ret = parent_op.write_meta(dpp, size, accounted_size, *params.attrs, y);
  params.canceled = parent_op.meta.canceled;

  return ret;
}

int RGWRadosObject::swift_versioning_restore(RGWObjectCtx* obj_ctx,
					     bool& restored,
					     const DoutPrefixProvider *dpp)
{
  return store->getRados()->swift_versioning_restore(*obj_ctx,
						     bucket->get_owner()->get_id(),
						     bucket,
						     this,
						     restored,
						     dpp);
}

int RGWRadosObject::swift_versioning_copy(RGWObjectCtx* obj_ctx,
					  const DoutPrefixProvider *dpp,
					  optional_yield y)
{
  return store->getRados()->swift_versioning_copy(*obj_ctx,
                                        bucket->get_info().owner,
                                        bucket,
                                        this,
                                        dpp,
                                        y);
}

MPRadosSerializer::MPRadosSerializer(RGWRadosStore* store, RGWRadosObject* obj, const std::string& lock_name) :
  lock(lock_name)
{
  rgw_pool meta_pool;
  rgw_raw_obj raw_obj;

  obj->get_raw_obj(&raw_obj);
  oid = raw_obj.oid;
  store->getRados()->get_obj_data_pool(obj->get_bucket()->get_placement_rule(),
				       obj->get_obj(), &meta_pool);
  store->getRados()->open_pool_ctx(meta_pool, ioctx, true);
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

LCRadosSerializer::LCRadosSerializer(RGWRadosStore* store, const std::string& _oid, const std::string& lock_name, const std::string& cookie) :
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

int RadosLifecycle::get_entry(const string& oid, const std::string& marker,
			      LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;
  int ret = cls_rgw_lc_get_entry(*store->getRados()->get_lc_pool_ctx(), oid, marker, cls_entry);

  entry.bucket = cls_entry.bucket;
  entry.start_time = cls_entry.start_time;
  entry.status = cls_entry.status;

  return ret;
}

int RadosLifecycle::get_next_entry(const string& oid, std::string& marker,
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

int RadosLifecycle::set_entry(const string& oid, const LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;

  cls_entry.bucket = entry.bucket;
  cls_entry.start_time = entry.start_time;
  cls_entry.status = entry.status;

  return cls_rgw_lc_set_entry(*store->getRados()->get_lc_pool_ctx(), oid, cls_entry);
}

int RadosLifecycle::list_entries(const string& oid, const string& marker,
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

int RadosLifecycle::rm_entry(const string& oid, const LCEntry& entry)
{
  cls_rgw_lc_entry cls_entry;

  cls_entry.bucket = entry.bucket;
  cls_entry.start_time = entry.start_time;
  cls_entry.status = entry.status;

  return cls_rgw_lc_rm_entry(*store->getRados()->get_lc_pool_ctx(), oid, cls_entry);
}

int RadosLifecycle::get_head(const string& oid, LCHead& head)
{
  cls_rgw_lc_obj_head cls_head;
  int ret = cls_rgw_lc_get_head(*store->getRados()->get_lc_pool_ctx(), oid, cls_head);

  head.marker = cls_head.marker;
  head.start_date = cls_head.start_date;

  return ret;
}

int RadosLifecycle::put_head(const string& oid, const LCHead& head)
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

int RadosNotification::publish_commit(const DoutPrefixProvider *dpp, uint64_t size,
				     const ceph::real_time& mtime, const std::string& etag)
{
  return rgw::notify::publish_commit(obj, size, mtime, etag, event_type, res, dpp);
}

void RadosGCChain::update(const DoutPrefixProvider *dpp, RGWObjManifest* manifest)
{
  rgw_obj target = obj->get_obj();
  store->getRados()->update_gc_chain(dpp, target, *manifest, &chain);
}

int RadosGCChain::send(const std::string& tag)
{
  return store->getRados()->send_chain_to_gc(chain, tag);
}

void RadosGCChain::delete_inline(const std::string& tag)
{
  store->getRados()->delete_objs_inline(chain, tag);
}

int RadosWriter::set_stripe_obj(const rgw_raw_obj& raw_obj)
{
  stripe_obj = store->svc()->rados->obj(raw_obj);
  return stripe_obj.open();
}

int RadosWriter::process(bufferlist&& bl, uint64_t offset)
{
  bufferlist data = std::move(bl);
  const uint64_t cost = data.length();
  if (cost == 0) { // no empty writes, use aio directly for creates
    return 0;
  }
  librados::ObjectWriteOperation op;
  if (offset == 0) {
    op.write_full(data);
  } else {
    op.write(offset, data);
  }
  constexpr uint64_t id = 0; // unused
  auto c = aio->get(stripe_obj, Aio::librados_op(std::move(op), y), cost, id);
  return process_completed(c, &written);
}

int RadosWriter::write_exclusive(const bufferlist& data)
{
  const uint64_t cost = data.length();

  librados::ObjectWriteOperation op;
  op.create(true); // exclusive create
  op.write_full(data);

  constexpr uint64_t id = 0; // unused
  auto c = aio->get(stripe_obj, Aio::librados_op(std::move(op), y), cost, id);
  auto d = aio->drain();
  c.splice(c.end(), d);
  return process_completed(c, &written);
}

int RadosWriter::drain()
{
  return process_completed(aio->drain(), &written);
}

RadosWriter::~RadosWriter()
{
  // wait on any outstanding aio completions
  process_completed(aio->drain(), &written);

  bool need_to_remove_head = false;
  std::optional<rgw_raw_obj> raw_head;
  if (!rgw::sal::RGWObject::empty(head_obj.get())) {
    raw_head.emplace();
    head_obj->get_raw_obj(&*raw_head);
  }

  /**
   * We should delete the object in the "multipart" namespace to avoid race condition.
   * Such race condition is caused by the fact that the multipart object is the gatekeeper of a multipart
   * upload, when it is deleted, a second upload would start with the same suffix("2/"), therefore, objects
   * written by the second upload may be deleted by the first upload.
   * details is describled on #11749
   *
   * The above comment still stands, but instead of searching for a specific object in the multipart
   * namespace, we just make sure that we remove the object that is marked as the head object after
   * we remove all the other raw objects. Note that we use different call to remove the head object,
   * as this one needs to go via the bucket index prepare/complete 2-phase commit scheme.
   */
  for (const auto& obj : written) {
    if (raw_head && obj == *raw_head) {
      ldpp_dout(dpp, 5) << "NOTE: we should not process the head object (" << obj << ") here" << dendl;
      need_to_remove_head = true;
      continue;
    }

    int r = store->delete_raw_obj(dpp, obj);
    if (r < 0 && r != -ENOENT) {
      ldpp_dout(dpp, 0) << "WARNING: failed to remove obj (" << obj << "), leaked" << dendl;
    }
  }

  if (need_to_remove_head) {
    ldpp_dout(dpp, 5) << "NOTE: we are going to process the head obj (" << *raw_head << ")" << dendl;
    std::unique_ptr<rgw::sal::RGWObject::DeleteOp> del_op = head_obj->get_delete_op(&obj_ctx);
    del_op->params.bucket_owner = bucket->get_acl_owner();

    int r = del_op->delete_obj(dpp, y);
    if (r < 0 && r != -ENOENT) {
      ldpp_dout(dpp, 0) << "WARNING: failed to remove obj (" << *raw_head << "), leaked" << dendl;
    }
  }
}

const RGWZoneGroup& RadosZone::get_zonegroup()
{
  return store->svc()->zone->get_zonegroup();
}

int RadosZone::get_zonegroup(const string& id, RGWZoneGroup& zonegroup)
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

bool RadosZone::get_redirect_endpoint(std::string *endpoint)
{
  return store->svc()->zone->get_redirect_zone_endpoint(endpoint);
}

bool RadosZone::has_zonegroup_api(const std::string& api) const
{
  return store->svc()->zone->has_zonegroup_api(api);
}

const string& RadosZone::get_current_period_id()
{
  return store->svc()->zone->get_current_period_id();
}

} // namespace rgw::sal

extern "C" {

void *newRGWStore(void)
{
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();
  if (store) {
    RGWRados *rados = new RGWRados();

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
