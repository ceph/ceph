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

#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "rgw_service.h"
#include "rgw_lc.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"
#include "services/svc_tier_rados.h"
#include "cls/rgw/cls_rgw_client.h"

#include "rgw_pubsub.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

int RGWRadosUser::list_buckets(const DoutPrefixProvider *dpp, const string& marker, const string& end_marker,
			       uint64_t max, bool need_stats, RGWBucketList &buckets,
			       optional_yield y)
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

int RGWRadosUser::load_by_id(const DoutPrefixProvider *dpp, optional_yield y)

{
    return store->ctl()->user->get_info_by_uid(dpp, info.user_id, &info, y);
}

std::unique_ptr<RGWObject> RGWRadosStore::get_object(const rgw_obj_key& k)
{
  return std::unique_ptr<RGWObject>(new RGWRadosObject(this, k));
}

/* Placeholder */
RGWObject *RGWRadosBucket::create_object(const rgw_obj_key &key)
{
  return nullptr;
}

int RGWRadosBucket::remove_bucket(const DoutPrefixProvider *dpp,
			       bool delete_children,
			       bool forward_to_master,
			       req_info* req_info, optional_yield y)
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
      ret = rgw_remove_object(dpp, store, info, info.bucket, key);
      if (ret < 0 && ret != -ENOENT) {
	return ret;
      }
    }
  } while(results.is_truncated);

  ret = abort_bucket_multiparts(dpp, store, store->ctx(), info);
  if (ret < 0) {
    return ret;
  }

  // remove lifecycle config, if any (XXX note could be made generic)
  (void) store->getRados()->get_lc()->remove_bucket_config(
    this->info, get_attrs());

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
  RGWSI_MetaBackend_CtxParams bectx_params = RGWSI_MetaBackend_CtxParams_SObj(&obj_ctx);
  RGWObjVersionTracker ep_ot;
  int ret = store->ctl()->bucket->read_bucket_info(info.bucket, &info, y, dpp,
				      RGWBucketCtl::BucketInstance::GetParams()
				      .set_mtime(&mtime)
				      .set_attrs(&attrs)
                                      .set_bectx_params(bectx_params),
				      &ep_ot);
  if (ret == 0) {
    bucket_version = ep_ot.read_version;
    ent.placement_rule = info.placement_rule;
    ent.bucket = info.bucket; // we looked up bucket_id
  }
  return ret;
}

int RGWRadosBucket::load_by_name(const DoutPrefixProvider *dpp, const std::string& tenant, const std::string& bucket_name, const std::string bucket_instance_id, RGWSysObjectCtx *rctx, optional_yield y)
{
  info.bucket.tenant = tenant;
  info.bucket.name = bucket_name;
  info.bucket.bucket_id = bucket_instance_id;
  ent.bucket = info.bucket;

  if (bucket_instance_id.empty()) {
    return get_bucket_info(dpp, y);
  }

  return store->getRados()->get_bucket_instance_info(*rctx, info.bucket, info, NULL, &attrs, y, dpp);
}

int RGWRadosBucket::get_bucket_stats(const DoutPrefixProvider *dpp, RGWBucketInfo& bucket_info, int shard_id,
				     std::string *bucket_ver, std::string *master_ver,
				     std::map<RGWObjCategory, RGWStorageStats>& stats,
				     std::string *max_marker, bool *syncstopped)
{
  return store->getRados()->get_bucket_stats(dpp, bucket_info, shard_id, bucket_ver, master_ver, stats, max_marker, syncstopped);
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

int RGWRadosBucket::link(const DoutPrefixProvider *dpp, RGWUser* new_user, optional_yield y)
{
  RGWBucketEntryPoint ep;
  ep.bucket = info.bucket;
  ep.owner = new_user->get_user();
  ep.creation_time = get_creation_time();
  ep.linked = true;
  RGWAttrs ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  return store->ctl()->bucket->link_bucket(new_user->get_user(), info.bucket,
					   ceph::real_time(), y, dpp, true, &ep_data);
}

int RGWRadosBucket::unlink(RGWUser* new_user, optional_yield y)
{
  return -1;
}

int RGWRadosBucket::chown(RGWUser* new_user, RGWUser* old_user, optional_yield y, const DoutPrefixProvider *dpp)
{
  string obj_marker;

  return store->ctl()->bucket->chown(store, info, new_user->get_user(),
			   old_user->get_display_name(), obj_marker, y, dpp);
}

int RGWRadosBucket::put_instance_info(const DoutPrefixProvider *dpp, bool exclusive, ceph::real_time _mtime)
{
  mtime = _mtime;
  return store->getRados()->put_bucket_instance_info(info, exclusive, mtime, &attrs, dpp);
}

/* Make sure to call get_bucket_info() if you need it first */
bool RGWRadosBucket::is_owner(RGWUser* user)
{
  return (info.owner.compare(user->get_user()) == 0);
}

int RGWRadosBucket::check_empty(const DoutPrefixProvider *dpp, optional_yield y)
{
  return store->getRados()->check_bucket_empty(dpp, info, y);
}

int RGWRadosBucket::check_quota(RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size,
				optional_yield y, bool check_size_only)
{
    return store->getRados()->check_quota(info.owner, get_key(),
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

int RGWRadosBucket::read_usage(const DoutPrefixProvider *dpp, uint64_t start_epoch, uint64_t end_epoch,
			       uint32_t max_entries, bool *is_truncated,
			       RGWUsageIter& usage_iter,
			       map<rgw_user_bucket, rgw_usage_log_entry>& usage)
{
    return store->getRados()->read_usage(dpp, owner->get_id(), get_name(), start_epoch,
					 end_epoch, max_entries, is_truncated,
					 usage_iter, usage);
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
  list_op.params.list_versions = params.list_versions;
  list_op.params.allow_unordered = params.allow_unordered;

  int ret = list_op.list_objects(dpp, max, &results.objs, &results.common_prefixes, &results.is_truncated, y);
  if (ret >= 0) {
    results.next_marker = list_op.get_next_marker();
    params.marker = results.next_marker;
  }

  return ret;
}

std::unique_ptr<RGWUser> RGWRadosStore::get_user(const rgw_user &u)
{
  return std::unique_ptr<RGWUser>(new RGWRadosUser(this, u));
}

//RGWBucket *RGWRadosStore::create_bucket(RGWUser &u, const rgw_bucket &b)
//{
  //if (!bucket) {
    //bucket = new RGWRadosBucket(this, u, b);
  //}
//
  //return bucket;
//}
//
void RGWRadosStore::finalize(void)
{
  if (rados)
    rados->finalize();
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

int RGWRadosObject::get_obj_state(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, RGWBucket& bucket, RGWObjState **state, optional_yield y, bool follow_olh)
{
  //rgw_obj obj(bucket.get_key(), key);
  //obj.set_in_extra_data(in_extra_data);
  //obj.index_hash_source = index_hash_source;
  return store->getRados()->get_obj_state(dpp, rctx, bucket.get_info(), get_obj(), state, follow_olh, y);
}

int RGWRadosObject::read_attrs(RGWRados::Object::Read &read_op, optional_yield y, const DoutPrefixProvider *dpp, rgw_obj *target_obj)
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

  return read_attrs(read_op, y, dpp, target_obj);
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

  int ret = read_attrs(read_op, y, dpp);
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

int RGWRadosObject::omap_get_vals_by_keys(const DoutPrefixProvider *dpp,
                                          const std::string& oid,
					  const std::set<std::string>& keys,
					  RGWAttrs *vals)
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

MPSerializer* RGWRadosObject::get_serializer(const DoutPrefixProvider *dpp, const std::string& lock_name)
{
  return new MPRadosSerializer(dpp, store, this, lock_name);
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

int RGWRadosObject::delete_object(const DoutPrefixProvider *dpp,
				  RGWObjectCtx* obj_ctx,
				  ACLOwner obj_owner,
				  ACLOwner bucket_owner,
				  ceph::real_time unmod_since,
				  bool high_precision_time,
				  uint64_t epoch,
				  std::string& version_id,
				  optional_yield y,
				  bool prevent_versioning)
{
  int ret = 0;
  RGWRados::Object del_target(store->getRados(), bucket->get_info(), *obj_ctx, get_obj());
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.olh_epoch = epoch;
  del_op.params.marker_version_id = version_id;
  del_op.params.bucket_owner = bucket_owner.get_id();
  del_op.params.versioning_status =
    prevent_versioning ? 0 : bucket->get_info().versioning_status();
  del_op.params.obj_owner = obj_owner;
  del_op.params.unmod_since = unmod_since;
  del_op.params.high_precision_time = high_precision_time;

  ret = del_op.delete_obj(y, dpp);
  if (ret >= 0) {
    delete_marker = del_op.result.delete_marker;
    version_id = del_op.result.version_id;
  }

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
  op_target.set_meta_placement_rule(params.pmeta_placement_rule);
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

int RGWRadosStore::get_bucket(const DoutPrefixProvider *dpp, RGWUser* u, const std::string& tenant, const std::string&name, std::unique_ptr<RGWBucket>* bucket, optional_yield y)
{
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(dpp, u, b, bucket, y);
}

static int decode_policy(const DoutPrefixProvider *dpp,
                         CephContext *cct,
                         bufferlist& bl,
                         RGWAccessControlPolicy *policy)
{
  auto iter = bl.cbegin();
  try {
    policy->decode(iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: could not decode policy, caught buffer::error" << dendl;
    return -EIO;
  }
  if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 15>()) {
    ldpp_dout(dpp, 15) << __func__ << " Read AccessControlPolicy";
    RGWAccessControlPolicy_S3 *s3policy = static_cast<RGWAccessControlPolicy_S3 *>(policy);
    s3policy->to_xml(*_dout);
    *_dout << dendl;
  }
  return 0;
}

static int rgw_op_get_bucket_policy_from_attr(const DoutPrefixProvider *dpp, RGWRadosStore *store,
					      RGWUser& user,
					      RGWAttrs& bucket_attrs,
					      RGWAccessControlPolicy *policy,
					      optional_yield y)
{
  auto aiter = bucket_attrs.find(RGW_ATTR_ACL);

  if (aiter != bucket_attrs.end()) {
    int ret = decode_policy(dpp, store->ctx(), aiter->second, policy);
    if (ret < 0)
      return ret;
  } else {
    ldout(store->ctx(), 0) << "WARNING: couldn't find acl header for bucket, generating default" << dendl;
    /* object exists, but policy is broken */
    int r = user.load_by_id(dpp, y);
    if (r < 0)
      return r;

    policy->create_default(user.get_user(), user.get_display_name());
  }
  return 0;
}

bool RGWRadosStore::is_meta_master()
{
  return svc()->zone->is_meta_master();
}

int RGWRadosStore::forward_request_to_master(const DoutPrefixProvider *dpp,
                                             RGWUser* user, obj_version *objv,
					     bufferlist& in_data,
					     JSONParser *jp, req_info& info,
					     optional_yield y)
{
  if (is_meta_master()) {
    /* We're master, don't forward */
    return 0;
  }

  if (!svc()->zone->get_master_conn()) {
    ldout(ctx(), 0) << "rest connection is invalid" << dendl;
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
    ldout(ctx(), 0) << "failed parsing response from master zonegroup" << dendl;
    return -EINVAL;
  }

  return 0;
}

int RGWRadosStore::defer_gc(const DoutPrefixProvider *dpp, RGWObjectCtx *rctx, RGWBucket* bucket, RGWObject* obj, optional_yield y)
{
  return rados->defer_gc(dpp, rctx, bucket->get_info(), obj->get_obj(), y);
}

const RGWZoneGroup& RGWRadosStore::get_zonegroup()
{
  return rados->svc.zone->get_zonegroup();
}

int RGWRadosStore::get_zonegroup(const string& id, RGWZoneGroup& zonegroup)
{
  return rados->svc.zone->get_zonegroup(id, zonegroup);
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
				    zid, placement_rule, swift_ver_location,
				    pquota_info, attrs,
				    info, pobjv, &ep_objv, creation_time,
				    pmaster_bucket, pmaster_num_shards, y, dpp, exclusive);
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

std::unique_ptr<Lifecycle> RGWRadosStore::get_lifecycle(void)
{
  return std::unique_ptr<Lifecycle>(new RadosLifecycle(this));
}

int RGWRadosStore::delete_raw_obj(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj)
{
  return rados->delete_raw_obj(dpp, obj);
}

void RGWRadosStore::get_raw_obj(const rgw_placement_rule& placement_rule, const rgw_obj& obj, rgw_raw_obj* raw_obj)
{
    rados->obj_to_raw(placement_rule, obj, raw_obj);
}

int RGWRadosStore::get_raw_chunk_size(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, uint64_t* chunk_size)
{
  return rados->get_max_chunk_size(obj.pool, chunk_size, dpp);
}

MPRadosSerializer::MPRadosSerializer(const DoutPrefixProvider *dpp, RGWRadosStore* store, RGWRadosObject* obj, const std::string& lock_name) :
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

} // namespace rgw::sal

rgw::sal::RGWRadosStore *RGWStoreManager::init_storage_provider(const DoutPrefixProvider *dpp, CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_cache, bool use_gc)
{
  RGWRados *rados = new RGWRados;
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();

  store->setRados(rados);
  rados->set_store(store);

  if ((*rados).set_use_cache(use_cache)
              .set_use_gc(use_gc)
              .set_run_gc_thread(use_gc_thread)
              .set_run_lc_thread(use_lc_thread)
              .set_run_quota_threads(quota_threads)
              .set_run_sync_thread(run_sync_thread)
              .set_run_reshard_thread(run_reshard_thread)
              .initialize(cct, dpp) < 0) {
    delete store;
    return NULL;
  }

  return store;
}

rgw::sal::RGWRadosStore *RGWStoreManager::init_raw_storage_provider(const DoutPrefixProvider *dpp, CephContext *cct)
{
  RGWRados *rados = new RGWRados;
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();

  store->setRados(rados);
  rados->set_store(store);

  rados->set_context(cct);

  int ret = rados->init_svc(true, dpp);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to init services (ret=" << cpp_strerror(-ret) << ")" << dendl;
    delete store;
    return nullptr;
  }

  if (rados->init_rados() < 0) {
    delete store;
    return nullptr;
  }

  return store;
}

int rgw::sal::RGWRadosStore::get_obj_head_ioctx(const DoutPrefixProvider *dpp, const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx *ioctx)
{
  return rados->get_obj_head_ioctx(dpp, bucket_info, obj, ioctx);
}

void RGWStoreManager::close_storage(rgw::sal::RGWRadosStore *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}
