// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
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
#include "rgw_bucket.h"
#include "rgw_multi.h"
#include "rgw_acl_s3.h"

/* Stuff for RGWRadosStore.  Move to separate file when store split out */
#include "rgw_zone.h"
#include "rgw_rest_conn.h"
#include "services/svc_sys_obj.h"
#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::sal {

int RGWRadosUser::list_buckets(const string& marker, const string& end_marker,
			       uint64_t max, bool need_stats, RGWBucketList &buckets, const Span& parent_span)
{
   
   
  Span span_1 = child_span(__PRETTY_FUNCTION__, parent_span);
  

  RGWUserBuckets ulist;
  bool is_truncated = false;
  int ret;

  buckets.clear();
  Span span_2 = child_span("rgw_user.cc : RGWUSerCtl::list_buckets", span_1);
  ret = store->ctl()->user->list_buckets(info.user_id, marker, end_marker, max,
					 need_stats, &ulist, &is_truncated);
  finish_trace(span_2);
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

int RGWRadosUser::load_by_id(optional_yield y)

{
    return store->ctl()->user->get_info_by_uid(info.user_id, &info, y);
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

int RGWRadosBucket::remove_bucket(bool delete_children, std::string prefix, std::string delimiter, bool forward_to_master, req_info* req_info, optional_yield y, const Span& parent_span)
{
  Span span_1 = child_span(__PRETTY_FUNCTION__, parent_span);
  Span span_2 = child_span("RGWRadosBucket::get_bucket_info", span_1);
  int ret = get_bucket_info(y);
  finish_trace(span_2);
  if (ret < 0)
    return ret;

  ListParams params;
  params.list_versions = true;
  params.allow_unordered = true;

  ListResults results;

  bool is_truncated = false;
  do {
    results.objs.clear();
    ret = list(params, 1000, results, y, span_1);
    if (ret < 0)
      return ret;

    if (!results.objs.empty() && !delete_children) {
      lderr(store->ctx()) << "ERROR: could not remove non-empty bucket " << info.bucket.name <<
	dendl;
      return -ENOTEMPTY;
    }

    for (const auto& obj : results.objs) {
      rgw_obj_key key(obj.key);
      /* xxx dang */
      ret = rgw_remove_object(store, info, info.bucket, key, span_1);
      if (ret < 0 && ret != -ENOENT) {
	return ret;
      }
    }
  } while(is_truncated);

  ret = abort_bucket_multiparts(store, store->ctx(), info, prefix, delimiter, span_1);
  if (ret < 0) {
    return ret;
  }
  Span span_4 = child_span("rgw_bucket.cc : RGWBucketCtl::sync_user_stats", span_1);
  ret = store->ctl()->bucket->sync_user_stats(info.owner, info);
  finish_trace(span_4);

  if ( ret < 0) {
     ldout(store->ctx(), 1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker ot;

  // if we deleted children above we will force delete, as any that
  // remain is detrius from a prior bug
  ret = store->getRados()->delete_bucket(info, ot, null_yield, !delete_children, span_1);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not remove bucket " <<
      info.bucket.name << dendl;
    return ret;
  }
  Span span_5 = child_span("rgw_bucket.cc : RGWBucketCtl::unlink_bucket", span_1);
  ret = store->ctl()->bucket->unlink_bucket(info.owner, info.bucket, null_yield, false, span_1);
  finish_trace(span_5);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: unable to remove user bucket information" << dendl;
  }

  if (forward_to_master) {
    bufferlist in_data;
    ret = store->forward_request_to_master(owner, &ot.read_version, in_data, nullptr, *req_info);
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

int RGWRadosBucket::get_bucket_info(optional_yield y)
{
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  RGWSI_MetaBackend_CtxParams bectx_params = RGWSI_MetaBackend_CtxParams_SObj(&obj_ctx);
  RGWObjVersionTracker ep_ot;
  int ret = store->ctl()->bucket->read_bucket_info(info.bucket, &info, y,
				      RGWBucketCtl::BucketInstance::GetParams()
				      .set_mtime(&mtime)
				      .set_attrs(&attrs.attrs)
                                      .set_bectx_params(bectx_params),
				      &ep_ot);
  if (ret == 0) {
    bucket_version = ep_ot.read_version;
    ent.placement_rule = info.placement_rule;
  }
  return ret;
}

int RGWRadosBucket::load_by_name(const std::string& tenant, const std::string& bucket_name, const std::string bucket_instance_id, RGWSysObjectCtx *rctx, optional_yield y)
{
  info.bucket.tenant = tenant;
  info.bucket.name = bucket_name;
  info.bucket.bucket_id = bucket_instance_id;
  ent.bucket = info.bucket;

  if (bucket_instance_id.empty()) {
    return get_bucket_info(y);
  }

  return store->getRados()->get_bucket_instance_info(*rctx, info.bucket, info, NULL, &attrs.attrs, y);
}

int RGWRadosBucket::get_bucket_stats(RGWBucketInfo& bucket_info, int shard_id,
				     std::string *bucket_ver, std::string *master_ver,
				     std::map<RGWObjCategory, RGWStorageStats>& stats,
				     std::string *max_marker, bool *syncstopped)
{
  return store->getRados()->get_bucket_stats(bucket_info, shard_id, bucket_ver, master_ver, stats, max_marker, syncstopped);
}

int RGWRadosBucket::read_bucket_stats(optional_yield y)
{
      int ret = store->ctl()->bucket->read_bucket_stats(info.bucket, &ent, y);
      info.placement_rule = ent.placement_rule;
      return ret;
}

int RGWRadosBucket::sync_user_stats()
{
      return store->ctl()->bucket->sync_user_stats(owner->get_id(), info, &ent);
}

int RGWRadosBucket::update_container_stats(const Span& parent_span)
{
   
      
  Span span_1 = child_span(__PRETTY_FUNCTION__, parent_span);

  int ret;
  map<std::string, RGWBucketEnt> m;

  m[info.bucket.name] = ent;
  ret = store->getRados()->update_containers_stats(m);
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
  info.placement_rule = ent.placement_rule;

  return 0;
}

int RGWRadosBucket::check_bucket_shards(void)
{
      return store->getRados()->check_bucket_shards(info, info.bucket, get_count());
}

int RGWRadosBucket::link(RGWUser* new_user, optional_yield y)
{
  RGWBucketEntryPoint ep;
  ep.bucket = info.bucket;
  ep.owner = new_user->get_user();
  ep.creation_time = get_creation_time();
  ep.linked = true;
  map<string, bufferlist> ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  return store->ctl()->bucket->link_bucket(new_user->get_user(), info.bucket,
					   ceph::real_time(), y, true, &ep_data);
}

int RGWRadosBucket::unlink(RGWUser* new_user, optional_yield y)
{
  return -1;
}

int RGWRadosBucket::chown(RGWUser* new_user, RGWUser* old_user, optional_yield y)
{
  string obj_marker;

  return store->ctl()->bucket->chown(store, info, new_user->get_user(),
			   old_user->get_display_name(), obj_marker, y);
}

int RGWRadosBucket::put_instance_info(bool exclusive, ceph::real_time _mtime, const Span& parent_span)
{
  mtime = _mtime;
  return store->getRados()->put_bucket_instance_info(info, exclusive, mtime, &attrs.attrs, parent_span);
}

/* Make sure to call get_bucket_info() if you need it first */
bool RGWRadosBucket::is_owner(RGWUser* user)
{
  return (info.owner.compare(user->get_user()) == 0);
}

int RGWRadosBucket::check_empty(optional_yield y, const Span& parent_span)
{
  return store->getRados()->check_bucket_empty(info, y, parent_span);
}

int RGWRadosBucket::check_quota(RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota, uint64_t obj_size, bool check_size_only)
{
    return store->getRados()->check_quota(owner->get_user(), get_key(),
					  user_quota, bucket_quota, obj_size, check_size_only);
}

int RGWRadosBucket::set_instance_attrs(RGWAttrs& attrs, optional_yield y, const Span& parent_span)
{
    return store->ctl()->bucket->set_bucket_instance_attrs(get_info(),
				attrs.attrs, &get_info().objv_tracker, y, parent_span);
}

int RGWRadosBucket::try_refresh_info(ceph::real_time *pmtime)
{
  return store->getRados()->try_refresh_bucket_info(info, pmtime, &attrs.attrs);
}

int RGWRadosBucket::set_acl(RGWAccessControlPolicy &acl, optional_yield y)
{
  bufferlist aclbl;

  acls = acl;
  acl.encode(aclbl);

  return store->ctl()->bucket->set_acl(acl.get_owner(), info.bucket, info, aclbl, null_yield);
}

std::unique_ptr<RGWObject> RGWRadosBucket::get_object(const rgw_obj_key& k)
{
  return std::unique_ptr<RGWObject>(new RGWRadosObject(this->store, k, this));
}

int RGWRadosBucket::list(ListParams& params, int max, ListResults& results, optional_yield y, const Span& parent_span)
{
  RGWRados::Bucket target(store->getRados(), get_info());
  if (params.shard_id >= 0) {
    target.set_shard_id(params.shard_id);
  }
  RGWRados::Bucket::List list_op(&target);

  list_op.params.prefix = params.prefix;
  list_op.params.delim = params.delim;
  list_op.params.marker = params.marker;
  list_op.params.end_marker = params.end_marker;
  list_op.params.list_versions = params.list_versions;
  list_op.params.allow_unordered = params.allow_unordered;

  int ret = list_op.list_objects(max, &results.objs, &results.common_prefixes, &results.is_truncated, y, parent_span);
  if (ret >= 0) {
    results.next_marker = list_op.get_next_marker();
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

int RGWRadosObject::get_obj_state(RGWObjectCtx *rctx, RGWBucket& bucket, RGWObjState **state, optional_yield y, bool follow_olh)
{
  rgw_obj obj(bucket.get_key(), key.name);

  return store->getRados()->get_obj_state(rctx, bucket.get_info(), obj, state, follow_olh, y);
}

int RGWRadosObject::read_attrs(RGWRados::Object::Read &read_op, optional_yield y, rgw_obj *target_obj, const Span& parent_span)
{
  read_op.params.attrs = &attrs.attrs;
  read_op.params.target_obj = target_obj;
  read_op.params.obj_size = &obj_size;
  read_op.params.lastmod = &mtime;

  return read_op.prepare(y,parent_span);
}

int RGWRadosObject::set_obj_attrs(RGWObjectCtx* rctx, RGWAttrs* setattrs, RGWAttrs* delattrs, optional_yield y, rgw_obj* target_obj)
{
  map<string, bufferlist> empty;
  rgw_obj target = get_obj();

  if (!target_obj)
    target_obj = &target;

  return store->getRados()->set_attrs(rctx,
			bucket->get_info(),
			*target_obj,
			setattrs ? setattrs->attrs : empty,
			delattrs ? &delattrs->attrs : nullptr,
			y);
}

int RGWRadosObject::get_obj_attrs(RGWObjectCtx *rctx, optional_yield y, rgw_obj *target_obj, const Span& parent_span)
{
  RGWRados::Object op_target(store->getRados(), bucket->get_info(), *rctx, get_obj());
  RGWRados::Object::Read read_op(&op_target);

  return read_attrs(read_op, y, target_obj, parent_span);
}

int RGWRadosObject::modify_obj_attrs(RGWObjectCtx *rctx, const char *attr_name, bufferlist& attr_val, optional_yield y, const Span& parent_span)
{
   
    
  Span span_1 = child_span(__PRETTY_FUNCTION__, parent_span);
  

  rgw_obj target_obj;
  Span span_2 = child_span("RGWRadosObject::get_obj_attrs", parent_span);
  int r = get_obj_attrs(rctx, y, &target_obj);
  finish_trace(span_2);
  if (r < 0) {
    return r;
  }
  set_atomic(rctx);
  attrs.attrs[attr_name] = attr_val;
  return store->getRados()->set_attrs(rctx, bucket->get_info(), target_obj, attrs.attrs, NULL, y, span_1);
}

int RGWRadosObject::copy_obj_data(RGWObjectCtx& rctx, RGWBucket* dest_bucket,
				  RGWObject* dest_obj,
				  uint16_t olh_epoch,
				  std::string* petag,
				  const DoutPrefixProvider *dpp,
				  optional_yield y)
{
  map<string, bufferlist> attrset;
  RGWRados::Object op_target(store->getRados(), dest_bucket->get_info(), rctx, get_obj());
  RGWRados::Object::Read read_op(&op_target);

  int ret = read_attrs(read_op, y);
  if (ret < 0)
    return ret;

  attrset = attrs.attrs;

  attrset.erase(RGW_ATTR_ID_TAG);
  attrset.erase(RGW_ATTR_TAIL_TAG);

  return store->getRados()->copy_obj_data(rctx, dest_bucket,
					   dest_bucket->get_info().placement_rule, read_op,
					   obj_size - 1, dest_obj, NULL, mtime, attrset, 0, real_time(), NULL,
					   dpp, y);
}

int RGWRadosObject::delete_obj_attrs(RGWObjectCtx *rctx, const char *attr_name, optional_yield y, const Span& parent_span)
{
  map <string, bufferlist> attrs;
  map <string, bufferlist> rmattr;
  bufferlist bl;

  set_atomic(rctx);
  rmattr[attr_name] = bl;
  rgw_obj obj = get_obj();
  return store->getRados()->set_attrs(rctx, bucket->get_info(), obj, attrs, &rmattr, y, parent_span);
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
  map<string, bufferlist>::iterator iter = attrs.find(RGW_ATTR_DELETE_AT);
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

int RGWRadosObject::omap_get_vals_by_keys(const std::string& oid,
					  const std::set<std::string>& keys,
					  std::map<std::string, bufferlist> *vals)
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

int RGWRadosObject::RadosReadOp::prepare(optional_yield y, const Span& parent_span)
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
  parent_op.params.attrs = &source->get_attrs().attrs;

  int ret = parent_op.prepare(y, parent_span);
  if (ret < 0)
    return ret;

  source->set_key(parent_op.state.obj.key);
  source->set_obj_size(obj_size);
  result.head_obj = parent_op.state.head_obj;

  return ret;
}

int RGWRadosObject::RadosReadOp::read(int64_t ofs, int64_t end, bufferlist& bl, optional_yield y)
{
  return parent_op.read(ofs, end, bl, y);
}

int RGWRadosObject::RadosReadOp::get_manifest(RGWObjManifest **pmanifest,
					      optional_yield y)
{
  return op_target.get_manifest(pmanifest, y);
}

int RGWRadosObject::delete_object(RGWObjectCtx* obj_ctx, ACLOwner obj_owner, ACLOwner bucket_owner, ceph::real_time unmod_since, bool high_precision_time, uint64_t epoch, string& version_id, optional_yield y, const Span& parent_span)
{
  int ret = 0;
  RGWRados::Object del_target(store->getRados(), bucket->get_info(), *obj_ctx, get_obj());
  RGWRados::Object::Delete del_op(&del_target);

  del_op.params.olh_epoch = epoch;
  del_op.params.marker_version_id = version_id;
  del_op.params.bucket_owner = bucket_owner.get_id();
  del_op.params.versioning_status = bucket->get_info().versioning_status();
  del_op.params.obj_owner = obj_owner;
  del_op.params.unmod_since = unmod_since;
  del_op.params.high_precision_time = high_precision_time;

  ret = del_op.delete_obj(y, parent_span);
  if (ret >= 0) {
    delete_marker = del_op.result.delete_marker;
    version_id = del_op.result.version_id;
  }

  return ret;
}


int RGWRadosObject::RadosReadOp::iterate(int64_t ofs, int64_t end, RGWGetDataCB *cb, optional_yield y, const Span& parent_span)
{
  return parent_op.iterate(ofs, end, cb, y, parent_span);
}

int RGWRadosStore::get_bucket(RGWUser* u, const rgw_bucket& b, std::unique_ptr<RGWBucket>* bucket)
{
  int ret;
  RGWBucket* bp;

  bp = new RGWRadosBucket(this, b, u);
  ret = bp->get_bucket_info(null_yield);
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

int RGWRadosStore::get_bucket(RGWUser* u, const std::string& tenant, const std::string&name, std::unique_ptr<RGWBucket>* bucket)
{
  rgw_bucket b;

  b.tenant = tenant;
  b.name = name;

  return get_bucket(u, b, bucket);
}

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

static int rgw_op_get_bucket_policy_from_attr(RGWRadosStore *store,
				       RGWUser& user,
				       map<string, bufferlist>& bucket_attrs,
				       RGWAccessControlPolicy *policy)
{
  map<string, bufferlist>::iterator aiter = bucket_attrs.find(RGW_ATTR_ACL);

  if (aiter != bucket_attrs.end()) {
    int ret = decode_policy(store->ctx(), aiter->second, policy);
    if (ret < 0)
      return ret;
  } else {
    ldout(store->ctx(), 0) << "WARNING: couldn't find acl header for bucket, generating default" << dendl;
    /* object exists, but policy is broken */
    int r = user.load_by_id(null_yield);
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

int RGWRadosStore::forward_request_to_master(RGWUser* user, obj_version *objv,
					     bufferlist& in_data,
					     JSONParser *jp, req_info& info)
{
  if (is_meta_master()) {
    /* We're master, don't forward */
    return 0;
  }

  if (!svc()->zone->get_master_conn()) {
    ldout(ctx(), 0) << "rest connection is invalid" << dendl;
    return -EINVAL;
  }
  ldout(ctx(), 0) << "sending request to master zonegroup" << dendl;
  bufferlist response;
  string uid_str = user->get_id().to_str();
#define MAX_REST_RESPONSE (128 * 1024) // we expect a very small response
  int ret = svc()->zone->get_master_conn()->forward(rgw_user(uid_str), info,
                                                    objv, MAX_REST_RESPONSE,
						    &in_data, &response);
  if (ret < 0)
    return ret;

  ldout(ctx(), 20) << "response: " << response.c_str() << dendl;
  if (jp && !jp->parse(response.c_str(), response.length())) {
    ldout(ctx(), 0) << "failed parsing response from master zonegroup" << dendl;
    return -EINVAL;
  }

  return 0;
}


int RGWRadosStore::create_bucket(RGWUser& u, const rgw_bucket& b,
				 const string& zonegroup_id,
				 rgw_placement_rule& placement_rule,
				 string& swift_ver_location,
				 const RGWQuotaInfo * pquota_info,
				 map<std::string, bufferlist>& attrs,
				 RGWBucketInfo& info,
				 obj_version& ep_objv,
				 bool exclusive,
				 bool obj_lock_enabled,
				 bool *existed,
				 req_info& req_info,
				 std::unique_ptr<RGWBucket>* bucket_out, const Span& parent_span)
{
   
      
  Span span_1 = child_span(__PRETTY_FUNCTION__, parent_span);
  

  int ret;
  bufferlist in_data;
  RGWBucketInfo master_info;
  rgw_bucket *pmaster_bucket;
  uint32_t *pmaster_num_shards;
  real_time creation_time;
  RGWAccessControlPolicy old_policy(ctx());
  std::unique_ptr<RGWBucket> bucket;
  obj_version objv, *pobjv = NULL;

  /* If it exists, look it up; otherwise create it */
  ret = get_bucket(&u, b, &bucket);
  if (ret < 0 && ret != -ENOENT)
    return ret;

  if (ret != -ENOENT) {
    *existed = true;
    Span span_2 = child_span("rgw_sal.cc : rgw_op_get_bucket_policy_from_attr", span_1);
    int r = rgw_op_get_bucket_policy_from_attr(this, u, bucket->get_attrs().attrs,
					       &old_policy);
    finish_trace(span_2);
    if (r >= 0)  {
      if (old_policy.get_owner().get_id().compare(u.get_id()) != 0) {
	bucket_out->swap(bucket);
	ret = -EEXIST;
	return ret;
      }
    }
  } else {
    bucket = std::unique_ptr<RGWBucket>(new RGWRadosBucket(this, b, &u));
    *existed = false;
    bucket->set_attrs(attrs);
  }

  if (!svc()->zone->is_meta_master()) {
    JSONParser jp;
    ret = forward_request_to_master(&u, NULL, in_data, &jp, req_info);
    if (ret < 0) {
      return ret;
    }

    JSONDecoder::decode_json("entry_point_object_ver", ep_objv, &jp);
    JSONDecoder::decode_json("object_ver", objv, &jp);
    JSONDecoder::decode_json("bucket_info", master_info, &jp);
    ldpp_dout(this, 20) << "parsed: objv.tag=" << objv.tag << " objv.ver=" << objv.ver << dendl;
    std::time_t ctime = ceph::real_clock::to_time_t(master_info.creation_time);
    ldpp_dout(this, 20) << "got creation time: << " << std::put_time(std::localtime(&ctime), "%F %T") << dendl;
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
    Span span_3 = child_span("svc_zone.cc : RGWSI_Zone::select_bucket_placement", span_1);
    ret = svc()->zone->select_bucket_placement(u.get_info(),
					    zid, placement_rule,
					    &selected_placement_rule, nullptr);
    finish_trace(span_3);
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
				    pmaster_bucket, pmaster_num_shards, exclusive, span_1);
    if (ret == -EEXIST) {
      *existed = true;
    } else if (ret != 0) {
      return ret;
    }
  }

  bucket->set_version(ep_objv);
  bucket->get_info() = info;

  bucket_out->swap(bucket);

  return ret;
}

} // namespace rgw::sal

rgw::sal::RGWRadosStore *RGWStoreManager::init_storage_provider(CephContext *cct, bool use_gc_thread, bool use_lc_thread, bool quota_threads, bool run_sync_thread, bool run_reshard_thread, bool use_cache)
{
  RGWRados *rados = new RGWRados;
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();

  store->setRados(rados);
  rados->set_store(store);

  if ((*rados).set_use_cache(use_cache)
              .set_run_gc_thread(use_gc_thread)
              .set_run_lc_thread(use_lc_thread)
              .set_run_quota_threads(quota_threads)
              .set_run_sync_thread(run_sync_thread)
              .set_run_reshard_thread(run_reshard_thread)
              .initialize(cct) < 0) {
    delete store;
    return NULL;
  }

  return store;
}

rgw::sal::RGWRadosStore *RGWStoreManager::init_raw_storage_provider(CephContext *cct)
{
  RGWRados *rados = new RGWRados;
  rgw::sal::RGWRadosStore *store = new rgw::sal::RGWRadosStore();

  store->setRados(rados);
  rados->set_store(store);

  rados->set_context(cct);

  int ret = rados->init_svc(true);
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

int rgw::sal::RGWRadosStore::get_obj_head_ioctx(const RGWBucketInfo& bucket_info, const rgw_obj& obj, librados::IoCtx *ioctx)
{
  return rados->get_obj_head_ioctx(bucket_info, obj, ioctx);
}

void RGWStoreManager::close_storage(rgw::sal::RGWRadosStore *store)
{
  if (!store)
    return;

  store->finalize();

  delete store;
}
