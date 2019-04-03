// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include <string>
#include <map>
#include <sstream>

#include <boost/utility/string_ref.hpp>
#include <boost/format.hpp>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/scope_guard.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"

#include "include/types.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "rgw_string.h"
#include "rgw_multi.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#include "include/rados/librados.hpp"
// until everything is moved from rgw_common
#include "rgw_common.h"
#include "rgw_reshard.h"
#include "rgw_lc.h"
#include "cls/user/cls_user_types.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define BUCKET_TAG_TIMEOUT 30


static RGWMetadataHandler *bucket_meta_handler = NULL;
static RGWMetadataHandler *bucket_instance_meta_handler = NULL;

// define as static when RGWBucket implementation completes
void rgw_get_buckets_obj(const rgw_user& user_id, string& buckets_obj_id)
{
  buckets_obj_id = user_id.to_str();
  buckets_obj_id += RGW_BUCKETS_OBJ_SUFFIX;
}

/*
 * Note that this is not a reversal of parse_bucket(). That one deals
 * with the syntax we need in metadata and such. This one deals with
 * the representation in RADOS pools. We chose '/' because it's not
 * acceptable in bucket names and thus qualified buckets cannot conflict
 * with the legacy or S3 buckets.
 */
std::string rgw_make_bucket_entry_name(const std::string& tenant_name,
                                       const std::string& bucket_name) {
  std::string bucket_entry;

  if (bucket_name.empty()) {
    bucket_entry.clear();
  } else if (tenant_name.empty()) {
    bucket_entry = bucket_name;
  } else {
    bucket_entry = tenant_name + "/" + bucket_name;
  }

  return bucket_entry;
}

/*
 * Tenants are separated from buckets in URLs by a colon in S3.
 * This function is not to be used on Swift URLs, not even for COPY arguments.
 */
void rgw_parse_url_bucket(const string &bucket, const string& auth_tenant,
                          string &tenant_name, string &bucket_name) {

  int pos = bucket.find(':');
  if (pos >= 0) {
    /*
     * N.B.: We allow ":bucket" syntax with explicit empty tenant in order
     * to refer to the legacy tenant, in case users in new named tenants
     * want to access old global buckets.
     */
    tenant_name = bucket.substr(0, pos);
    bucket_name = bucket.substr(pos + 1);
  } else {
    tenant_name = auth_tenant;
    bucket_name = bucket;
  }
}

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_read_user_buckets(RGWRados * store,
                          const rgw_user& user_id,
                          RGWUserBuckets& buckets,
                          const string& marker,
                          const string& end_marker,
                          uint64_t max,
                          bool need_stats,
			  bool *is_truncated,
			  uint64_t default_amount)
{
  int ret;
  buckets.clear();
  std::string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_raw_obj obj(store->svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);

  bool truncated = false;
  string m = marker;

  uint64_t total = 0;

  if (!max) {
    max = default_amount;
  }

  do {
    std::list<cls_user_bucket_entry> entries;
    ret = store->cls_user_list_buckets(obj, m, end_marker, max - total, entries, &m, &truncated);
    if (ret == -ENOENT) {
      ret = 0;
    }

    if (ret < 0) {
      return ret;
    }

    for (auto& entry : entries) {
      buckets.add(RGWBucketEnt(user_id, std::move(entry)));
      total++;
    }

  } while (truncated && total < max);

  if (is_truncated != nullptr) {
    *is_truncated = truncated;
  }

  if (need_stats) {
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    ret = store->update_containers_stats(m);
    if (ret < 0 && ret != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: could not get stats for buckets" << dendl;
      return ret;
    }
  }
  return 0;
}

int rgw_bucket_sync_user_stats(RGWRados *store, const rgw_user& user_id, const RGWBucketInfo& bucket_info)
{
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  rgw_raw_obj obj(store->svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);

  return store->cls_user_sync_bucket_stats(obj, bucket_info);
}

int rgw_bucket_sync_user_stats(RGWRados *store, const string& tenant_name, const string& bucket_name)
{
  RGWBucketInfo bucket_info;
  RGWSysObjectCtx obj_ctx = store->svc.sysobj->init_obj_ctx();
  int ret = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, NULL);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: could not fetch bucket info: ret=" << ret << dendl;
    return ret;
  }

  ret = rgw_bucket_sync_user_stats(store, bucket_info.owner, bucket_info);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: could not sync user stats for bucket " << bucket_name << ": ret=" << ret << dendl;
    return ret;
  }

  return 0;
}

int rgw_link_bucket(RGWRados* const store,
                    const rgw_user& user_id,
                    rgw_bucket& bucket,
                    ceph::real_time creation_time,
                    bool update_entrypoint)
{
  int ret;
  string& tenant_name = bucket.tenant;
  string& bucket_name = bucket.name;

  cls_user_bucket_entry new_bucket;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;

  bucket.convert(&new_bucket.bucket);
  new_bucket.size = 0;
  if (real_clock::is_zero(creation_time))
    new_bucket.creation_time = real_clock::now();
  else
    new_bucket.creation_time = creation_time;

  map<string, bufferlist> attrs;
  RGWSysObjectCtx obj_ctx = store->svc.sysobj->init_obj_ctx();

  if (update_entrypoint) {
    ret = store->get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, ep, &ot, NULL, &attrs);
    if (ret < 0 && ret != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: store->get_bucket_entrypoint_info() returned: "
                             << cpp_strerror(-ret) << dendl;
    }
  }

  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);

  rgw_raw_obj obj(store->svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);
  ret = store->cls_user_add_bucket(obj, new_bucket);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: error adding bucket to directory: "
                           << cpp_strerror(-ret) << dendl;
    goto done_err;
  }

  if (!update_entrypoint)
    return 0;

  ep.linked = true;
  ep.owner = user_id;
  ep.bucket = bucket;
  ret = store->put_bucket_entrypoint_info(tenant_name, bucket_name, ep, false, ot, real_time(), &attrs);
  if (ret < 0)
    goto done_err;

  return 0;
done_err:
  int r = rgw_unlink_bucket(store, user_id, bucket.tenant, bucket.name);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed unlinking bucket on error cleanup: "
                           << cpp_strerror(-r) << dendl;
  }
  return ret;
}

int rgw_unlink_bucket(RGWRados *store, const rgw_user& user_id, const string& tenant_name, const string& bucket_name, bool update_entrypoint)
{
  int ret;

  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);

  cls_user_bucket bucket;
  bucket.name = bucket_name;
  rgw_raw_obj obj(store->svc.zone->get_zone_params().user_uid_pool, buckets_obj_id);
  ret = store->cls_user_remove_bucket(obj, bucket);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: error removing bucket from directory: "
        << cpp_strerror(-ret)<< dendl;
  }

  if (!update_entrypoint)
    return 0;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  RGWSysObjectCtx obj_ctx = store->svc.sysobj->init_obj_ctx();
  ret = store->get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, ep, &ot, NULL, &attrs);
  if (ret == -ENOENT)
    return 0;
  if (ret < 0)
    return ret;

  if (!ep.linked)
    return 0;

  if (ep.owner != user_id) {
    ldout(store->ctx(), 0) << "bucket entry point user mismatch, can't unlink bucket: " << ep.owner << " != " << user_id << dendl;
    return -EINVAL;
  }

  ep.linked = false;
  return store->put_bucket_entrypoint_info(tenant_name, bucket_name, ep, false, ot, real_time(), &attrs);
}

int rgw_bucket_store_info(RGWRados *store, const string& bucket_name, bufferlist& bl, bool exclusive,
                          map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker,
                          real_time mtime) {
  return store->meta_mgr->put_entry(bucket_meta_handler, bucket_name, bl, exclusive, objv_tracker, mtime, pattrs);
}

int rgw_bucket_instance_store_info(RGWRados *store, string& entry, bufferlist& bl, bool exclusive,
                          map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker,
                          real_time mtime) {
  return store->meta_mgr->put_entry(bucket_instance_meta_handler, entry, bl, exclusive, objv_tracker, mtime, pattrs);
}

int rgw_bucket_instance_remove_entry(RGWRados *store, const string& entry,
                                     RGWObjVersionTracker *objv_tracker) {
  return store->meta_mgr->remove_entry(bucket_instance_meta_handler, entry, objv_tracker);
}

// 'tenant/' is used in bucket instance keys for sync to avoid parsing ambiguity
// with the existing instance[:shard] format. once we parse the shard, the / is
// replaced with a : to match the [tenant:]instance format
void rgw_bucket_instance_key_to_oid(string& key)
{
  // replace tenant/ with tenant:
  auto c = key.find('/');
  if (c != string::npos) {
    key[c] = ':';
  }
}

// convert bucket instance oids back to the tenant/ format for metadata keys.
// it's safe to parse 'tenant:' only for oids, because they won't contain the
// optional :shard at the end
void rgw_bucket_instance_oid_to_key(string& oid)
{
  // find first : (could be tenant:bucket or bucket:instance)
  auto c = oid.find(':');
  if (c != string::npos) {
    // if we find another :, the first one was for tenant
    if (oid.find(':', c + 1) != string::npos) {
      oid[c] = '/';
    }
  }
}

int rgw_bucket_parse_bucket_instance(const string& bucket_instance, string *target_bucket_instance, int *shard_id)
{
  ssize_t pos = bucket_instance.rfind(':');
  if (pos < 0) {
    return -EINVAL;
  }

  string first = bucket_instance.substr(0, pos);
  string second = bucket_instance.substr(pos + 1);

  if (first.find(':') == string::npos) {
    *shard_id = -1;
    *target_bucket_instance = bucket_instance;
    return 0;
  }

  *target_bucket_instance = first;
  string err;
  *shard_id = strict_strtol(second.c_str(), 10, &err);
  if (!err.empty()) {
    return -EINVAL;
  }

  return 0;
}

// parse key in format: [tenant/]name:instance[:shard_id]
int rgw_bucket_parse_bucket_key(CephContext *cct, const string& key,
                                rgw_bucket *bucket, int *shard_id)
{
  boost::string_ref name{key};
  boost::string_ref instance;

  // split tenant/name
  auto pos = name.find('/');
  if (pos != boost::string_ref::npos) {
    auto tenant = name.substr(0, pos);
    bucket->tenant.assign(tenant.begin(), tenant.end());
    name = name.substr(pos + 1);
  }

  // split name:instance
  pos = name.find(':');
  if (pos != boost::string_ref::npos) {
    instance = name.substr(pos + 1);
    name = name.substr(0, pos);
  }
  bucket->name.assign(name.begin(), name.end());

  // split instance:shard
  pos = instance.find(':');
  if (pos == boost::string_ref::npos) {
    bucket->bucket_id.assign(instance.begin(), instance.end());
    *shard_id = -1;
    return 0;
  }

  // parse shard id
  auto shard = instance.substr(pos + 1);
  string err;
  auto id = strict_strtol(shard.data(), 10, &err);
  if (!err.empty()) {
    ldout(cct, 0) << "ERROR: failed to parse bucket shard '"
        << instance.data() << "': " << err << dendl;
    return -EINVAL;
  }

  *shard_id = id;
  instance = instance.substr(0, pos);
  bucket->bucket_id.assign(instance.begin(), instance.end());
  return 0;
}

int rgw_bucket_set_attrs(RGWRados *store, RGWBucketInfo& bucket_info,
                         map<string, bufferlist>& attrs,
                         RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket& bucket = bucket_info.bucket;

  if (!bucket_info.has_instance_obj) {
    /* an old bucket object, need to convert it */
    RGWSysObjectCtx obj_ctx = store->svc.sysobj->init_obj_ctx();
    int ret = store->convert_old_bucket_info(obj_ctx, bucket.tenant, bucket.name);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed converting old bucket info: " << ret << dendl;
      return ret;
    }
  }

  /* we want the bucket instance name without the oid prefix cruft */
  string key = bucket.get_key();
  bufferlist bl;

  encode(bucket_info, bl);

  return rgw_bucket_instance_store_info(store, key, bl, false, &attrs, objv_tracker, real_time());
}

static void dump_mulipart_index_results(list<rgw_obj_index_key>& objs_to_unlink,
        Formatter *f)
{
  for (const auto& o : objs_to_unlink) {
    f->dump_string("object",  o.name);
  }
}

void check_bad_user_bucket_mapping(RGWRados *store, const rgw_user& user_id,
				   bool fix)
{
  RGWUserBuckets user_buckets;
  bool is_truncated = false;
  string marker;

  CephContext *cct = store->ctx();

  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;

  do {
    int ret = rgw_read_user_buckets(store, user_id, user_buckets, marker,
				    string(), max_entries, false,
				    &is_truncated);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "failed to read user buckets: "
			     << cpp_strerror(-ret) << dendl;
      return;
    }

    map<string, RGWBucketEnt>& buckets = user_buckets.get_buckets();
    for (map<string, RGWBucketEnt>::iterator i = buckets.begin();
         i != buckets.end();
         ++i) {
      marker = i->first;

      RGWBucketEnt& bucket_ent = i->second;
      rgw_bucket& bucket = bucket_ent.bucket;

      RGWBucketInfo bucket_info;
      real_time mtime;
      RGWSysObjectCtx obj_ctx = store->svc.sysobj->init_obj_ctx();
      int r = store->get_bucket_info(obj_ctx, user_id.tenant, bucket.name, bucket_info, &mtime);
      if (r < 0) {
        ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket << dendl;
        continue;
      }

      rgw_bucket& actual_bucket = bucket_info.bucket;

      if (actual_bucket.name.compare(bucket.name) != 0 ||
          actual_bucket.tenant.compare(bucket.tenant) != 0 ||
          actual_bucket.marker.compare(bucket.marker) != 0 ||
          actual_bucket.bucket_id.compare(bucket.bucket_id) != 0) {
        cout << "bucket info mismatch: expected " << actual_bucket << " got " << bucket << std::endl;
        if (fix) {
          cout << "fixing" << std::endl;
          r = rgw_link_bucket(store, user_id, actual_bucket,
                              bucket_info.creation_time);
          if (r < 0) {
            cerr << "failed to fix bucket: " << cpp_strerror(-r) << std::endl;
          }
        }
      }
    }
  } while (is_truncated);
}

static bool bucket_object_check_filter(const string& oid)
{
  rgw_obj_key key;
  string ns;
  return rgw_obj_key::oid_to_key_in_ns(oid, &key, ns);
}

int rgw_remove_object(RGWRados *store, RGWBucketInfo& bucket_info, rgw_bucket& bucket, rgw_obj_key& key)
{
  RGWObjectCtx rctx(store);

  if (key.instance.empty()) {
    key.instance = "null";
  }

  rgw_obj obj(bucket, key);

  return store->delete_obj(rctx, bucket_info, obj, bucket_info.versioning_status());
}

int rgw_remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> stats;
  std::vector<rgw_bucket_dir_entry> objs;
  map<string, bool> common_prefixes;
  RGWBucketInfo info;
  RGWSysObjectCtx obj_ctx = store->svc.sysobj->init_obj_ctx();

  string bucket_ver, master_ver;

  ret = store->get_bucket_info(obj_ctx, bucket.tenant, bucket.name, info, NULL);
  if (ret < 0)
    return ret;

  ret = store->get_bucket_stats(info, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, NULL);
  if (ret < 0)
    return ret;

  RGWRados::Bucket target(store, info);
  RGWRados::Bucket::List list_op(&target);
  CephContext *cct = store->ctx();
  int max = 1000;

  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true;

  bool is_truncated = false;
  do {
    objs.clear();

    ret = list_op.list_objects(max, &objs, &common_prefixes, &is_truncated);
    if (ret < 0)
      return ret;

    if (!objs.empty() && !delete_children) {
      lderr(store->ctx()) << "ERROR: could not remove non-empty bucket " << bucket.name << dendl;
      return -ENOTEMPTY;
    }

    for (const auto& obj : objs) {
      rgw_obj_key key(obj.key);
      ret = rgw_remove_object(store, info, bucket, key);
      if (ret < 0 && ret != -ENOENT) {
        return ret;
      }
    }
  } while(is_truncated);

  string prefix, delimiter;

  ret = abort_bucket_multiparts(store, cct, info, prefix, delimiter);
  if (ret < 0) {
    return ret;
  }

  ret = rgw_bucket_sync_user_stats(store, info.owner, info);
  if ( ret < 0) {
     dout(1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker objv_tracker;

  // if we deleted children above we will force delete, as any that
  // remain is detrius from a prior bug
  ret = store->delete_bucket(info, objv_tracker, !delete_children);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not remove bucket " <<
      bucket.name << dendl;
    return ret;
  }

  ret = rgw_unlink_bucket(store, info.owner, bucket.tenant, bucket.name, false);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: unable to remove user bucket information" << dendl;
  }

  return ret;
}

static int aio_wait(librados::AioCompletion *handle)
{
  librados::AioCompletion *c = (librados::AioCompletion *)handle;
  c->wait_for_safe();
  int ret = c->get_return_value();
  c->release();
  return ret;
}

static int drain_handles(list<librados::AioCompletion *>& pending)
{
  int ret = 0;
  while (!pending.empty()) {
    librados::AioCompletion *handle = pending.front();
    pending.pop_front();
    int r = aio_wait(handle);
    if (r < 0) {
      ret = r;
    }
  }
  return ret;
}

int rgw_remove_bucket_bypass_gc(RGWRados *store, rgw_bucket& bucket,
                                int concurrent_max, bool keep_index_consistent)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> stats;
  std::vector<rgw_bucket_dir_entry> objs;
  map<string, bool> common_prefixes;
  RGWBucketInfo info;
  RGWObjectCtx obj_ctx(store);
  RGWSysObjectCtx sysobj_ctx = store->svc.sysobj->init_obj_ctx();
  CephContext *cct = store->ctx();

  string bucket_ver, master_ver;

  ret = store->get_bucket_info(sysobj_ctx, bucket.tenant, bucket.name, info, NULL);
  if (ret < 0)
    return ret;

  ret = store->get_bucket_stats(info, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, NULL);
  if (ret < 0)
    return ret;

  string prefix, delimiter;

  ret = abort_bucket_multiparts(store, cct, info, prefix, delimiter);
  if (ret < 0) {
    return ret;
  }

  RGWRados::Bucket target(store, info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true;

  std::list<librados::AioCompletion*> handles;

  int max = 1000;
  int max_aio = concurrent_max;
  bool is_truncated = true;

  while (is_truncated) {
    objs.clear();
    ret = list_op.list_objects(max, &objs, &common_prefixes, &is_truncated);
    if (ret < 0)
      return ret;

    std::vector<rgw_bucket_dir_entry>::iterator it = objs.begin();
    for (; it != objs.end(); ++it) {
      RGWObjState *astate = NULL;
      rgw_obj obj(bucket, (*it).key);

      ret = store->get_obj_state(&obj_ctx, info, obj, &astate, false);
      if (ret == -ENOENT) {
        dout(1) << "WARNING: cannot find obj state for obj " << obj.get_oid() << dendl;
        continue;
      }
      if (ret < 0) {
        lderr(store->ctx()) << "ERROR: get obj state returned with error " << ret << dendl;
        return ret;
      }

      if (astate->has_manifest) {
        RGWObjManifest& manifest = astate->manifest;
        RGWObjManifest::obj_iterator miter = manifest.obj_begin();
        rgw_obj head_obj = manifest.get_obj();
        rgw_raw_obj raw_head_obj;
        store->obj_to_raw(info.placement_rule, head_obj, &raw_head_obj);


        for (; miter != manifest.obj_end() && max_aio--; ++miter) {
          if (!max_aio) {
            ret = drain_handles(handles);
            if (ret < 0) {
              lderr(store->ctx()) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
              return ret;
            }
            max_aio = concurrent_max;
          }

          rgw_raw_obj last_obj = miter.get_location().get_raw_obj(store);
          if (last_obj == raw_head_obj) {
            // have the head obj deleted at the end
            continue;
          }

          ret = store->delete_raw_obj_aio(last_obj, handles);
          if (ret < 0) {
            lderr(store->ctx()) << "ERROR: delete obj aio failed with " << ret << dendl;
            return ret;
          }
        } // for all shadow objs

        ret = store->delete_obj_aio(head_obj, info, astate, handles, keep_index_consistent);
        if (ret < 0) {
          lderr(store->ctx()) << "ERROR: delete obj aio failed with " << ret << dendl;
          return ret;
        }
      }

      if (!max_aio) {
        ret = drain_handles(handles);
        if (ret < 0) {
          lderr(store->ctx()) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
          return ret;
        }
        max_aio = concurrent_max;
      }
    } // for all RGW objects
  }

  ret = drain_handles(handles);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
    return ret;
  }

  ret = rgw_bucket_sync_user_stats(store, info.owner, info);
  if (ret < 0) {
     dout(1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker objv_tracker;

  // this function can only be run if caller wanted children to be
  // deleted, so we can ignore the check for children as any that
  // remain are detritus from a prior bug
  ret = store->delete_bucket(info, objv_tracker, false);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not remove bucket " << bucket.name << dendl;
    return ret;
  }

  ret = rgw_unlink_bucket(store, info.owner, bucket.tenant, bucket.name, false);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: unable to remove user bucket information" << dendl;
  }

  return ret;
}

int rgw_bucket_delete_bucket_obj(RGWRados *store,
                                 const string& tenant_name,
                                 const string& bucket_name,
                                 RGWObjVersionTracker& objv_tracker)
{
  string key;

  rgw_make_bucket_entry_name(tenant_name, bucket_name, key);
  return store->meta_mgr->remove_entry(bucket_meta_handler, key, &objv_tracker);
}

static void set_err_msg(std::string *sink, std::string msg)
{
  if (sink && !msg.empty())
    *sink = msg;
}

int RGWBucket::init(RGWRados *storage, RGWBucketAdminOpState& op_state)
{
  if (!storage)
    return -EINVAL;

  store = storage;

  rgw_user user_id = op_state.get_user_id();
  tenant = user_id.tenant;
  bucket_name = op_state.get_bucket_name();
  RGWUserBuckets user_buckets;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  if (bucket_name.empty() && user_id.empty())
    return -EINVAL;

  if (!bucket_name.empty()) {
    int r = store->get_bucket_info(obj_ctx, tenant, bucket_name, bucket_info, NULL);
    if (r < 0) {
      ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket_name << dendl;
      return r;
    }

    op_state.set_bucket(bucket_info.bucket);
  }

  if (!user_id.empty()) {
    int r = rgw_get_user_info_by_uid(store, user_id, user_info);
    if (r < 0)
      return r;

    op_state.display_name = user_info.display_name;
  }

  clear_failure();
  return 0;
}

int RGWBucket::link(RGWBucketAdminOpState& op_state, std::string *err_msg)
{
  if (!op_state.is_user_op()) {
    set_err_msg(err_msg, "empty user id");
    return -EINVAL;
  }

  string bucket_id = op_state.get_bucket_id();
  if (bucket_id.empty()) {
    set_err_msg(err_msg, "empty bucket instance id");
    return -EINVAL;
  }

  std::string display_name = op_state.get_user_display_name();
  rgw_bucket bucket = op_state.get_bucket();

  const rgw_pool& root_pool = store->svc.zone->get_zone_params().domain_root;
  std::string bucket_entry;
  rgw_make_bucket_entry_name(tenant, bucket_name, bucket_entry);
  rgw_raw_obj obj(root_pool, bucket_entry);
  RGWObjVersionTracker objv_tracker;

  map<string, bufferlist> attrs;
  RGWBucketInfo bucket_info;

  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  int r = store->get_bucket_instance_info(obj_ctx, bucket, bucket_info, NULL, &attrs);
  if (r < 0) {
    return r;
  }

  map<string, bufferlist>::iterator aiter = attrs.find(RGW_ATTR_ACL);
  if (aiter != attrs.end()) {
    bufferlist aclbl = aiter->second;
    RGWAccessControlPolicy policy;
    ACLOwner owner;
    try {
     auto iter = aclbl.cbegin();
     decode(policy, iter);
     owner = policy.get_owner();
    } catch (buffer::error& err) {
      set_err_msg(err_msg, "couldn't decode policy");
      return -EIO;
    }

    r = rgw_unlink_bucket(store, owner.get_id(), bucket.tenant, bucket.name, false);
    if (r < 0) {
      set_err_msg(err_msg, "could not unlink policy from user " + owner.get_id().to_str());
      return r;
    }

    // now update the user for the bucket...
    if (display_name.empty()) {
      ldout(store->ctx(), 0) << "WARNING: user " << user_info.user_id << " has no display name set" << dendl;
    }
    policy.create_default(user_info.user_id, display_name);

    owner = policy.get_owner();
    r = store->set_bucket_owner(bucket_info.bucket, owner);
    if (r < 0) {
      set_err_msg(err_msg, "failed to set bucket owner: " + cpp_strerror(-r));
      return r;
    }

    // ...and encode the acl
    aclbl.clear();
    policy.encode(aclbl);

    auto sysobj = obj_ctx.get_obj(obj);
    r = sysobj.wop()
              .set_objv_tracker(&objv_tracker)
              .write_attr(RGW_ATTR_ACL, aclbl, null_yield);
    if (r < 0) {
      return r;
    }

    RGWAccessControlPolicy policy_instance;
    policy_instance.create_default(user_info.user_id, display_name);
    aclbl.clear();
    policy_instance.encode(aclbl);

    rgw_raw_obj obj_bucket_instance;
    store->get_bucket_instance_obj(bucket, obj_bucket_instance);
    auto inst_sysobj = obj_ctx.get_obj(obj_bucket_instance);
    r = inst_sysobj.wop()
                   .set_objv_tracker(&objv_tracker)
                   .write_attr(RGW_ATTR_ACL, aclbl, null_yield);
    if (r < 0) {
      return r;
    }

    r = rgw_link_bucket(store, user_info.user_id, bucket_info.bucket,
                        ceph::real_time());
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

int RGWBucket::unlink(RGWBucketAdminOpState& op_state, std::string *err_msg)
{
  rgw_bucket bucket = op_state.get_bucket();

  if (!op_state.is_user_op()) {
    set_err_msg(err_msg, "could not fetch user or user bucket info");
    return -EINVAL;
  }

  int r = rgw_unlink_bucket(store, user_info.user_id, bucket.tenant, bucket.name);
  if (r < 0) {
    set_err_msg(err_msg, "error unlinking bucket" + cpp_strerror(-r));
  }

  return r;
}

int RGWBucket::set_quota(RGWBucketAdminOpState& op_state, std::string *err_msg)
{
  rgw_bucket bucket = op_state.get_bucket();
  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  int r = store->get_bucket_info(obj_ctx, bucket.tenant, bucket.name, bucket_info, NULL, &attrs);
  if (r < 0) {
    set_err_msg(err_msg, "could not get bucket info for bucket=" + bucket.name + ": " + cpp_strerror(-r));
    return r;
  }

  bucket_info.quota = op_state.quota;
  r = store->put_bucket_instance_info(bucket_info, false, real_time(), &attrs);
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info: " + cpp_strerror(-r));
    return r;
  }
  return r;
}

int RGWBucket::remove(RGWBucketAdminOpState& op_state, bool bypass_gc,
                      bool keep_index_consistent, std::string *err_msg)
{
  bool delete_children = op_state.will_delete_children();
  rgw_bucket bucket = op_state.get_bucket();
  int ret;

  if (bypass_gc) {
    if (delete_children) {
      ret = rgw_remove_bucket_bypass_gc(store, bucket, op_state.get_max_aio(), keep_index_consistent);
    } else {
      set_err_msg(err_msg, "purge objects should be set for gc to be bypassed");
      return -EINVAL;
    }
  } else {
    ret = rgw_remove_bucket(store, bucket, delete_children);
  }

  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove bucket" + cpp_strerror(-ret));
    return ret;
  }

  return 0;
}

int RGWBucket::remove_object(RGWBucketAdminOpState& op_state, std::string *err_msg)
{
  rgw_bucket bucket = op_state.get_bucket();
  std::string object_name = op_state.get_object_name();

  rgw_obj_key key(object_name);

  int ret = rgw_remove_object(store, bucket_info, bucket, key);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove object" + cpp_strerror(-ret));
    return ret;
  }

  return 0;
}

static void dump_bucket_index(map<string, rgw_bucket_dir_entry> result,  Formatter *f)
{
  map<string, rgw_bucket_dir_entry>::iterator iter;
  for (iter = result.begin(); iter != result.end(); ++iter) {
    f->dump_string("object", iter->first);
   }
}

static void dump_bucket_usage(map<RGWObjCategory, RGWStorageStats>& stats, Formatter *formatter)
{
  map<RGWObjCategory, RGWStorageStats>::iterator iter;

  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWStorageStats& s = iter->second;
    const char *cat_name = rgw_obj_category_name(iter->first);
    formatter->open_object_section(cat_name);
    s.dump(formatter);
    formatter->close_section();
  }
  formatter->close_section();
}

static void dump_index_check(map<RGWObjCategory, RGWStorageStats> existing_stats,
        map<RGWObjCategory, RGWStorageStats> calculated_stats,
        Formatter *formatter)
{
  formatter->open_object_section("check_result");
  formatter->open_object_section("existing_header");
  dump_bucket_usage(existing_stats, formatter);
  formatter->close_section();
  formatter->open_object_section("calculated_header");
  dump_bucket_usage(calculated_stats, formatter);
  formatter->close_section();
  formatter->close_section();
}

int RGWBucket::check_bad_index_multipart(RGWBucketAdminOpState& op_state,
               RGWFormatterFlusher& flusher ,std::string *err_msg)
{
  bool fix_index = op_state.will_fix_index();
  rgw_bucket bucket = op_state.get_bucket();

  size_t max = 1000;

  map<string, bool> common_prefixes;

  bool is_truncated;
  map<string, bool> meta_objs;
  map<rgw_obj_index_key, string> all_objs;

  RGWBucketInfo bucket_info;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  int r = store->get_bucket_instance_info(obj_ctx, bucket, bucket_info, nullptr, nullptr);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: " << __func__ << "(): get_bucket_instance_info(bucket=" << bucket << ") returned r=" << r << dendl;
    return r;
  }

  RGWRados::Bucket target(store, bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.list_versions = true;
  list_op.params.ns = RGW_OBJ_NS_MULTIPART;

  do {
    vector<rgw_bucket_dir_entry> result;
    int r = list_op.list_objects(max, &result, &common_prefixes, &is_truncated);
    if (r < 0) {
      set_err_msg(err_msg, "failed to list objects in bucket=" + bucket.name +
              " err=" +  cpp_strerror(-r));

      return r;
    }

    vector<rgw_bucket_dir_entry>::iterator iter;
    for (iter = result.begin(); iter != result.end(); ++iter) {
      rgw_obj_index_key key = iter->key;
      rgw_obj obj(bucket, key);
      string oid = obj.get_oid();

      int pos = oid.find_last_of('.');
      if (pos < 0) {
        /* obj has no suffix */
        all_objs[key] = oid;
      } else {
        /* obj has suffix */
        string name = oid.substr(0, pos);
        string suffix = oid.substr(pos + 1);

        if (suffix.compare("meta") == 0) {
          meta_objs[name] = true;
        } else {
          all_objs[key] = name;
        }
      }
    }

  } while (is_truncated);

  list<rgw_obj_index_key> objs_to_unlink;
  Formatter *f =  flusher.get_formatter();

  f->open_array_section("invalid_multipart_entries");

  for (auto aiter = all_objs.begin(); aiter != all_objs.end(); ++aiter) {
    string& name = aiter->second;

    if (meta_objs.find(name) == meta_objs.end()) {
      objs_to_unlink.push_back(aiter->first);
    }

    if (objs_to_unlink.size() > max) {
      if (fix_index) {
	int r = store->remove_objs_from_index(bucket_info, objs_to_unlink);
	if (r < 0) {
	  set_err_msg(err_msg, "ERROR: remove_obj_from_index() returned error: " +
		      cpp_strerror(-r));
	  return r;
	}
      }

      dump_mulipart_index_results(objs_to_unlink, flusher.get_formatter());
      flusher.flush();
      objs_to_unlink.clear();
    }
  }

  if (fix_index) {
    int r = store->remove_objs_from_index(bucket_info, objs_to_unlink);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: remove_obj_from_index() returned error: " +
              cpp_strerror(-r));

      return r;
    }
  }

  dump_mulipart_index_results(objs_to_unlink, f);
  f->close_section();
  flusher.flush();

  return 0;
}

int RGWBucket::check_object_index(RGWBucketAdminOpState& op_state,
                                  RGWFormatterFlusher& flusher,
                                  std::string *err_msg)
{

  bool fix_index = op_state.will_fix_index();

  if (!fix_index) {
    set_err_msg(err_msg, "check-objects flag requires fix index enabled");
    return -EINVAL;
  }

  store->cls_obj_set_bucket_tag_timeout(bucket_info, BUCKET_TAG_TIMEOUT);

  string prefix;
  rgw_obj_index_key marker;
  bool is_truncated = true;

  Formatter *formatter = flusher.get_formatter();
  formatter->open_object_section("objects");
  while (is_truncated) {
    map<string, rgw_bucket_dir_entry> result;

    int r = store->cls_bucket_list_ordered(bucket_info, RGW_NO_SHARD,
					   marker, prefix, 1000, true,
					   result, &is_truncated, &marker,
					   bucket_object_check_filter);
    if (r == -ENOENT) {
      break;
    } else if (r < 0 && r != -ENOENT) {
      set_err_msg(err_msg, "ERROR: failed operation r=" + cpp_strerror(-r));
    }

    dump_bucket_index(result, formatter);
    flusher.flush();
  }

  formatter->close_section();

  store->cls_obj_set_bucket_tag_timeout(bucket_info, 0);

  return 0;
}


int RGWBucket::check_index(RGWBucketAdminOpState& op_state,
        map<RGWObjCategory, RGWStorageStats>& existing_stats,
        map<RGWObjCategory, RGWStorageStats>& calculated_stats,
        std::string *err_msg)
{
  bool fix_index = op_state.will_fix_index();

  int r = store->bucket_check_index(bucket_info, &existing_stats, &calculated_stats);
  if (r < 0) {
    set_err_msg(err_msg, "failed to check index error=" + cpp_strerror(-r));
    return r;
  }

  if (fix_index) {
    r = store->bucket_rebuild_index(bucket_info);
    if (r < 0) {
      set_err_msg(err_msg, "failed to rebuild index err=" + cpp_strerror(-r));
      return r;
    }
  }

  return 0;
}


int RGWBucket::policy_bl_to_stream(bufferlist& bl, ostream& o)
{
  RGWAccessControlPolicy_S3 policy(g_ceph_context);
  auto iter = bl.cbegin();
  try {
    policy.decode(iter);
  } catch (buffer::error& err) {
    dout(0) << "ERROR: caught buffer::error, could not decode policy" << dendl;
    return -EIO;
  }
  policy.to_xml(o);
  return 0;
}

static int policy_decode(RGWRados *store, bufferlist& bl, RGWAccessControlPolicy& policy)
{
  auto iter = bl.cbegin();
  try {
    policy.decode(iter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: caught buffer::error, could not decode policy" << dendl;
    return -EIO;
  }
  return 0;
}

int RGWBucket::get_policy(RGWBucketAdminOpState& op_state, RGWAccessControlPolicy& policy)
{
  std::string object_name = op_state.get_object_name();
  rgw_bucket bucket = op_state.get_bucket();
  auto sysobj_ctx = store->svc.sysobj->init_obj_ctx();
  RGWObjectCtx obj_ctx(store);

  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
  int ret = store->get_bucket_info(sysobj_ctx, bucket.tenant, bucket.name, bucket_info, NULL, &attrs);
  if (ret < 0) {
    return ret;
  }

  if (!object_name.empty()) {
    bufferlist bl;
    rgw_obj obj(bucket, object_name);

    RGWRados::Object op_target(store, bucket_info, obj_ctx, obj);
    RGWRados::Object::Read rop(&op_target);

    int ret = rop.get_attr(RGW_ATTR_ACL, bl);
    if (ret < 0)
      return ret;

    return policy_decode(store, bl, policy);
  }

  map<string, bufferlist>::iterator aiter = attrs.find(RGW_ATTR_ACL);
  if (aiter == attrs.end()) {
    return -ENOENT;
  }

  return policy_decode(store, aiter->second, policy);
}


int RGWBucketAdminOp::get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  ret = bucket.get_policy(op_state, policy);
  if (ret < 0)
    return ret;

  return 0;
}

/* Wrappers to facilitate RESTful interface */


int RGWBucketAdminOp::get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWAccessControlPolicy policy(store->ctx());

  int ret = get_policy(store, op_state, policy);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  flusher.start(0);

  formatter->open_object_section("policy");
  policy.dump(formatter);
  formatter->close_section();

  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::dump_s3_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  ostream& os)
{
  RGWAccessControlPolicy_S3 policy(store->ctx());

  int ret = get_policy(store, op_state, policy);
  if (ret < 0)
    return ret;

  policy.to_xml(os);

  return 0;
}

int RGWBucketAdminOp::unlink(RGWRados *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  return bucket.unlink(op_state);
}

int RGWBucketAdminOp::link(RGWRados *store, RGWBucketAdminOpState& op_state, string *err)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  return bucket.link(op_state, err);

}

int RGWBucketAdminOp::check_index(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> existing_stats;
  map<RGWObjCategory, RGWStorageStats> calculated_stats;


  RGWBucket bucket;

  ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  ret = bucket.check_bad_index_multipart(op_state, flusher);
  if (ret < 0)
    return ret;

  ret = bucket.check_object_index(op_state, flusher);
  if (ret < 0)
    return ret;

  ret = bucket.check_index(op_state, existing_stats, calculated_stats);
  if (ret < 0)
    return ret;

  dump_index_check(existing_stats, calculated_stats, formatter);
  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::remove_bucket(RGWRados *store, RGWBucketAdminOpState& op_state,
                                    bool bypass_gc, bool keep_index_consistent)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  std::string err_msg;
  ret = bucket.remove(op_state, bypass_gc, keep_index_consistent, &err_msg);
  if (!err_msg.empty()) {
    lderr(store->ctx()) << "ERROR: " << err_msg << dendl;
  }
  return ret;
}

int RGWBucketAdminOp::remove_object(RGWRados *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  return bucket.remove_object(op_state);
}

static int bucket_stats(RGWRados *store, const std::string& tenant_name, std::string&  bucket_name, Formatter *formatter)
{
  RGWBucketInfo bucket_info;
  map<RGWObjCategory, RGWStorageStats> stats;

  real_time mtime;
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  int r = store->get_bucket_info(obj_ctx, tenant_name, bucket_name, bucket_info, &mtime);
  if (r < 0)
    return r;

  rgw_bucket& bucket = bucket_info.bucket;

  string bucket_ver, master_ver;
  string max_marker;
  int ret = store->get_bucket_stats(bucket_info, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0) {
    cerr << "error getting bucket stats ret=" << ret << std::endl;
    return ret;
  }

  utime_t ut(mtime);

  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket.name);
  formatter->dump_string("tenant", bucket.tenant);
  formatter->dump_string("zonegroup", bucket_info.zonegroup);
  formatter->dump_string("placement_rule", bucket_info.placement_rule.to_str());
  ::encode_json("explicit_placement", bucket.explicit_placement, formatter);
  formatter->dump_string("id", bucket.bucket_id);
  formatter->dump_string("marker", bucket.marker);
  formatter->dump_stream("index_type") << bucket_info.index_type;
  ::encode_json("owner", bucket_info.owner, formatter);
  formatter->dump_string("ver", bucket_ver);
  formatter->dump_string("master_ver", master_ver);
  formatter->dump_stream("mtime") << ut;
  formatter->dump_string("max_marker", max_marker);
  dump_bucket_usage(stats, formatter);
  encode_json("bucket_quota", bucket_info.quota, formatter);
  formatter->close_section();

  return 0;
}

int RGWBucketAdminOp::limit_check(RGWRados *store,
				  RGWBucketAdminOpState& op_state,
				  const std::list<std::string>& user_ids,
				  RGWFormatterFlusher& flusher,
				  bool warnings_only)
{
  int ret = 0;
  const size_t max_entries =
    store->ctx()->_conf->rgw_list_buckets_max_chunk;

  const size_t safe_max_objs_per_shard =
    store->ctx()->_conf->rgw_safe_max_objects_per_shard;

  uint16_t shard_warn_pct =
    store->ctx()->_conf->rgw_shard_warning_threshold;
  if (shard_warn_pct > 100)
    shard_warn_pct = 90;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  formatter->open_array_section("users");

  for (const auto& user_id : user_ids) {

    formatter->open_object_section("user");
    formatter->dump_string("user_id", user_id);
    formatter->open_array_section("buckets");

    string marker;
    bool is_truncated{false};
    do {
      RGWUserBuckets buckets;

      ret = rgw_read_user_buckets(store, user_id, buckets,
				  marker, string(), max_entries, false,
				  &is_truncated);
      if (ret < 0)
        return ret;

      map<string, RGWBucketEnt>& m_buckets = buckets.get_buckets();

      for (const auto& iter : m_buckets) {
	auto& bucket = iter.second.bucket;
	uint32_t num_shards = 1;
	uint64_t num_objects = 0;

	/* need info for num_shards */
	RGWBucketInfo info;
	auto obj_ctx = store->svc.sysobj->init_obj_ctx();

	marker = bucket.name; /* Casey's location for marker update,
			       * as we may now not reach the end of
			       * the loop body */

	ret = store->get_bucket_info(obj_ctx, bucket.tenant, bucket.name,
				     info, nullptr);
	if (ret < 0)
	  continue;

	/* need stats for num_entries */
	string bucket_ver, master_ver;
	std::map<RGWObjCategory, RGWStorageStats> stats;
	ret = store->get_bucket_stats(info, RGW_NO_SHARD, &bucket_ver,
				      &master_ver, stats, nullptr);

	if (ret < 0)
	  continue;

	for (const auto& s : stats) {
	    num_objects += s.second.num_objects;
	}

	num_shards = info.num_shards;
	uint64_t objs_per_shard =
	  (num_shards) ? num_objects/num_shards : num_objects;
	{
	  bool warn = false;
	  stringstream ss;
	  if (objs_per_shard > safe_max_objs_per_shard) {
	    double over =
	      100 - (safe_max_objs_per_shard/objs_per_shard * 100);
	      ss << boost::format("OVER %4f%%") % over;
	      warn = true;
	  } else {
	    double fill_pct =
	      objs_per_shard / safe_max_objs_per_shard * 100;
	    if (fill_pct >= shard_warn_pct) {
	      ss << boost::format("WARN %4f%%") % fill_pct;
	      warn = true;
	    } else {
	      ss << "OK";
	    }
	  }

	  if (warn || (! warnings_only)) {
	    formatter->open_object_section("bucket");
	    formatter->dump_string("bucket", bucket.name);
	    formatter->dump_string("tenant", bucket.tenant);
	    formatter->dump_int("num_objects", num_objects);
	    formatter->dump_int("num_shards", num_shards);
	    formatter->dump_int("objects_per_shard", objs_per_shard);
	    formatter->dump_string("fill_status", ss.str());
	    formatter->close_section();
	  }
	}
      }
      formatter->flush(cout);
    } while (is_truncated); /* foreach: bucket */

    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);

  } /* foreach: user_id */

  formatter->close_section();
  formatter->flush(cout);

  return ret;
} /* RGWBucketAdminOp::limit_check */

int RGWBucketAdminOp::info(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  int ret = 0;
  string bucket_name = op_state.get_bucket_name();
  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  CephContext *cct = store->ctx();

  const size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;

  bool show_stats = op_state.will_fetch_stats();
  rgw_user user_id = op_state.get_user_id();
  if (op_state.is_user_op()) {
    formatter->open_array_section("buckets");

    RGWUserBuckets buckets;
    string marker;
    bool is_truncated = false;

    do {
      ret = rgw_read_user_buckets(store, op_state.get_user_id(), buckets,
				  marker, string(), max_entries, false,
				  &is_truncated);
      if (ret < 0)
        return ret;

      map<string, RGWBucketEnt>& m = buckets.get_buckets();
      map<string, RGWBucketEnt>::iterator iter;

      for (iter = m.begin(); iter != m.end(); ++iter) {
        std::string obj_name = iter->first;
        if (!bucket_name.empty() && bucket_name != obj_name) {
          continue;
        }

        if (show_stats)
          bucket_stats(store, user_id.tenant, obj_name, formatter);
        else
          formatter->dump_string("bucket", obj_name);

        marker = obj_name;
      }

      flusher.flush();
    } while (is_truncated);

    formatter->close_section();
  } else if (!bucket_name.empty()) {
    ret = bucket_stats(store, user_id.tenant, bucket_name, formatter);
    if (ret < 0) {
      return ret;
    }
  } else {
    void *handle = nullptr;
    bool truncated = true;

    formatter->open_array_section("buckets");
    ret = store->meta_mgr->list_keys_init("bucket", &handle);
    while (ret == 0 && truncated) {
      std::list<std::string> buckets;
      const int max_keys = 1000;
      ret = store->meta_mgr->list_keys_next(handle, max_keys, buckets,
                                            &truncated);
      for (auto& bucket_name : buckets) {
        if (show_stats)
          bucket_stats(store, user_id.tenant, bucket_name, formatter);
        else
          formatter->dump_string("bucket", bucket_name);
      }
    }

    formatter->close_section();
  }

  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::set_quota(RGWRados *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;
  return bucket.set_quota(op_state);
}

static int purge_bucket_instance(RGWRados *store, const RGWBucketInfo& bucket_info)
{
  int max_shards = (bucket_info.num_shards > 0 ? bucket_info.num_shards : 1);
  for (int i = 0; i < max_shards; i++) {
    RGWRados::BucketShard bs(store);
    int shard_id = (bucket_info.num_shards > 0  ? i : -1);
    int ret = bs.init(bucket_info.bucket, shard_id, nullptr);
    if (ret < 0) {
      cerr << "ERROR: bs.init(bucket=" << bucket_info.bucket << ", shard=" << shard_id
           << "): " << cpp_strerror(-ret) << std::endl;
      return ret;
    }
    ret = store->bi_remove(bs);
    if (ret < 0) {
      cerr << "ERROR: failed to remove bucket index object: "
           << cpp_strerror(-ret) << std::endl;
      return ret;
    }
  }
  return 0;
}

inline auto split_tenant(const std::string& bucket_name){
  auto p = bucket_name.find('/');
  if(p != std::string::npos) {
    return std::make_pair(bucket_name.substr(0,p), bucket_name.substr(p+1));
  }
  return std::make_pair(std::string(), bucket_name);
}

using bucket_instance_ls = std::vector<RGWBucketInfo>;
void get_stale_instances(RGWRados *store, const std::string& bucket_name,
                         const vector<std::string>& lst,
                         bucket_instance_ls& stale_instances)
{

  auto obj_ctx = store->svc.sysobj->init_obj_ctx();

  bucket_instance_ls other_instances;
// first iterate over the entries, and pick up the done buckets; these
// are guaranteed to be stale
  for (const auto& bucket_instance : lst){
    RGWBucketInfo binfo;
    int r = store->get_bucket_instance_info(obj_ctx, bucket_instance,
                                            binfo, nullptr,nullptr);
    if (r < 0){
      // this can only happen if someone deletes us right when we're processing
      lderr(store->ctx()) << "Bucket instance is invalid: " << bucket_instance
                          << cpp_strerror(-r) << dendl;
      continue;
    }
    if (binfo.reshard_status == CLS_RGW_RESHARD_DONE)
      stale_instances.emplace_back(std::move(binfo));
    else {
      other_instances.emplace_back(std::move(binfo));
    }
  }

  // Read the cur bucket info, if the bucket doesn't exist we can simply return
  // all the instances
  auto [tenant, bucket] = split_tenant(bucket_name);
  RGWBucketInfo cur_bucket_info;
  int r = store->get_bucket_info(obj_ctx, tenant, bucket, cur_bucket_info, nullptr);
  if (r < 0) {
    if (r == -ENOENT) {
      // bucket doesn't exist, everything is stale then
      stale_instances.insert(std::end(stale_instances),
                             std::make_move_iterator(other_instances.begin()),
                             std::make_move_iterator(other_instances.end()));
    } else {
      // all bets are off if we can't read the bucket, just return the sureshot stale instances
      lderr(store->ctx()) << "error: reading bucket info for bucket: "
                          << bucket << cpp_strerror(-r) << dendl;
    }
    return;
  }

  // Don't process further in this round if bucket is resharding
  if (cur_bucket_info.reshard_status == CLS_RGW_RESHARD_IN_PROGRESS)
    return;

  other_instances.erase(std::remove_if(other_instances.begin(), other_instances.end(),
                                       [&cur_bucket_info](const RGWBucketInfo& b){
                                         return (b.bucket.bucket_id == cur_bucket_info.bucket.bucket_id ||
                                                 b.bucket.bucket_id == cur_bucket_info.new_bucket_instance_id);
                                       }),
                        other_instances.end());

  // check if there are still instances left
  if (other_instances.empty()) {
    return;
  }

  // Now we have a bucket with instances where the reshard status is none, this
  // usually happens when the reshard process couldn't complete, lockdown the
  // bucket and walk through these instances to make sure no one else interferes
  // with these
  {
    RGWBucketReshardLock reshard_lock(store, cur_bucket_info, true);
    r = reshard_lock.lock();
    if (r < 0) {
      // most likely bucket is under reshard, return the sureshot stale instances
      ldout(store->ctx(), 5) << __func__
                             << "failed to take reshard lock; reshard underway likey" << dendl;
      return;
    }
    auto sg = make_scope_guard([&reshard_lock](){ reshard_lock.unlock();} );
    // this should be fast enough that we may not need to renew locks and check
    // exit status?, should we read the values of the instances again?
    stale_instances.insert(std::end(stale_instances),
                           std::make_move_iterator(other_instances.begin()),
                           std::make_move_iterator(other_instances.end()));
  }

  return;
}

static int process_stale_instances(RGWRados *store, RGWBucketAdminOpState& op_state,
                                   RGWFormatterFlusher& flusher,
                                   std::function<void(const bucket_instance_ls&,
                                                      Formatter *,
                                                      RGWRados*)> process_f)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  int ret = store->meta_mgr->list_keys_init("bucket.instance", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  bool truncated;

  formatter->open_array_section("keys");

  do {
    list<std::string> keys;

    ret = store->meta_mgr->list_keys_next(handle, default_max_keys, keys, &truncated);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
      return ret;
    } if (ret != -ENOENT) {
      // partition the list of buckets by buckets as the listing is un sorted,
      // since it would minimize the reads to bucket_info
      std::unordered_map<std::string, std::vector<std::string>> bucket_instance_map;
      for (auto &key: keys) {
        auto pos = key.find(':');
        if(pos != std::string::npos)
          bucket_instance_map[key.substr(0,pos)].emplace_back(std::move(key));
      }
      for (const auto& kv: bucket_instance_map) {
        bucket_instance_ls stale_lst;
        get_stale_instances(store, kv.first, kv.second, stale_lst);
        process_f(stale_lst, formatter, store);
      }
    }
  } while (truncated);

  formatter->close_section(); // keys
  formatter->flush(cout);
  return 0;
}

int RGWBucketAdminOp::list_stale_instances(RGWRados *store,
                                           RGWBucketAdminOpState& op_state,
                                           RGWFormatterFlusher& flusher)
{
  auto process_f = [](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      RGWRados*){
                     for (const auto& binfo: lst)
                       formatter->dump_string("key", binfo.bucket.get_key());
                   };
  return process_stale_instances(store, op_state, flusher, process_f);
}


int RGWBucketAdminOp::clear_stale_instances(RGWRados *store,
                                            RGWBucketAdminOpState& op_state,
                                            RGWFormatterFlusher& flusher)
{
  auto process_f = [](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      RGWRados *store){
                     for (const auto &binfo: lst) {
                       int ret = purge_bucket_instance(store, binfo);
                       if (ret == 0){
                         auto md_key = "bucket.instance:" + binfo.bucket.get_key();
                         ret = store->meta_mgr->remove(md_key);
                       }
                       formatter->open_object_section("delete_status");
                       formatter->dump_string("bucket_instance", binfo.bucket.get_key());
                       formatter->dump_int("status", -ret);
                       formatter->close_section();
                     }
                   };

  return process_stale_instances(store, op_state, flusher, process_f);
}

static int fix_single_bucket_lc(RGWRados *store,
                                const std::string& tenant_name,
                                const std::string& bucket_name)
{
  auto obj_ctx = store->svc.sysobj->init_obj_ctx();
  RGWBucketInfo bucket_info;
  map <std::string, bufferlist> bucket_attrs;
  int ret = store->get_bucket_info(obj_ctx, tenant_name, bucket_name,
                                   bucket_info, nullptr, &bucket_attrs);
  if (ret < 0) {
    // TODO: Should we handle the case where the bucket could've been removed between
    // listing and fetching?
    return ret;
  }

  return rgw::lc::fix_lc_shard_entry(store, bucket_info, bucket_attrs);
}

static void format_lc_status(Formatter* formatter,
                             const std::string& tenant_name,
                             const std::string& bucket_name,
                             int status)
{
  formatter->open_object_section("bucket_entry");
  std::string entry = tenant_name.empty() ? bucket_name : tenant_name + "/" + bucket_name;
  formatter->dump_string("bucket", entry);
  formatter->dump_int("status", status);
  formatter->close_section(); // bucket_entry
}

static void process_single_lc_entry(RGWRados *store, Formatter *formatter,
                                    const std::string& tenant_name,
                                    const std::string& bucket_name)
{
  int ret = fix_single_bucket_lc(store, tenant_name, bucket_name);
  format_lc_status(formatter, tenant_name, bucket_name, -ret);
}

int RGWBucketAdminOp::fix_lc_shards(RGWRados *store,
                                    RGWBucketAdminOpState& op_state,
                                    RGWFormatterFlusher& flusher)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  bool truncated;
  if (const std::string& bucket_name = op_state.get_bucket_name();
      ! bucket_name.empty()) {
    const rgw_user user_id = op_state.get_user_id();
    process_single_lc_entry(store, formatter, user_id.tenant, bucket_name);
    formatter->flush(cout);
  } else {
    int ret = store->meta_mgr->list_keys_init("bucket", marker, &handle);
    if (ret < 0) {
      std::cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    {
      formatter->open_array_section("lc_fix_status");
      auto sg = make_scope_guard([&store, &handle, &formatter](){
                                   store->meta_mgr->list_keys_complete(handle);
                                   formatter->close_section(); // lc_fix_status
                                   formatter->flush(cout);
                                 });
      do {
        list<std::string> keys;
        ret = store->meta_mgr->list_keys_next(handle, default_max_keys, keys, &truncated);
        if (ret < 0 && ret != -ENOENT) {
          std::cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
          return ret;
        } if (ret != -ENOENT) {
          for (const auto &key:keys) {
            auto [tenant_name, bucket_name] = split_tenant(key);
            process_single_lc_entry(store, formatter, tenant_name, bucket_name);
          }
        }
        formatter->flush(cout); // regularly flush every 1k entries
      } while (truncated);
    }

  }
  return 0;

}

void rgw_data_change::dump(Formatter *f) const
{
  string type;
  switch (entity_type) {
    case ENTITY_TYPE_BUCKET:
      type = "bucket";
      break;
    default:
      type = "unknown";
  }
  encode_json("entity_type", type, f);
  encode_json("key", key, f);
  utime_t ut(timestamp);
  encode_json("timestamp", ut, f);
}

void rgw_data_change::decode_json(JSONObj *obj) {
  string s;
  JSONDecoder::decode_json("entity_type", s, obj);
  if (s == "bucket") {
    entity_type = ENTITY_TYPE_BUCKET;
  } else {
    entity_type = ENTITY_TYPE_UNKNOWN;
  }
  JSONDecoder::decode_json("key", key, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
}

void rgw_data_change_log_entry::dump(Formatter *f) const
{
  encode_json("log_id", log_id, f);
  utime_t ut(log_timestamp);
  encode_json("log_timestamp", ut, f);
  encode_json("entry", entry, f);
}

void rgw_data_change_log_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("log_id", log_id, obj);
  utime_t ut;
  JSONDecoder::decode_json("log_timestamp", ut, obj);
  log_timestamp = ut.to_real_time();
  JSONDecoder::decode_json("entry", entry, obj);
}

int RGWDataChangesLog::choose_oid(const rgw_bucket_shard& bs) {
    const string& name = bs.bucket.name;
    int shard_shift = (bs.shard_id > 0 ? bs.shard_id : 0);
    uint32_t r = (ceph_str_hash_linux(name.c_str(), name.size()) + shard_shift) % num_shards;

    return (int)r;
}

int RGWDataChangesLog::renew_entries()
{
  if (!store->svc.zone->need_to_log_data())
    return 0;

  /* we can't keep the bucket name as part of the cls_log_entry, and we need
   * it later, so we keep two lists under the map */
  map<int, pair<list<rgw_bucket_shard>, list<cls_log_entry> > > m;

  lock.Lock();
  map<rgw_bucket_shard, bool> entries;
  entries.swap(cur_cycle);
  lock.Unlock();

  map<rgw_bucket_shard, bool>::iterator iter;
  string section;
  real_time ut = real_clock::now();
  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    const rgw_bucket_shard& bs = iter->first;

    int index = choose_oid(bs);

    cls_log_entry entry;

    rgw_data_change change;
    bufferlist bl;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = ut;
    encode(change, bl);

    store->time_log_prepare_entry(entry, ut, section, change.key, bl);

    m[index].first.push_back(bs);
    m[index].second.emplace_back(std::move(entry));
  }

  map<int, pair<list<rgw_bucket_shard>, list<cls_log_entry> > >::iterator miter;
  for (miter = m.begin(); miter != m.end(); ++miter) {
    list<cls_log_entry>& entries = miter->second.second;

    real_time now = real_clock::now();

    int ret = store->time_log_add(oids[miter->first], entries, NULL);
    if (ret < 0) {
      /* we don't really need to have a special handling for failed cases here,
       * as this is just an optimization. */
      lderr(cct) << "ERROR: store->time_log_add() returned " << ret << dendl;
      return ret;
    }

    real_time expiration = now;
    expiration += make_timespan(cct->_conf->rgw_data_log_window);

    list<rgw_bucket_shard>& buckets = miter->second.first;
    list<rgw_bucket_shard>::iterator liter;
    for (liter = buckets.begin(); liter != buckets.end(); ++liter) {
      update_renewed(*liter, expiration);
    }
  }

  return 0;
}

void RGWDataChangesLog::_get_change(const rgw_bucket_shard& bs, ChangeStatusPtr& status)
{
  ceph_assert(lock.is_locked());
  if (!changes.find(bs, status)) {
    status = ChangeStatusPtr(new ChangeStatus);
    changes.add(bs, status);
  }
}

void RGWDataChangesLog::register_renew(rgw_bucket_shard& bs)
{
  Mutex::Locker l(lock);
  cur_cycle[bs] = true;
}

void RGWDataChangesLog::update_renewed(rgw_bucket_shard& bs, real_time& expiration)
{
  Mutex::Locker l(lock);
  ChangeStatusPtr status;
  _get_change(bs, status);

  ldout(cct, 20) << "RGWDataChangesLog::update_renewd() bucket_name=" << bs.bucket.name << " shard_id=" << bs.shard_id << " expiration=" << expiration << dendl;
  status->cur_expiration = expiration;
}

int RGWDataChangesLog::get_log_shard_id(rgw_bucket& bucket, int shard_id) {
  rgw_bucket_shard bs(bucket, shard_id);

  return choose_oid(bs);
}

int RGWDataChangesLog::add_entry(rgw_bucket& bucket, int shard_id) {
  if (!store->svc.zone->need_to_log_data())
    return 0;

  if (observer) {
    observer->on_bucket_changed(bucket.get_key());
  }

  rgw_bucket_shard bs(bucket, shard_id);

  int index = choose_oid(bs);
  mark_modified(index, bs);

  lock.Lock();

  ChangeStatusPtr status;
  _get_change(bs, status);

  lock.Unlock();

  real_time now = real_clock::now();

  status->lock->Lock();

  ldout(cct, 20) << "RGWDataChangesLog::add_entry() bucket.name=" << bucket.name << " shard_id=" << shard_id << " now=" << now << " cur_expiration=" << status->cur_expiration << dendl;

  if (now < status->cur_expiration) {
    /* no need to send, recently completed */
    status->lock->Unlock();

    register_renew(bs);
    return 0;
  }

  RefCountedCond *cond;

  if (status->pending) {
    cond = status->cond;

    ceph_assert(cond);

    status->cond->get();
    status->lock->Unlock();

    int ret = cond->wait();
    cond->put();
    if (!ret) {
      register_renew(bs);
    }
    return ret;
  }

  status->cond = new RefCountedCond;
  status->pending = true;

  string& oid = oids[index];
  real_time expiration;

  int ret;

  do {
    status->cur_sent = now;

    expiration = now;
    expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);

    status->lock->Unlock();
  
    bufferlist bl;
    rgw_data_change change;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = now;
    encode(change, bl);
    string section;

    ldout(cct, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now << " cur_expiration=" << expiration << dendl;

    ret = store->time_log_add(oid, now, section, change.key, bl);

    now = real_clock::now();

    status->lock->Lock();

  } while (!ret && real_clock::now() > expiration);

  cond = status->cond;

  status->pending = false;
  status->cur_expiration = status->cur_sent; /* time of when operation started, not completed */
  status->cur_expiration += make_timespan(cct->_conf->rgw_data_log_window);
  status->cond = NULL;
  status->lock->Unlock();

  cond->done(ret);
  cond->put();

  return ret;
}

int RGWDataChangesLog::list_entries(int shard, const real_time& start_time, const real_time& end_time, int max_entries,
				    list<rgw_data_change_log_entry>& entries,
				    const string& marker,
				    string *out_marker,
				    bool *truncated) {
  if (shard >= num_shards)
    return -EINVAL;

  list<cls_log_entry> log_entries;

  int ret = store->time_log_list(oids[shard], start_time, end_time,
				 max_entries, log_entries, marker,
				 out_marker, truncated);
  if (ret < 0)
    return ret;

  list<cls_log_entry>::iterator iter;
  for (iter = log_entries.begin(); iter != log_entries.end(); ++iter) {
    rgw_data_change_log_entry log_entry;
    log_entry.log_id = iter->id;
    real_time rt = iter->timestamp.to_real_time();
    log_entry.log_timestamp = rt;
    auto liter = iter->data.cbegin();
    try {
      decode(log_entry.entry, liter);
    } catch (buffer::error& err) {
      lderr(cct) << "ERROR: failed to decode data changes log entry" << dendl;
      return -EIO;
    }
    entries.push_back(log_entry);
  }

  return 0;
}

int RGWDataChangesLog::list_entries(const real_time& start_time, const real_time& end_time, int max_entries,
             list<rgw_data_change_log_entry>& entries, LogMarker& marker, bool *ptruncated) {
  bool truncated;
  entries.clear();

  for (; marker.shard < num_shards && (int)entries.size() < max_entries;
       marker.shard++, marker.marker.clear()) {
    int ret = list_entries(marker.shard, start_time, end_time, max_entries - entries.size(), entries,
			   marker.marker, NULL, &truncated);
    if (ret == -ENOENT) {
      continue;
    }
    if (ret < 0) {
      return ret;
    }
    if (truncated) {
      *ptruncated = true;
      return 0;
    }
  }

  *ptruncated = (marker.shard < num_shards);

  return 0;
}

int RGWDataChangesLog::get_info(int shard_id, RGWDataChangesLogInfo *info)
{
  if (shard_id >= num_shards)
    return -EINVAL;

  string oid = oids[shard_id];

  cls_log_header header;

  int ret = store->time_log_info(oid, &header);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  info->marker = header.max_marker;
  info->last_update = header.max_time.to_real_time();

  return 0;
}

int RGWDataChangesLog::trim_entries(int shard_id, const real_time& start_time, const real_time& end_time,
                                    const string& start_marker, const string& end_marker)
{
  int ret;

  if (shard_id > num_shards)
    return -EINVAL;

  ret = store->time_log_trim(oids[shard_id], start_time, end_time, start_marker, end_marker);

  if (ret == -ENOENT || ret == -ENODATA)
    ret = 0;

  return ret;
}

int RGWDataChangesLog::trim_entries(const real_time& start_time, const real_time& end_time,
                                    const string& start_marker, const string& end_marker)
{
  for (int shard = 0; shard < num_shards; shard++) {
    int ret = store->time_log_trim(oids[shard], start_time, end_time, start_marker, end_marker);
    if (ret == -ENOENT || ret == -ENODATA) {
      continue;
    }
    if (ret < 0)
      return ret;
  }

  return 0;
}

int RGWDataChangesLog::lock_exclusive(int shard_id, timespan duration, string& zone_id, string& owner_id) {
  return store->lock_exclusive(store->svc.zone->get_zone_params().log_pool, oids[shard_id], duration, zone_id, owner_id);
}

int RGWDataChangesLog::unlock(int shard_id, string& zone_id, string& owner_id) {
  return store->unlock(store->svc.zone->get_zone_params().log_pool, oids[shard_id], zone_id, owner_id);
}

bool RGWDataChangesLog::going_down()
{
  return down_flag;
}

RGWDataChangesLog::~RGWDataChangesLog() {
  down_flag = true;
  renew_thread->stop();
  renew_thread->join();
  delete renew_thread;
  delete[] oids;
}

void *RGWDataChangesLog::ChangesRenewThread::entry() {
  do {
    dout(2) << "RGWDataChangesLog::ChangesRenewThread: start" << dendl;
    int r = log->renew_entries();
    if (r < 0) {
      dout(0) << "ERROR: RGWDataChangesLog::renew_entries returned error r=" << r << dendl;
    }

    if (log->going_down())
      break;

    int interval = cct->_conf->rgw_data_log_window * 3 / 4;
    lock.Lock();
    cond.WaitInterval(lock, utime_t(interval, 0));
    lock.Unlock();
  } while (!log->going_down());

  return NULL;
}

void RGWDataChangesLog::ChangesRenewThread::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

void RGWDataChangesLog::mark_modified(int shard_id, const rgw_bucket_shard& bs)
{
  auto key = bs.get_key();
  modified_lock.get_read();
  map<int, set<string> >::iterator iter = modified_shards.find(shard_id);
  if (iter != modified_shards.end()) {
    set<string>& keys = iter->second;
    if (keys.find(key) != keys.end()) {
      modified_lock.unlock();
      return;
    }
  }
  modified_lock.unlock();

  RWLock::WLocker wl(modified_lock);
  modified_shards[shard_id].insert(key);
}

void RGWDataChangesLog::read_clear_modified(map<int, set<string> > &modified)
{
  RWLock::WLocker wl(modified_lock);
  modified.swap(modified_shards);
  modified_shards.clear();
}

void RGWBucketCompleteInfo::dump(Formatter *f) const {
  encode_json("bucket_info", info, f);
  encode_json("attrs", attrs, f);
}

void RGWBucketCompleteInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket_info", info, obj);
  JSONDecoder::decode_json("attrs", attrs, obj);
}

class RGWBucketMetadataHandler : public RGWMetadataHandler {

public:
  string get_type() override { return "bucket"; }

  int get(RGWRados *store, string& entry, RGWMetadataObject **obj) override {
    RGWObjVersionTracker ot;
    RGWBucketEntryPoint be;

    real_time mtime;
    map<string, bufferlist> attrs;
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();

    string tenant_name, bucket_name;
    parse_bucket(entry, &tenant_name, &bucket_name);
    int ret = store->get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, be, &ot, &mtime, &attrs);
    if (ret < 0)
      return ret;

    RGWBucketEntryMetadataObject *mdo = new RGWBucketEntryMetadataObject(be, ot.read_version, mtime);

    *obj = mdo;

    return 0;
  }

  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          real_time mtime, JSONObj *obj, sync_type_t sync_type) override {
    RGWBucketEntryPoint be, old_be;
    try {
      decode_json_obj(be, obj);
    } catch (JSONDecoder::err& e) {
      return -EINVAL;
    }

    real_time orig_mtime;
    map<string, bufferlist> attrs;

    RGWObjVersionTracker old_ot;
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();

    string tenant_name, bucket_name;
    parse_bucket(entry, &tenant_name, &bucket_name);
    int ret = store->get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, old_be, &old_ot, &orig_mtime, &attrs);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    // are we actually going to perform this put, or is it too old?
    if (ret != -ENOENT &&
        !check_versions(old_ot.read_version, orig_mtime,
			objv_tracker.write_version, mtime, sync_type)) {
      return STATUS_NO_APPLY;
    }

    objv_tracker.read_version = old_ot.read_version; /* maintain the obj version we just read */

    ret = store->put_bucket_entrypoint_info(tenant_name, bucket_name, be, false, objv_tracker, mtime, &attrs);
    if (ret < 0)
      return ret;

    /* link bucket */
    if (be.linked) {
      ret = rgw_link_bucket(store, be.owner, be.bucket, be.creation_time, false);
    } else {
      ret = rgw_unlink_bucket(store, be.owner, be.bucket.tenant,
                              be.bucket.name, false);
    }

    return ret;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
    RGWBucketEntryPoint be;
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();

    string tenant_name, bucket_name;
    parse_bucket(entry, &tenant_name, &bucket_name);
    int ret = store->get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, be, &objv_tracker, NULL, NULL);
    if (ret < 0)
      return ret;

    /*
     * We're unlinking the bucket but we don't want to update the entrypoint here - we're removing
     * it immediately and don't want to invalidate our cached objv_version or the bucket obj removal
     * will incorrectly fail.
     */
    ret = rgw_unlink_bucket(store, be.owner, tenant_name, bucket_name, false);
    if (ret < 0) {
      lderr(store->ctx()) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    ret = rgw_bucket_delete_bucket_obj(store, tenant_name, bucket_name, objv_tracker);
    if (ret < 0) {
      lderr(store->ctx()) << "could not delete bucket=" << entry << dendl;
    }
    /* idempotent */
    return 0;
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) override {
    oid = key;
    pool = store->svc.zone->get_zone_params().domain_root;
  }

  int list_keys_init(RGWRados *store, const string& marker, void **phandle) override {
    auto info = std::make_unique<list_keys_info>();

    info->store = store;

    int ret = store->list_raw_objects_init(store->svc.zone->get_zone_params().domain_root, marker,
                                           &info->ctx);
    if (ret < 0) {
      return ret;
    }
    *phandle = (void *)info.release();

    return 0;
  }

  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);

    string no_filter;

    keys.clear();

    RGWRados *store = info->store;

    list<string> unfiltered_keys;

    int ret = store->list_raw_objects_next(no_filter, max, info->ctx,
                                           unfiltered_keys, truncated);
    if (ret < 0 && ret != -ENOENT)
      return ret;
    if (ret == -ENOENT) {
      if (truncated)
        *truncated = false;
      return 0;
    }

    // now filter out the system entries
    list<string>::iterator iter;
    for (iter = unfiltered_keys.begin(); iter != unfiltered_keys.end(); ++iter) {
      string& k = *iter;

      if (k[0] != '.') {
        keys.push_back(k);
      }
    }

    return 0;
  }

  void list_keys_complete(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }

  string get_marker(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    return info->store->list_raw_objs_get_cursor(info->ctx);
  }
};

void get_md5_digest(const RGWBucketEntryPoint *be, string& md5_digest) {

   char md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
   unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
   bufferlist bl;

   Formatter *f = new JSONFormatter(false);
   be->dump(f);
   f->flush(bl);

   MD5 hash;
   hash.Update((const unsigned char *)bl.c_str(), bl.length());
   hash.Final(m);

   buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, md5);

   delete f;

   md5_digest = md5;
}

#define ARCHIVE_META_ATTR RGW_ATTR_PREFIX "zone.archive.info" 

struct archive_meta_info {
  rgw_bucket orig_bucket;

  bool from_attrs(CephContext *cct, map<string, bufferlist>& attrs) {
    auto iter = attrs.find(ARCHIVE_META_ATTR);
    if (iter == attrs.end()) {
      return false;
    }

    auto bliter = iter->second.cbegin();
    try {
      decode(bliter);
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: failed to decode archive meta info" << dendl;
      return false;
    }

    return true;
  }

  void store_in_attrs(map<string, bufferlist>& attrs) const {
    encode(attrs[ARCHIVE_META_ATTR]);
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(orig_bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(orig_bucket, bl);
     DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(archive_meta_info)

class RGWArchiveBucketMetadataHandler : public RGWBucketMetadataHandler {
public:
  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
    ldout(store->ctx(), 5) << "SKIP: bucket removal is not allowed on archive zone: bucket:" << entry << " ... proceeding to rename" << dendl;

    string tenant_name, bucket_name;
    parse_bucket(entry, &tenant_name, &bucket_name);

    real_time mtime;

    /* read original entrypoint */

    RGWBucketEntryPoint be;
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();
    map<string, bufferlist> attrs;
    int ret = store->get_bucket_entrypoint_info(obj_ctx, tenant_name, bucket_name, be, &objv_tracker, &mtime, &attrs);
    if (ret < 0) {
        return ret;
    }

    string meta_name = bucket_name + ":" + be.bucket.bucket_id;

    /* read original bucket instance info */

    map<string, bufferlist> attrs_m;
    ceph::real_time orig_mtime;
    RGWBucketInfo old_bi;

    ret = store->get_bucket_instance_info(obj_ctx, be.bucket, old_bi, &orig_mtime, &attrs_m);
    if (ret < 0) {
        return ret;
    }

    archive_meta_info ami;

    if (!ami.from_attrs(store->ctx(), attrs_m)) {
      ami.orig_bucket = old_bi.bucket;
      ami.store_in_attrs(attrs_m);
    }

    /* generate a new bucket instance. We could have avoided this if we could just point a new
     * bucket entry point to the old bucket instance, however, due to limitation in the way
     * we index buckets under the user, bucket entrypoint and bucket instance of the same
     * bucket need to have the same name, so we need to copy the old bucket instance into
     * to a new entry with the new name
     */

    string new_bucket_name;

    RGWBucketInfo new_bi = old_bi;
    RGWBucketEntryPoint new_be = be;

    string md5_digest;

    get_md5_digest(&new_be, md5_digest);
    new_bucket_name = ami.orig_bucket.name + "-deleted-" + md5_digest;

    new_bi.bucket.name = new_bucket_name;
    new_bi.objv_tracker.clear();

    new_be.bucket.name = new_bucket_name;

    ret = store->put_bucket_instance_info(new_bi, false, orig_mtime, &attrs_m);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to put new bucket instance info for bucket=" << new_bi.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* store a new entrypoint */

    RGWObjVersionTracker ot;
    ot.generate_new_write_ver(store->ctx());

    ret = store->put_bucket_entrypoint_info(tenant_name, new_bucket_name, new_be, true, ot, mtime, &attrs);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* link new bucket */

    ret = rgw_link_bucket(store, new_be.owner, new_be.bucket, new_be.creation_time, false);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed to link new bucket for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* clean up old stuff */

    ret = rgw_unlink_bucket(store, be.owner, tenant_name, bucket_name, false);
    if (ret < 0) {
        lderr(store->ctx()) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    // if (ret == -ECANCELED) it means that there was a race here, and someone
    // wrote to the bucket entrypoint just before we removed it. The question is
    // whether it was a newly created bucket entrypoint ...  in which case we
    // should ignore the error and move forward, or whether it is a higher version
    // of the same bucket instance ... in which we should retry
    ret = rgw_bucket_delete_bucket_obj(store, tenant_name, bucket_name, objv_tracker);
    if (ret < 0) {
        lderr(store->ctx()) << "could not delete bucket=" << entry << dendl;
    }

    ret = rgw_delete_system_obj(store, store->svc.zone->get_zone_params().domain_root, RGW_BUCKET_INSTANCE_MD_PREFIX + meta_name, NULL);

    /* idempotent */

    return 0;
  }

  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          real_time mtime, JSONObj *obj, sync_type_t sync_type) override {
    if (entry.find("-deleted-") != string::npos) {
      RGWObjVersionTracker ot;
      RGWMetadataObject *robj;
      int ret = get(store, entry, &robj);
      if (ret != -ENOENT) {
        if (ret < 0) {
          return ret;
        }
        ot.read_version = robj->get_version();
        delete robj;

        ret = remove(store, entry, ot);
        if (ret < 0) {
          return ret;
        }
      }
    }

    return RGWBucketMetadataHandler::put(store, entry, objv_tracker,
                                         mtime, obj, sync_type);
  }

};

class RGWBucketInstanceMetadataHandler : public RGWMetadataHandler {

public:
  string get_type() override { return "bucket.instance"; }

  int get(RGWRados *store, string& oid, RGWMetadataObject **obj) override {
    RGWBucketCompleteInfo bci;

    real_time mtime;
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();

    int ret = store->get_bucket_instance_info(obj_ctx, oid, bci.info, &mtime, &bci.attrs);
    if (ret < 0)
      return ret;

    RGWBucketInstanceMetadataObject *mdo = new RGWBucketInstanceMetadataObject(bci, bci.info.objv_tracker.read_version, mtime);

    *obj = mdo;

    return 0;
  }

  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          real_time mtime, JSONObj *obj, sync_type_t sync_type) override {
    RGWBucketCompleteInfo bci, old_bci;
    try {
      decode_json_obj(bci, obj);
    } catch (JSONDecoder::err& e) {
      return -EINVAL;
    }

    real_time orig_mtime;
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();

    int ret = store->get_bucket_instance_info(obj_ctx, entry, old_bci.info,
            &orig_mtime, &old_bci.attrs);
    bool exists = (ret != -ENOENT);
    if (ret < 0 && exists)
      return ret;

    if (!exists || old_bci.info.bucket.bucket_id != bci.info.bucket.bucket_id) {
      /* a new bucket, we need to select a new bucket placement for it */
      auto key(entry);
      rgw_bucket_instance_oid_to_key(key);
      string tenant_name;
      string bucket_name;
      string bucket_instance;
      parse_bucket(key, &tenant_name, &bucket_name, &bucket_instance);

      RGWZonePlacementInfo rule_info;
      bci.info.bucket.name = bucket_name;
      bci.info.bucket.bucket_id = bucket_instance;
      bci.info.bucket.tenant = tenant_name;
      ret = store->svc.zone->select_bucket_location_by_rule(bci.info.placement_rule, &rule_info);
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: select_bucket_placement() returned " << ret << dendl;
        return ret;
      }
      bci.info.index_type = rule_info.index_type;
    } else {
      /* existing bucket, keep its placement */
      bci.info.bucket.explicit_placement = old_bci.info.bucket.explicit_placement;
      bci.info.placement_rule = old_bci.info.placement_rule;
    }

    if (exists && old_bci.info.datasync_flag_enabled() != bci.info.datasync_flag_enabled()) {
      int shards_num = bci.info.num_shards? bci.info.num_shards : 1;
      int shard_id = bci.info.num_shards? 0 : -1;

      if (!bci.info.datasync_flag_enabled()) {
      ret = store->stop_bi_log_entries(bci.info, -1);
        if (ret < 0) {
	   lderr(store->ctx()) << "ERROR: failed writing bilog" << dendl;
	   return ret;
        }
      } else {
        ret = store->resync_bi_log_entries(bci.info, -1);
        if (ret < 0) {
	   lderr(store->ctx()) << "ERROR: failed writing bilog" << dendl;
	   return ret;
        }
      }

      for (int i = 0; i < shards_num; ++i, ++shard_id) {
        ret = store->data_log->add_entry(bci.info.bucket, shard_id);
        if (ret < 0) {
	   lderr(store->ctx()) << "ERROR: failed writing data log" << dendl;
	   return ret;
        }
      }
    }

    // are we actually going to perform this put, or is it too old?
    if (exists &&
        !check_versions(old_bci.info.objv_tracker.read_version, orig_mtime,
			objv_tracker.write_version, mtime, sync_type)) {
      objv_tracker.read_version = old_bci.info.objv_tracker.read_version;
      return STATUS_NO_APPLY;
    }

    /* record the read version (if any), store the new version */
    bci.info.objv_tracker.read_version = old_bci.info.objv_tracker.read_version;
    bci.info.objv_tracker.write_version = objv_tracker.write_version;

    ret = store->put_bucket_instance_info(bci.info, false, mtime, &bci.attrs);
    if (ret < 0)
      return ret;

    objv_tracker = bci.info.objv_tracker;

    ret = store->init_bucket_index(bci.info, bci.info.num_shards);
    if (ret < 0)
      return ret;

    return STATUS_APPLIED;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int remove(RGWRados *store, string& entry,
	     RGWObjVersionTracker& objv_tracker) override {
    RGWBucketInfo info;
    auto obj_ctx = store->svc.sysobj->init_obj_ctx();

    int ret = store->get_bucket_instance_info(obj_ctx, entry, info, NULL, NULL);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    return rgw_bucket_instance_remove_entry(store, entry,
					    &info.objv_tracker);
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_pool& pool, string& oid) override {
    oid = RGW_BUCKET_INSTANCE_MD_PREFIX + key;
    rgw_bucket_instance_key_to_oid(oid);
    pool = store->svc.zone->get_zone_params().domain_root;
  }

  int list_keys_init(RGWRados *store, const string& marker, void **phandle) override {
    auto info = std::make_unique<list_keys_info>();

    info->store = store;

    int ret = store->list_raw_objects_init(store->svc.zone->get_zone_params().domain_root, marker,
                                           &info->ctx);
    if (ret < 0) {
      return ret;
    }
    *phandle = (void *)info.release();

    return 0;
  }

  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);

    string no_filter;

    keys.clear();

    RGWRados *store = info->store;

    list<string> unfiltered_keys;

    int ret = store->list_raw_objects_next(no_filter, max, info->ctx,
                                           unfiltered_keys, truncated);
    if (ret < 0 && ret != -ENOENT)
      return ret;
    if (ret == -ENOENT) {
      if (truncated)
        *truncated = false;
      return 0;
    }

    constexpr int prefix_size = sizeof(RGW_BUCKET_INSTANCE_MD_PREFIX) - 1;
    // now filter in the relevant entries
    list<string>::iterator iter;
    for (iter = unfiltered_keys.begin(); iter != unfiltered_keys.end(); ++iter) {
      string& k = *iter;

      if (k.compare(0, prefix_size, RGW_BUCKET_INSTANCE_MD_PREFIX) == 0) {
        auto oid = k.substr(prefix_size);
        rgw_bucket_instance_oid_to_key(oid);
        keys.emplace_back(std::move(oid));
      }
    }

    return 0;
  }

  void list_keys_complete(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }

  string get_marker(void *handle) override {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    return info->store->list_raw_objs_get_cursor(info->ctx);
  }

  /*
   * hash entry for mdlog placement. Use the same hash key we'd have for the bucket entry
   * point, so that the log entries end up at the same log shard, so that we process them
   * in order
   */
  void get_hash_key(const string& section, const string& key, string& hash_key) override {
    string k;
    int pos = key.find(':');
    if (pos < 0)
      k = key;
    else
      k = key.substr(0, pos);
    hash_key = "bucket:" + k;
  }
};

class RGWArchiveBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandler {
public:

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) override {
    ldout(store->ctx(), 0) << "SKIP: bucket instance removal is not allowed on archive zone: bucket.instance:" << entry << dendl;
    return 0;
  }
};

RGWMetadataHandler *RGWBucketMetaHandlerAllocator::alloc() {
  return new RGWBucketMetadataHandler;
}

RGWMetadataHandler *RGWBucketInstanceMetaHandlerAllocator::alloc() {
  return new RGWBucketInstanceMetadataHandler;
}

RGWMetadataHandler *RGWArchiveBucketMetaHandlerAllocator::alloc() {
  return new RGWArchiveBucketMetadataHandler;
}

RGWMetadataHandler *RGWArchiveBucketInstanceMetaHandlerAllocator::alloc() {
  return new RGWArchiveBucketInstanceMetadataHandler;
}

void rgw_bucket_init(RGWMetadataManager *mm)
{
  auto sync_module = mm->get_store()->get_sync_module();
  if (sync_module) {
    bucket_meta_handler = sync_module->alloc_bucket_meta_handler();
    bucket_instance_meta_handler = sync_module->alloc_bucket_instance_meta_handler();
  } else {
    bucket_meta_handler = RGWBucketMetaHandlerAllocator::alloc();
    bucket_instance_meta_handler = RGWBucketInstanceMetaHandlerAllocator::alloc();
  }
  mm->register_handler(bucket_meta_handler);
  mm->register_handler(bucket_instance_meta_handler);
}
