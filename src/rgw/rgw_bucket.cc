// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include "rgw_tag_s3.h"

#include "include/types.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "rgw_string.h"
#include "rgw_multi.h"
#include "rgw_op.h"
#include "rgw_bucket_sync.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_bucket.h"
#include "services/svc_bucket_sync.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_user.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_datalog_rados.h"

#include "include/rados/librados.hpp"
// until everything is moved from rgw_common
#include "rgw_common.h"
#include "rgw_reshard.h"
#include "rgw_lc.h"

// stolen from src/cls/version/cls_version.cc
#define VERSION_ATTR "ceph.objclass.version"

#include "cls/user/cls_user_types.h"

#include "rgw_sal.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define BUCKET_TAG_TIMEOUT 30

// default number of entries to list with each bucket listing call
// (use marker to bridge between calls)
static constexpr size_t listing_max_entries = 1000;


/*
 * The tenant_name is always returned on purpose. May be empty, of course.
 */
static void parse_bucket(const string& bucket,
                         string *tenant_name,
                         string *bucket_name,
                         string *bucket_instance = nullptr /* optional */)
{
  /*
   * expected format: [tenant/]bucket:bucket_instance
   */
  int pos = bucket.find('/');
  if (pos >= 0) {
    *tenant_name = bucket.substr(0, pos);
  } else {
    tenant_name->clear();
  }
  string bn = bucket.substr(pos + 1);
  pos = bn.find (':');
  if (pos < 0) {
    *bucket_name = std::move(bn);
    return;
  }
  *bucket_name = bn.substr(0, pos);
  if (bucket_instance) {
    *bucket_instance = bn.substr(pos + 1);
  }

  /*
   * deal with the possible tenant:bucket:bucket_instance case
   */
  if (tenant_name->empty()) {
    pos = bucket_instance->find(':');
    if (pos >= 0) {
      *tenant_name = *bucket_name;
      *bucket_name = bucket_instance->substr(0, pos);
      *bucket_instance = bucket_instance->substr(pos + 1);
    }
  }
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
int rgw_read_user_buckets(rgw::sal::RGWRadosStore * store,
                          const rgw_user& user_id,
                          rgw::sal::RGWBucketList& buckets,
                          const string& marker,
                          const string& end_marker,
                          uint64_t max,
                          bool need_stats)
{
  rgw::sal::RGWRadosUser user(store, user_id);
  return user.list_buckets(marker, end_marker, max, need_stats, buckets);
}

int rgw_bucket_parse_bucket_instance(const string& bucket_instance, string *bucket_name, string *bucket_id, int *shard_id)
{
  auto pos = bucket_instance.rfind(':');
  if (pos == string::npos) {
    return -EINVAL;
  }

  string first = bucket_instance.substr(0, pos);
  string second = bucket_instance.substr(pos + 1);

  pos = first.find(':');

  if (pos == string::npos) {
    *shard_id = -1;
    *bucket_name = first;
    *bucket_id = second;
    return 0;
  }

  *bucket_name = first.substr(0, pos);
  *bucket_id = first.substr(pos + 1);

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
  if (pos != string::npos) {
    auto tenant = name.substr(0, pos);
    bucket->tenant.assign(tenant.begin(), tenant.end());
    name = name.substr(pos + 1);
  } else {
    bucket->tenant.clear();
  }

  // split name:instance
  pos = name.find(':');
  if (pos != string::npos) {
    instance = name.substr(pos + 1);
    name = name.substr(0, pos);
  }
  bucket->name.assign(name.begin(), name.end());

  // split instance:shard
  pos = instance.find(':');
  if (pos == string::npos) {
    bucket->bucket_id.assign(instance.begin(), instance.end());
    if (shard_id) {
      *shard_id = -1;
    }
    return 0;
  }

  // parse shard id
  auto shard = instance.substr(pos + 1);
  string err;
  auto id = strict_strtol(shard.data(), 10, &err);
  if (!err.empty()) {
    if (cct) {
      ldout(cct, 0) << "ERROR: failed to parse bucket shard '"
          << instance.data() << "': " << err << dendl;
    }
    return -EINVAL;
  }

  if (shard_id) {
    *shard_id = id;
  }
  instance = instance.substr(0, pos);
  bucket->bucket_id.assign(instance.begin(), instance.end());
  return 0;
}

static void dump_mulipart_index_results(list<rgw_obj_index_key>& objs_to_unlink,
        Formatter *f)
{
  for (const auto& o : objs_to_unlink) {
    f->dump_string("object",  o.name);
  }
}

void check_bad_user_bucket_mapping(rgw::sal::RGWRadosStore *store, const rgw_user& user_id,
				   bool fix)
{
  rgw::sal::RGWBucketList user_buckets;
  rgw::sal::RGWRadosUser user(store, user_id);
  string marker;

  CephContext *cct = store->ctx();

  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;

  do {
    int ret = user.list_buckets(marker, string(), max_entries, false, user_buckets);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "failed to read user buckets: "
			     << cpp_strerror(-ret) << dendl;
      return;
    }

    map<string, rgw::sal::RGWBucket*>& buckets = user_buckets.get_buckets();
    for (map<string, rgw::sal::RGWBucket*>::iterator i = buckets.begin();
         i != buckets.end();
         ++i) {
      marker = i->first;

      rgw::sal::RGWBucket* bucket = i->second;

      RGWBucketInfo bucket_info;
      real_time mtime;
      int r = store->getRados()->get_bucket_info(store->svc(), user_id.tenant, bucket->get_name(), bucket_info, &mtime, null_yield);
      if (r < 0) {
        ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket << dendl;
        continue;
      }

      rgw_bucket& actual_bucket = bucket_info.bucket;

      if (actual_bucket.name.compare(bucket->get_name()) != 0 ||
          actual_bucket.tenant.compare(bucket->get_tenant()) != 0 ||
          actual_bucket.marker.compare(bucket->get_marker()) != 0 ||
          actual_bucket.bucket_id.compare(bucket->get_bucket_id()) != 0) {
        cout << "bucket info mismatch: expected " << actual_bucket << " got " << bucket << std::endl;
        if (fix) {
          cout << "fixing" << std::endl;
          r = store->ctl()->bucket->link_bucket(user_id, actual_bucket,
                                             bucket_info.creation_time,
					     null_yield);
          if (r < 0) {
            cerr << "failed to fix bucket: " << cpp_strerror(-r) << std::endl;
          }
        }
      }
    }
  } while (user_buckets.is_truncated());
}

// note: function type conforms to RGWRados::check_filter_t
bool rgw_bucket_object_check_filter(const string& oid)
{
  rgw_obj_key key;
  string ns;
  return rgw_obj_key::oid_to_key_in_ns(oid, &key, ns);
}

int rgw_remove_object(rgw::sal::RGWRadosStore *store, const RGWBucketInfo& bucket_info, const rgw_bucket& bucket, rgw_obj_key& key)
{
  RGWObjectCtx rctx(store);

  if (key.instance.empty()) {
    key.instance = "null";
  }

  rgw_obj obj(bucket, key);

  return store->getRados()->delete_obj(rctx, bucket_info, obj, bucket_info.versioning_status());
}

/* xxx dang */
static int rgw_remove_bucket(rgw::sal::RGWRadosStore *store, rgw_bucket& bucket, bool delete_children, optional_yield y)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> stats;
  std::vector<rgw_bucket_dir_entry> objs;
  map<string, bool> common_prefixes;
  RGWBucketInfo info;

  string bucket_ver, master_ver;

  ret = store->getRados()->get_bucket_info(store->svc(), bucket.tenant, bucket.name, info, NULL, null_yield);
  if (ret < 0)
    return ret;

  ret = store->getRados()->get_bucket_stats(info, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, NULL);
  if (ret < 0)
    return ret;

  RGWRados::Bucket target(store->getRados(), info);
  RGWRados::Bucket::List list_op(&target);
  CephContext *cct = store->ctx();

  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true;

  bool is_truncated = false;
  do {
    objs.clear();

    ret = list_op.list_objects(listing_max_entries, &objs, &common_prefixes,
			       &is_truncated, null_yield);
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

  ret = store->ctl()->bucket->sync_user_stats(info.owner, info);
  if ( ret < 0) {
     dout(1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker objv_tracker;

  // if we deleted children above we will force delete, as any that
  // remain is detrius from a prior bug
  ret = store->getRados()->delete_bucket(info, objv_tracker, null_yield, !delete_children);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not remove bucket " <<
      bucket.name << dendl;
    return ret;
  }

  ret = store->ctl()->bucket->unlink_bucket(info.owner, bucket, null_yield, false);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: unable to remove user bucket information" << dendl;
  }

  return ret;
}

static int aio_wait(librados::AioCompletion *handle)
{
  librados::AioCompletion *c = (librados::AioCompletion *)handle;
  c->wait_for_complete();
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

int rgw_remove_bucket_bypass_gc(rgw::sal::RGWRadosStore *store, rgw_bucket& bucket,
                                int concurrent_max, bool keep_index_consistent,
                                optional_yield y)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> stats;
  std::vector<rgw_bucket_dir_entry> objs;
  map<string, bool> common_prefixes;
  RGWBucketInfo info;
  RGWObjectCtx obj_ctx(store);
  CephContext *cct = store->ctx();

  string bucket_ver, master_ver;

  ret = store->getRados()->get_bucket_info(store->svc(), bucket.tenant, bucket.name, info, NULL, null_yield);
  if (ret < 0)
    return ret;

  ret = store->getRados()->get_bucket_stats(info, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, NULL);
  if (ret < 0)
    return ret;

  string prefix, delimiter;

  ret = abort_bucket_multiparts(store, cct, info, prefix, delimiter);
  if (ret < 0) {
    return ret;
  }

  RGWRados::Bucket target(store->getRados(), info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true;

  std::list<librados::AioCompletion*> handles;

  int max_aio = concurrent_max;
  bool is_truncated = true;

  while (is_truncated) {
    objs.clear();
    ret = list_op.list_objects(listing_max_entries, &objs, &common_prefixes,
			       &is_truncated, null_yield);
    if (ret < 0)
      return ret;

    std::vector<rgw_bucket_dir_entry>::iterator it = objs.begin();
    for (; it != objs.end(); ++it) {
      RGWObjState *astate = NULL;
      rgw_obj obj(bucket, (*it).key);

      ret = store->getRados()->get_obj_state(&obj_ctx, info, obj, &astate, false, y);
      if (ret == -ENOENT) {
        dout(1) << "WARNING: cannot find obj state for obj " << obj.get_oid() << dendl;
        continue;
      }
      if (ret < 0) {
        lderr(store->ctx()) << "ERROR: get obj state returned with error " << ret << dendl;
        return ret;
      }

      if (astate->manifest) {
        RGWObjManifest& manifest = *astate->manifest;
        RGWObjManifest::obj_iterator miter = manifest.obj_begin();
        rgw_obj head_obj = manifest.get_obj();
        rgw_raw_obj raw_head_obj;
        store->getRados()->obj_to_raw(info.placement_rule, head_obj, &raw_head_obj);


        for (; miter != manifest.obj_end() && max_aio--; ++miter) {
          if (!max_aio) {
            ret = drain_handles(handles);
            if (ret < 0) {
              lderr(store->ctx()) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
              return ret;
            }
            max_aio = concurrent_max;
          }

          rgw_raw_obj last_obj = miter.get_location().get_raw_obj(store->getRados());
          if (last_obj == raw_head_obj) {
            // have the head obj deleted at the end
            continue;
          }

          ret = store->getRados()->delete_raw_obj_aio(last_obj, handles);
          if (ret < 0) {
            lderr(store->ctx()) << "ERROR: delete obj aio failed with " << ret << dendl;
            return ret;
          }
        } // for all shadow objs

        ret = store->getRados()->delete_obj_aio(head_obj, info, astate, handles, keep_index_consistent, null_yield);
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
      obj_ctx.invalidate(obj);
    } // for all RGW objects
  }

  ret = drain_handles(handles);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not drain handles as aio completion returned with " << ret << dendl;
    return ret;
  }

  ret = store->ctl()->bucket->sync_user_stats(info.owner, info);
  if (ret < 0) {
     dout(1) << "WARNING: failed sync user stats before bucket delete. ret=" <<  ret << dendl;
  }

  RGWObjVersionTracker objv_tracker;

  // this function can only be run if caller wanted children to be
  // deleted, so we can ignore the check for children as any that
  // remain are detritus from a prior bug
  ret = store->getRados()->delete_bucket(info, objv_tracker, y, false);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not remove bucket " << bucket.name << dendl;
    return ret;
  }

  ret = store->ctl()->bucket->unlink_bucket(info.owner, bucket, null_yield, false);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: unable to remove user bucket information" << dendl;
  }

  return ret;
}

static void set_err_msg(std::string *sink, std::string msg)
{
  if (sink && !msg.empty())
    *sink = msg;
}

int RGWBucket::init(rgw::sal::RGWRadosStore *storage, RGWBucketAdminOpState& op_state,
                    optional_yield y, std::string *err_msg,
                    map<string, bufferlist> *pattrs)
{
  if (!storage) {
    set_err_msg(err_msg, "no storage!");
    return -EINVAL;
  }

  store = storage;

  rgw_user user_id = op_state.get_user_id();
  bucket.tenant = user_id.tenant;
  bucket.name = op_state.get_bucket_name();

  if (bucket.name.empty() && user_id.empty())
    return -EINVAL;
  
  // split possible tenant/name
  auto pos = bucket.name.find('/');
  if (pos != string::npos) {
    bucket.tenant = bucket.name.substr(0, pos);
    bucket.name = bucket.name.substr(pos + 1);
  }

  if (!bucket.name.empty()) {
    int r = store->ctl()->bucket->read_bucket_info(
        bucket, &bucket_info, y,
        RGWBucketCtl::BucketInstance::GetParams().set_attrs(pattrs),
        &ep_objv);
    if (r < 0) {
      set_err_msg(err_msg, "failed to fetch bucket info for bucket=" + bucket.name);
      return r;
    }

    op_state.set_bucket(bucket_info.bucket);
  }

  if (!user_id.empty()) {
    int r = store->ctl()->user->get_info_by_uid(user_id, &user_info, y);
    if (r < 0) {
      set_err_msg(err_msg, "failed to fetch user info");
      return r;
    }

    op_state.display_name = user_info.display_name;
  }

  clear_failure();
  return 0;
}

bool rgw_find_bucket_by_id(CephContext *cct, RGWMetadataManager *mgr,
                           const string& marker, const string& bucket_id, rgw_bucket* bucket_out)
{
  void *handle = NULL;
  bool truncated = false;
  string s;

  int ret = mgr->list_keys_init("bucket.instance", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    mgr->list_keys_complete(handle);
    return -ret;
  }
  do {
      list<string> keys;
      ret = mgr->list_keys_next(handle, 1000, keys, &truncated);
      if (ret < 0) {
        cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        mgr->list_keys_complete(handle);
        return -ret;
      }
      for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
        s = *iter;
        ret = rgw_bucket_parse_bucket_key(cct, s, bucket_out, nullptr);
        if (ret < 0) {
          continue;
        }
        if (bucket_id == bucket_out->bucket_id) {
          mgr->list_keys_complete(handle);
          return true;
        }
      }
  } while (truncated);
  mgr->list_keys_complete(handle);
  return false;
}

int RGWBucket::link(RGWBucketAdminOpState& op_state, optional_yield y,
                    map<string, bufferlist>& attrs, std::string *err_msg)
{
  if (!op_state.is_user_op()) {
    set_err_msg(err_msg, "empty user id");
    return -EINVAL;
  }

  string bucket_id = op_state.get_bucket_id();

  std::string display_name = op_state.get_user_display_name();
  rgw_bucket& bucket = op_state.get_bucket();
  if (!bucket_id.empty() && bucket_id != bucket.bucket_id) {
    set_err_msg(err_msg,
	"specified bucket id does not match " + bucket.bucket_id);
    return -EINVAL;
  }
  rgw_bucket old_bucket = bucket;
  rgw_user user_id = op_state.get_user_id();
  bucket.tenant = user_id.tenant;
  if (!op_state.new_bucket_name.empty()) {
    auto pos = op_state.new_bucket_name.find('/');
    if (pos != string::npos) {
      bucket.tenant = op_state.new_bucket_name.substr(0, pos);
      bucket.name = op_state.new_bucket_name.substr(pos + 1);
    } else {
      bucket.name = op_state.new_bucket_name;
    }
  }

  RGWObjVersionTracker objv_tracker;
  RGWObjVersionTracker old_version = bucket_info.objv_tracker;

  map<string, bufferlist>::iterator aiter = attrs.find(RGW_ATTR_ACL);
  if (aiter == attrs.end()) {
	// should never happen; only pre-argonaut buckets lacked this.
    ldout(store->ctx(), 0) << "WARNING: can't bucket link because no acl on bucket=" << old_bucket.name << dendl;
    set_err_msg(err_msg,
	"While crossing the Anavros you have displeased the goddess Hera."
	"  You must sacrifice your ancient bucket " + bucket.bucket_id);
    return -EINVAL;
  }
  bufferlist& aclbl = aiter->second;
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

  auto bucket_ctl = store->ctl()->bucket;
  int r = bucket_ctl->unlink_bucket(owner.get_id(), old_bucket, y, false);
  if (r < 0) {
    set_err_msg(err_msg, "could not unlink policy from user " + owner.get_id().to_str());
    return r;
  }

  // now update the user for the bucket...
  if (display_name.empty()) {
    ldout(store->ctx(), 0) << "WARNING: user " << user_info.user_id << " has no display name set" << dendl;
  }

  RGWAccessControlPolicy policy_instance;
  policy_instance.create_default(user_info.user_id, display_name);
  owner = policy_instance.get_owner();

  aclbl.clear();
  policy_instance.encode(aclbl);

  auto instance_params = RGWBucketCtl::BucketInstance::PutParams().set_attrs(&attrs);

  bucket_info.owner = user_info.user_id;
  if (bucket != old_bucket) {
    bucket_info.bucket = bucket;
    bucket_info.objv_tracker.version_for_read()->ver = 0;
    instance_params.set_exclusive(true);
  }

  r = bucket_ctl->store_bucket_instance_info(bucket, bucket_info, y, instance_params);
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info: " + cpp_strerror(-r));
    return r;
  }

  RGWBucketEntryPoint ep;
  ep.bucket = bucket_info.bucket;
  ep.owner = user_info.user_id;
  ep.creation_time = bucket_info.creation_time;
  ep.linked = true;
  map<string, bufferlist> ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  /* link to user */
  r = store->ctl()->bucket->link_bucket(user_info.user_id,
                                     bucket_info.bucket,
                                     ep.creation_time,
                                     y, true, &ep_data);
  if (r < 0) {
    set_err_msg(err_msg, "failed to relink bucket");
    return r;
  }

  if (bucket != old_bucket) {
    // like RGWRados::delete_bucket -- excepting no bucket_index work.
    r = bucket_ctl->remove_bucket_entrypoint_info(old_bucket, y,
                                                  RGWBucketCtl::Bucket::RemoveParams()
                                                  .set_objv_tracker(&ep_data.ep_objv));
    if (r < 0) {
      set_err_msg(err_msg, "failed to unlink old bucket endpoint " + old_bucket.tenant + "/" + old_bucket.name);
      return r;
    }

    r = bucket_ctl->remove_bucket_instance_info(old_bucket, bucket_info, y,
                                                RGWBucketCtl::BucketInstance::RemoveParams()
                                                .set_objv_tracker(&old_version));
    if (r < 0) {
      set_err_msg(err_msg, "failed to unlink old bucket info");
      return r;
    }
  }

  return 0;
}

int RGWBucket::chown(RGWBucketAdminOpState& op_state, const string& marker,
                     optional_yield y, std::string *err_msg)
{
  int ret = store->ctl()->bucket->chown(store, bucket_info, user_info.user_id,
                                     user_info.display_name, marker, y);
  if (ret < 0) {
    set_err_msg(err_msg, "Failed to change object ownership: " + cpp_strerror(-ret));
  }
  
  return ret;
}

int RGWBucket::unlink(RGWBucketAdminOpState& op_state, optional_yield y, std::string *err_msg)
{
  rgw_bucket bucket = op_state.get_bucket();

  if (!op_state.is_user_op()) {
    set_err_msg(err_msg, "could not fetch user or user bucket info");
    return -EINVAL;
  }

  int r = store->ctl()->bucket->unlink_bucket(user_info.user_id, bucket, y);
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
  int r = store->getRados()->get_bucket_info(store->svc(), bucket.tenant, bucket.name, bucket_info, NULL, null_yield, &attrs);
  if (r < 0) {
    set_err_msg(err_msg, "could not get bucket info for bucket=" + bucket.name + ": " + cpp_strerror(-r));
    return r;
  }

  bucket_info.quota = op_state.quota;
  r = store->getRados()->put_bucket_instance_info(bucket_info, false, real_time(), &attrs);
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info: " + cpp_strerror(-r));
    return r;
  }
  return r;
}

int RGWBucket::remove(RGWBucketAdminOpState& op_state, optional_yield y, bool bypass_gc,
                      bool keep_index_consistent, std::string *err_msg)
{
  bool delete_children = op_state.will_delete_children();
  rgw_bucket bucket = op_state.get_bucket();
  int ret;

  if (bypass_gc) {
    if (delete_children) {
      ret = rgw_remove_bucket_bypass_gc(store, bucket, op_state.get_max_aio(), keep_index_consistent, y);
    } else {
      set_err_msg(err_msg, "purge objects should be set for gc to be bypassed");
      return -EINVAL;
    }
  } else {
    ret = rgw_remove_bucket(store, bucket, delete_children, y);
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

static void dump_bucket_index(const RGWRados::ent_map_t& result,  Formatter *f)
{
  for (auto iter = result.begin(); iter != result.end(); ++iter) {
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

  map<string, bool> common_prefixes;

  bool is_truncated;
  map<string, bool> meta_objs;
  map<rgw_obj_index_key, string> all_objs;

  RGWBucketInfo bucket_info;
  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();
  int r = store->getRados()->get_bucket_instance_info(obj_ctx, bucket, bucket_info, nullptr, nullptr, null_yield);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: " << __func__ << "(): get_bucket_instance_info(bucket=" << bucket << ") returned r=" << r << dendl;
    return r;
  }

  RGWRados::Bucket target(store->getRados(), bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.list_versions = true;
  list_op.params.ns = RGW_OBJ_NS_MULTIPART;

  do {
    vector<rgw_bucket_dir_entry> result;
    int r = list_op.list_objects(listing_max_entries, &result,
				 &common_prefixes, &is_truncated, null_yield);
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

    if (objs_to_unlink.size() > listing_max_entries) {
      if (fix_index) {
	int r = store->getRados()->remove_objs_from_index(bucket_info, objs_to_unlink);
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
    int r = store->getRados()->remove_objs_from_index(bucket_info, objs_to_unlink);
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
                                  optional_yield y,
                                  std::string *err_msg)
{

  bool fix_index = op_state.will_fix_index();

  if (!fix_index) {
    set_err_msg(err_msg, "check-objects flag requires fix index enabled");
    return -EINVAL;
  }

  store->getRados()->cls_obj_set_bucket_tag_timeout(bucket_info, BUCKET_TAG_TIMEOUT);

  string prefix;
  string empty_delimiter;
  rgw_obj_index_key marker;
  bool is_truncated = true;
  bool cls_filtered = true;

  Formatter *formatter = flusher.get_formatter();
  formatter->open_object_section("objects");
  uint16_t expansion_factor = 1;
  while (is_truncated) {
    RGWRados::ent_map_t result;
    result.reserve(listing_max_entries);

    int r = store->getRados()->cls_bucket_list_ordered(
      bucket_info, RGW_NO_SHARD, marker, prefix, empty_delimiter,
      listing_max_entries, true, expansion_factor,
      result, &is_truncated, &cls_filtered, &marker,
      y, rgw_bucket_object_check_filter);
    if (r == -ENOENT) {
      break;
    } else if (r < 0 && r != -ENOENT) {
      set_err_msg(err_msg, "ERROR: failed operation r=" + cpp_strerror(-r));
    }

    if (result.size() < listing_max_entries / 8) {
      ++expansion_factor;
    } else if (result.size() > listing_max_entries * 7 / 8 &&
	       expansion_factor > 1) {
      --expansion_factor;
    }

    dump_bucket_index(result, formatter);
    flusher.flush();
  }

  formatter->close_section();

  store->getRados()->cls_obj_set_bucket_tag_timeout(bucket_info, 0);

  return 0;
}


int RGWBucket::check_index(RGWBucketAdminOpState& op_state,
        map<RGWObjCategory, RGWStorageStats>& existing_stats,
        map<RGWObjCategory, RGWStorageStats>& calculated_stats,
        std::string *err_msg)
{
  bool fix_index = op_state.will_fix_index();

  int r = store->getRados()->bucket_check_index(bucket_info, &existing_stats, &calculated_stats);
  if (r < 0) {
    set_err_msg(err_msg, "failed to check index error=" + cpp_strerror(-r));
    return r;
  }

  if (fix_index) {
    r = store->getRados()->bucket_rebuild_index(bucket_info);
    if (r < 0) {
      set_err_msg(err_msg, "failed to rebuild index err=" + cpp_strerror(-r));
      return r;
    }
  }

  return 0;
}

int RGWBucket::sync(RGWBucketAdminOpState& op_state, map<string, bufferlist> *attrs, std::string *err_msg)
{
  if (!store->svc()->zone->is_meta_master()) {
    set_err_msg(err_msg, "ERROR: failed to update bucket sync: only allowed on meta master zone");
    return EINVAL;
  }
  bool sync = op_state.will_sync_bucket();
  if (sync) {
    bucket_info.flags &= ~BUCKET_DATASYNC_DISABLED;
  } else {
    bucket_info.flags |= BUCKET_DATASYNC_DISABLED;
  }

  int r = store->getRados()->put_bucket_instance_info(bucket_info, false, real_time(), attrs);
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info:" + cpp_strerror(-r));
    return r;
  }

  int shards_num = bucket_info.layout.current_index.layout.normal.num_shards? bucket_info.layout.current_index.layout.normal.num_shards : 1;
  int shard_id = bucket_info.layout.current_index.layout.normal.num_shards? 0 : -1;

  if (!sync) {
    r = store->svc()->bilog_rados->log_stop(bucket_info, -1);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing stop bilog:" + cpp_strerror(-r));
      return r;
    }
  } else {
    r = store->svc()->bilog_rados->log_start(bucket_info, -1);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing resync bilog:" + cpp_strerror(-r));
      return r;
    }
  }

  for (int i = 0; i < shards_num; ++i, ++shard_id) {
    r = store->svc()->datalog_rados->add_entry(bucket_info, shard_id);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing data log:" + cpp_strerror(-r));
      return r;
    }
  }

  return 0;
}


int RGWBucket::policy_bl_to_stream(bufferlist& bl, ostream& o)
{
  RGWAccessControlPolicy_S3 policy(g_ceph_context);
  int ret = decode_bl(bl, policy);
  if (ret < 0) {
    ldout(store->ctx(),0) << "failed to decode RGWAccessControlPolicy" << dendl;
  }
  policy.to_xml(o);
  return 0;
}

int rgw_object_get_attr(rgw::sal::RGWRadosStore* store, const RGWBucketInfo& bucket_info,
			const rgw_obj& obj, const char* attr_name,
			bufferlist& out_bl, optional_yield y)
{
  RGWObjectCtx obj_ctx(store);
  RGWRados::Object op_target(store->getRados(), bucket_info, obj_ctx, obj);
  RGWRados::Object::Read rop(&op_target);

  return rop.get_attr(attr_name, out_bl, y);
}

int RGWBucket::get_policy(RGWBucketAdminOpState& op_state, RGWAccessControlPolicy& policy, optional_yield y)
{
  std::string object_name = op_state.get_object_name();
  rgw_bucket bucket = op_state.get_bucket();

  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
  int ret = store->getRados()->get_bucket_info(store->svc(), bucket.tenant, bucket.name, bucket_info, NULL, null_yield, &attrs);
  if (ret < 0) {
    return ret;
  }

  if (!object_name.empty()) {
    bufferlist bl;
    rgw_obj obj(bucket, object_name);

    ret = rgw_object_get_attr(store, bucket_info, obj, RGW_ATTR_ACL, bl, y);
    if (ret < 0){
      return ret;
    }

    ret = decode_bl(bl, policy);
    if (ret < 0) {
      ldout(store->ctx(),0) << "failed to decode RGWAccessControlPolicy" << dendl;
    }
    return ret;
  }

  map<string, bufferlist>::iterator aiter = attrs.find(RGW_ATTR_ACL);
  if (aiter == attrs.end()) {
    return -ENOENT;
  }

  ret = decode_bl(aiter->second, policy);
  if (ret < 0) {
    ldout(store->ctx(),0) << "failed to decode RGWAccessControlPolicy" << dendl;
  }

  return ret;
}


int RGWBucketAdminOp::get_policy(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield);
  if (ret < 0)
    return ret;

  ret = bucket.get_policy(op_state, policy, null_yield);
  if (ret < 0)
    return ret;

  return 0;
}

/* Wrappers to facilitate RESTful interface */


int RGWBucketAdminOp::get_policy(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state,
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

int RGWBucketAdminOp::dump_s3_policy(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state,
                  ostream& os)
{
  RGWAccessControlPolicy_S3 policy(store->ctx());

  int ret = get_policy(store, op_state, policy);
  if (ret < 0)
    return ret;

  policy.to_xml(os);

  return 0;
}

int RGWBucketAdminOp::unlink(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield);
  if (ret < 0)
    return ret;

  return bucket.unlink(op_state, null_yield);
}

int RGWBucketAdminOp::link(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state, string *err)
{
  RGWBucket bucket;
  map<string, bufferlist> attrs;

  int ret = bucket.init(store, op_state, null_yield, err, &attrs);
  if (ret < 0)
    return ret;

  return bucket.link(op_state, null_yield, attrs, err);

}

int RGWBucketAdminOp::chown(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state, const string& marker, string *err)
{
  RGWBucket bucket;
  map<string, bufferlist> attrs;

  int ret = bucket.init(store, op_state, null_yield, err, &attrs);
  if (ret < 0)
    return ret;

  ret = bucket.link(op_state, null_yield, attrs, err);
  if (ret < 0)
    return ret;

  return bucket.chown(op_state, marker, null_yield, err);

}

int RGWBucketAdminOp::check_index(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, optional_yield y)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> existing_stats;
  map<RGWObjCategory, RGWStorageStats> calculated_stats;


  RGWBucket bucket;

  ret = bucket.init(store, op_state, null_yield);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  ret = bucket.check_bad_index_multipart(op_state, flusher);
  if (ret < 0)
    return ret;

  ret = bucket.check_object_index(op_state, flusher, y);
  if (ret < 0)
    return ret;

  ret = bucket.check_index(op_state, existing_stats, calculated_stats);
  if (ret < 0)
    return ret;

  dump_index_check(existing_stats, calculated_stats, formatter);
  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::remove_bucket(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state,
                                    optional_yield y, bool bypass_gc, bool keep_index_consistent)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, y);
  if (ret < 0)
    return ret;

  std::string err_msg;
  ret = bucket.remove(op_state, y, bypass_gc, keep_index_consistent, &err_msg);
  if (!err_msg.empty()) {
    lderr(store->ctx()) << "ERROR: " << err_msg << dendl;
  }
  return ret;
}

int RGWBucketAdminOp::remove_object(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield);
  if (ret < 0)
    return ret;

  return bucket.remove_object(op_state);
}

int RGWBucketAdminOp::sync_bucket(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state, string *err_msg)
{
  RGWBucket bucket;
  map<string, bufferlist> attrs;
  int ret = bucket.init(store, op_state, null_yield, err_msg, &attrs);
  if (ret < 0)
  {
    return ret;
  }
  return bucket.sync(op_state, &attrs, err_msg);
}

static int bucket_stats(rgw::sal::RGWRadosStore *store, const std::string& tenant_name, std::string&  bucket_name, Formatter *formatter)
{
  RGWBucketInfo bucket_info;
  map<RGWObjCategory, RGWStorageStats> stats;
  map<string, bufferlist> attrs;

  real_time mtime;
  int r = store->getRados()->get_bucket_info(store->svc(), tenant_name, bucket_name, bucket_info, &mtime, null_yield, &attrs);
  if (r < 0)
    return r;

  rgw_bucket& bucket = bucket_info.bucket;

  string bucket_ver, master_ver;
  string max_marker;
  int ret = store->getRados()->get_bucket_stats(bucket_info, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0) {
    cerr << "error getting bucket stats ret=" << ret << std::endl;
    return ret;
  }

  utime_t ut(mtime);
  utime_t ctime_ut(bucket_info.creation_time);

  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket.name);
  formatter->dump_int("num_shards", bucket_info.layout.current_index.layout.normal.num_shards);
  formatter->dump_string("tenant", bucket.tenant);
  formatter->dump_string("zonegroup", bucket_info.zonegroup);
  formatter->dump_string("placement_rule", bucket_info.placement_rule.to_str());
  ::encode_json("explicit_placement", bucket.explicit_placement, formatter);
  formatter->dump_string("id", bucket.bucket_id);
  formatter->dump_string("marker", bucket.marker);
  formatter->dump_stream("index_type") << bucket_info.layout.current_index.layout.type;
  ::encode_json("owner", bucket_info.owner, formatter);
  formatter->dump_string("ver", bucket_ver);
  formatter->dump_string("master_ver", master_ver);
  ut.gmtime(formatter->dump_stream("mtime"));
  ctime_ut.gmtime(formatter->dump_stream("creation_time"));
  formatter->dump_string("max_marker", max_marker);
  dump_bucket_usage(stats, formatter);
  encode_json("bucket_quota", bucket_info.quota, formatter);

  // bucket tags
  auto iter = attrs.find(RGW_ATTR_TAGS);
  if (iter != attrs.end()) {
    RGWObjTagSet_S3 tagset;
    bufferlist::const_iterator piter{&iter->second};
    try {
      tagset.decode(piter);
      tagset.dump(formatter); 
    } catch (buffer::error& err) {
      cerr << "ERROR: caught buffer:error, couldn't decode TagSet" << std::endl;
    }
  }

  // TODO: bucket CORS
  // TODO: bucket LC
  formatter->close_section();

  return 0;
}

int RGWBucketAdminOp::limit_check(rgw::sal::RGWRadosStore *store,
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
    rgw::sal::RGWBucketList buckets;
    do {
      rgw::sal::RGWRadosUser user(store, rgw_user(user_id));

      ret = user.list_buckets(marker, string(), max_entries, false, buckets);

      if (ret < 0)
        return ret;

      map<string, rgw::sal::RGWBucket*>& m_buckets = buckets.get_buckets();

      for (const auto& iter : m_buckets) {
	auto bucket = iter.second;
	uint32_t num_shards = 1;
	uint64_t num_objects = 0;

	/* need info for num_shards */
	RGWBucketInfo info;

	marker = bucket->get_name(); /* Casey's location for marker update,
				     * as we may now not reach the end of
				     * the loop body */

	ret = store->getRados()->get_bucket_info(store->svc(), bucket->get_tenant(),
						 bucket->get_name(), info, nullptr,
						 null_yield);
	if (ret < 0)
	  continue;

	/* need stats for num_entries */
	string bucket_ver, master_ver;
	std::map<RGWObjCategory, RGWStorageStats> stats;
	ret = store->getRados()->get_bucket_stats(info, RGW_NO_SHARD, &bucket_ver,
				      &master_ver, stats, nullptr);

	if (ret < 0)
	  continue;

	for (const auto& s : stats) {
	    num_objects += s.second.num_objects;
	}

	num_shards = info.layout.current_index.layout.normal.num_shards;
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
	    formatter->dump_string("bucket", bucket->get_name());
	    formatter->dump_string("tenant", bucket->get_tenant());
	    formatter->dump_int("num_objects", num_objects);
	    formatter->dump_int("num_shards", num_shards);
	    formatter->dump_int("objects_per_shard", objs_per_shard);
	    formatter->dump_string("fill_status", ss.str());
	    formatter->close_section();
	  }
	}
      }
      formatter->flush(cout);
    } while (buckets.is_truncated()); /* foreach: bucket */

    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);

  } /* foreach: user_id */

  formatter->close_section();
  formatter->flush(cout);

  return ret;
} /* RGWBucketAdminOp::limit_check */

int RGWBucketAdminOp::info(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state,
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

    rgw::sal::RGWBucketList buckets;
    rgw::sal::RGWRadosUser user(store, op_state.get_user_id());
    string marker;

    do {
      buckets.clear();
      ret = user.list_buckets(marker, string(), max_entries, false, buckets);
      if (ret < 0)
        return ret;

      map<string, rgw::sal::RGWBucket*>& m = buckets.get_buckets();
      map<string, rgw::sal::RGWBucket*>::iterator iter;

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
    } while (buckets.is_truncated());

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
    ret = store->ctl()->meta.mgr->list_keys_init("bucket", &handle);
    while (ret == 0 && truncated) {
      std::list<std::string> buckets;
      const int max_keys = 1000;
      ret = store->ctl()->meta.mgr->list_keys_next(handle, max_keys, buckets,
                                            &truncated);
      for (auto& bucket_name : buckets) {
        if (show_stats)
          bucket_stats(store, user_id.tenant, bucket_name, formatter);
        else
          formatter->dump_string("bucket", bucket_name);
      }
    }
    store->ctl()->meta.mgr->list_keys_complete(handle);

    formatter->close_section();
  }

  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::set_quota(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield);
  if (ret < 0)
    return ret;
  return bucket.set_quota(op_state);
}

static int purge_bucket_instance(rgw::sal::RGWRadosStore *store, const RGWBucketInfo& bucket_info)
{
  int max_shards = (bucket_info.layout.current_index.layout.normal.num_shards > 0 ? bucket_info.layout.current_index.layout.normal.num_shards : 1);
  for (int i = 0; i < max_shards; i++) {
    RGWRados::BucketShard bs(store->getRados());
    int shard_id = (bucket_info.layout.current_index.layout.normal.num_shards > 0  ? i : -1);
    int ret = bs.init(bucket_info.bucket, shard_id, bucket_info.layout.current_index, nullptr);
    if (ret < 0) {
      cerr << "ERROR: bs.init(bucket=" << bucket_info.bucket << ", shard=" << shard_id
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

inline auto split_tenant(const std::string& bucket_name){
  auto p = bucket_name.find('/');
  if(p != std::string::npos) {
    return std::make_pair(bucket_name.substr(0,p), bucket_name.substr(p+1));
  }
  return std::make_pair(std::string(), bucket_name);
}

using bucket_instance_ls = std::vector<RGWBucketInfo>;
void get_stale_instances(rgw::sal::RGWRadosStore *store, const std::string& bucket_name,
                         const vector<std::string>& lst,
                         bucket_instance_ls& stale_instances)
{

  auto obj_ctx = store->svc()->sysobj->init_obj_ctx();

  bucket_instance_ls other_instances;
// first iterate over the entries, and pick up the done buckets; these
// are guaranteed to be stale
  for (const auto& bucket_instance : lst){
    RGWBucketInfo binfo;
    int r = store->getRados()->get_bucket_instance_info(obj_ctx, bucket_instance,
                                            binfo, nullptr,nullptr, null_yield);
    if (r < 0){
      // this can only happen if someone deletes us right when we're processing
      lderr(store->ctx()) << "Bucket instance is invalid: " << bucket_instance
                          << cpp_strerror(-r) << dendl;
      continue;
    }
    if (binfo.reshard_status == cls_rgw_reshard_status::DONE)
      stale_instances.emplace_back(std::move(binfo));
    else {
      other_instances.emplace_back(std::move(binfo));
    }
  }

  // Read the cur bucket info, if the bucket doesn't exist we can simply return
  // all the instances
  auto [tenant, bucket] = split_tenant(bucket_name);
  RGWBucketInfo cur_bucket_info;
  int r = store->getRados()->get_bucket_info(store->svc(), tenant, bucket, cur_bucket_info, nullptr, null_yield);
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
  if (cur_bucket_info.reshard_status == cls_rgw_reshard_status::IN_PROGRESS)
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

static int process_stale_instances(rgw::sal::RGWRadosStore *store, RGWBucketAdminOpState& op_state,
                                   RGWFormatterFlusher& flusher,
                                   std::function<void(const bucket_instance_ls&,
                                                      Formatter *,
                                                      rgw::sal::RGWRadosStore*)> process_f)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  int ret = store->ctl()->meta.mgr->list_keys_init("bucket.instance", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  bool truncated;

  formatter->open_array_section("keys");
  auto g = make_scope_guard([&store, &handle, &formatter]() {
                              store->ctl()->meta.mgr->list_keys_complete(handle);
                              formatter->close_section(); // keys
                              formatter->flush(cout);
                            });

  do {
    list<std::string> keys;

    ret = store->ctl()->meta.mgr->list_keys_next(handle, default_max_keys, keys, &truncated);
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

  return 0;
}

int RGWBucketAdminOp::list_stale_instances(rgw::sal::RGWRadosStore *store,
                                           RGWBucketAdminOpState& op_state,
                                           RGWFormatterFlusher& flusher)
{
  auto process_f = [](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      rgw::sal::RGWRadosStore*){
                     for (const auto& binfo: lst)
                       formatter->dump_string("key", binfo.bucket.get_key());
                   };
  return process_stale_instances(store, op_state, flusher, process_f);
}


int RGWBucketAdminOp::clear_stale_instances(rgw::sal::RGWRadosStore *store,
                                            RGWBucketAdminOpState& op_state,
                                            RGWFormatterFlusher& flusher)
{
  auto process_f = [](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      rgw::sal::RGWRadosStore *store){
                     for (const auto &binfo: lst) {
                       int ret = purge_bucket_instance(store, binfo);
                       if (ret == 0){
                         auto md_key = "bucket.instance:" + binfo.bucket.get_key();
                         ret = store->ctl()->meta.mgr->remove(md_key, null_yield);
                       }
                       formatter->open_object_section("delete_status");
                       formatter->dump_string("bucket_instance", binfo.bucket.get_key());
                       formatter->dump_int("status", -ret);
                       formatter->close_section();
                     }
                   };

  return process_stale_instances(store, op_state, flusher, process_f);
}

static int fix_single_bucket_lc(rgw::sal::RGWRadosStore *store,
                                const std::string& tenant_name,
                                const std::string& bucket_name)
{
  RGWBucketInfo bucket_info;
  map <std::string, bufferlist> bucket_attrs;
  int ret = store->getRados()->get_bucket_info(store->svc(), tenant_name, bucket_name,
                                   bucket_info, nullptr, null_yield, &bucket_attrs);
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

static void process_single_lc_entry(rgw::sal::RGWRadosStore *store,
				    Formatter *formatter,
                                    const std::string& tenant_name,
                                    const std::string& bucket_name)
{
  int ret = fix_single_bucket_lc(store, tenant_name, bucket_name);
  format_lc_status(formatter, tenant_name, bucket_name, -ret);
}

int RGWBucketAdminOp::fix_lc_shards(rgw::sal::RGWRadosStore *store,
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
    int ret = store->ctl()->meta.mgr->list_keys_init("bucket", marker, &handle);
    if (ret < 0) {
      std::cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    {
      formatter->open_array_section("lc_fix_status");
      auto sg = make_scope_guard([&store, &handle, &formatter](){
                                   store->ctl()->meta.mgr->list_keys_complete(handle);
                                   formatter->close_section(); // lc_fix_status
                                   formatter->flush(cout);
                                 });
      do {
        list<std::string> keys;
        ret = store->ctl()->meta.mgr->list_keys_next(handle, default_max_keys, keys, &truncated);
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

static bool has_object_expired(rgw::sal::RGWRadosStore *store,
			       const RGWBucketInfo& bucket_info,
			       const rgw_obj_key& key, utime_t& delete_at)
{
  rgw_obj obj(bucket_info.bucket, key);
  bufferlist delete_at_bl;

  int ret = rgw_object_get_attr(store, bucket_info, obj, RGW_ATTR_DELETE_AT, delete_at_bl, null_yield);
  if (ret < 0) {
    return false;  // no delete at attr, proceed
  }

  ret = decode_bl(delete_at_bl, delete_at);
  if (ret < 0) {
    return false;  // failed to parse
  }

  if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
    return true;
  }

  return false;
}

static int fix_bucket_obj_expiry(rgw::sal::RGWRadosStore *store,
				 const RGWBucketInfo& bucket_info,
				 RGWFormatterFlusher& flusher, bool dry_run)
{
  if (bucket_info.bucket.bucket_id == bucket_info.bucket.marker) {
    lderr(store->ctx()) << "Not a resharded bucket skipping" << dendl;
    return 0;  // not a resharded bucket, move along
  }

  Formatter *formatter = flusher.get_formatter();
  formatter->open_array_section("expired_deletion_status");
  auto sg = make_scope_guard([&formatter] {
			       formatter->close_section();
			       formatter->flush(std::cout);
			     });

  RGWRados::Bucket target(store->getRados(), bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.list_versions = bucket_info.versioned();
  list_op.params.allow_unordered = true;

  bool is_truncated {false};
  do {
    std::vector<rgw_bucket_dir_entry> objs;

    int ret = list_op.list_objects(listing_max_entries, &objs, nullptr,
				   &is_truncated, null_yield);
    if (ret < 0) {
      lderr(store->ctx()) << "ERROR failed to list objects in the bucket" << dendl;
      return ret;
    }
    for (const auto& obj : objs) {
      rgw_obj_key key(obj.key);
      utime_t delete_at;
      if (has_object_expired(store, bucket_info, key, delete_at)) {
	formatter->open_object_section("object_status");
	formatter->dump_string("object", key.name);
	formatter->dump_stream("delete_at") << delete_at;

	if (!dry_run) {
	  ret = rgw_remove_object(store, bucket_info, bucket_info.bucket, key);
	  formatter->dump_int("status", ret);
	}

	formatter->close_section();  // object_status
      }
    }
    formatter->flush(cout); // regularly flush every 1k entries
  } while (is_truncated);

  return 0;
}

int RGWBucketAdminOp::fix_obj_expiry(rgw::sal::RGWRadosStore *store,
				     RGWBucketAdminOpState& op_state,
				     RGWFormatterFlusher& flusher, bool dry_run)
{
  RGWBucket admin_bucket;
  int ret = admin_bucket.init(store, op_state, null_yield);
  if (ret < 0) {
    lderr(store->ctx()) << "failed to initialize bucket" << dendl;
    return ret;
  }

  return fix_bucket_obj_expiry(store, admin_bucket.get_bucket_info(), flusher, dry_run);
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


RGWDataChangesLog::RGWDataChangesLog(RGWSI_Zone *zone_svc, RGWSI_Cls *cls_svc)
  : cct(zone_svc->ctx()), changes(cct->_conf->rgw_data_log_changes_size)
{
  svc.zone = zone_svc;
  svc.cls = cls_svc;

  num_shards = cct->_conf->rgw_data_log_num_shards;

  oids = new string[num_shards];

  string prefix = cct->_conf->rgw_data_log_obj_prefix;

  if (prefix.empty()) {
    prefix = "data_log";
  }

  for (int i = 0; i < num_shards; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%s.%d", prefix.c_str(), i);
    oids[i] = buf;
  }

  renew_thread = new ChangesRenewThread(cct, this);
  renew_thread->create("rgw_dt_lg_renew");
}

int RGWDataChangesLog::choose_oid(const rgw_bucket_shard& bs) {
    const string& name = bs.bucket.name;
    int shard_shift = (bs.shard_id > 0 ? bs.shard_id : 0);
    uint32_t r = (ceph_str_hash_linux(name.c_str(), name.size()) + shard_shift) % num_shards;

    return (int)r;
}

int RGWDataChangesLog::renew_entries()
{
  if (!svc.zone->need_to_log_data())
    return 0;

  /* we can't keep the bucket name as part of the cls_log_entry, and we need
   * it later, so we keep two lists under the map */
  map<int, pair<list<rgw_bucket_shard>, list<cls_log_entry> > > m;

  lock.lock();
  map<rgw_bucket_shard, bool> entries;
  entries.swap(cur_cycle);
  lock.unlock();

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

    svc.cls->timelog.prepare_entry(entry, ut, section, change.key, bl);

    m[index].first.push_back(bs);
    m[index].second.emplace_back(std::move(entry));
  }

  map<int, pair<list<rgw_bucket_shard>, list<cls_log_entry> > >::iterator miter;
  for (miter = m.begin(); miter != m.end(); ++miter) {
    list<cls_log_entry>& entries = miter->second.second;

    real_time now = real_clock::now();

    int ret = svc.cls->timelog.add(oids[miter->first], entries, nullptr, true, null_yield);
    if (ret < 0) {
      /* we don't really need to have a special handling for failed cases here,
       * as this is just an optimization. */
      lderr(cct) << "ERROR: svc.cls->timelog.add() returned " << ret << dendl;
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
  ceph_assert(ceph_mutex_is_locked(lock));
  if (!changes.find(bs, status)) {
    status = ChangeStatusPtr(new ChangeStatus);
    changes.add(bs, status);
  }
}

void RGWDataChangesLog::register_renew(rgw_bucket_shard& bs)
{
  std::lock_guard l{lock};
  cur_cycle[bs] = true;
}

void RGWDataChangesLog::update_renewed(rgw_bucket_shard& bs, real_time& expiration)
{
  std::lock_guard l{lock};
  ChangeStatusPtr status;
  _get_change(bs, status);

  ldout(cct, 20) << "RGWDataChangesLog::update_renewd() bucket_name=" << bs.bucket.name << " shard_id=" << bs.shard_id << " expiration=" << expiration << dendl;
  status->cur_expiration = expiration;
}

int RGWDataChangesLog::get_log_shard_id(rgw_bucket& bucket, int shard_id) {
  rgw_bucket_shard bs(bucket, shard_id);

  return choose_oid(bs);
}

bool RGWDataChangesLog::filter_bucket(const rgw_bucket& bucket, optional_yield y) const
{
  if (!bucket_filter) {
    return true;
  }

  return bucket_filter->filter(bucket, y);
}

int RGWDataChangesLog::add_entry(const RGWBucketInfo& bucket_info, int shard_id) {
  auto& bucket = bucket_info.bucket;

  if (!filter_bucket(bucket, null_yield)) {
    return 0;
  }

  if (observer) {
    observer->on_bucket_changed(bucket.get_key());
  }

  rgw_bucket_shard bs(bucket, shard_id);

  int index = choose_oid(bs);
  mark_modified(index, bs);

  lock.lock();

  ChangeStatusPtr status;
  _get_change(bs, status);

  lock.unlock();

  real_time now = real_clock::now();

  status->lock.lock();

  ldout(cct, 20) << "RGWDataChangesLog::add_entry() bucket.name=" << bucket.name << " shard_id=" << shard_id << " now=" << now << " cur_expiration=" << status->cur_expiration << dendl;

  if (now < status->cur_expiration) {
    /* no need to send, recently completed */
    status->lock.unlock();

    register_renew(bs);
    return 0;
  }

  RefCountedCond *cond;

  if (status->pending) {
    cond = status->cond;

    ceph_assert(cond);

    status->cond->get();
    status->lock.unlock();

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

    status->lock.unlock();
  
    bufferlist bl;
    rgw_data_change change;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = now;
    encode(change, bl);
    string section;

    ldout(cct, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now << " cur_expiration=" << expiration << dendl;

    ret = svc.cls->timelog.add(oid, now, section, change.key, bl, null_yield);

    now = real_clock::now();

    status->lock.lock();

  } while (!ret && real_clock::now() > expiration);

  cond = status->cond;

  status->pending = false;
  status->cur_expiration = status->cur_sent; /* time of when operation started, not completed */
  status->cur_expiration += make_timespan(cct->_conf->rgw_data_log_window);
  status->cond = NULL;
  status->lock.unlock();

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

  int ret = svc.cls->timelog.list(oids[shard], start_time, end_time,
				 max_entries, log_entries, marker,
				 out_marker, truncated, null_yield);
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

  int ret = svc.cls->timelog.info(oid, &header, null_yield);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  info->marker = header.max_marker;
  info->last_update = header.max_time.to_real_time();

  return 0;
}

int RGWDataChangesLog::trim_entries(int shard_id, const real_time& start_time, const real_time& end_time,
                                    const string& start_marker, const string& end_marker)
{
  if (shard_id > num_shards)
    return -EINVAL;

  return svc.cls->timelog.trim(oids[shard_id], start_time, end_time,
                               start_marker, end_marker, nullptr, null_yield);
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
  for (;;) {
    dout(2) << "RGWDataChangesLog::ChangesRenewThread: start" << dendl;
    int r = log->renew_entries();
    if (r < 0) {
      dout(0) << "ERROR: RGWDataChangesLog::renew_entries returned error r=" << r << dendl;
    }

    if (log->going_down())
      break;

    int interval = cct->_conf->rgw_data_log_window * 3 / 4;
    std::unique_lock locker{lock};
    cond.wait_for(locker, std::chrono::seconds(interval));
  }

  return NULL;
}

void RGWDataChangesLog::ChangesRenewThread::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}

void RGWDataChangesLog::mark_modified(int shard_id, const rgw_bucket_shard& bs)
{
  auto key = bs.get_key();
  {
    std::shared_lock rl{modified_lock}; // read lock to check for existence
    auto shard = modified_shards.find(shard_id);
    if (shard != modified_shards.end() && shard->second.count(key)) {
      return;
    }
  }

  std::unique_lock wl{modified_lock}; // write lock for insertion
  modified_shards[shard_id].insert(key);
}

void RGWDataChangesLog::read_clear_modified(map<int, set<string> > &modified)
{
  std::unique_lock wl{modified_lock};
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

class RGWBucketMetadataHandler : public RGWBucketMetadataHandlerBase {
public:
  struct Svc {
    RGWSI_Bucket *bucket{nullptr};
  } svc;

  struct Ctl {
    RGWBucketCtl *bucket{nullptr};
  } ctl;

  RGWBucketMetadataHandler() {}

  void init(RGWSI_Bucket *bucket_svc,
            RGWBucketCtl *bucket_ctl) override {
    base_init(bucket_svc->ctx(),
              bucket_svc->get_ep_be_handler().get());
    svc.bucket = bucket_svc;
    ctl.bucket = bucket_ctl;
  }

  string get_type() override { return "bucket"; }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) override {
    RGWBucketEntryPoint be;

    try {
      decode_json_obj(be, jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }

    return new RGWBucketEntryMetadataObject(be, objv, mtime);
  }

  int do_get(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWMetadataObject **obj, optional_yield y) override {
    RGWObjVersionTracker ot;
    RGWBucketEntryPoint be;

    real_time mtime;
    map<string, bufferlist> attrs;

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &ot, &mtime, &attrs, y);
    if (ret < 0)
      return ret;

    RGWBucketEntryMetadataObject *mdo = new RGWBucketEntryMetadataObject(be, ot.read_version, mtime, std::move(attrs));

    *obj = mdo;

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker,
             optional_yield y,
             RGWMDLogSyncType type) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y) override {
    RGWBucketEntryPoint be;

    real_time orig_mtime;

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &objv_tracker, &orig_mtime, nullptr, y);
    if (ret < 0)
      return ret;

    /*
     * We're unlinking the bucket but we don't want to update the entrypoint here - we're removing
     * it immediately and don't want to invalidate our cached objv_version or the bucket obj removal
     * will incorrectly fail.
     */
    ret = ctl.bucket->unlink_bucket(be.owner, be.bucket, y, false);
    if (ret < 0) {
      lderr(svc.bucket->ctx()) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    ret = svc.bucket->remove_bucket_entrypoint_info(ctx, entry, &objv_tracker, y);
    if (ret < 0) {
      lderr(svc.bucket->ctx()) << "could not delete bucket=" << entry << dendl;
    }
    /* idempotent */
    return 0;
  }

  int call(std::function<int(RGWSI_Bucket_EP_Ctx& ctx)> f) {
    return call(nullopt, f);
  }

  int call(std::optional<RGWSI_MetaBackend_CtxParams> bectx_params,
           std::function<int(RGWSI_Bucket_EP_Ctx& ctx)> f) {
    return be_handler->call(bectx_params, [&](RGWSI_MetaBackend_Handler::Op *op) {
      RGWSI_Bucket_EP_Ctx ctx(op->ctx());
      return f(ctx);
    });
  }
};

class RGWMetadataHandlerPut_Bucket : public RGWMetadataHandlerPut_SObj
{
  RGWBucketMetadataHandler *bhandler;
  RGWBucketEntryMetadataObject *obj;
public:
  RGWMetadataHandlerPut_Bucket(RGWBucketMetadataHandler *_handler,
                               RGWSI_MetaBackend_Handler::Op *op, string& entry,
                               RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
			       optional_yield y,
                               RGWMDLogSyncType type) : RGWMetadataHandlerPut_SObj(_handler, op, entry, obj, objv_tracker, y, type),
                                                        bhandler(_handler) {
    obj = static_cast<RGWBucketEntryMetadataObject *>(_obj);
  }
  ~RGWMetadataHandlerPut_Bucket() {}

  void encode_obj(bufferlist *bl) override {
    obj->get_ep().encode(*bl);
  }

  int put_checked() override;
  int put_post() override;
};

int RGWBucketMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
                                     RGWMetadataObject *obj,
                                     RGWObjVersionTracker& objv_tracker,
				     optional_yield y,
                                     RGWMDLogSyncType type)
{
  RGWMetadataHandlerPut_Bucket put_op(this, op, entry, obj, objv_tracker, y, type);
  return do_put_operate(&put_op);
}

int RGWMetadataHandlerPut_Bucket::put_checked()
{
  RGWBucketEntryMetadataObject *orig_obj = static_cast<RGWBucketEntryMetadataObject *>(old_obj);

  if (orig_obj) {
    obj->set_pattrs(&orig_obj->get_attrs());
  }

  auto& be = obj->get_ep();
  auto mtime = obj->get_mtime();
  auto pattrs = obj->get_pattrs();

  RGWSI_Bucket_EP_Ctx ctx(op->ctx());

  return bhandler->svc.bucket->store_bucket_entrypoint_info(ctx, entry,
                                                           be,
                                                           false,
                                                           mtime,
                                                           pattrs,
                                                           &objv_tracker,
							   y);
}

int RGWMetadataHandlerPut_Bucket::put_post()
{
  auto& be = obj->get_ep();

  int ret;

  /* link bucket */
  if (be.linked) {
    ret = bhandler->ctl.bucket->link_bucket(be.owner, be.bucket, be.creation_time, y, false);
  } else {
    ret = bhandler->ctl.bucket->unlink_bucket(be.owner, be.bucket, y, false);
  }

  return ret;
}

static void get_md5_digest(const RGWBucketEntryPoint *be, string& md5_digest) {

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
  RGWArchiveBucketMetadataHandler() {}

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y) override {
    auto cct = svc.bucket->ctx();

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    ldout(cct, 5) << "SKIP: bucket removal is not allowed on archive zone: bucket:" << entry << " ... proceeding to rename" << dendl;

    string tenant_name, bucket_name;
    parse_bucket(entry, &tenant_name, &bucket_name);
    rgw_bucket entry_bucket;
    entry_bucket.tenant = tenant_name;
    entry_bucket.name = bucket_name;

    real_time mtime;

    /* read original entrypoint */

    RGWBucketEntryPoint be;
    map<string, bufferlist> attrs;
    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &objv_tracker, &mtime, &attrs, y);
    if (ret < 0) {
        return ret;
    }

    string bi_meta_name = RGWSI_Bucket::get_bi_meta_key(be.bucket);

    /* read original bucket instance info */

    map<string, bufferlist> attrs_m;
    ceph::real_time orig_mtime;
    RGWBucketInfo old_bi;

    ret = ctl.bucket->read_bucket_instance_info(be.bucket, &old_bi, y, RGWBucketCtl::BucketInstance::GetParams()
                                                                    .set_mtime(&orig_mtime)
                                                                    .set_attrs(&attrs_m));
    if (ret < 0) {
        return ret;
    }

    archive_meta_info ami;

    if (!ami.from_attrs(svc.bucket->ctx(), attrs_m)) {
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

    ret = ctl.bucket->store_bucket_instance_info(be.bucket, new_bi, y, RGWBucketCtl::BucketInstance::PutParams()
                                                                    .set_exclusive(false)
                                                                    .set_mtime(orig_mtime)
                                                                    .set_attrs(&attrs_m)
                                                                    .set_orig_info(&old_bi));
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to put new bucket instance info for bucket=" << new_bi.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* store a new entrypoint */

    RGWObjVersionTracker ot;
    ot.generate_new_write_ver(cct);

    ret = svc.bucket->store_bucket_entrypoint_info(ctx, RGWSI_Bucket::get_entrypoint_meta_key(new_be.bucket),
                                                   new_be, true, mtime, &attrs, nullptr, y);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* link new bucket */

    ret = ctl.bucket->link_bucket(new_be.owner, new_be.bucket, new_be.creation_time, y, false);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to link new bucket for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* clean up old stuff */

    ret = ctl.bucket->unlink_bucket(be.owner, entry_bucket, y, false);
    if (ret < 0) {
        lderr(cct) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    // if (ret == -ECANCELED) it means that there was a race here, and someone
    // wrote to the bucket entrypoint just before we removed it. The question is
    // whether it was a newly created bucket entrypoint ...  in which case we
    // should ignore the error and move forward, or whether it is a higher version
    // of the same bucket instance ... in which we should retry
    ret = svc.bucket->remove_bucket_entrypoint_info(ctx,
                                                    RGWSI_Bucket::get_entrypoint_meta_key(be.bucket),
                                                    &objv_tracker,
                                                    y);
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    ret = ctl.bucket->remove_bucket_instance_info(be.bucket, old_bi, y);
    if (ret < 0) {
        lderr(cct) << "could not delete bucket=" << entry << dendl;
    }


    /* idempotent */

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker,
             optional_yield y,
             RGWMDLogSyncType type) override {
    if (entry.find("-deleted-") != string::npos) {
      RGWObjVersionTracker ot;
      RGWMetadataObject *robj;
      int ret = do_get(op, entry, &robj, y);
      if (ret != -ENOENT) {
        if (ret < 0) {
          return ret;
        }
        ot.read_version = robj->get_version();
        delete robj;

        ret = do_remove(op, entry, ot, y);
        if (ret < 0) {
          return ret;
        }
      }
    }

    return RGWBucketMetadataHandler::do_put(op, entry, obj,
                                            objv_tracker, y, type);
  }

};

class RGWBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandlerBase {
  int read_bucket_instance_entry(RGWSI_Bucket_BI_Ctx& ctx,
                                 const string& entry,
                                 RGWBucketCompleteInfo *bi,
                                 ceph::real_time *pmtime,
                                 optional_yield y) {
    return svc.bucket->read_bucket_instance_info(ctx,
                                                 entry,
                                                 &bi->info,
                                                 pmtime, &bi->attrs,
                                                 y);
  }

public:
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Bucket *bucket{nullptr};
    RGWSI_BucketIndex *bi{nullptr};
  } svc;

  RGWBucketInstanceMetadataHandler() {}

  void init(RGWSI_Zone *zone_svc,
           RGWSI_Bucket *bucket_svc,
           RGWSI_BucketIndex *bi_svc) {
    base_init(bucket_svc->ctx(),
              bucket_svc->get_bi_be_handler().get());
    svc.zone = zone_svc;
    svc.bucket = bucket_svc;
    svc.bi = bi_svc;
  }

  string get_type() override { return "bucket.instance"; }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) override {
    RGWBucketCompleteInfo bci;

    try {
      decode_json_obj(bci, jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }

    return new RGWBucketInstanceMetadataObject(bci, objv, mtime);
  }

  int do_get(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWMetadataObject **obj, optional_yield y) override {
    RGWBucketCompleteInfo bci;
    real_time mtime;

    RGWSI_Bucket_BI_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_instance_info(ctx, entry, &bci.info, &mtime, &bci.attrs, y);
    if (ret < 0)
      return ret;

    RGWBucketInstanceMetadataObject *mdo = new RGWBucketInstanceMetadataObject(bci, bci.info.objv_tracker.read_version, mtime);

    *obj = mdo;

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
	     optional_yield y,
             RGWMDLogSyncType sync_type) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y) override {
    RGWBucketCompleteInfo bci;

    RGWSI_Bucket_BI_Ctx ctx(op->ctx());

    int ret = read_bucket_instance_entry(ctx, entry, &bci, nullptr, y);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    return svc.bucket->remove_bucket_instance_info(ctx, entry, bci.info, &bci.info.objv_tracker, y);
  }

  int call(std::function<int(RGWSI_Bucket_BI_Ctx& ctx)> f) {
    return call(nullopt, f);
  }

  int call(std::optional<RGWSI_MetaBackend_CtxParams> bectx_params,
           std::function<int(RGWSI_Bucket_BI_Ctx& ctx)> f) {
    return be_handler->call(bectx_params, [&](RGWSI_MetaBackend_Handler::Op *op) {
      RGWSI_Bucket_BI_Ctx ctx(op->ctx());
      return f(ctx);
    });
  }
};

class RGWMetadataHandlerPut_BucketInstance : public RGWMetadataHandlerPut_SObj
{
  CephContext *cct;
  RGWBucketInstanceMetadataHandler *bihandler;
  RGWBucketInstanceMetadataObject *obj;
public:
  RGWMetadataHandlerPut_BucketInstance(CephContext *cct,
                                       RGWBucketInstanceMetadataHandler *_handler,
                                       RGWSI_MetaBackend_Handler::Op *_op, string& entry,
                                       RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
				       optional_yield y,
                                       RGWMDLogSyncType type) : RGWMetadataHandlerPut_SObj(_handler, _op, entry, obj, objv_tracker, y, type),
                                                                bihandler(_handler) {
    obj = static_cast<RGWBucketInstanceMetadataObject *>(_obj);

    auto& bci = obj->get_bci();
    obj->set_pattrs(&bci.attrs);
  }

  void encode_obj(bufferlist *bl) override {
    obj->get_bucket_info().encode(*bl);
  }

  int put_check() override;
  int put_checked() override;
  int put_post() override;
};

int RGWBucketInstanceMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op,
                                             string& entry,
                                             RGWMetadataObject *obj,
                                             RGWObjVersionTracker& objv_tracker,
                                             optional_yield y,
                                             RGWMDLogSyncType type)
{
  RGWMetadataHandlerPut_BucketInstance put_op(svc.bucket->ctx(), this, op, entry, obj,
                                              objv_tracker, y, type);
  return do_put_operate(&put_op);
}

int RGWMetadataHandlerPut_BucketInstance::put_check()
{
  int ret;

  RGWBucketCompleteInfo& bci = obj->get_bci();

  RGWBucketInstanceMetadataObject *orig_obj = static_cast<RGWBucketInstanceMetadataObject *>(old_obj);

  RGWBucketCompleteInfo *old_bci = (orig_obj ? &orig_obj->get_bci() : nullptr);

  bool exists = (!!orig_obj);

  if (!exists || old_bci->info.bucket.bucket_id != bci.info.bucket.bucket_id) {
    /* a new bucket, we need to select a new bucket placement for it */
    string tenant_name;
    string bucket_name;
    string bucket_instance;
    parse_bucket(entry, &tenant_name, &bucket_name, &bucket_instance);

    RGWZonePlacementInfo rule_info;
    bci.info.bucket.name = bucket_name;
    bci.info.bucket.bucket_id = bucket_instance;
    bci.info.bucket.tenant = tenant_name;
    // if the sync module never writes data, don't require the zone to specify all placement targets
    if (bihandler->svc.zone->sync_module_supports_writes()) {
      ret = bihandler->svc.zone->select_bucket_location_by_rule(bci.info.placement_rule, &rule_info);
      if (ret < 0) {
        ldout(cct, 0) << "ERROR: select_bucket_placement() returned " << ret << dendl;
        return ret;
      }
    }
    bci.info.layout.current_index.layout.type = rule_info.index_type;
  } else {
    /* existing bucket, keep its placement */
    bci.info.bucket.explicit_placement = old_bci->info.bucket.explicit_placement;
    bci.info.placement_rule = old_bci->info.placement_rule;
  }

  /* record the read version (if any), store the new version */
  bci.info.objv_tracker.read_version = objv_tracker.read_version;
  bci.info.objv_tracker.write_version = objv_tracker.write_version;

  return 0;
}

int RGWMetadataHandlerPut_BucketInstance::put_checked()
{
  RGWBucketInstanceMetadataObject *orig_obj = static_cast<RGWBucketInstanceMetadataObject *>(old_obj);

  RGWBucketInfo *orig_info = (orig_obj ? &orig_obj->get_bucket_info() : nullptr);

  auto& info = obj->get_bucket_info();
  auto mtime = obj->get_mtime();
  auto pattrs = obj->get_pattrs();

  RGWSI_Bucket_BI_Ctx ctx(op->ctx());

  return bihandler->svc.bucket->store_bucket_instance_info(ctx,
                                                         entry,
                                                         info,
                                                         orig_info,
                                                         false,
                                                         mtime,
                                                         pattrs,
							 y);
}

int RGWMetadataHandlerPut_BucketInstance::put_post()
{
  RGWBucketCompleteInfo& bci = obj->get_bci();

  objv_tracker = bci.info.objv_tracker;

  int ret = bihandler->svc.bi->init_index(bci.info);
  if (ret < 0) {
    return ret;
  }

  return STATUS_APPLIED;
}

class RGWArchiveBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandler {
public:
  RGWArchiveBucketInstanceMetadataHandler() {}

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y) override {
    ldout(cct, 0) << "SKIP: bucket instance removal is not allowed on archive zone: bucket.instance:" << entry << dendl;
    return 0;
  }
};

bool RGWBucketCtl::DataLogFilter::filter(const rgw_bucket& bucket, optional_yield y) const
{
  return bucket_ctl->bucket_exports_data(bucket, null_yield);
}

RGWBucketCtl::RGWBucketCtl(RGWSI_Zone *zone_svc,
                           RGWSI_Bucket *bucket_svc,
                           RGWSI_Bucket_Sync *bucket_sync_svc,
                           RGWSI_BucketIndex *bi_svc) : cct(zone_svc->ctx()),
                                                        datalog_filter(this)
{
  svc.zone = zone_svc;
  svc.bucket = bucket_svc;
  svc.bucket_sync = bucket_sync_svc;
  svc.bi = bi_svc;
}

void RGWBucketCtl::init(RGWUserCtl *user_ctl,
                        RGWBucketMetadataHandler *_bm_handler,
                        RGWBucketInstanceMetadataHandler *_bmi_handler,
                        RGWDataChangesLog *datalog)
{
  ctl.user = user_ctl;

  bm_handler = _bm_handler;
  bmi_handler = _bmi_handler;

  bucket_be_handler = bm_handler->get_be_handler();
  bi_be_handler = bmi_handler->get_be_handler();

  datalog->set_bucket_filter(&datalog_filter);
}

int RGWBucketCtl::call(std::function<int(RGWSI_Bucket_X_Ctx& ctx)> f) {
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ep_ctx) {
    return bmi_handler->call([&](RGWSI_Bucket_BI_Ctx& bi_ctx) {
      RGWSI_Bucket_X_Ctx ctx{ep_ctx, bi_ctx};
      return f(ctx);
    });
  });
}

int RGWBucketCtl::read_bucket_entrypoint_info(const rgw_bucket& bucket,
                                              RGWBucketEntryPoint *info,
                                              optional_yield y,
                                              const Bucket::GetParams& params)
{
  return bm_handler->call(params.bectx_params, [&](RGWSI_Bucket_EP_Ctx& ctx) {
    return svc.bucket->read_bucket_entrypoint_info(ctx,
                                                   RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                   info,
                                                   params.objv_tracker,
                                                   params.mtime,
                                                   params.attrs,
						   y,
                                                   params.cache_info,
                                                   params.refresh_version);
  });
}

int RGWBucketCtl::store_bucket_entrypoint_info(const rgw_bucket& bucket,
                                               RGWBucketEntryPoint& info,
                                               optional_yield y,
                                               const Bucket::PutParams& params)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return svc.bucket->store_bucket_entrypoint_info(ctx,
                                                    RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                    info,
                                                    params.exclusive,
                                                    params.mtime,
                                                    params.attrs,
                                                    params.objv_tracker,
                                                    y);
  });
}

int RGWBucketCtl::remove_bucket_entrypoint_info(const rgw_bucket& bucket,
                                                optional_yield y,
                                                const Bucket::RemoveParams& params)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return svc.bucket->remove_bucket_entrypoint_info(ctx,
                                                     RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                     params.objv_tracker,
						     y);
  });
}

int RGWBucketCtl::read_bucket_instance_info(const rgw_bucket& bucket,
                                            RGWBucketInfo *info,
                                            optional_yield y,
                                            const BucketInstance::GetParams& params)
{
  int ret = bmi_handler->call(params.bectx_params, [&](RGWSI_Bucket_BI_Ctx& ctx) {
    return svc.bucket->read_bucket_instance_info(ctx,
                                                 RGWSI_Bucket::get_bi_meta_key(bucket),
                                                 info,
                                                 params.mtime,
                                                 params.attrs,
						 y,
                                                 params.cache_info,
                                                 params.refresh_version);
  });

  if (ret < 0) {
    return ret;
  }

  if (params.objv_tracker) {
    *params.objv_tracker = info->objv_tracker;
  }

  return 0;
}

int RGWBucketCtl::read_bucket_info(const rgw_bucket& bucket,
                                   RGWBucketInfo *info,
                                   optional_yield y,
                                   const BucketInstance::GetParams& params,
                                   RGWObjVersionTracker *ep_objv_tracker)
{
  const rgw_bucket *b = &bucket;

  std::optional<RGWBucketEntryPoint> ep;

  if (b->bucket_id.empty()) {
    ep.emplace();

    int r = read_bucket_entrypoint_info(*b, &(*ep), y, RGWBucketCtl::Bucket::GetParams()
                                                    .set_bectx_params(params.bectx_params)
                                                    .set_objv_tracker(ep_objv_tracker));
    if (r < 0) {
      return r;
    }

    b = &ep->bucket;
  }

  int ret = bmi_handler->call(params.bectx_params, [&](RGWSI_Bucket_BI_Ctx& ctx) {
    return svc.bucket->read_bucket_instance_info(ctx,
                                                 RGWSI_Bucket::get_bi_meta_key(*b),
                                                 info,
                                                 params.mtime,
                                                 params.attrs,
						 y,
                                                 params.cache_info,
                                                 params.refresh_version);
  });

  if (ret < 0) {
    return ret;
  }

  if (params.objv_tracker) {
    *params.objv_tracker = info->objv_tracker;
  }

  return 0;
}

int RGWBucketCtl::do_store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                                const rgw_bucket& bucket,
                                                RGWBucketInfo& info,
                                                optional_yield y,
                                                const BucketInstance::PutParams& params)
{
  if (params.objv_tracker) {
    info.objv_tracker = *params.objv_tracker;
  }

  return svc.bucket->store_bucket_instance_info(ctx,
                                                RGWSI_Bucket::get_bi_meta_key(bucket),
                                                info,
                                                params.orig_info,
                                                params.exclusive,
                                                params.mtime,
                                                params.attrs,
                                                y);
}

int RGWBucketCtl::store_bucket_instance_info(const rgw_bucket& bucket,
                                            RGWBucketInfo& info,
                                            optional_yield y,
                                            const BucketInstance::PutParams& params)
{
  return bmi_handler->call([&](RGWSI_Bucket_BI_Ctx& ctx) {
    return do_store_bucket_instance_info(ctx, bucket, info, y, params);
  });
}

int RGWBucketCtl::remove_bucket_instance_info(const rgw_bucket& bucket,
                                              RGWBucketInfo& info,
                                              optional_yield y,
                                              const BucketInstance::RemoveParams& params)
{
  if (params.objv_tracker) {
    info.objv_tracker = *params.objv_tracker;
  }

  return bmi_handler->call([&](RGWSI_Bucket_BI_Ctx& ctx) {
    return svc.bucket->remove_bucket_instance_info(ctx,
                                                   RGWSI_Bucket::get_bi_meta_key(bucket),
                                                   info,
                                                   &info.objv_tracker,
                                                   y);
  });
}

int RGWBucketCtl::do_store_linked_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                              RGWBucketInfo& info,
                                              RGWBucketInfo *orig_info,
                                              bool exclusive, real_time mtime,
                                              obj_version *pep_objv,
                                              map<string, bufferlist> *pattrs,
                                              bool create_entry_point,
					      optional_yield y)
{
  bool create_head = !info.has_instance_obj || create_entry_point;

  int ret = svc.bucket->store_bucket_instance_info(ctx.bi,
                                                   RGWSI_Bucket::get_bi_meta_key(info.bucket),
                                                   info,
                                                   orig_info,
                                                   exclusive,
                                                   mtime, pattrs,
						   y);
  if (ret < 0) {
    return ret;
  }

  if (!create_head)
    return 0; /* done! */

  RGWBucketEntryPoint entry_point;
  entry_point.bucket = info.bucket;
  entry_point.owner = info.owner;
  entry_point.creation_time = info.creation_time;
  entry_point.linked = true;
  RGWObjVersionTracker ot;
  if (pep_objv && !pep_objv->tag.empty()) {
    ot.write_version = *pep_objv;
  } else {
    ot.generate_new_write_ver(cct);
    if (pep_objv) {
      *pep_objv = ot.write_version;
    }
  }
  ret = svc.bucket->store_bucket_entrypoint_info(ctx.ep,
                                                 RGWSI_Bucket::get_entrypoint_meta_key(info.bucket),
                                                 entry_point,
                                                 exclusive,
                                                 mtime,
                                                 pattrs,
                                                 &ot,
						 y);
  if (ret < 0)
    return ret;

  return 0;
}
int RGWBucketCtl::convert_old_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                          const rgw_bucket& bucket,
                                          optional_yield y)
{
  RGWBucketEntryPoint entry_point;
  real_time ep_mtime;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  RGWBucketInfo info;
  auto cct = svc.bucket->ctx();

  ldout(cct, 10) << "RGWRados::convert_old_bucket_info(): bucket=" << bucket << dendl;

  int ret = svc.bucket->read_bucket_entrypoint_info(ctx.ep,
                                                    RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                    &entry_point, &ot, &ep_mtime, &attrs, y);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: get_bucket_entrypoint_info() returned " << ret << " bucket=" << bucket << dendl;
    return ret;
  }

  if (!entry_point.has_bucket_info) {
    /* already converted! */
    return 0;
  }

  info = entry_point.old_bucket_info;

  ot.generate_new_write_ver(cct);

  ret = do_store_linked_bucket_info(ctx, info, nullptr, false, ep_mtime, &ot.write_version, &attrs, true, y);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: failed to put_linked_bucket_info(): " << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWBucketCtl::set_bucket_instance_attrs(RGWBucketInfo& bucket_info,
                                            map<string, bufferlist>& attrs,
                                            RGWObjVersionTracker *objv_tracker,
                                            optional_yield y)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    rgw_bucket& bucket = bucket_info.bucket;

    if (!bucket_info.has_instance_obj) {
      /* an old bucket object, need to convert it */
        int ret = convert_old_bucket_info(ctx, bucket, y);
        if (ret < 0) {
          ldout(cct, 0) << "ERROR: failed converting old bucket info: " << ret << dendl;
          return ret;
        }
    }

    return do_store_bucket_instance_info(ctx.bi,
                                         bucket,
                                         bucket_info,
                                         y,
                                         BucketInstance::PutParams().set_attrs(&attrs)
                                                                    .set_objv_tracker(objv_tracker)
                                                                    .set_orig_info(&bucket_info));
    });
}


int RGWBucketCtl::link_bucket(const rgw_user& user_id,
                              const rgw_bucket& bucket,
                              ceph::real_time creation_time,
			      optional_yield y,
                              bool update_entrypoint,
                              rgw_ep_info *pinfo)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return do_link_bucket(ctx, user_id, bucket, creation_time, y,
                          update_entrypoint, pinfo);
  });
}

int RGWBucketCtl::do_link_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                                 const rgw_user& user_id,
                                 const rgw_bucket& bucket,
                                 ceph::real_time creation_time,
				 optional_yield y,
                                 bool update_entrypoint,
                                 rgw_ep_info *pinfo)
{
  int ret;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;
  RGWObjVersionTracker& rot = (pinfo) ? pinfo->ep_objv : ot;
  map<string, bufferlist> attrs, *pattrs = nullptr;
  string meta_key;

  if (update_entrypoint) {
    meta_key = RGWSI_Bucket::get_entrypoint_meta_key(bucket);
    if (pinfo) {
      ep = pinfo->ep;
      pattrs = &pinfo->attrs;
    } else {
      ret = svc.bucket->read_bucket_entrypoint_info(ctx,
                                                    meta_key,
                                                    &ep, &rot,
                                                    nullptr, &attrs,
                                                    y);
      if (ret < 0 && ret != -ENOENT) {
        ldout(cct, 0) << "ERROR: store->get_bucket_entrypoint_info() returned: "
                      << cpp_strerror(-ret) << dendl;
      }
      pattrs = &attrs;
    }
  }

  ret = ctl.user->add_bucket(user_id, bucket, creation_time);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: error adding bucket to user directory:"
		  << " user=" << user_id
                  << " bucket=" << bucket
		  << " err=" << cpp_strerror(-ret)
		  << dendl;
    goto done_err;
  }

  if (!update_entrypoint)
    return 0;

  ep.linked = true;
  ep.owner = user_id;
  ep.bucket = bucket;
  ret = svc.bucket->store_bucket_entrypoint_info(
    ctx, meta_key, ep, false, real_time(), pattrs, &rot, y);
  if (ret < 0)
    goto done_err;

  return 0;

done_err:
  int r = do_unlink_bucket(ctx, user_id, bucket, y, true);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: failed unlinking bucket on error cleanup: "
                           << cpp_strerror(-r) << dendl;
  }
  return ret;
}

int RGWBucketCtl::unlink_bucket(const rgw_user& user_id, const rgw_bucket& bucket, optional_yield y, bool update_entrypoint)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return do_unlink_bucket(ctx, user_id, bucket, y, update_entrypoint);
  });
}

int RGWBucketCtl::do_unlink_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                                   const rgw_user& user_id,
                                   const rgw_bucket& bucket,
				   optional_yield y,
                                   bool update_entrypoint)
{
  int ret = ctl.user->remove_bucket(user_id, bucket);
  if (ret < 0) {
    ldout(cct, 0) << "ERROR: error removing bucket from directory: "
        << cpp_strerror(-ret)<< dendl;
  }

  if (!update_entrypoint)
    return 0;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  string meta_key = RGWSI_Bucket::get_entrypoint_meta_key(bucket);
  ret = svc.bucket->read_bucket_entrypoint_info(ctx, meta_key, &ep, &ot, nullptr, &attrs, y);
  if (ret == -ENOENT)
    return 0;
  if (ret < 0)
    return ret;

  if (!ep.linked)
    return 0;

  if (ep.owner != user_id) {
    ldout(cct, 0) << "bucket entry point user mismatch, can't unlink bucket: " << ep.owner << " != " << user_id << dendl;
    return -EINVAL;
  }

  ep.linked = false;
  return svc.bucket->store_bucket_entrypoint_info(ctx, meta_key, ep, false, real_time(), &attrs, &ot, y);
}

int RGWBucketCtl::set_acl(ACLOwner& owner, rgw_bucket& bucket,
                          RGWBucketInfo& bucket_info, bufferlist& bl,
                          optional_yield y)
{
  // set owner and acl
  bucket_info.owner = owner.get_id();
  std::map<std::string, bufferlist> attrs{{RGW_ATTR_ACL, bl}};

  int r = store_bucket_instance_info(bucket, bucket_info, y,
                                     BucketInstance::PutParams().set_attrs(&attrs));
  if (r < 0) {
    cerr << "ERROR: failed to set bucket owner: " << cpp_strerror(-r) << std::endl;
    return r;
  }
  
  return 0;
}

// TODO: remove RGWRados dependency for bucket listing
int RGWBucketCtl::chown(rgw::sal::RGWRadosStore *store, RGWBucketInfo& bucket_info,
                        const rgw_user& user_id, const std::string& display_name,
                        const std::string& marker, optional_yield y)
{
  RGWObjectCtx obj_ctx(store);
  std::vector<rgw_bucket_dir_entry> objs;
  map<string, bool> common_prefixes;

  RGWRados::Bucket target(store->getRados(), bucket_info);
  RGWRados::Bucket::List list_op(&target);

  list_op.params.list_versions = true;
  list_op.params.allow_unordered = true;
  list_op.params.marker = marker;

  bool is_truncated = false;
  int count = 0;
  int max_entries = 1000;

  //Loop through objects and update object acls to point to bucket owner

  do {
    objs.clear();
    int ret = list_op.list_objects(max_entries, &objs, &common_prefixes, &is_truncated, y);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: list objects failed: " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    list_op.params.marker = list_op.get_next_marker();
    count += objs.size();

    for (const auto& obj : objs) {

      rgw_obj r_obj(bucket_info.bucket, obj.key);
      RGWRados::Object op_target(store->getRados(), bucket_info, obj_ctx, r_obj);
      RGWRados::Object::Read read_op(&op_target);

      map<string, bufferlist> attrs;
      read_op.params.attrs = &attrs;
      ret = read_op.prepare(y);
      if (ret < 0){
        ldout(store->ctx(), 0) << "ERROR: failed to read object " << obj.key.name << cpp_strerror(-ret) << dendl;
        continue;
      }
      const auto& aiter = attrs.find(RGW_ATTR_ACL);
      if (aiter == attrs.end()) {
        ldout(store->ctx(), 0) << "ERROR: no acls found for object " << obj.key.name << " .Continuing with next object." << dendl;
        continue;
      } else {
        bufferlist& bl = aiter->second;
        RGWAccessControlPolicy policy(store->ctx());
        ACLOwner owner;
        try {
          decode(policy, bl);
          owner = policy.get_owner();
        } catch (buffer::error& err) {
          ldout(store->ctx(), 0) << "ERROR: decode policy failed" << err.what()
				 << dendl;
          return -EIO;
        }

        //Get the ACL from the policy
        RGWAccessControlList& acl = policy.get_acl();

        //Remove grant that is set to old owner
        acl.remove_canon_user_grant(owner.get_id());

        //Create a grant and add grant
        ACLGrant grant;
        grant.set_canon(user_id, display_name, RGW_PERM_FULL_CONTROL);
        acl.add_grant(&grant);

        //Update the ACL owner to the new user
        owner.set_id(user_id);
        owner.set_name(display_name);
        policy.set_owner(owner);

        bl.clear();
        encode(policy, bl);

        obj_ctx.set_atomic(r_obj);
        ret = store->getRados()->set_attr(&obj_ctx, bucket_info, r_obj, RGW_ATTR_ACL, bl);
        if (ret < 0) {
          ldout(store->ctx(), 0) << "ERROR: modify attr failed " << cpp_strerror(-ret) << dendl;
          return ret;
        }
      }
    }
    cerr << count << " objects processed in " << bucket_info.bucket.name
        << ". Next marker " << list_op.params.marker.name << std::endl;
  } while(is_truncated);
  return 0;
}

int RGWBucketCtl::read_bucket_stats(const rgw_bucket& bucket,
                                    RGWBucketEnt *result,
                                    optional_yield y)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket->read_bucket_stats(ctx, bucket, result, y);
  });
}

int RGWBucketCtl::read_buckets_stats(map<string, RGWBucketEnt>& m,
                                     optional_yield y)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket->read_buckets_stats(ctx, m, y);
  });
}

int RGWBucketCtl::sync_user_stats(const rgw_user& user_id,
                                  const RGWBucketInfo& bucket_info,
                                  RGWBucketEnt* pent)
{
  RGWBucketEnt ent;
  if (!pent) {
    pent = &ent;
  }
  int r = svc.bi->read_stats(bucket_info, pent, null_yield);
  if (r < 0) {
    ldout(cct, 20) << __func__ << "(): failed to read bucket stats (r=" << r << ")" << dendl;
    return r;
  }

  return ctl.user->flush_bucket_stats(user_id, *pent);
}

int RGWBucketCtl::get_sync_policy_handler(std::optional<rgw_zone_id> zone,
                                          std::optional<rgw_bucket> bucket,
                                          RGWBucketSyncPolicyHandlerRef *phandler,
                                          optional_yield y)
{
  int r = call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket_sync->get_policy_handler(ctx, zone, bucket, phandler, y);
  });
  if (r < 0) {
    ldout(cct, 20) << __func__ << "(): failed to get policy handler for bucket=" << bucket << " (r=" << r << ")" << dendl;
    return r;
  }
  return 0;
}

int RGWBucketCtl::bucket_exports_data(const rgw_bucket& bucket,
                                      optional_yield y)
{

  RGWBucketSyncPolicyHandlerRef handler;

  int r = get_sync_policy_handler(std::nullopt, bucket, &handler, y);
  if (r < 0) {
    return r;
  }

  return handler->bucket_exports_data();
}

int RGWBucketCtl::bucket_imports_data(const rgw_bucket& bucket,
                                      optional_yield y)
{

  RGWBucketSyncPolicyHandlerRef handler;

  int r = get_sync_policy_handler(std::nullopt, bucket, &handler, y);
  if (r < 0) {
    return r;
  }

  return handler->bucket_imports_data();
}

RGWBucketMetadataHandlerBase *RGWBucketMetaHandlerAllocator::alloc()
{
  return new RGWBucketMetadataHandler();
}

RGWBucketInstanceMetadataHandlerBase *RGWBucketInstanceMetaHandlerAllocator::alloc()
{
  return new RGWBucketInstanceMetadataHandler();
}

RGWBucketMetadataHandlerBase *RGWArchiveBucketMetaHandlerAllocator::alloc()
{
  return new RGWArchiveBucketMetadataHandler();
}

RGWBucketInstanceMetadataHandlerBase *RGWArchiveBucketInstanceMetaHandlerAllocator::alloc()
{
  return new RGWArchiveBucketInstanceMetadataHandler();
}

