#include <errno.h>

#include <string>
#include <map>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "rgw_rados.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"

#include "include/types.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "rgw_string.h"

// until everything is moved from rgw_common
#include "rgw_common.h"

#define dout_subsys ceph_subsys_rgw

#define BUCKET_TAG_TIMEOUT 30

using namespace std;

static RGWMetadataHandler *bucket_meta_handler = NULL;
static RGWMetadataHandler *bucket_instance_meta_handler = NULL;

// define as static when RGWBucket implementation compete
void rgw_get_buckets_obj(string& user_id, string& buckets_obj_id)
{
  buckets_obj_id = user_id;
  buckets_obj_id += RGW_BUCKETS_OBJ_SUFFIX;
}

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_read_user_buckets(RGWRados *store, string user_id, RGWUserBuckets& buckets,
                          const string& marker, uint64_t max, bool need_stats)
{
  int ret;
  buckets.clear();
  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);
  bufferlist bl;
  rgw_obj obj(store->zone.user_uid_pool, buckets_obj_id);
  bufferlist header;
  map<string,bufferlist> m;

  ret = store->omap_get_vals(obj, header, marker, max, m);
  if (ret == -ENOENT)
    ret = 0;

  if (ret < 0)
    return ret;

  for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); ++q) {
    bufferlist::iterator iter = q->second.begin();
    RGWBucketEnt bucket;
    ::decode(bucket, iter);
    buckets.add(bucket);
  }

  if (need_stats) {
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    int r = store->update_containers_stats(m);
    if (r < 0) {
      ldout(store->ctx(), 0) << "ERROR: could not get stats for buckets" << dendl;
    }
  }
  return 0;
}

int rgw_link_bucket(RGWRados *store, string user_id, rgw_bucket& bucket, time_t creation_time, bool update_entrypoint)
{
  int ret;
  string& bucket_name = bucket.name;

  bufferlist bl;

  RGWBucketEnt new_bucket;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;

  new_bucket.bucket = bucket;
  new_bucket.size = 0;
  if (!creation_time)
    time(&new_bucket.creation_time);
  else
    new_bucket.creation_time = creation_time;
  ::encode(new_bucket, bl);

  map<string, bufferlist> attrs;

  if (update_entrypoint) {
    ret = store->get_bucket_entrypoint_info(NULL, bucket_name, ep, &ot, NULL, &attrs);
    if (ret < 0 && ret != -ENOENT) {
      ldout(store->ctx(), 0) << "ERROR: store->get_bucket_entrypoint_info() returned " << ret << dendl;
    } else if (ret >= 0 && ep.linked && ep.owner != user_id) {
      ldout(store->ctx(), 0) << "can't link bucket, already linked to a different user: " << ep.owner << dendl;
      return -EINVAL;
    }
  }

  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);

  rgw_obj obj(store->zone.user_uid_pool, buckets_obj_id);
  ret = store->omap_set(obj, bucket_name, bl);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: error adding bucket to directory: "
        << cpp_strerror(-ret)<< dendl;
    goto done_err;
  }

  if (!update_entrypoint)
    return 0;

  ep.linked = true;
  ep.owner = user_id;
  ret = store->put_bucket_entrypoint_info(bucket_name, ep, false, ot, 0, &attrs);
  if (ret < 0)
    goto done_err;

  return 0;
done_err:
  int r = rgw_unlink_bucket(store, user_id, bucket.name);
  if (r < 0) {
    ldout(store->ctx(), 0) << "ERROR: failed unlinking bucket on error cleanup: " << cpp_strerror(-r) << dendl;
  }
  return ret;
}

int rgw_unlink_bucket(RGWRados *store, string user_id, const string& bucket_name, bool update_entrypoint)
{
  int ret;

  bufferlist bl;

  string buckets_obj_id;
  rgw_get_buckets_obj(user_id, buckets_obj_id);

  rgw_obj obj(store->zone.user_uid_pool, buckets_obj_id);
  ret = store->omap_del(obj, bucket_name);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "ERROR: error removing bucket from directory: "
        << cpp_strerror(-ret)<< dendl;
  }

  if (!update_entrypoint)
    return 0;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  ret = store->get_bucket_entrypoint_info(NULL, bucket_name, ep, &ot, NULL, &attrs);
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
  ret = store->put_bucket_entrypoint_info(bucket_name, ep, false, ot, 0, &attrs);
  if (ret < 0)
    return ret;

  return ret;
}

int rgw_bucket_store_info(RGWRados *store, const string& bucket_name, bufferlist& bl, bool exclusive,
                          map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker,
                          time_t mtime) {
  return store->meta_mgr->put_entry(bucket_meta_handler, bucket_name, bl, exclusive, objv_tracker, mtime, pattrs);
}

int rgw_bucket_instance_store_info(RGWRados *store, string& entry, bufferlist& bl, bool exclusive,
                          map<string, bufferlist> *pattrs, RGWObjVersionTracker *objv_tracker,
                          time_t mtime) {
  return store->meta_mgr->put_entry(bucket_instance_meta_handler, entry, bl, exclusive, objv_tracker, mtime, pattrs);
}

int rgw_bucket_instance_remove_entry(RGWRados *store, string& entry, RGWObjVersionTracker *objv_tracker) {
  return store->meta_mgr->remove_entry(bucket_instance_meta_handler, entry, objv_tracker);
}

int rgw_bucket_set_attrs(RGWRados *store, RGWBucketInfo& bucket_info,
                         map<string, bufferlist>& attrs,
                         map<string, bufferlist>* rmattrs,
                         RGWObjVersionTracker *objv_tracker)
{
  rgw_bucket& bucket = bucket_info.bucket;

  if (!bucket_info.has_instance_obj) {
    /* an old bucket object, need to convert it */
    int ret = store->convert_old_bucket_info(NULL, bucket.name);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: failed converting old bucket info: " << ret << dendl;
      return ret;
    }
  }
  string oid;
  store->get_bucket_meta_oid(bucket, oid);
  rgw_obj obj(store->zone.domain_root, oid);

  string key;
  store->get_bucket_instance_entry(bucket, key); /* we want the bucket instance name without
						    the oid prefix cruft */
  return store->meta_mgr->set_attrs(bucket_instance_meta_handler, key,
                                    obj, attrs, rmattrs, objv_tracker);
}

static void dump_mulipart_index_results(list<std::string>& objs_to_unlink,
        Formatter *f)
{
  // make sure that an appropiately titled header has been opened previously
  list<std::string>::iterator oiter = objs_to_unlink.begin();

  f->open_array_section("invalid_multipart_entries");

  for ( ; oiter != objs_to_unlink.end(); ++oiter) {
    f->dump_string("object",  *oiter);
  }

  f->close_section();
}

void check_bad_user_bucket_mapping(RGWRados *store, const string& user_id, bool fix)
{
  RGWUserBuckets user_buckets;
  bool done;
  string marker;

  CephContext *cct = store->ctx();

  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;

  do {
    int ret = rgw_read_user_buckets(store, user_id, user_buckets, marker, max_entries, false);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "failed to read user buckets: " << cpp_strerror(-ret) << dendl;
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
      time_t mtime;
      int r = store->get_bucket_info(NULL, bucket.name, bucket_info, &mtime);
      if (r < 0) {
        ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket << dendl;
        continue;
      }

      rgw_bucket& actual_bucket = bucket_info.bucket;

      if (actual_bucket.name.compare(bucket.name) != 0 ||
          actual_bucket.data_pool.compare(bucket.data_pool) != 0 ||
          actual_bucket.index_pool.compare(bucket.index_pool) != 0 ||
          actual_bucket.marker.compare(bucket.marker) != 0 ||
          actual_bucket.bucket_id.compare(bucket.bucket_id) != 0) {
        cout << "bucket info mismatch: expected " << actual_bucket << " got " << bucket << std::endl;
        if (fix) {
          cout << "fixing" << std::endl;
          r = rgw_link_bucket(store, user_id, actual_bucket, bucket_info.creation_time);
          if (r < 0) {
            cerr << "failed to fix bucket: " << cpp_strerror(-r) << std::endl;
          }
        }
      }
    }
    done = (buckets.size() < max_entries);
  } while (!done);
}

static bool bucket_object_check_filter(const string& name)
{
  string ns;
  string obj = name;
  return rgw_obj::translate_raw_obj_to_obj_in_ns(obj, ns);
}

int rgw_remove_object(RGWRados *store, rgw_bucket& bucket, std::string& object)
{
  RGWRadosCtx rctx(store);

  rgw_obj obj(bucket,object);

  int ret = store->delete_obj((void *)&rctx, obj);

  return ret;
}

int rgw_remove_bucket(RGWRados *store, rgw_bucket& bucket, bool delete_children)
{
  int ret;
  map<RGWObjCategory, RGWBucketStats> stats;
  std::vector<RGWObjEnt> objs;
  std::string prefix, delim, marker, ns;
  map<string, bool> common_prefixes;
  rgw_obj obj;
  RGWBucketInfo info;
  bufferlist bl;

  uint64_t bucket_ver, master_ver;

  ret = store->get_bucket_stats(bucket, &bucket_ver, &master_ver, stats, NULL);
  if (ret < 0)
    return ret;

  obj.bucket = bucket;

  ret = store->get_bucket_info(NULL, bucket.name, info, NULL);
  if (ret < 0)
    return ret;

  if (delete_children) {
    int max = 1000;
    ret = store->list_objects(bucket, max, prefix, delim, marker,
            objs, common_prefixes,
            false, ns, true, NULL, NULL);

    if (ret < 0)
      return ret;

    while (!objs.empty()) {
      std::vector<RGWObjEnt>::iterator it = objs.begin();
      for (it = objs.begin(); it != objs.end(); ++it) {
        ret = rgw_remove_object(store, bucket, (*it).name);
        if (ret < 0)
          return ret;
      }
      objs.clear();

      ret = store->list_objects(bucket, max, prefix, delim, marker, objs, common_prefixes,
                                false, ns, true, NULL, NULL);
      if (ret < 0)
        return ret;
    }
  }

  RGWObjVersionTracker objv_tracker;

  ret = store->delete_bucket(bucket, objv_tracker);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not remove bucket " << bucket.name << dendl;
    return ret;
  }

  ret = rgw_unlink_bucket(store, info.owner, bucket.name);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: unable to remove user bucket information" << dendl;
  }

  return ret;
}

int rgw_bucket_delete_bucket_obj(RGWRados *store, string& bucket_name, RGWObjVersionTracker& objv_tracker)
{
  return store->meta_mgr->remove_entry(bucket_meta_handler, bucket_name, &objv_tracker);
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

  RGWBucketInfo bucket_info;

  string user_id = op_state.get_user_id();
  bucket_name = op_state.get_bucket_name();
  RGWUserBuckets user_buckets;

  if (bucket_name.empty() && user_id.empty())
    return -EINVAL;

  if (!bucket_name.empty()) {
    int r = store->get_bucket_info(NULL, bucket_name, bucket_info, NULL);
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

  std::string no_oid;

  std::string display_name = op_state.get_user_display_name();
  rgw_bucket bucket = op_state.get_bucket();

  rgw_obj obj(bucket, no_oid);
  RGWObjVersionTracker objv_tracker;

  map<string, bufferlist> attrs;
  RGWBucketInfo bucket_info;

  int r = store->get_bucket_info(NULL, bucket.name, bucket_info, NULL, &attrs);
  if (r < 0) {
    return r;
  }

  map<string, bufferlist>::iterator aiter = attrs.find(RGW_ATTR_ACL);
  if (aiter != attrs.end()) {
    bufferlist aclbl = aiter->second;
    RGWAccessControlPolicy policy;
    ACLOwner owner;
    try {
     bufferlist::iterator iter = aclbl.begin();
     ::decode(policy, iter);
     owner = policy.get_owner();
    } catch (buffer::error& err) {
      set_err_msg(err_msg, "couldn't decode policy");
      return -EIO;
    }

    r = rgw_unlink_bucket(store, owner.get_id(), bucket.name);
    if (r < 0) {
      set_err_msg(err_msg, "could not unlink policy from user " + owner.get_id());
      return r;
    }

    // now update the user for the bucket...
    if (display_name.empty()) {
      ldout(store->ctx(), 0) << "WARNING: user " << user_info.user_id << " has no display name set" << dendl;
    }
    policy.create_default(user_info.user_id, display_name);

    owner = policy.get_owner();
    r = store->set_bucket_owner(bucket, owner);
    if (r < 0) {
      set_err_msg(err_msg, "failed to set bucket owner: " + cpp_strerror(-r));
      return r;
    }

    // ...and encode the acl
    aclbl.clear();
    policy.encode(aclbl);

    r = store->set_attr(NULL, obj, RGW_ATTR_ACL, aclbl, &objv_tracker);
    if (r < 0)
      return r;

    r = rgw_link_bucket(store, user_info.user_id, bucket, 0);
    if (r < 0)
      return r;
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

  int r = rgw_unlink_bucket(store, user_info.user_id, bucket.name);
  if (r < 0) {
    set_err_msg(err_msg, "error unlinking bucket" + cpp_strerror(-r));
  }

  return r;
}

int RGWBucket::remove(RGWBucketAdminOpState& op_state, std::string *err_msg)
{
  bool delete_children = op_state.will_delete_children();
  rgw_bucket bucket = op_state.get_bucket();

  int ret = rgw_remove_bucket(store, bucket, delete_children);
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

  int ret = rgw_remove_object(store, bucket, object_name);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove object" + cpp_strerror(-ret));
    return ret;
  }

  return 0;
}

static void dump_bucket_index(map<string, RGWObjEnt> result,  Formatter *f)
{
  map<string, RGWObjEnt>::iterator iter;
  for (iter = result.begin(); iter != result.end(); ++iter) {
    f->dump_string("object", iter->first);
   }
}

static void dump_bucket_usage(map<RGWObjCategory, RGWBucketStats>& stats, Formatter *formatter)
{
  map<RGWObjCategory, RGWBucketStats>::iterator iter;

  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWBucketStats& s = iter->second;
    const char *cat_name = rgw_obj_category_name(iter->first);
    formatter->open_object_section(cat_name);
    formatter->dump_int("size_kb", s.num_kb);
    formatter->dump_int("size_kb_actual", s.num_kb_rounded);
    formatter->dump_int("num_objects", s.num_objects);
    formatter->close_section();
  }
  formatter->close_section();
}

static void dump_index_check(map<RGWObjCategory, RGWBucketStats> existing_stats,
        map<RGWObjCategory, RGWBucketStats> calculated_stats,
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
        list<std::string>& objs_to_unlink, std::string *err_msg)
{
  bool fix_index = op_state.will_fix_index();
  rgw_bucket bucket = op_state.get_bucket();

  int max = 1000;
  string prefix;
  string marker;
  string delim;

  map<string, bool> common_prefixes;
  string ns = "multipart";

  bool is_truncated;
  map<string, bool> meta_objs;
  map<string, string> all_objs;

  do {
    vector<RGWObjEnt> result;
    int r = store->list_objects(bucket, max, prefix, delim, marker,
                                result, common_prefixes, false,
                                ns, true,
                                &is_truncated, NULL);

    if (r < 0) {
      set_err_msg(err_msg, "failed to list objects in bucket=" + bucket.name +
              " err=" +  cpp_strerror(-r));

      return r;
    }

    vector<RGWObjEnt>::iterator iter;
    for (iter = result.begin(); iter != result.end(); ++iter) {
      RGWObjEnt& ent = *iter;

      rgw_obj obj(bucket, ent.name);
      obj.set_ns(ns);

      string& oid = obj.object;
      marker = oid;

      int pos = oid.find_last_of('.');
      if (pos < 0)
        continue;

      string name = oid.substr(0, pos);
      string suffix = oid.substr(pos + 1);

      if (suffix.compare("meta") == 0) {
        meta_objs[name] = true;
      } else {
        all_objs[oid] = name;
      }
    }

  } while (is_truncated);

  map<string, string>::iterator aiter;
  for (aiter = all_objs.begin(); aiter != all_objs.end(); ++aiter) {
    string& name = aiter->second;

    if (meta_objs.find(name) == meta_objs.end()) {
      objs_to_unlink.push_back(aiter->first);
    }
  }

  if (objs_to_unlink.empty())
    return 0;

  if (fix_index) {
    int r = store->remove_objs_from_index(bucket, objs_to_unlink);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: remove_obj_from_index() returned error: " +
              cpp_strerror(-r));

      return r;
    }
  }

  return 0;
}

int RGWBucket::check_object_index(RGWBucketAdminOpState& op_state,
        map<string, RGWObjEnt> result, std::string *err_msg)
{

  bool fix_index = op_state.will_fix_index();
  rgw_bucket bucket = op_state.get_bucket();

  if (!fix_index) {
    set_err_msg(err_msg, "check-objects flag requires fix index enabled");
    return -EINVAL;
  }

/*
  dout(0) << "Checking objects, decreasing bucket 2-phase commit timeout.\n"\
	  << "** Note that timeout will reset only when operation completes successfully **" << dendl;
*/
  store->cls_obj_set_bucket_tag_timeout(bucket, BUCKET_TAG_TIMEOUT);

  string prefix;
  string marker;
  bool is_truncated = true;

  while (is_truncated) {
    map<string, RGWObjEnt> result;

    int r = store->cls_bucket_list(bucket, marker, prefix, 1000, result,
             &is_truncated, &marker,
             bucket_object_check_filter);

    if (r == -ENOENT) {
      break;
    } else if (r < 0 && r != -ENOENT) {
      set_err_msg(err_msg, "ERROR: failed operation r=" + cpp_strerror(-r));
    }
  }

  store->cls_obj_set_bucket_tag_timeout(bucket, 0);

  return 0;
}


int RGWBucket::check_index(RGWBucketAdminOpState& op_state,
        map<RGWObjCategory, RGWBucketStats>& existing_stats,
        map<RGWObjCategory, RGWBucketStats>& calculated_stats,
        std::string *err_msg)
{
  rgw_bucket bucket = op_state.get_bucket();
  bool fix_index = op_state.will_fix_index();

  int r = store->bucket_check_index(bucket, &existing_stats, &calculated_stats);
  if (r < 0) {
    set_err_msg(err_msg, "failed to check index error=" + cpp_strerror(-r));
    return r;
  }

  if (fix_index) {
    r = store->bucket_rebuild_index(bucket);
    if (r < 0) {
      set_err_msg(err_msg, "failed to rebuild index err=" + cpp_strerror(-r));
      return r;
    }
  }

  return 0;
}

int RGWBucket::get_policy(RGWBucketAdminOpState& op_state, ostream& o)
{
  std::string object_name = op_state.get_object_name();
  rgw_bucket bucket = op_state.get_bucket();

  bufferlist bl;
  rgw_obj obj(bucket, object_name);
  int ret = store->get_attr(NULL, obj, RGW_ATTR_ACL, bl);
  if (ret < 0)
    return ret;

  RGWAccessControlPolicy_S3 policy(g_ceph_context);
  bufferlist::iterator iter = bl.begin();
  try {
    policy.decode(iter);
  } catch (buffer::error& err) {
    dout(0) << "ERROR: caught buffer::error, could not decode policy" << dendl;
    return -EIO;
  }
  policy.to_xml(o);

  return 0;
}


int RGWBucketAdminOp::get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  ostream& os)
{
   RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  ret = bucket.get_policy(op_state, os);
  if (ret < 0)
    return ret;

  return 0;
}

/* Wrappers to facilitate RESTful interface */


int RGWBucketAdminOp::get_policy(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  std::ostringstream policy_stream;

  int ret = get_policy(store, op_state, policy_stream);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  flusher.start(0);

  formatter->dump_string("policy", policy_stream.str());

  flusher.flush();

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

int RGWBucketAdminOp::link(RGWRados *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  return bucket.link(op_state);

}

int RGWBucketAdminOp::check_index(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  int ret;
  map<string, RGWObjEnt> result;
  map<RGWObjCategory, RGWBucketStats> existing_stats;
  map<RGWObjCategory, RGWBucketStats> calculated_stats;
  list<std::string> objs_to_unlink;

  RGWBucket bucket;

  ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  ret = bucket.check_bad_index_multipart(op_state, objs_to_unlink);
  if (ret < 0)
    return ret;

  dump_mulipart_index_results(objs_to_unlink, formatter);
  flusher.flush();

  ret = bucket.check_object_index(op_state, result);
  if (ret < 0)
    return ret;

  dump_bucket_index(result,  formatter);
  flusher.flush();

  ret = bucket.check_index(op_state, existing_stats, calculated_stats);
  if (ret < 0)
    return ret;

  dump_index_check(existing_stats, calculated_stats, formatter);
  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::remove_bucket(RGWRados *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  return bucket.remove(op_state);
}

int RGWBucketAdminOp::remove_object(RGWRados *store, RGWBucketAdminOpState& op_state)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;

  return bucket.remove_object(op_state);
}

static int bucket_stats(RGWRados *store, std::string&  bucket_name, Formatter *formatter)
{
  RGWBucketInfo bucket_info;
  rgw_bucket bucket;
  map<RGWObjCategory, RGWBucketStats> stats;

  time_t mtime;
  int r = store->get_bucket_info(NULL, bucket_name, bucket_info, &mtime);
  if (r < 0)
    return r;

  bucket = bucket_info.bucket;

  uint64_t bucket_ver, master_ver;
  string max_marker;
  int ret = store->get_bucket_stats(bucket, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0) {
    cerr << "error getting bucket stats ret=" << ret << std::endl;
    return ret;
  }

  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket.name);
  formatter->dump_string("pool", bucket.data_pool);
  formatter->dump_string("index_pool", bucket.index_pool);
  formatter->dump_string("id", bucket.bucket_id);
  formatter->dump_string("marker", bucket.marker);
  formatter->dump_string("owner", bucket_info.owner);
  formatter->dump_int("ver", bucket_ver);
  formatter->dump_int("master_ver", master_ver);
  formatter->dump_int("mtime", mtime);
  formatter->dump_string("max_marker", max_marker);
  dump_bucket_usage(stats, formatter);
  formatter->close_section();

  return 0;
}


int RGWBucketAdminOp::info(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWBucket bucket;
  int ret;

  string bucket_name = op_state.get_bucket_name();

  if (!bucket_name.empty()) {
    ret = bucket.init(store, op_state);
    if (ret < 0)
      return ret;
  }

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  CephContext *cct = store->ctx();

  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;

  bool show_stats = op_state.will_fetch_stats();
  if (op_state.is_user_op()) {
    formatter->open_array_section("buckets");

    RGWUserBuckets buckets;
    string marker;
    bool done;

    do {
      ret = rgw_read_user_buckets(store, op_state.get_user_id(), buckets, marker, max_entries, false);
      if (ret < 0)
        return ret;

      map<string, RGWBucketEnt>& m = buckets.get_buckets();
      map<string, RGWBucketEnt>::iterator iter;

      for (iter = m.begin(); iter != m.end(); ++iter) {
        std::string  obj_name = iter->first;
        if (show_stats)
          bucket_stats(store, obj_name, formatter);
        else
          formatter->dump_string("bucket", obj_name);

        marker = obj_name;
      }

      flusher.flush();
      done = (m.size() < max_entries);
    } while (!done);

    formatter->close_section();
  } else if (!bucket_name.empty()) {
    bucket_stats(store, bucket_name, formatter);
  } else {
    RGWAccessHandle handle;

    formatter->open_array_section("buckets");
    if (store->list_buckets_init(&handle) >= 0) {
      RGWObjEnt obj;
      while (store->list_buckets_next(obj, &handle) >= 0) {
	formatter->dump_string("bucket", obj.name);
        if (show_stats)
          bucket_stats(store, obj.name, formatter);
      }
    }

    formatter->close_section();
  }

  flusher.flush();

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
  encode_json("timestamp", timestamp, f);
}


int RGWDataChangesLog::choose_oid(rgw_bucket& bucket) {
    string& name = bucket.name;
    uint32_t r = ceph_str_hash_linux(name.c_str(), name.size()) % num_shards;

    return (int)r;
}

int RGWDataChangesLog::renew_entries()
{
  if (!store->need_to_log_data())
    return 0;

  /* we can't keep the bucket name as part of the cls_log_entry, and we need
   * it later, so we keep two lists under the map */
  map<int, pair<list<string>, list<cls_log_entry> > > m;

  lock.Lock();
  map<string, rgw_bucket> entries;
  entries.swap(cur_cycle);
  lock.Unlock();

  map<string, rgw_bucket>::iterator iter;
  string section;
  utime_t ut = ceph_clock_now(cct);
  for (iter = entries.begin(); iter != entries.end(); ++iter) {
    rgw_bucket& bucket = iter->second;
    int index = choose_oid(bucket);

    cls_log_entry entry;

    rgw_data_change change;
    bufferlist bl;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bucket.name + ":" + bucket.bucket_id;
    change.timestamp = ut;
    ::encode(change, bl);

    store->time_log_prepare_entry(entry, ut, section, bucket.name, bl);

    m[index].first.push_back(bucket.name);
    m[index].second.push_back(entry);
  }

  map<int, pair<list<string>, list<cls_log_entry> > >::iterator miter;
  for (miter = m.begin(); miter != m.end(); ++miter) {
    list<cls_log_entry>& entries = miter->second.second;

    utime_t now = ceph_clock_now(cct);

    int ret = store->time_log_add(oids[miter->first], entries);
    if (ret < 0) {
      /* we don't really need to have a special handling for failed cases here,
       * as this is just an optimization. */
      lderr(cct) << "ERROR: store->time_log_add() returned " << ret << dendl;
      return ret;
    }

    utime_t expiration = now;
    expiration += utime_t(cct->_conf->rgw_data_log_window, 0);

    list<string>& buckets = miter->second.first;
    list<string>::iterator liter;
    for (liter = buckets.begin(); liter != buckets.end(); ++liter) {
      update_renewed(*liter, expiration);
    }
  }

  return 0;
}

void RGWDataChangesLog::_get_change(string& bucket_name, ChangeStatusPtr& status)
{
  assert(lock.is_locked());
  if (!changes.find(bucket_name, status)) {
    status = ChangeStatusPtr(new ChangeStatus);
    changes.add(bucket_name, status);
  }
}

void RGWDataChangesLog::register_renew(rgw_bucket& bucket)
{
  Mutex::Locker l(lock);
  cur_cycle[bucket.name] = bucket;
}

void RGWDataChangesLog::update_renewed(string& bucket_name, utime_t& expiration)
{
  Mutex::Locker l(lock);
  ChangeStatusPtr status;
  _get_change(bucket_name, status);

  ldout(cct, 20) << "RGWDataChangesLog::update_renewd() bucket_name=" << bucket_name << " expiration=" << expiration << dendl;
  status->cur_expiration = expiration;
}

int RGWDataChangesLog::add_entry(rgw_bucket& bucket) {
  if (!store->need_to_log_data())
    return 0;

  lock.Lock();

  ChangeStatusPtr status;
  _get_change(bucket.name, status);

  lock.Unlock();

  utime_t now = ceph_clock_now(cct);

  status->lock->Lock();

  ldout(cct, 20) << "RGWDataChangesLog::add_entry() bucket.name=" << bucket.name << " now=" << now << " cur_expiration=" << status->cur_expiration << dendl;

  if (now < status->cur_expiration) {
    /* no need to send, recently completed */
    status->lock->Unlock();

    register_renew(bucket);
    return 0;
  }

  RefCountedCond *cond;

  if (status->pending) {
    cond = status->cond;

    assert(cond);

    status->cond->get();
    status->lock->Unlock();

    int ret = cond->wait();
    cond->put();
    if (!ret) {
      register_renew(bucket);
    }
    return ret;
  }

  status->cond = new RefCountedCond;
  status->pending = true;

  string& oid = oids[choose_oid(bucket)];
  utime_t expiration;

  int ret;

  do {
    status->cur_sent = now;

    expiration = now;
    expiration += utime_t(cct->_conf->rgw_data_log_window, 0);

    status->lock->Unlock();
  
    bufferlist bl;
    rgw_data_change change;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bucket.name + ":" + bucket.bucket_id;
    change.timestamp = now;
    ::encode(change, bl);
    string section;

    ldout(cct, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now << " cur_expiration=" << expiration << dendl;

    ret = store->time_log_add(oid, now, section, change.key, bl);

    now = ceph_clock_now(cct);

    status->lock->Lock();

  } while (!ret && ceph_clock_now(cct) > expiration);

  cond = status->cond;

  status->pending = false;
  status->cur_expiration = status->cur_sent; /* time of when operation started, not completed */
  status->cur_expiration += utime_t(cct->_conf->rgw_data_log_window, 0);
  status->cond = NULL;
  status->lock->Unlock();

  cond->done(ret);
  cond->put();

  return ret;
}

int RGWDataChangesLog::list_entries(int shard, utime_t& start_time, utime_t& end_time, int max_entries,
				    list<rgw_data_change>& entries,
				    const string& marker,
				    string *out_marker,
				    bool *truncated) {

  list<cls_log_entry> log_entries;

  int ret = store->time_log_list(oids[shard], start_time, end_time,
				 max_entries, log_entries, marker,
				 out_marker, truncated);
  if (ret < 0)
    return ret;

  list<cls_log_entry>::iterator iter;
  for (iter = log_entries.begin(); iter != log_entries.end(); ++iter) {
    rgw_data_change entry;
    bufferlist::iterator liter = iter->data.begin();
    try {
      ::decode(entry, liter);
    } catch (buffer::error& err) {
      lderr(cct) << "ERROR: failed to decode data changes log entry" << dendl;
      return -EIO;
    }
    entries.push_back(entry);
  }

  return 0;
}

int RGWDataChangesLog::list_entries(utime_t& start_time, utime_t& end_time, int max_entries,
             list<rgw_data_change>& entries, LogMarker& marker, bool *ptruncated) {
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
  if (shard_id > num_shards)
    return -EINVAL;

  string oid = oids[shard_id];

  cls_log_header header;

  int ret = store->time_log_info(oid, &header);
  if ((ret < 0) && (ret != -ENOENT))
    return ret;

  info->marker = header.max_marker;
  info->last_update = header.max_time;

  return 0;
}

int RGWDataChangesLog::trim_entries(int shard_id, const utime_t& start_time, const utime_t& end_time,
                                    const string& start_marker, const string& end_marker)
{
  int ret;

  if (shard_id > num_shards)
    return -EINVAL;

  ret = store->time_log_trim(oids[shard_id], start_time, end_time, start_marker, end_marker);

  if (ret == -ENOENT)
    ret = 0;

  return ret;
}

int RGWDataChangesLog::trim_entries(const utime_t& start_time, const utime_t& end_time,
                                    const string& start_marker, const string& end_marker)
{
  for (int shard = 0; shard < num_shards; shard++) {
    int ret = store->time_log_trim(oids[shard], start_time, end_time, start_marker, end_marker);
    if (ret == -ENOENT) {
      continue;
    }
    if (ret < 0)
      return ret;
  }

  return 0;
}

bool RGWDataChangesLog::going_down()
{
  return (down_flag.read() != 0);
}

RGWDataChangesLog::~RGWDataChangesLog() {
  down_flag.set(1);
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
    cond.WaitInterval(cct, lock, utime_t(interval, 0));
    lock.Unlock();
  } while (!log->going_down());

  return NULL;
}

void RGWDataChangesLog::ChangesRenewThread::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

struct RGWBucketCompleteInfo {
  RGWBucketInfo info;
  map<string, bufferlist> attrs;

 void dump(Formatter *f) const {
    encode_json("bucket_info", info, f);
    encode_json("attrs", attrs, f);
  }

  void decode_json(JSONObj *obj) {
    JSONDecoder::decode_json("bucket_info", info, obj);
    JSONDecoder::decode_json("attrs", attrs, obj);
  }
};

class RGWBucketEntryMetadataObject : public RGWMetadataObject {
  RGWBucketEntryPoint ep;
public:
  RGWBucketEntryMetadataObject(RGWBucketEntryPoint& _ep, obj_version& v, time_t m) : ep(_ep) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const {
    ep.dump(f);
  }
};

class RGWBucketInstanceMetadataObject : public RGWMetadataObject {
  RGWBucketCompleteInfo info;
public:
  RGWBucketInstanceMetadataObject(RGWBucketCompleteInfo& i, obj_version& v, time_t m) : info(i) {
    objv = v;
    mtime = m;
  }

  void dump(Formatter *f) const {
    info.dump(f);
  }
};

class RGWBucketMetadataHandler : public RGWMetadataHandler {

  int init_bucket(RGWRados *store, string& bucket_name, rgw_bucket& bucket, RGWObjVersionTracker *objv_tracker) {
    RGWBucketInfo bucket_info;
    int r = store->get_bucket_info(NULL, bucket_name, bucket_info, NULL);
    if (r < 0) {
      cerr << "could not get bucket info for bucket=" << bucket_name << std::endl;
      return r;
    }
    bucket = bucket_info.bucket;

    return 0;
  }

public:
  string get_type() { return "bucket"; }

  int get(RGWRados *store, string& entry, RGWMetadataObject **obj) {
    RGWObjVersionTracker ot;
    RGWBucketEntryPoint be;

    time_t mtime;
    map<string, bufferlist> attrs;

    int ret = store->get_bucket_entrypoint_info(NULL, entry, be, &ot, &mtime, &attrs);
    if (ret < 0)
      return ret;

    RGWBucketEntryMetadataObject *mdo = new RGWBucketEntryMetadataObject(be, ot.read_version, mtime);

    *obj = mdo;

    return 0;
  }

  int put(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker,
          time_t mtime, JSONObj *obj, sync_type_t sync_type) {
    RGWBucketEntryPoint be, old_be;
    decode_json_obj(be, obj);

    time_t orig_mtime;
    map<string, bufferlist> attrs;

    RGWObjVersionTracker old_ot;

    int ret = store->get_bucket_entrypoint_info(NULL, entry, old_be, &old_ot, &orig_mtime, &attrs);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    // are we actually going to perform this put, or is it too old?
    if (ret != -ENOENT &&
        !check_versions(old_ot.read_version, orig_mtime,
			objv_tracker.write_version, mtime, sync_type)) {
      return STATUS_NO_APPLY;
    }

    objv_tracker.read_version = old_ot.read_version; /* maintain the obj version we just read */

    ret = store->put_bucket_entrypoint_info(entry, be, false, objv_tracker, mtime, &attrs);
    if (ret < 0)
      return ret;

    /* link bucket */
    if (be.linked) {
      ret = rgw_link_bucket(store, be.owner, be.bucket, be.creation_time, false);
    } else {
      ret = rgw_unlink_bucket(store, be.owner, be.bucket.name, false);
    }

    return ret;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) {
    RGWBucketEntryPoint be;

    int ret = store->get_bucket_entrypoint_info(NULL, entry, be, &objv_tracker, NULL, NULL);
    if (ret < 0)
      return ret;

    /*
     * We're unlinking the bucket but we don't want to update the entrypoint here — we're removing
     * it immediately and don't want to invalidate our cached objv_version or the bucket obj removal
     * will incorrectly fail.
     */
    ret = rgw_unlink_bucket(store, be.owner, entry, false);
    if (ret < 0) {
      lderr(store->ctx()) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    ret = rgw_bucket_delete_bucket_obj(store, entry, objv_tracker);
    if (ret < 0) {
      lderr(store->ctx()) << "could not delete bucket=" << entry << dendl;
    }
    /* idempotent */
    return 0;
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_bucket& bucket, string& oid) {
    oid = key;
    bucket = store->zone.domain_root;
  }

  int list_keys_init(RGWRados *store, void **phandle)
  {
    list_keys_info *info = new list_keys_info;

    info->store = store;

    *phandle = (void *)info;

    return 0;
  }

  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) {
    list_keys_info *info = static_cast<list_keys_info *>(handle);

    string no_filter;

    keys.clear();

    RGWRados *store = info->store;

    list<string> unfiltered_keys;

    int ret = store->list_raw_objects(store->zone.domain_root, no_filter,
                                      max, info->ctx, unfiltered_keys, truncated);
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

  void list_keys_complete(void *handle) {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }
};

class RGWBucketInstanceMetadataHandler : public RGWMetadataHandler {

  int init_bucket(RGWRados *store, string& bucket_name, rgw_bucket& bucket, RGWObjVersionTracker *objv_tracker) {
    RGWBucketInfo bucket_info;
    int r = store->get_bucket_info(NULL, bucket_name, bucket_info, NULL);
    if (r < 0) {
      cerr << "could not get bucket info for bucket=" << bucket_name << std::endl;
      return r;
    }
    bucket = bucket_info.bucket;

    return 0;
  }

public:
  string get_type() { return "bucket.instance"; }

  int get(RGWRados *store, string& oid, RGWMetadataObject **obj) {
    RGWBucketCompleteInfo bci;

    time_t mtime;

    int ret = store->get_bucket_instance_info(NULL, oid, bci.info, &mtime, &bci.attrs);
    if (ret < 0)
      return ret;

    RGWBucketInstanceMetadataObject *mdo = new RGWBucketInstanceMetadataObject(bci, bci.info.objv_tracker.read_version, mtime);

    *obj = mdo;

    return 0;
  }

  int put(RGWRados *store, string& oid, RGWObjVersionTracker& objv_tracker,
          time_t mtime, JSONObj *obj, sync_type_t sync_type) {
    RGWBucketCompleteInfo bci, old_bci;
    decode_json_obj(bci, obj);

    time_t orig_mtime;

    int ret = store->get_bucket_instance_info(NULL, oid, old_bci.info, &orig_mtime, &old_bci.attrs);
    bool exists = (ret != -ENOENT);
    if (ret < 0 && exists)
      return ret;


    if (!exists || old_bci.info.bucket.bucket_id != bci.info.bucket.bucket_id) {
      /* a new bucket, we need to select a new bucket placement for it */
      rgw_bucket bucket;
      ret = store->set_bucket_location_by_rule(bci.info.placement_rule, oid, bucket);
      if (ret < 0) {
        ldout(store->ctx(), 0) << "ERROR: select_bucket_placement() returned " << ret << dendl;
        return ret;
      }
      bci.info.bucket.data_pool = bucket.data_pool;
      bci.info.bucket.index_pool = bucket.index_pool;
    } else {
      /* existing bucket, keep its placement pools */
      bci.info.bucket.data_pool = old_bci.info.bucket.data_pool;
      bci.info.bucket.index_pool = old_bci.info.bucket.index_pool;
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

    ret = store->init_bucket_index(bci.info.bucket);
    if (ret < 0)
      return ret;

    return STATUS_APPLIED;
  }

  struct list_keys_info {
    RGWRados *store;
    RGWListRawObjsCtx ctx;
  };

  int remove(RGWRados *store, string& entry, RGWObjVersionTracker& objv_tracker) {
    RGWBucketInfo info;

    int ret = store->get_bucket_instance_info(NULL, entry, info, NULL, NULL);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    return rgw_bucket_instance_remove_entry(store, entry, &info.objv_tracker);
  }

  void get_pool_and_oid(RGWRados *store, const string& key, rgw_bucket& bucket, string& oid) {
    oid = RGW_BUCKET_INSTANCE_MD_PREFIX + key;
    bucket = store->zone.domain_root;
  }

  int list_keys_init(RGWRados *store, void **phandle)
  {
    list_keys_info *info = new list_keys_info;

    info->store = store;

    *phandle = (void *)info;

    return 0;
  }

  int list_keys_next(void *handle, int max, list<string>& keys, bool *truncated) {
    list_keys_info *info = static_cast<list_keys_info *>(handle);

    string no_filter;

    keys.clear();

    RGWRados *store = info->store;

    list<string> unfiltered_keys;

    int ret = store->list_raw_objects(store->zone.domain_root, no_filter,
                                      max, info->ctx, unfiltered_keys, truncated);
    if (ret < 0 && ret != -ENOENT)
      return ret;
    if (ret == -ENOENT) {
      if (truncated)
        *truncated = false;
      return 0;
    }

    int prefix_size = sizeof(RGW_BUCKET_INSTANCE_MD_PREFIX) - 1;
    // now filter in the relevant entries
    list<string>::iterator iter;
    for (iter = unfiltered_keys.begin(); iter != unfiltered_keys.end(); ++iter) {
      string& k = *iter;

      if (k.compare(0, prefix_size, RGW_BUCKET_INSTANCE_MD_PREFIX) == 0) {
        keys.push_back(k.substr(prefix_size));
      }
    }

    return 0;
  }

  void list_keys_complete(void *handle) {
    list_keys_info *info = static_cast<list_keys_info *>(handle);
    delete info;
  }

  /*
   * hash entry for mdlog placement. Use the same hash key we'd have for the bucket entry
   * point, so that the log entries end up at the same log shard, so that we process them
   * in order
   */
  virtual void get_hash_key(const string& section, const string& key, string& hash_key) {
    string k;
    int pos = key.find(':');
    if (pos < 0)
      k = key;
    else
      k = key.substr(0, pos);
    hash_key = "bucket:" + k;
  }
};

void rgw_bucket_init(RGWMetadataManager *mm)
{
  bucket_meta_handler = new RGWBucketMetadataHandler;
  mm->register_handler(bucket_meta_handler);
  bucket_instance_meta_handler = new RGWBucketInstanceMetadataHandler;
  mm->register_handler(bucket_instance_meta_handler);
}
