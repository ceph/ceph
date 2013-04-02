#include <errno.h>

#include <string>
#include <map>

#include "common/errno.h"
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

// define as static when RGWBucket implementation compete
void rgw_get_buckets_obj(string& user_id, string& buckets_obj_id)
{
  buckets_obj_id = user_id;
  buckets_obj_id += RGW_BUCKETS_OBJ_PREFIX;
}

static int rgw_read_buckets_from_attr(RGWRados *store, string& user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  rgw_obj obj(store->zone.user_uid_pool, user_id);
  int ret = store->get_attr(NULL, obj, RGW_ATTR_BUCKETS, bl);
  if (ret)
    return ret;

  bufferlist::iterator iter = bl.begin();
  try {
    buckets.decode(iter);
  } catch (buffer::error& err) {
    ldout(store->ctx(), 0) << "ERROR: failed to decode buckets info, caught buffer::error" << dendl;
    return -EIO;
  }
  return 0;
}

/**
 * Get all the buckets owned by a user and fill up an RGWUserBuckets with them.
 * Returns: 0 on success, -ERR# on failure.
 */
int rgw_read_user_buckets(RGWRados *store, string user_id, RGWUserBuckets& buckets, bool need_stats)
{
  int ret;
  buckets.clear();
  if (store->supports_omap()) {
    string buckets_obj_id;
    rgw_get_buckets_obj(user_id, buckets_obj_id);
    bufferlist bl;
    rgw_obj obj(store->zone.user_uid_pool, buckets_obj_id);
    bufferlist header;
    map<string,bufferlist> m;

    ret = store->omap_get_all(obj, header, m);
    if (ret == -ENOENT)
      ret = 0;

    if (ret < 0)
      return ret;

    for (map<string,bufferlist>::iterator q = m.begin(); q != m.end(); q++) {
      bufferlist::iterator iter = q->second.begin();
      RGWBucketEnt bucket;
      ::decode(bucket, iter);
      buckets.add(bucket);
    }
  } else {
    ret = rgw_read_buckets_from_attr(store, user_id, buckets);
    switch (ret) {
    case 0:
      break;
    case -ENODATA:
      ret = 0;
      return 0;
    default:
      return ret;
    }
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

/**
 * Store the set of buckets associated with a user on a n xattr
 * not used with all backends
 * This completely overwrites any previously-stored list, so be careful!
 * Returns 0 on success, -ERR# otherwise.
 */
int rgw_write_buckets_attr(RGWRados *store, string user_id, RGWUserBuckets& buckets)
{
  bufferlist bl;
  buckets.encode(bl);

  rgw_obj obj(store->zone.user_uid_pool, user_id);

  int ret = store->set_attr(NULL, obj, RGW_ATTR_BUCKETS, bl);

  return ret;
}

int rgw_add_bucket(RGWRados *store, string user_id, rgw_bucket& bucket)
{
  int ret;
  string& bucket_name = bucket.name;

  if (store->supports_omap()) {
    bufferlist bl;

    RGWBucketEnt new_bucket;
    new_bucket.bucket = bucket;
    new_bucket.size = 0;
    time(&new_bucket.mtime);
    ::encode(new_bucket, bl);

    string buckets_obj_id;
    rgw_get_buckets_obj(user_id, buckets_obj_id);

    rgw_obj obj(store->zone.user_uid_pool, buckets_obj_id);
    ret = store->omap_set(obj, bucket_name, bl);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: error adding bucket to directory: "
          << cpp_strerror(-ret)<< dendl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(store, user_id, buckets, false);
    RGWBucketEnt new_bucket;

    switch (ret) {
    case 0:
    case -ENOENT:
    case -ENODATA:
      new_bucket.bucket = bucket;
      new_bucket.size = 0;
      time(&new_bucket.mtime);
      buckets.add(new_bucket);
      ret = rgw_write_buckets_attr(store, user_id, buckets);
      break;
    default:
      ldout(store->ctx(), 10) << "rgw_write_buckets_attr returned " << ret << dendl;
      break;
    }
  }

  return ret;
}

int rgw_remove_user_bucket_info(RGWRados *store, string user_id, rgw_bucket& bucket)
{
  int ret;

  if (store->supports_omap()) {
    bufferlist bl;

    string buckets_obj_id;
    rgw_get_buckets_obj(user_id, buckets_obj_id);

    rgw_obj obj(store->zone.user_uid_pool, buckets_obj_id);
    ret = store->omap_del(obj, bucket.name);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "ERROR: error removing bucket from directory: "
          << cpp_strerror(-ret)<< dendl;
    }
  } else {
    RGWUserBuckets buckets;

    ret = rgw_read_user_buckets(store, user_id, buckets, false);

    if (ret == 0 || ret == -ENOENT) {
      buckets.remove(bucket.name);
      ret = rgw_write_buckets_attr(store, user_id, buckets);
    }
  }

  return ret;
}

int RGWBucket::create_bucket(string bucket_str, string& user_id, string& display_name)
{
  RGWAccessControlPolicy policy, old_policy;
  map<string, bufferlist> attrs;
  bufferlist aclbl;
  string no_oid;
  rgw_obj obj;
  RGWBucketInfo bucket_info;

  int ret;

  // defaule policy (private)
  policy.create_default(user_id, display_name);
  policy.encode(aclbl);

  ret = store->get_bucket_info(NULL, bucket_str, bucket_info);
  if (ret < 0)
    return ret;

  rgw_bucket& bucket = bucket_info.bucket;

  ret = store->create_bucket(user_id, bucket, attrs);
  if (ret && ret != -EEXIST)
    goto done;

  obj.init(bucket, no_oid);

  ret = store->set_attr(NULL, obj, RGW_ATTR_ACL, aclbl);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: failed to set acl on bucket" << dendl;
    goto done;
  }

  ret = rgw_add_bucket(store, user_id, bucket);

  if (ret == -EEXIST)
    ret = 0;
done:
  return ret;
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
  int ret = rgw_read_user_buckets(store, user_id, user_buckets, false);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "failed to read user buckets: " << cpp_strerror(-ret) << dendl;
    return;
  }

  map<string, RGWBucketEnt>& buckets = user_buckets.get_buckets();
  for (map<string, RGWBucketEnt>::iterator i = buckets.begin();
       i != buckets.end();
       ++i) {
    RGWBucketEnt& bucket_ent = i->second;
    rgw_bucket& bucket = bucket_ent.bucket;

    RGWBucketInfo bucket_info;
    int r = store->get_bucket_info(NULL, bucket.name, bucket_info);
    if (r < 0) {
      ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket << dendl;
      continue;
    }

    rgw_bucket& actual_bucket = bucket_info.bucket;

    if (actual_bucket.name.compare(bucket.name) != 0 ||
        actual_bucket.pool.compare(bucket.pool) != 0 ||
        actual_bucket.marker.compare(bucket.marker) != 0 ||
        actual_bucket.bucket_id.compare(bucket.bucket_id) != 0) {
      cout << "bucket info mismatch: expected " << actual_bucket << " got " << bucket << std::endl;
      if (fix) {
        cout << "fixing" << std::endl;
        r = rgw_add_bucket(store, user_id, actual_bucket);
        if (r < 0) {
          cerr << "failed to fix bucket: " << cpp_strerror(-r) << std::endl;
        }
      }
    }
  }
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

  ret = store->get_bucket_stats(bucket, stats);
  if (ret < 0)
    return ret;

  obj.bucket = bucket;
  int max = 1000;

  ret = rgw_get_obj(store, NULL, store->zone.domain_root,\
           bucket.name, bl, NULL);

  bufferlist::iterator iter = bl.begin();
  try {
    ::decode(info, iter);
  } catch (buffer::error& err) {
    //cerr << "ERROR: could not decode buffer info, caught buffer::error" << std::endl;
    return -EIO;
  }

  if (delete_children) {
    ret = store->list_objects(bucket, max, prefix, delim, marker,\
            objs, common_prefixes,\
            false, ns, (bool *)false, NULL);

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
                                false, ns, (bool *)false, NULL);
      if (ret < 0)
        return ret;
    }
  }

  ret = store->delete_bucket(bucket);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: could not remove bucket " << bucket.name << dendl;
    return ret;
  }

  ret = rgw_remove_user_bucket_info(store, info.owner, bucket);
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

int RGWBucket::init(RGWRados *storage, RGWBucketAdminOpState& op_state)
{
  if (!storage)
    return -EINVAL;

  store = storage;

  RGWUserInfo info;
  RGWBucketInfo bucket_info;

  user_id = op_state.get_user_id();
  bucket_name = op_state.get_bucket_name();
  RGWUserBuckets user_buckets;

  if (bucket_name.empty() && user_id.empty())
    return -EINVAL;

  if (!bucket_name.empty()) {
    int r = store->get_bucket_info(NULL, bucket_name, bucket_info);
    if (r < 0) {
      ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket_name << dendl;
      return r;
    }

    op_state.set_bucket(bucket_info.bucket);
  }

  if (!user_id.empty()) {
    int r = rgw_get_user_info_by_uid(store, user_id, info);
    if (r < 0)
      return r;

    r = rgw_read_user_buckets(store, user_id, user_buckets, true);
    if (r < 0)
      return r;

    op_state.set_user_buckets(user_buckets);
    op_state.display_name = info.display_name;
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

  string uid_str(user_id);
  bufferlist aclbl;
  rgw_obj obj(bucket, no_oid);

  int r = store->get_attr(NULL, obj, RGW_ATTR_ACL, aclbl);
  if (r >= 0) {
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

    r = rgw_remove_user_bucket_info(store, owner.get_id(), bucket);
    if (r < 0) {
      set_err_msg(err_msg, "could not unlink policy from user " + owner.get_id());
      return r;
    }

    // now update the user for the bucket...
    if (display_name.empty()) {
      ldout(store->ctx(), 0) << "WARNING: user " << user_id << " has no display name set" << dendl;
    }
    policy.create_default(user_id, display_name);

    owner = policy.get_owner();
    r = store->set_bucket_owner(bucket, owner);
    if (r < 0) {
      set_err_msg(err_msg, "failed to set bucket owner: " + cpp_strerror(-r));
      return r;
    }

    // ...and encode the acl
    aclbl.clear();
    policy.encode(aclbl);

    r = store->set_attr(NULL, obj, RGW_ATTR_ACL, aclbl);
    if (r < 0)
      return r;

    r = rgw_add_bucket(store, user_id, bucket);
    if (r < 0)
      return r;
  } else {
    // the bucket seems not to exist, so we should probably create it...
    r = create_bucket(bucket_name.c_str(), uid_str, display_name);
    if (r < 0) {
      set_err_msg(err_msg, "error linking bucket to user r=" + cpp_strerror(-r));
    }

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

  int r = rgw_remove_user_bucket_info(store, user_id, bucket);
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
  formatter->flush(cout);
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
                                result, common_prefixes, false, ns,
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

  int r = store->get_bucket_info(NULL, bucket_name, bucket_info);
  if (r < 0)
    return r;

  bucket = bucket_info.bucket;

  int ret = store->get_bucket_stats(bucket, stats);
  if (ret < 0) {
    cerr << "error getting bucket stats ret=" << ret << std::endl;
    return ret;
  }

  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket.name);
  formatter->dump_string("pool", bucket.pool);

  formatter->dump_string("id", bucket.bucket_id);
  formatter->dump_string("marker", bucket.marker);
  formatter->dump_string("owner", bucket_info.owner);
  dump_bucket_usage(stats, formatter);
  formatter->close_section();

  return 0;
}


int RGWBucketAdminOp::info(RGWRados *store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher)
{
  RGWBucket bucket;
  int ret = bucket.init(store, op_state);
  if (ret < 0)
    return ret;
  string bucket_name = op_state.get_bucket_name();

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  bool show_stats = op_state.will_fetch_stats();
  if (op_state.is_user_op()) {
    formatter->open_array_section("buckets");

    RGWUserBuckets buckets = op_state.get_user_buckets();
    map<string, RGWBucketEnt>& m = buckets.get_buckets();
    map<string, RGWBucketEnt>::iterator iter;

    for (iter = m.begin(); iter != m.end(); ++iter) {
      std::string  obj_name = iter->first;
      if (show_stats)
        bucket_stats(store, obj_name, formatter);
      else
        formatter->dump_string("bucket", obj_name);
    }

    formatter->close_section();
  } else if (!bucket_name.empty()) {
    bucket_stats(store, bucket_name, formatter);
  } else {
    RGWAccessHandle handle;

    if (store->list_buckets_init(&handle) > 0) {
      RGWObjEnt obj;
      while (store->list_buckets_next(obj, &handle) >= 0) {
	formatter->dump_string("bucket", obj.name);
        if (show_stats)
          bucket_stats(store, obj.name, formatter);
      }
    }
  }

  flusher.flush();

  return 0;
}

