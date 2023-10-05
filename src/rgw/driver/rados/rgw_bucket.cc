// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_acl_s3.h"
#include "rgw_tag_s3.h"

#include "rgw_bucket.h"
#include "rgw_op.h"
#include "rgw_bucket_sync.h"

#include "services/svc_zone.h"
#include "services/svc_bucket.h"
#include "services/svc_user.h"

#include "rgw_reshard.h"
#include "rgw_pubsub.h"

// stolen from src/cls/version/cls_version.cc
#define VERSION_ATTR "ceph.objclass.version"

#include "cls/user/cls_user_types.h"

#include "rgw_sal_rados.h"

#define dout_subsys ceph_subsys_rgw

// seconds for timeout during RGWBucket::check_object_index
constexpr uint64_t BUCKET_TAG_QUICK_TIMEOUT = 30;

using namespace std;

// these values are copied from cls/rgw/cls_rgw.cc
static const string BI_OLH_ENTRY_NS_START = "\x80" "1001_";
static const string BI_INSTANCE_ENTRY_NS_START = "\x80" "1000_";

// number of characters that we should allow to be buffered by the formatter
// before flushing (used by index check methods with dump_keys=true)
static constexpr int FORMATTER_LEN_FLUSH_THRESHOLD = 4 * 1024 * 1024;

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
  if (tenant_name->empty() && bucket_instance) {
    pos = bucket_instance->find(':');
    if (pos >= 0) {
      *tenant_name = *bucket_name;
      *bucket_name = bucket_instance->substr(0, pos);
      *bucket_instance = bucket_instance->substr(pos + 1);
    }
  }
}

static void dump_mulipart_index_results(list<rgw_obj_index_key>& objs_to_unlink,
        Formatter *f)
{
  for (const auto& o : objs_to_unlink) {
    f->dump_string("object",  o.name);
  }
}

void check_bad_user_bucket_mapping(rgw::sal::Driver* driver, rgw::sal::User& user,
				   bool fix,
				   optional_yield y,
                                   const DoutPrefixProvider *dpp)
{
  size_t max_entries = dpp->get_cct()->_conf->rgw_list_buckets_max_chunk;

  rgw::sal::BucketList listing;
  do {
    int ret = user.list_buckets(dpp, listing.next_marker, string(),
                                max_entries, false, listing, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed to read user buckets: "
          << cpp_strerror(-ret) << dendl;
      return;
    }

    for (const auto& ent : listing.buckets) {
      std::unique_ptr<rgw::sal::Bucket> bucket;
      int r = driver->load_bucket(dpp, rgw_bucket(user.get_tenant(), ent.bucket.name),
                                  &bucket, y);
      if (r < 0) {
        ldpp_dout(dpp, 0) << "could not get bucket info for bucket=" << bucket << dendl;
        continue;
      }

      if (ent.bucket != bucket->get_key()) {
        cout << "bucket info mismatch: expected " << ent.bucket
            << " got " << bucket << std::endl;
        if (fix) {
          cout << "fixing" << std::endl;
	  r = bucket->chown(dpp, user.get_id(), y);
          if (r < 0) {
            cerr << "failed to fix bucket: " << cpp_strerror(-r) << std::endl;
          }
        }
      }
    }
  } while (!listing.next_marker.empty());
}

// returns true if entry is in the empty namespace. note: function
// type conforms to type RGWBucketListNameFilter
bool rgw_bucket_object_check_filter(const std::string& oid)
{
  const static std::string empty_ns;
  rgw_obj_key key; // thrown away but needed for parsing
  return rgw_obj_key::oid_to_key_in_ns(oid, &key, empty_ns);
}

int rgw_remove_object(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, rgw::sal::Bucket* bucket, rgw_obj_key& key, optional_yield y)
{

  std::unique_ptr<rgw::sal::Object> object = bucket->get_object(key);

  return object->delete_object(dpp, y, rgw::sal::FLAG_LOG_OP);
}

static void set_err_msg(std::string *sink, std::string msg)
{
  if (sink && !msg.empty())
    *sink = msg;
}

int RGWBucket::init(rgw::sal::Driver* _driver, RGWBucketAdminOpState& op_state,
                    optional_yield y, const DoutPrefixProvider *dpp, std::string *err_msg)
{
  if (!_driver) {
    set_err_msg(err_msg, "no storage!");
    return -EINVAL;
  }

  driver = _driver;

  std::string bucket_name = op_state.get_bucket_name();

  if (bucket_name.empty() && op_state.get_user_id().empty())
    return -EINVAL;

  user = driver->get_user(op_state.get_user_id());
  std::string tenant = user->get_tenant();

  // split possible tenant/name
  auto pos = bucket_name.find('/');
  if (pos != string::npos) {
    tenant = bucket_name.substr(0, pos);
    bucket_name = bucket_name.substr(pos + 1);
  }

  int r = driver->load_bucket(dpp, rgw_bucket(tenant, bucket_name),
                              &bucket, y);
  if (r < 0) {
      set_err_msg(err_msg, "failed to fetch bucket info for bucket=" + bucket_name);
      return r;
  }

  op_state.set_bucket(bucket->clone());

  if (!rgw::sal::User::empty(user.get())) {
    r = user->load_user(dpp, y);
    if (r < 0) {
      set_err_msg(err_msg, "failed to fetch user info");
      return r;
    }
  }

  op_state.display_name = user->get_display_name();

  clear_failure();
  return 0;
}

bool rgw_find_bucket_by_id(const DoutPrefixProvider *dpp, CephContext *cct, rgw::sal::Driver* driver,
                           const string& marker, const string& bucket_id, rgw_bucket* bucket_out)
{
  void *handle = NULL;
  bool truncated = false;
  string s;

  int ret = driver->meta_list_keys_init(dpp, "bucket.instance", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    driver->meta_list_keys_complete(handle);
    return -ret;
  }
  do {
      list<string> keys;
      ret = driver->meta_list_keys_next(dpp, handle, 1000, keys, &truncated);
      if (ret < 0) {
        cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        driver->meta_list_keys_complete(handle);
        return -ret;
      }
      for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
        s = *iter;
        ret = rgw_bucket_parse_bucket_key(cct, s, bucket_out, nullptr);
        if (ret < 0) {
          continue;
        }
        if (bucket_id == bucket_out->bucket_id) {
          driver->meta_list_keys_complete(handle);
          return true;
        }
      }
  } while (truncated);
  driver->meta_list_keys_complete(handle);
  return false;
}

int RGWBucket::chown(RGWBucketAdminOpState& op_state, const string& marker,
                     optional_yield y, const DoutPrefixProvider *dpp, std::string *err_msg)
{
  return rgw_chown_bucket_and_objects(driver, bucket.get(), user.get(), marker, err_msg, dpp, y);
}

int RGWBucket::set_quota(RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, std::string *err_msg)
{
  bucket = op_state.get_bucket()->clone();

  bucket->get_info().quota = op_state.quota;
  int r = bucket->put_info(dpp, false, real_time(), y);
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info: " + cpp_strerror(-r));
    return r;
  }
  return r;
}

int RGWBucket::remove_object(const DoutPrefixProvider *dpp, RGWBucketAdminOpState& op_state, optional_yield y, std::string *err_msg)
{
  std::string object_name = op_state.get_object_name();

  rgw_obj_key key(object_name);

  bucket = op_state.get_bucket()->clone();

  int ret = rgw_remove_object(dpp, driver, bucket.get(), key, y);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove object" + cpp_strerror(-ret));
    return ret;
  }

  return 0;
}

static void dump_bucket_index(const vector<rgw_bucket_dir_entry>& objs,  Formatter *f)
{
  for (auto iter = objs.begin(); iter != objs.end(); ++iter) {
    f->dump_string("object", iter->key.name);
  }
}

static void dump_bucket_usage(map<RGWObjCategory, RGWStorageStats>& stats, Formatter *formatter)
{
  map<RGWObjCategory, RGWStorageStats>::iterator iter;

  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWStorageStats& s = iter->second;
    formatter->open_object_section(to_string(iter->first));
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
					 RGWFormatterFlusher& flusher,
					 const DoutPrefixProvider *dpp, optional_yield y,
					 std::string *err_msg)
{
  const bool fix_index = op_state.will_fix_index();

  bucket = op_state.get_bucket()->clone();

  rgw::sal::Bucket::ListParams params;
  params.list_versions = true;
  params.ns = RGW_OBJ_NS_MULTIPART;

  std::map<std::string, bool> meta_objs;
  std::map<rgw_obj_index_key, std::string> all_objs;
  bool is_truncated;
  do {
    rgw::sal::Bucket::ListResults results;
    int r = bucket->list(dpp, params, listing_max_entries, results, y);
    if (r < 0) {
      set_err_msg(err_msg, "failed to list objects in bucket=" + bucket->get_name() +
              " err=" +  cpp_strerror(-r));

      return r;
    }
    is_truncated = results.is_truncated;

    for (const auto& o : results.objs) {
      rgw_obj_index_key key = o.key;
      rgw_obj obj(bucket->get_key(), key);
      std::string oid = obj.get_oid();

      int pos = oid.find_last_of('.');
      if (pos < 0) {
        /* obj has no suffix */
        all_objs[key] = oid;
      } else {
        /* obj has suffix */
	std::string name = oid.substr(0, pos);
	std::string suffix = oid.substr(pos + 1);

        if (suffix.compare("meta") == 0) {
          meta_objs[name] = true;
        } else {
          all_objs[key] = name;
        }
      }
    }
  } while (is_truncated);

  std::list<rgw_obj_index_key> objs_to_unlink;
  Formatter *f =  flusher.get_formatter();

  f->open_array_section("invalid_multipart_entries");

  for (const auto& o : all_objs) {
    const std::string& name = o.second;
    if (meta_objs.find(name) == meta_objs.end()) {
      objs_to_unlink.push_back(o.first);
    }

    if (objs_to_unlink.size() > listing_max_entries) {
      if (fix_index) {
	// note: under rados this removes directly from rados index objects
	int r = bucket->remove_objs_from_index(dpp, objs_to_unlink);
	if (r < 0) {
	  set_err_msg(err_msg, "ERROR: remove_obj_from_index() returned error: " +
		      cpp_strerror(-r));
	  return r;
	}
      }

      dump_mulipart_index_results(objs_to_unlink, f);
      flusher.flush();
      objs_to_unlink.clear();
    }
  }

  if (fix_index) {
    // note: under rados this removes directly from rados index objects
    int r = bucket->remove_objs_from_index(dpp, objs_to_unlink);
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

int RGWBucket::check_object_index(const DoutPrefixProvider *dpp, 
                                  RGWBucketAdminOpState& op_state,
                                  RGWFormatterFlusher& flusher,
                                  optional_yield y,
                                  std::string *err_msg)
{

  bool fix_index = op_state.will_fix_index();

  if (!fix_index) {
    set_err_msg(err_msg, "check-objects flag requires fix index enabled");
    return -EINVAL;
  }

  // use a quicker/shorter tag timeout during this process
  bucket->set_tag_timeout(dpp, BUCKET_TAG_QUICK_TIMEOUT);

  rgw::sal::Bucket::ListResults results;
  results.is_truncated = true;

  Formatter *formatter = flusher.get_formatter();
  formatter->open_object_section("objects");

  while (results.is_truncated) {
    rgw::sal::Bucket::ListParams params;
    params.marker = results.next_marker;
    params.force_check_filter = rgw_bucket_object_check_filter;

    int r = bucket->list(dpp, params, listing_max_entries, results, y);

    if (r == -ENOENT) {
      break;
    } else if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed operation r=" + cpp_strerror(-r));
    }

    dump_bucket_index(results.objs, formatter);
    flusher.flush();
  }

  formatter->close_section();

  // restore normal tag timeout for bucket
  bucket->set_tag_timeout(dpp, 0);

  return 0;
}

/**
 * Loops over all olh entries in a bucket shard and finds ones with
 * exists=false and pending_removal=true. If the pending log is empty on
 * these entries, they were left behind after the last remaining version of
 * an object was deleted or after an incomplete upload. This was known to
 * happen historically due to concurrency conflicts among requests referencing
 * the same object key. If op_state.fix_index is true, we continue where the
 * request left off by calling RGWRados::clear_olh. If the pending log is not
 * empty, we attempt to apply it.
 */
static int check_index_olh(rgw::sal::RadosStore* const rados_store,
                           rgw::sal::Bucket* const bucket,
                           const DoutPrefixProvider *dpp,
                           RGWBucketAdminOpState& op_state,
                           RGWFormatterFlusher& flusher,
                           const int shard, 
                           uint64_t* const count_out,
                           optional_yield y)
{
  string marker = BI_OLH_ENTRY_NS_START;
  bool is_truncated = true;
  list<rgw_cls_bi_entry> entries;

  RGWObjectCtx obj_ctx(rados_store);
  RGWRados* store = rados_store->getRados();
  RGWRados::BucketShard bs(store);

  int ret = bs.init(dpp, bucket->get_info(), bucket->get_info().layout.current_index, shard, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR bs.init(bucket=" << bucket << "): " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  
  *count_out = 0;
  do {
    entries.clear();
    ret = store->bi_list(bs, "", marker, -1, &entries, &is_truncated, y);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR bi_list(): " << cpp_strerror(-ret) << dendl;
      break;
    }
    list<rgw_cls_bi_entry>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      rgw_cls_bi_entry& entry = *iter;
      marker = entry.idx;
      if (entry.type != BIIndexType::OLH) {
        is_truncated = false;
        break;
      }
      rgw_bucket_olh_entry olh_entry;
      auto iiter = entry.data.cbegin();
      try {
        decode(olh_entry, iiter);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, -1) << "ERROR failed to decode olh entry for key: " << entry.idx << dendl;
        continue;
      }
      if (olh_entry.exists || !olh_entry.pending_removal) {
        continue;
      }
      if (op_state.will_fix_index()) {
        rgw_obj obj(bucket->get_key(), olh_entry.key.name);
        if (olh_entry.pending_log.empty()) {
          ret = store->clear_olh(dpp, obj_ctx, obj, bucket->get_info(), olh_entry.tag, olh_entry.epoch, y);
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR failed to clear olh for: " << olh_entry.key.name << " clear_olh(): " << cpp_strerror(-ret) << dendl;
            continue;
          }
        } else {
          std::unique_ptr<rgw::sal::Object> object = bucket->get_object({olh_entry.key.name});
          RGWObjState *state;
          ret = object->get_obj_state(dpp, &state, y, false);
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR failed to get state for: " << olh_entry.key.name << " get_obj_state(): " << cpp_strerror(-ret) << dendl;
            continue;
          }
          ret = store->update_olh(dpp, obj_ctx, state, bucket->get_info(), obj, y);
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR failed to update olh for: " << olh_entry.key.name << " update_olh(): " << cpp_strerror(-ret) << dendl;
            continue;
          }
        }
      }
      if (op_state.dump_keys) {
        flusher.get_formatter()->dump_string("", olh_entry.key.name);
        if (flusher.get_formatter()->get_len() > FORMATTER_LEN_FLUSH_THRESHOLD) {
          flusher.flush();
        }
      }
      *count_out += 1;
    }
  } while (is_truncated);
  flusher.flush();
  return 0;
}


/**
 * Spawns separate coroutines to check each bucket shard for leftover
 * olh entries (and remove them if op_state.fix_index is true).
 */
int RGWBucket::check_index_olh(rgw::sal::RadosStore* const rados_store,
                               const DoutPrefixProvider *dpp,
                               RGWBucketAdminOpState& op_state,
                               RGWFormatterFlusher& flusher)
{
  const RGWBucketInfo& bucket_info = get_bucket_info();
  if ((bucket_info.versioning_status() & BUCKET_VERSIONED) == 0) {
    ldpp_dout(dpp, 0) << "WARNING: this command is only applicable to versioned buckets" << dendl;
    return 0;
  }
  
  Formatter* formatter = flusher.get_formatter();
  if (op_state.dump_keys) {
    formatter->open_array_section("");
  }

  const int max_shards = rgw::num_shards(bucket_info.layout.current_index);
  std::string verb = op_state.will_fix_index() ? "removed" : "found";
  uint64_t count_out = 0;
  
  boost::asio::io_context context;
  int next_shard = 0;
  
  const int max_aio = std::max(1, op_state.get_max_aio());

  for (int i=0; i<max_aio; i++) {
    spawn::spawn(context, [&](spawn::yield_context yield) {
      while (true) {
        int shard = next_shard;
        next_shard += 1;
        if (shard >= max_shards) {
          return;
        }
        optional_yield y(context, yield);
        uint64_t shard_count;
        int r = ::check_index_olh(rados_store, &*bucket, dpp, op_state, flusher, shard, &shard_count, y);
        if (r < 0) {
          ldpp_dout(dpp, -1) << "NOTICE: error processing shard " << shard << 
            " check_index_olh(): " << r << dendl;
        }
        count_out += shard_count;
        if (!op_state.hide_progress) {
          ldpp_dout(dpp, 1) << "NOTICE: finished shard " << shard << " (" << shard_count <<
            " entries " << verb << ")" << dendl;
        }
      }
    });
  }
  try {
    context.run();
  } catch (const std::system_error& e) {
    return -e.code().value();
  }
  if (!op_state.hide_progress) {
    ldpp_dout(dpp, 1) << "NOTICE: finished all shards (" << count_out <<
      " entries " << verb << ")" << dendl;
  }
  if (op_state.dump_keys) {
    formatter->close_section();
    flusher.flush();
  }
  return 0;
}

/**
 * Indicates whether a versioned bucket instance entry is listable in the
 * index. It does this by looping over all plain entries with prefix equal to
 * the key name, and checking whether any have an instance ID matching the one
 * on the specified key. The existence of an instance entry without a matching
 * plain entry indicates that the object was uploaded successfully, but the
 * request exited prior to linking the object into the index (via the creation
 * of a plain entry).
 */
static int is_versioned_instance_listable(const DoutPrefixProvider *dpp,
                                          RGWRados::BucketShard& bs,
                                          const cls_rgw_obj_key& key,
                                          bool& listable,
                                          optional_yield y)
{
  const std::string empty_delim;
  cls_rgw_obj_key marker;
  rgw_cls_list_ret result;
  listable = false;

  do {
    librados::ObjectReadOperation op;
    cls_rgw_bucket_list_op(op, marker, key.name, empty_delim, 1000,
                           true, &result);
    bufferlist ibl;
    int r = bs.bucket_obj.operate(dpp, &op, &ibl, y);
    if (r < 0) {
      return r;
    }

    for (auto const& entry : result.dir.m) {
      if (entry.second.key == key) {
        listable = true;
        return 0;
      }
      marker = entry.second.key;
    }
  } while (result.is_truncated);
  return 0;
}

/**
 * Loops over all instance entries in a bucket shard and finds ones with
 * versioned_epoch=0 and an mtime that is earlier than op_state.min_age
 * relative to the current time. These entries represent objects that were
 * uploaded successfully but were not successfully linked into the object
 * index. As an extra precaution, we also verify that these entries are indeed
 * non listable (have no corresponding plain entry in the index). We can assume
 * that clients received an error response for the associated upload requests
 * since the bucket index linking transaction did not complete. Therefore, if
 * op_state.fix_index is true, we remove the object that is associated with the
 * instance entry.
 */
static int check_index_unlinked(rgw::sal::RadosStore* const rados_store,
                                rgw::sal::Bucket* const bucket,
                                const DoutPrefixProvider *dpp,
                                RGWBucketAdminOpState& op_state,
                                RGWFormatterFlusher& flusher,
                                const int shard, 
                                uint64_t* const count_out,
                                optional_yield y)
{
  string marker = BI_INSTANCE_ENTRY_NS_START;
  bool is_truncated = true;
  list<rgw_cls_bi_entry> entries;

  RGWObjectCtx obj_ctx(rados_store);
  RGWRados* store = rados_store->getRados();
  RGWRados::BucketShard bs(store);

  int ret = bs.init(dpp, bucket->get_info(), bucket->get_info().layout.current_index, shard, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR bs.init(bucket=" << bucket << "): " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ceph::real_clock::time_point now = ceph::real_clock::now();
  ceph::real_clock::time_point not_after = now - op_state.min_age;
  
  *count_out = 0;
  do {
    entries.clear();
    ret = store->bi_list(bs, "", marker, -1, &entries, &is_truncated, y);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR bi_list(): " << cpp_strerror(-ret) << dendl;
      break;
    }
    list<rgw_cls_bi_entry>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      rgw_cls_bi_entry& entry = *iter;
      marker = entry.idx;
      if (entry.type != BIIndexType::Instance) {
        is_truncated = false;
        break;
      }
      rgw_bucket_dir_entry dir_entry;
      auto iiter = entry.data.cbegin();
      try {
        decode(dir_entry, iiter);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, -1) << "ERROR failed to decode instance entry for key: " <<
          entry.idx << dendl;
        continue;
      }
      if (dir_entry.versioned_epoch != 0 || dir_entry.meta.mtime > not_after) {
        continue;
      }
      bool listable;
      ret = is_versioned_instance_listable(dpp, bs, dir_entry.key, listable, y);
      if (ret < 0) {
        ldpp_dout(dpp, -1) << "ERROR is_versioned_instance_listable(key='" <<
          dir_entry.key << "'): " << cpp_strerror(-ret) << dendl;
        continue;
      }
      if (listable) {
        continue;
      }
      if (op_state.will_fix_index()) {
        rgw_obj_key key(dir_entry.key.name, dir_entry.key.instance);
        ret = rgw_remove_object(dpp, rados_store, bucket, key, y);
        if (ret < 0) {
          ldpp_dout(dpp, -1) << "ERROR rgw_remove_obj(key='" <<
            dir_entry.key << "'): " << cpp_strerror(-ret) << dendl;
          continue;
        }
      }
      if (op_state.dump_keys) {
        Formatter* const formatter = flusher.get_formatter();
        formatter->open_object_section("object_instance");
        formatter->dump_string("name", dir_entry.key.name);
        formatter->dump_string("instance", dir_entry.key.instance);
        formatter->close_section();
        if (formatter->get_len() > FORMATTER_LEN_FLUSH_THRESHOLD) {
          flusher.flush();
        }
      }
      *count_out += 1;
    }
  } while (is_truncated);
  flusher.flush();
  return 0;
}

/**
 * Spawns separate coroutines to check each bucket shard for unlinked
 * instance entries (and remove them if op_state.fix_index is true).
 */
int RGWBucket::check_index_unlinked(rgw::sal::RadosStore* const rados_store,
                                    const DoutPrefixProvider *dpp,
                                    RGWBucketAdminOpState& op_state,
                                    RGWFormatterFlusher& flusher)
{
  const RGWBucketInfo& bucket_info = get_bucket_info();
  if ((bucket_info.versioning_status() & BUCKET_VERSIONED) == 0) {
    ldpp_dout(dpp, 0) << "WARNING: this command is only applicable to versioned buckets" << dendl;
    return 0;
  }
  
  Formatter* formatter = flusher.get_formatter();
  if (op_state.dump_keys) {
    formatter->open_array_section("");
  }

  const int max_shards = rgw::num_shards(bucket_info.layout.current_index);
  std::string verb = op_state.will_fix_index() ? "removed" : "found";
  uint64_t count_out = 0;
  
  int max_aio = std::max(1, op_state.get_max_aio());
  int next_shard = 0;
  boost::asio::io_context context;
  for (int i=0; i<max_aio; i++) {
    spawn::spawn(context, [&](spawn::yield_context yield) {
      while (true) {
        int shard = next_shard;
        next_shard += 1;
        if (shard >= max_shards) {
          return;
        }
        uint64_t shard_count;
        optional_yield y {context, yield};
        int r = ::check_index_unlinked(rados_store, &*bucket, dpp, op_state, flusher, shard, &shard_count, y);
        if (r < 0) {
          ldpp_dout(dpp, -1) << "ERROR: error processing shard " << shard << 
            " check_index_unlinked(): " << r << dendl;
        }
        count_out += shard_count;
        if (!op_state.hide_progress) {
          ldpp_dout(dpp, 1) << "NOTICE: finished shard " << shard << " (" << shard_count <<
            " entries " << verb << ")" << dendl;
        }
      }
    });
  }
  try {
    context.run();
  } catch (const std::system_error& e) {
    return -e.code().value();
  }

  if (!op_state.hide_progress) {
    ldpp_dout(dpp, 1) << "NOTICE: finished all shards (" << count_out <<
      " entries " << verb << ")" << dendl;
  }
  if (op_state.dump_keys) {
    formatter->close_section();
    flusher.flush();
  }
  return 0;
}

int RGWBucket::check_index(const DoutPrefixProvider *dpp,
        RGWBucketAdminOpState& op_state,
        map<RGWObjCategory, RGWStorageStats>& existing_stats,
        map<RGWObjCategory, RGWStorageStats>& calculated_stats,
        std::string *err_msg)
{
  bool fix_index = op_state.will_fix_index();

  int r = bucket->check_index(dpp, existing_stats, calculated_stats);
  if (r < 0) {
    set_err_msg(err_msg, "failed to check index error=" + cpp_strerror(-r));
    return r;
  }

  if (fix_index) {
    r = bucket->rebuild_index(dpp);
    if (r < 0) {
      set_err_msg(err_msg, "failed to rebuild index err=" + cpp_strerror(-r));
      return r;
    }
  }

  return 0;
}

int RGWBucket::sync(RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, std::string *err_msg)
{
  if (!driver->is_meta_master()) {
    set_err_msg(err_msg, "ERROR: failed to update bucket sync: only allowed on meta master zone");
    return -EINVAL;
  }
  bool sync = op_state.will_sync_bucket();
  if (sync) {
    bucket->get_info().flags &= ~BUCKET_DATASYNC_DISABLED;
  } else {
    bucket->get_info().flags |= BUCKET_DATASYNC_DISABLED;
  }

  // when writing this metadata, RGWSI_BucketIndex_RADOS::handle_overwrite()
  // will write the corresponding datalog and bilog entries
  int r = bucket->put_info(dpp, false, real_time(), y);
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info:" + cpp_strerror(-r));
    return r;
  }

  return 0;
}


int rgw_object_get_attr(const DoutPrefixProvider *dpp,
			rgw::sal::Driver* driver, rgw::sal::Object* obj,
			const char* attr_name, bufferlist& out_bl, optional_yield y)
{
  std::unique_ptr<rgw::sal::Object::ReadOp> rop = obj->get_read_op();

  return rop->get_attr(dpp, attr_name, out_bl, y);
}

int RGWBucket::get_policy(RGWBucketAdminOpState& op_state, RGWAccessControlPolicy& policy, optional_yield y, const DoutPrefixProvider *dpp)
{
  int ret;
  std::string object_name = op_state.get_object_name();

  bucket = op_state.get_bucket()->clone();

  if (!object_name.empty()) {
    bufferlist bl;
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(object_name));

    ret = rgw_object_get_attr(dpp, driver, obj.get(), RGW_ATTR_ACL, bl, y);
    if (ret < 0){
      return ret;
    }

    ret = decode_bl(bl, policy);
    if (ret < 0) {
      ldout(driver->ctx(),0) << "failed to decode RGWAccessControlPolicy" << dendl;
    }
    return ret;
  }

  map<string, bufferlist>::iterator aiter = bucket->get_attrs().find(RGW_ATTR_ACL);
  if (aiter == bucket->get_attrs().end()) {
    return -ENOENT;
  }

  ret = decode_bl(aiter->second, policy);
  if (ret < 0) {
    ldout(driver->ctx(),0) << "failed to decode RGWAccessControlPolicy" << dendl;
  }

  return ret;
}


int RGWBucketAdminOp::get_policy(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy, const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWBucket bucket;

  int ret = bucket.init(driver, op_state, y, dpp);
  if (ret < 0)
    return ret;

  ret = bucket.get_policy(op_state, policy, y, dpp);
  if (ret < 0)
    return ret;

  return 0;
}

/* Wrappers to facilitate RESTful interface */


int RGWBucketAdminOp::get_policy(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWAccessControlPolicy policy;

  int ret = get_policy(driver, op_state, policy, dpp, y);
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

int RGWBucketAdminOp::dump_s3_policy(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                  ostream& os, const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWAccessControlPolicy policy;

  int ret = get_policy(driver, op_state, policy, dpp, y);
  if (ret < 0)
    return ret;

  rgw::s3::write_policy_xml(policy, os);

  return 0;
}

int RGWBucketAdminOp::unlink(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWBucket bucket;

  int ret = bucket.init(driver, op_state, y, dpp);
  if (ret < 0)
    return ret;

  return static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->unlink_bucket(op_state.get_user_id(), op_state.get_bucket()->get_info().bucket, y, dpp, true);
}

int RGWBucketAdminOp::link(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, string *err)
{
  if (!op_state.is_user_op()) {
    set_err_msg(err, "empty user id");
    return -EINVAL;
  }

  RGWBucket bucket;
  int ret = bucket.init(driver, op_state, y, dpp, err);
  if (ret < 0)
    return ret;

  string bucket_id = op_state.get_bucket_id();
  std::string display_name = op_state.get_user_display_name();
  std::unique_ptr<rgw::sal::Bucket> loc_bucket;
  std::unique_ptr<rgw::sal::Bucket> old_bucket;

  loc_bucket = op_state.get_bucket()->clone();

  if (!bucket_id.empty() && bucket_id != loc_bucket->get_bucket_id()) {
    set_err_msg(err,
	"specified bucket id does not match " + loc_bucket->get_bucket_id());
    return -EINVAL;
  }

  old_bucket = loc_bucket->clone();

  loc_bucket->get_key().tenant = op_state.get_user_id().tenant;

  if (!op_state.new_bucket_name.empty()) {
    auto pos = op_state.new_bucket_name.find('/');
    if (pos != string::npos) {
      loc_bucket->get_key().tenant = op_state.new_bucket_name.substr(0, pos);
      loc_bucket->get_key().name = op_state.new_bucket_name.substr(pos + 1);
    } else {
      loc_bucket->get_key().name = op_state.new_bucket_name;
    }
  }

  RGWObjVersionTracker objv_tracker;
  RGWObjVersionTracker old_version = loc_bucket->get_info().objv_tracker;

  map<string, bufferlist>::iterator aiter = loc_bucket->get_attrs().find(RGW_ATTR_ACL);
  if (aiter == loc_bucket->get_attrs().end()) {
	// should never happen; only pre-argonaut buckets lacked this.
    ldpp_dout(dpp, 0) << "WARNING: can't bucket link because no acl on bucket=" << old_bucket << dendl;
    set_err_msg(err,
	"While crossing the Anavros you have displeased the goddess Hera."
	"  You must sacrifice your ancient bucket " + loc_bucket->get_bucket_id());
    return -EINVAL;
  }
  bufferlist& aclbl = aiter->second;
  RGWAccessControlPolicy policy;
  ACLOwner owner;
  try {
   auto iter = aclbl.cbegin();
   decode(policy, iter);
   owner = policy.get_owner();
  } catch (buffer::error& e) {
    set_err_msg(err, "couldn't decode policy");
    return -EIO;
  }

  int r = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->unlink_bucket(owner.id, old_bucket->get_info().bucket, y, dpp, false);
  if (r < 0) {
    set_err_msg(err, "could not unlink policy from user " + owner.id.to_str());
    return r;
  }

  // now update the user for the bucket...
  if (display_name.empty()) {
    ldpp_dout(dpp, 0) << "WARNING: user " << op_state.get_user_id() << " has no display name set" << dendl;
  }

  RGWAccessControlPolicy policy_instance;
  policy_instance.create_default(op_state.get_user_id(), display_name);
  owner = policy_instance.get_owner();

  aclbl.clear();
  policy_instance.encode(aclbl);

  bool exclusive = false;
  loc_bucket->get_info().owner = op_state.get_user_id();
  if (*loc_bucket != *old_bucket) {
    loc_bucket->get_info().bucket = loc_bucket->get_key();
    loc_bucket->get_info().objv_tracker.version_for_read()->ver = 0;
    exclusive = true;
  }

  r = loc_bucket->put_info(dpp, exclusive, ceph::real_time(), y);
  if (r < 0) {
    set_err_msg(err, "ERROR: failed writing bucket instance info: " + cpp_strerror(-r));
    return r;
  }

  /* link to user */
  RGWBucketEntryPoint ep;
  ep.bucket = loc_bucket->get_info().bucket;
  ep.owner = op_state.get_user_id();
  ep.creation_time = loc_bucket->get_info().creation_time;
  ep.linked = true;
  rgw::sal::Attrs ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  r = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->link_bucket(op_state.get_user_id(), loc_bucket->get_info().bucket, loc_bucket->get_info().creation_time, y, dpp, true, &ep_data);
  if (r < 0) {
    set_err_msg(err, "failed to relink bucket");
    return r;
  }

  if (*loc_bucket != *old_bucket) {
    // like RGWRados::delete_bucket -- excepting no bucket_index work.
    r = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->remove_bucket_entrypoint_info(
					old_bucket->get_key(), y, dpp,
					RGWBucketCtl::Bucket::RemoveParams()
					.set_objv_tracker(&ep_data.ep_objv));
    if (r < 0) {
      set_err_msg(err, "failed to unlink old bucket " + old_bucket->get_tenant() + "/" + old_bucket->get_name());
      return r;
    }
    r = static_cast<rgw::sal::RadosStore*>(driver)->ctl()->bucket->remove_bucket_instance_info(
					old_bucket->get_key(), old_bucket->get_info(),
					y, dpp,
					RGWBucketCtl::BucketInstance::RemoveParams()
					.set_objv_tracker(&ep_data.ep_objv));
    if (r < 0) {
      set_err_msg(err, "failed to unlink old bucket " + old_bucket->get_tenant() + "/" + old_bucket->get_name());
      return r;
    }
  }

  return 0;
}

int RGWBucketAdminOp::chown(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const string& marker, const DoutPrefixProvider *dpp, optional_yield y, string *err)
{
  RGWBucket bucket;

  int ret = bucket.init(driver, op_state, y, dpp, err);
  if (ret < 0)
    return ret;

  return bucket.chown(op_state, marker, y, dpp, err);

}

int RGWBucketAdminOp::check_index_olh(rgw::sal::RadosStore* store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;
  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "bucket.init(): " << ret << dendl;
    return ret;
  }
  flusher.start(0);
  ret = bucket.check_index_olh(store, dpp, op_state, flusher);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "check_index_olh(): " << ret << dendl;
    return ret;
  }
  flusher.flush();
  return 0;
}

int RGWBucketAdminOp::check_index_unlinked(rgw::sal::RadosStore* store,
                                           RGWBucketAdminOpState& op_state,
                                           RGWFormatterFlusher& flusher,
                                           const DoutPrefixProvider *dpp)
{
  flusher.start(0);
  RGWBucket bucket;
  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "bucket.init(): " << ret << dendl;
    return ret;
  }
  ret = bucket.check_index_unlinked(store, dpp, op_state, flusher);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "check_index_unlinked(): " << ret << dendl;
    return ret;
  }
  flusher.flush();
  return 0;
}

int RGWBucketAdminOp::check_index(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, optional_yield y, const DoutPrefixProvider *dpp)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> existing_stats;
  map<RGWObjCategory, RGWStorageStats> calculated_stats;


  RGWBucket bucket;

  ret = bucket.init(driver, op_state, y, dpp);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  formatter->open_object_section("bucket_check");

  ret = bucket.check_bad_index_multipart(op_state, flusher, dpp, y);
  if (ret < 0)
    return ret;

  if (op_state.will_check_objects()) {
    ret = bucket.check_object_index(dpp, op_state, flusher, y);
    if (ret < 0)
      return ret;
  }

  ret = bucket.check_index(dpp, op_state, existing_stats, calculated_stats);
  if (ret < 0)
    return ret;

  dump_index_check(existing_stats, calculated_stats, formatter);
  
  formatter->close_section();
  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::remove_bucket(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
				    optional_yield y, const DoutPrefixProvider *dpp, 
                                    bool bypass_gc, bool keep_index_consistent)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;

  int ret = driver->load_bucket(dpp, rgw_bucket(op_state.get_tenant(),
                                                op_state.get_bucket_name()),
                                &bucket, y);
  if (ret < 0)
    return ret;

  if (bypass_gc)
    ret = bucket->remove_bypass_gc(op_state.get_max_aio(), keep_index_consistent, y, dpp);
  else
    ret = bucket->remove(dpp, op_state.will_delete_children(), y);

  return ret;
}

int RGWBucketAdminOp::remove_object(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWBucket bucket;

  int ret = bucket.init(driver, op_state, y, dpp);
  if (ret < 0)
    return ret;

  return bucket.remove_object(dpp, op_state, y);
}

int RGWBucketAdminOp::sync_bucket(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, string *err_msg)
{
  RGWBucket bucket;
  int ret = bucket.init(driver, op_state, y, dpp, err_msg);
  if (ret < 0)
  {
    return ret;
  }
  return bucket.sync(op_state, dpp, y, err_msg);
}

static int bucket_stats(rgw::sal::Driver* driver,
                        const std::string& tenant_name,
                        const std::string& bucket_name, Formatter* formatter,
                        const DoutPrefixProvider* dpp, optional_yield y) {
  std::unique_ptr<rgw::sal::Bucket> bucket;
  map<RGWObjCategory, RGWStorageStats> stats;

  int ret = driver->load_bucket(dpp, rgw_bucket(tenant_name, bucket_name),
                                &bucket, y);
  if (ret < 0) {
    return ret;
  }

  const RGWBucketInfo& bucket_info = bucket->get_info();

  const auto& index = bucket->get_info().get_current_index();
  if (is_layout_indexless(index)) {
    cerr << "error, indexless buckets do not maintain stats; bucket=" <<
      bucket->get_name() << std::endl;
    return -EINVAL;
  }

  std::string bucket_ver, master_ver;
  std::string max_marker;
  ret = bucket->read_stats(dpp, index, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0) {
    cerr << "error getting bucket stats bucket=" << bucket->get_name() << " ret=" << ret << std::endl;
    return ret;
  }

  utime_t ut(bucket->get_modification_time());
  utime_t ctime_ut(bucket->get_creation_time());

  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket->get_name());
  formatter->dump_string("tenant", bucket->get_tenant());
  formatter->dump_string("versioning",
			 bucket->versioned()
			 ? (bucket->versioning_enabled() ? "enabled" : "suspended")
			 : "off");
  formatter->dump_string("zonegroup", bucket->get_info().zonegroup);
  formatter->dump_string("placement_rule", bucket->get_info().placement_rule.to_str());
  ::encode_json("explicit_placement", bucket->get_key().explicit_placement, formatter);
  formatter->dump_string("id", bucket->get_bucket_id());
  formatter->dump_string("marker", bucket->get_marker());
  formatter->dump_stream("index_type") << bucket->get_info().layout.current_index.layout.type;
  formatter->dump_int("index_generation", bucket->get_info().layout.current_index.gen);
  formatter->dump_int("num_shards",
		      bucket->get_info().layout.current_index.layout.normal.num_shards);
  formatter->dump_bool("object_lock_enabled", bucket_info.obj_lock_enabled());
  formatter->dump_bool("mfa_enabled", bucket_info.mfa_enabled());
  ::encode_json("owner", bucket->get_info().owner, formatter);
  formatter->dump_string("ver", bucket_ver);
  formatter->dump_string("master_ver", master_ver);
  ut.gmtime(formatter->dump_stream("mtime"));
  ctime_ut.gmtime(formatter->dump_stream("creation_time"));
  formatter->dump_string("max_marker", max_marker);
  dump_bucket_usage(stats, formatter);
  encode_json("bucket_quota", bucket->get_info().quota, formatter);

  // bucket tags
  auto iter = bucket->get_attrs().find(RGW_ATTR_TAGS);
  if (iter != bucket->get_attrs().end()) {
    RGWObjTagSet_S3 tagset;
    bufferlist::const_iterator piter{&iter->second};
    try {
      tagset.decode(piter);
      tagset.dump(formatter); 
    } catch (buffer::error& err) {
      cerr << "ERROR: caught buffer:error, couldn't decode TagSet" << std::endl;
    }
  }

  // bucket notifications
  RGWPubSub ps(driver, tenant_name);
  rgw_pubsub_bucket_topics result;
  const RGWPubSub::Bucket b(ps, bucket.get());
  ret = b.get_topics(dpp, result, y);
  if (ret < 0 && ret != -ENOENT) {
    cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
    return -ret;
  }
  result.dump(formatter);

  // TODO: bucket CORS
  // TODO: bucket LC
  formatter->close_section();

  return 0;
}

int RGWBucketAdminOp::limit_check(rgw::sal::Driver* driver,
				  RGWBucketAdminOpState& op_state,
				  const std::list<std::string>& user_ids,
				  RGWFormatterFlusher& flusher, optional_yield y,
                                  const DoutPrefixProvider *dpp,
				  bool warnings_only)
{
  int ret = 0;
  const size_t max_entries =
    driver->ctx()->_conf->rgw_list_buckets_max_chunk;

  const size_t safe_max_objs_per_shard =
    driver->ctx()->_conf->rgw_safe_max_objects_per_shard;

  uint16_t shard_warn_pct =
    driver->ctx()->_conf->rgw_shard_warning_threshold;
  if (shard_warn_pct > 100)
    shard_warn_pct = 90;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  formatter->open_array_section("users");

  for (const auto& user_id : user_ids) {
    std::unique_ptr<rgw::sal::User> user = driver->get_user(rgw_user(user_id));

    formatter->open_object_section("user");
    formatter->dump_string("user_id", user_id);
    formatter->open_array_section("buckets");

    rgw::sal::BucketList listing;
    do {
      ret = user->list_buckets(dpp, listing.next_marker, string(),
                               max_entries, false, listing, y);
      if (ret < 0)
        return ret;

      for (const auto& ent : listing.buckets) {
	uint64_t num_objects = 0;

	std::unique_ptr<rgw::sal::Bucket> bucket;
	ret = driver->load_bucket(dpp, ent.bucket, &bucket, y);
	if (ret < 0)
	  continue;

	const auto& index = bucket->get_info().get_current_index();
	if (is_layout_indexless(index)) {
	  continue; // indexless buckets don't have stats
	}

	/* need stats for num_entries */
	string bucket_ver, master_ver;
	std::map<RGWObjCategory, RGWStorageStats> stats;
	ret = bucket->read_stats(dpp, index, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, nullptr);

	if (ret < 0)
	  continue;

	for (const auto& s : stats) {
	  num_objects += s.second.num_objects;
	}

	const uint32_t num_shards = rgw::num_shards(index.layout.normal);
	uint64_t objs_per_shard =
	  (num_shards) ? num_objects/num_shards : num_objects;
	{
	  bool warn;
	  stringstream ss;
	  uint64_t fill_pct = objs_per_shard * 100 / safe_max_objs_per_shard;
	  if (fill_pct > 100) {
	    ss << "OVER " << fill_pct << "%";
	    warn = true;
	  } else if (fill_pct >= shard_warn_pct) {
	    ss << "WARN " << fill_pct << "%";
	    warn = true;
	  } else {
	    ss << "OK";
	    warn = false;
	  }

	  if (warn || !warnings_only) {
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
    } while (!listing.next_marker.empty()); /* foreach: bucket */

    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);

  } /* foreach: user_id */

  formatter->close_section();
  formatter->flush(cout);

  return ret;
} /* RGWBucketAdminOp::limit_check */

int RGWBucketAdminOp::info(rgw::sal::Driver* driver,
			   RGWBucketAdminOpState& op_state,
			   RGWFormatterFlusher& flusher,
			   optional_yield y,
                           const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;
  int ret = 0;
  const std::string& bucket_name = op_state.get_bucket_name();
  if (!bucket_name.empty()) {
    ret = bucket.init(driver, op_state, y, dpp);
    if (-ENOENT == ret)
      return -ERR_NO_SUCH_BUCKET;
    else if (ret < 0)
      return ret;
  }

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  const bool show_stats = op_state.will_fetch_stats();
  const rgw_user& user_id = op_state.get_user_id();
  if (!bucket_name.empty()) {
    ret = bucket_stats(driver, user_id.tenant, bucket_name, formatter, dpp, y);
    if (ret < 0) {
      return ret;
    }
  } else if (op_state.is_user_op()) {
    formatter->open_array_section("buckets");

    std::unique_ptr<rgw::sal::User> user = driver->get_user(op_state.get_user_id());
    const std::string empty_end_marker;
    const size_t max_entries = dpp->get_cct()->_conf->rgw_list_buckets_max_chunk;
    constexpr bool no_need_stats = false; // set need_stats to false

    rgw::sal::BucketList listing;
    listing.next_marker = op_state.marker;
    do {
      ret = user->list_buckets(dpp, listing.next_marker, empty_end_marker,
                               max_entries, no_need_stats, listing, y);
      if (ret < 0) {
        return ret;
      }

      for (const auto& ent : listing.buckets) {
        if (show_stats) {
          bucket_stats(driver, user_id.tenant, ent.bucket.name, formatter, dpp, y);
	} else {
          formatter->dump_string("bucket", ent.bucket.name);
	}
      } // for loop

      flusher.flush();
    } while (!listing.next_marker.empty());

    formatter->close_section();
  } else {
    void *handle = nullptr;
    bool truncated = true;

    formatter->open_array_section("buckets");
    ret = driver->meta_list_keys_init(dpp, "bucket", string(), &handle);
    while (ret == 0 && truncated) {
      std::list<std::string> buckets;
      constexpr int max_keys = 1000;
      ret = driver->meta_list_keys_next(dpp, handle, max_keys, buckets,
						   &truncated);
      for (auto& bucket_name : buckets) {
        if (show_stats) {
          bucket_stats(driver, user_id.tenant, bucket_name, formatter, dpp, y);
	} else {
          formatter->dump_string("bucket", bucket_name);
	}
      }
    }
    driver->meta_list_keys_complete(handle);

    formatter->close_section();
  }

  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::set_quota(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWBucket bucket;

  int ret = bucket.init(driver, op_state, y, dpp);
  if (ret < 0)
    return ret;
  return bucket.set_quota(op_state, dpp, y);
}

inline auto split_tenant(const std::string& bucket_name){
  auto p = bucket_name.find('/');
  if(p != std::string::npos) {
    return std::make_pair(bucket_name.substr(0,p), bucket_name.substr(p+1));
  }
  return std::make_pair(std::string(), bucket_name);
}

using bucket_instance_ls = std::vector<RGWBucketInfo>;
void get_stale_instances(rgw::sal::Driver* driver, const std::string& bucket_name,
                         const vector<std::string>& lst,
                         bucket_instance_ls& stale_instances,
                         const DoutPrefixProvider *dpp, optional_yield y)
{

  bucket_instance_ls other_instances;
// first iterate over the entries, and pick up the done buckets; these
// are guaranteed to be stale
  for (const auto& bucket_instance : lst){
    RGWBucketInfo binfo;
    std::unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket rbucket;
    rgw_bucket_parse_bucket_key(driver->ctx(), bucket_instance, &rbucket, nullptr);
    int r = driver->load_bucket(dpp, rbucket, &bucket, y);
    if (r < 0){
      // this can only happen if someone deletes us right when we're processing
      ldpp_dout(dpp, -1) << "Bucket instance is invalid: " << bucket_instance
                          << cpp_strerror(-r) << dendl;
      continue;
    }
    binfo = bucket->get_info();
    if (binfo.reshard_status == cls_rgw_reshard_status::DONE)
      stale_instances.emplace_back(std::move(binfo));
    else {
      other_instances.emplace_back(std::move(binfo));
    }
  }

  // Read the cur bucket info, if the bucket doesn't exist we can simply return
  // all the instances
  auto [tenant, bname] = split_tenant(bucket_name);
  RGWBucketInfo cur_bucket_info;
  std::unique_ptr<rgw::sal::Bucket> cur_bucket;
  int r = driver->load_bucket(dpp, rgw_bucket(tenant, bname),
                              &cur_bucket, y);
  if (r < 0) {
    if (r == -ENOENT) {
      // bucket doesn't exist, everything is stale then
      stale_instances.insert(std::end(stale_instances),
                             std::make_move_iterator(other_instances.begin()),
                             std::make_move_iterator(other_instances.end()));
    } else {
      // all bets are off if we can't read the bucket, just return the sureshot stale instances
      ldpp_dout(dpp, -1) << "error: reading bucket info for bucket: "
                          << bname << cpp_strerror(-r) << dendl;
    }
    return;
  }

  // Don't process further in this round if bucket is resharding
  cur_bucket_info = cur_bucket->get_info();
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
    RGWBucketReshardLock reshard_lock(static_cast<rgw::sal::RadosStore*>(driver), cur_bucket->get_info(), true);
    r = reshard_lock.lock(dpp);
    if (r < 0) {
      // most likely bucket is under reshard, return the sureshot stale instances
      ldpp_dout(dpp, 5) << __func__
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

static int process_stale_instances(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state,
                                   RGWFormatterFlusher& flusher,
                                   const DoutPrefixProvider *dpp,
                                   std::function<void(const bucket_instance_ls&,
                                                      Formatter *,
                                                      rgw::sal::Driver*)> process_f, optional_yield y)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  int ret = driver->meta_list_keys_init(dpp, "bucket.instance", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  bool truncated;

  formatter->open_array_section("keys");
  auto g = make_scope_guard([&driver, &handle, &formatter]() {
                              driver->meta_list_keys_complete(handle);
                              formatter->close_section(); // keys
                              formatter->flush(cout);
                            });

  do {
    list<std::string> keys;

    ret = driver->meta_list_keys_next(dpp, handle, default_max_keys, keys, &truncated);
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
        get_stale_instances(driver, kv.first, kv.second, stale_lst, dpp, y);
        process_f(stale_lst, formatter, driver);
      }
    }
  } while (truncated);

  return 0;
}

int RGWBucketAdminOp::list_stale_instances(rgw::sal::Driver* driver,
                                           RGWBucketAdminOpState& op_state,
                                           RGWFormatterFlusher& flusher,
                                           const DoutPrefixProvider *dpp, optional_yield y)
{
  auto process_f = [](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      rgw::sal::Driver*){
                     for (const auto& binfo: lst)
                       formatter->dump_string("key", binfo.bucket.get_key());
                   };
  return process_stale_instances(driver, op_state, flusher, dpp, process_f, y);
}


int RGWBucketAdminOp::clear_stale_instances(rgw::sal::Driver* driver,
                                            RGWBucketAdminOpState& op_state,
                                            RGWFormatterFlusher& flusher,
                                            const DoutPrefixProvider *dpp, optional_yield y)
{
  auto process_f = [dpp, y](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      rgw::sal::Driver* driver){
                     for (const auto &binfo: lst) {
		       auto bucket = driver->get_bucket(binfo);
		       int ret = bucket->purge_instance(dpp, y);
                       if (ret == 0){
                         auto md_key = "bucket.instance:" + binfo.bucket.get_key();
                         ret = driver->meta_remove(dpp, md_key, y);
                       }
                       formatter->open_object_section("delete_status");
                       formatter->dump_string("bucket_instance", binfo.bucket.get_key());
                       formatter->dump_int("status", -ret);
                       formatter->close_section();
                     }
                   };

  return process_stale_instances(driver, op_state, flusher, dpp, process_f, y);
}

static int fix_single_bucket_lc(rgw::sal::Driver* driver,
                                const std::string& tenant_name,
                                const std::string& bucket_name,
                                const DoutPrefixProvider *dpp, optional_yield y)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = driver->load_bucket(dpp, rgw_bucket(tenant_name, bucket_name),
                                &bucket, y);
  if (ret < 0) {
    // TODO: Should we handle the case where the bucket could've been removed between
    // listing and fetching?
    return ret;
  }

  return rgw::lc::fix_lc_shard_entry(dpp, driver, driver->get_rgwlc()->get_lc(), bucket.get());
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

static void process_single_lc_entry(rgw::sal::Driver* driver,
				    Formatter *formatter,
                                    const std::string& tenant_name,
                                    const std::string& bucket_name,
                                    const DoutPrefixProvider *dpp, optional_yield y)
{
  int ret = fix_single_bucket_lc(driver, tenant_name, bucket_name, dpp, y);
  format_lc_status(formatter, tenant_name, bucket_name, -ret);
}

int RGWBucketAdminOp::fix_lc_shards(rgw::sal::Driver* driver,
                                    RGWBucketAdminOpState& op_state,
                                    RGWFormatterFlusher& flusher,
                                    const DoutPrefixProvider *dpp, optional_yield y)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  bool truncated;
  if (const std::string& bucket_name = op_state.get_bucket_name();
      ! bucket_name.empty()) {
    const rgw_user user_id = op_state.get_user_id();
    process_single_lc_entry(driver, formatter, user_id.tenant, bucket_name, dpp, y);
    formatter->flush(cout);
  } else {
    int ret = driver->meta_list_keys_init(dpp, "bucket", marker, &handle);
    if (ret < 0) {
      std::cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    {
      formatter->open_array_section("lc_fix_status");
      auto sg = make_scope_guard([&driver, &handle, &formatter](){
                                   driver->meta_list_keys_complete(handle);
                                   formatter->close_section(); // lc_fix_status
                                   formatter->flush(cout);
                                 });
      do {
        list<std::string> keys;
        ret = driver->meta_list_keys_next(dpp, handle, default_max_keys, keys, &truncated);
        if (ret < 0 && ret != -ENOENT) {
          std::cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
          return ret;
        } if (ret != -ENOENT) {
          for (const auto &key:keys) {
            auto [tenant_name, bucket_name] = split_tenant(key);
            process_single_lc_entry(driver, formatter, tenant_name, bucket_name, dpp, y);
          }
        }
        formatter->flush(cout); // regularly flush every 1k entries
      } while (truncated);
    }

  }
  return 0;

}

static bool has_object_expired(const DoutPrefixProvider *dpp,
			       rgw::sal::Driver* driver,
			       rgw::sal::Bucket* bucket,
			       const rgw_obj_key& key, utime_t& delete_at, optional_yield y)
{
  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);
  bufferlist delete_at_bl;

  int ret = rgw_object_get_attr(dpp, driver, obj.get(), RGW_ATTR_DELETE_AT, delete_at_bl, y);
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

static int fix_bucket_obj_expiry(const DoutPrefixProvider *dpp,
				 rgw::sal::Driver* driver,
				 rgw::sal::Bucket* bucket,
				 RGWFormatterFlusher& flusher, bool dry_run, optional_yield y)
{
  if (bucket->get_key().bucket_id == bucket->get_key().marker) {
    ldpp_dout(dpp, -1) << "Not a resharded bucket skipping" << dendl;
    return 0;  // not a resharded bucket, move along
  }

  Formatter *formatter = flusher.get_formatter();
  formatter->open_array_section("expired_deletion_status");
  auto sg = make_scope_guard([&formatter] {
			       formatter->close_section();
			       formatter->flush(std::cout);
			     });

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.list_versions = bucket->versioned();
  params.allow_unordered = true;

  do {
    int ret = bucket->list(dpp, params, listing_max_entries, results, y);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR failed to list objects in the bucket" << dendl;
      return ret;
    }
    for (const auto& obj : results.objs) {
      rgw_obj_key key(obj.key);
      utime_t delete_at;
      if (has_object_expired(dpp, driver, bucket, key, delete_at, y)) {
	formatter->open_object_section("object_status");
	formatter->dump_string("object", key.name);
	formatter->dump_stream("delete_at") << delete_at;

	if (!dry_run) {
	  ret = rgw_remove_object(dpp, driver, bucket, key, y);
	  formatter->dump_int("status", ret);
	}

	formatter->close_section();  // object_status
      }
    }
    formatter->flush(cout); // regularly flush every 1k entries
  } while (results.is_truncated);

  return 0;
}

int RGWBucketAdminOp::fix_obj_expiry(rgw::sal::Driver* driver,
				     RGWBucketAdminOpState& op_state,
				     RGWFormatterFlusher& flusher,
                                     const DoutPrefixProvider *dpp, optional_yield y, bool dry_run)
{
  RGWBucket admin_bucket;
  int ret = admin_bucket.init(driver, op_state, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "failed to initialize bucket" << dendl;
    return ret;
  }
  auto bucket = driver->get_bucket(admin_bucket.get_bucket_info());

  return fix_bucket_obj_expiry(dpp, driver, bucket.get(), flusher, dry_run, y);
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

  int do_get(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWObjVersionTracker ot;
    RGWBucketEntryPoint be;

    real_time mtime;
    map<string, bufferlist> attrs;

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &ot, &mtime, &attrs, y, dpp);
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
             const DoutPrefixProvider *dpp,
             RGWMDLogSyncType type, bool from_remote_zone) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWBucketEntryPoint be;

    real_time orig_mtime;

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &objv_tracker, &orig_mtime, nullptr, y, dpp);
    if (ret < 0)
      return ret;

    /*
     * We're unlinking the bucket but we don't want to update the entrypoint here - we're removing
     * it immediately and don't want to invalidate our cached objv_version or the bucket obj removal
     * will incorrectly fail.
     */
    ret = ctl.bucket->unlink_bucket(be.owner, be.bucket, y, dpp, false);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    ret = svc.bucket->remove_bucket_entrypoint_info(ctx, entry, &objv_tracker, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "could not delete bucket=" << entry << dendl;
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
                               RGWMDLogSyncType type, bool from_remote_zone) : RGWMetadataHandlerPut_SObj(_handler, op, entry, _obj, objv_tracker, y, type, from_remote_zone),
                                                        bhandler(_handler) {
    obj = static_cast<RGWBucketEntryMetadataObject *>(_obj);
  }
  ~RGWMetadataHandlerPut_Bucket() {}

  void encode_obj(bufferlist *bl) override {
    obj->get_ep().encode(*bl);
  }

  int put_checked(const DoutPrefixProvider *dpp) override;
  int put_post(const DoutPrefixProvider *dpp) override;
};

int RGWBucketMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
                                     RGWMetadataObject *obj,
                                     RGWObjVersionTracker& objv_tracker,
				     optional_yield y,
                                     const DoutPrefixProvider *dpp,
                                     RGWMDLogSyncType type, bool from_remote_zone)
{
  RGWMetadataHandlerPut_Bucket put_op(this, op, entry, obj, objv_tracker, y, type, from_remote_zone);
  return do_put_operate(&put_op, dpp);
}

int RGWMetadataHandlerPut_Bucket::put_checked(const DoutPrefixProvider *dpp)
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
							   y,
                                                           dpp);
}

int RGWMetadataHandlerPut_Bucket::put_post(const DoutPrefixProvider *dpp)
{
  auto& be = obj->get_ep();

  int ret;

  /* link bucket */
  if (be.linked) {
    ret = bhandler->ctl.bucket->link_bucket(be.owner, be.bucket, be.creation_time, y, dpp, false);
  } else {
    ret = bhandler->ctl.bucket->unlink_bucket(be.owner, be.bucket, y, dpp, false);
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
   // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
   hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
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
                optional_yield y, const DoutPrefixProvider *dpp) override {
    auto cct = svc.bucket->ctx();

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    ldpp_dout(dpp, 5) << "SKIP: bucket removal is not allowed on archive zone: bucket:" << entry << " ... proceeding to rename" << dendl;

    string tenant_name, bucket_name;
    parse_bucket(entry, &tenant_name, &bucket_name);
    rgw_bucket entry_bucket;
    entry_bucket.tenant = tenant_name;
    entry_bucket.name = bucket_name;

    real_time mtime;

    /* read original entrypoint */

    RGWBucketEntryPoint be;
    map<string, bufferlist> attrs;
    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &objv_tracker, &mtime, &attrs, y, dpp);
    if (ret < 0) {
        return ret;
    }

    string bi_meta_name = RGWSI_Bucket::get_bi_meta_key(be.bucket);

    /* read original bucket instance info */

    map<string, bufferlist> attrs_m;
    ceph::real_time orig_mtime;
    RGWBucketInfo old_bi;

    ret = ctl.bucket->read_bucket_instance_info(be.bucket, &old_bi, y, dpp, RGWBucketCtl::BucketInstance::GetParams()
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

    ret = ctl.bucket->store_bucket_instance_info(new_be.bucket, new_bi, y, dpp, RGWBucketCtl::BucketInstance::PutParams()
                                                                    .set_exclusive(false)
                                                                    .set_mtime(orig_mtime)
                                                                    .set_attrs(&attrs_m)
                                                                    .set_orig_info(&old_bi));
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put new bucket instance info for bucket=" << new_bi.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* store a new entrypoint */

    RGWObjVersionTracker ot;
    ot.generate_new_write_ver(cct);

    ret = svc.bucket->store_bucket_entrypoint_info(ctx, RGWSI_Bucket::get_entrypoint_meta_key(new_be.bucket),
                                                   new_be, true, mtime, &attrs, nullptr, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* link new bucket */

    ret = ctl.bucket->link_bucket(new_be.owner, new_be.bucket, new_be.creation_time, y, dpp, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to link new bucket for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* clean up old stuff */

    ret = ctl.bucket->unlink_bucket(be.owner, entry_bucket, y, dpp, false);
    if (ret < 0) {
        ldpp_dout(dpp, -1) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    // if (ret == -ECANCELED) it means that there was a race here, and someone
    // wrote to the bucket entrypoint just before we removed it. The question is
    // whether it was a newly created bucket entrypoint ...  in which case we
    // should ignore the error and move forward, or whether it is a higher version
    // of the same bucket instance ... in which we should retry
    ret = svc.bucket->remove_bucket_entrypoint_info(ctx,
                                                    RGWSI_Bucket::get_entrypoint_meta_key(be.bucket),
                                                    &objv_tracker,
                                                    y,
                                                    dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    ret = ctl.bucket->remove_bucket_instance_info(be.bucket, old_bi, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "could not delete bucket=" << entry << dendl;
    }


    /* idempotent */

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp,
             RGWMDLogSyncType type, bool from_remote_zone) override {
    if (entry.find("-deleted-") != string::npos) {
      RGWObjVersionTracker ot;
      RGWMetadataObject *robj;
      int ret = do_get(op, entry, &robj, y, dpp);
      if (ret != -ENOENT) {
        if (ret < 0) {
          return ret;
        }
        ot.read_version = robj->get_version();
        delete robj;

        ret = do_remove(op, entry, ot, y, dpp);
        if (ret < 0) {
          return ret;
        }
      }
    }

    return RGWBucketMetadataHandler::do_put(op, entry, obj,
                                            objv_tracker, y, dpp, type, from_remote_zone);
  }

};

class RGWBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandlerBase {
  int read_bucket_instance_entry(RGWSI_Bucket_BI_Ctx& ctx,
                                 const string& entry,
                                 RGWBucketCompleteInfo *bi,
                                 ceph::real_time *pmtime,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp) {
    return svc.bucket->read_bucket_instance_info(ctx,
                                                 entry,
                                                 &bi->info,
                                                 pmtime, &bi->attrs,
                                                 y,
                                                 dpp);
  }

public:
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Bucket *bucket{nullptr};
    RGWSI_BucketIndex *bi{nullptr};
  } svc;

  rgw::sal::Driver* driver;

  RGWBucketInstanceMetadataHandler(rgw::sal::Driver* driver)
    : driver(driver) {}

  void init(RGWSI_Zone *zone_svc,
	    RGWSI_Bucket *bucket_svc,
	    RGWSI_BucketIndex *bi_svc) override {
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

  int do_get(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWBucketCompleteInfo bci;
    real_time mtime;

    RGWSI_Bucket_BI_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_instance_info(ctx, entry, &bci.info, &mtime, &bci.attrs, y, dpp);
    if (ret < 0)
      return ret;

    RGWBucketInstanceMetadataObject *mdo = new RGWBucketInstanceMetadataObject(bci, bci.info.objv_tracker.read_version, mtime);

    *obj = mdo;

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
	     optional_yield y, const DoutPrefixProvider *dpp,
             RGWMDLogSyncType sync_type, bool from_remote_zone) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWBucketCompleteInfo bci;

    RGWSI_Bucket_BI_Ctx ctx(op->ctx());

    int ret = read_bucket_instance_entry(ctx, entry, &bci, nullptr, y, dpp);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    return svc.bucket->remove_bucket_instance_info(ctx, entry, bci.info, &bci.info.objv_tracker, y, dpp);
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
  RGWMetadataHandlerPut_BucketInstance(CephContext *_cct,
                                       RGWBucketInstanceMetadataHandler *_handler,
                                       RGWSI_MetaBackend_Handler::Op *_op, string& entry,
                                       RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
				       optional_yield y,
                                       RGWMDLogSyncType type, bool from_remote_zone) : RGWMetadataHandlerPut_SObj(_handler, _op, entry, _obj, objv_tracker, y, type, from_remote_zone),
                                       cct(_cct), bihandler(_handler) {
    obj = static_cast<RGWBucketInstanceMetadataObject *>(_obj);

    auto& bci = obj->get_bci();
    obj->set_pattrs(&bci.attrs);
  }

  void encode_obj(bufferlist *bl) override {
    obj->get_bucket_info().encode(*bl);
  }

  int put_check(const DoutPrefixProvider *dpp) override;
  int put_checked(const DoutPrefixProvider *dpp) override;
  int put_post(const DoutPrefixProvider *dpp) override;
};

int RGWBucketInstanceMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op,
                                             string& entry,
                                             RGWMetadataObject *obj,
                                             RGWObjVersionTracker& objv_tracker,
                                             optional_yield y,
                                             const DoutPrefixProvider *dpp,
                                             RGWMDLogSyncType type, bool from_remote_zone)
{
  RGWMetadataHandlerPut_BucketInstance put_op(svc.bucket->ctx(), this, op, entry, obj,
                                              objv_tracker, y, type, from_remote_zone);
  return do_put_operate(&put_op, dpp);
}

void init_default_bucket_layout(CephContext *cct, rgw::BucketLayout& layout,
				const RGWZone& zone,
				std::optional<rgw::BucketIndexType> type) {
  layout.current_index.gen = 0;
  layout.current_index.layout.normal.hash_type = rgw::BucketHashType::Mod;

  layout.current_index.layout.type =
    type.value_or(rgw::BucketIndexType::Normal);

  if (cct->_conf->rgw_override_bucket_index_max_shards > 0) {
    layout.current_index.layout.normal.num_shards =
      cct->_conf->rgw_override_bucket_index_max_shards;
  } else {
    layout.current_index.layout.normal.num_shards =
      zone.bucket_index_max_shards;
  }

  if (layout.current_index.layout.type == rgw::BucketIndexType::Normal) {
    layout.logs.push_back(log_layout_from_index(0, layout.current_index));
  }
}

int RGWMetadataHandlerPut_BucketInstance::put_check(const DoutPrefixProvider *dpp)
{
  int ret;

  RGWBucketCompleteInfo& bci = obj->get_bci();

  RGWBucketInstanceMetadataObject *orig_obj = static_cast<RGWBucketInstanceMetadataObject *>(old_obj);

  RGWBucketCompleteInfo *old_bci = (orig_obj ? &orig_obj->get_bci() : nullptr);

  const bool exists = (!!orig_obj);

  if (from_remote_zone) {
    // don't sync bucket layout changes
    if (!exists) {
      // replace peer's layout with default-constructed, then apply our defaults
      bci.info.layout = rgw::BucketLayout{};
      init_default_bucket_layout(cct, bci.info.layout,
				 bihandler->svc.zone->get_zone(),
				 std::nullopt);
    } else {
      bci.info.layout = old_bci->info.layout;
    }
  }

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
      ret = bihandler->svc.zone->select_bucket_location_by_rule(dpp, bci.info.placement_rule, &rule_info, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: select_bucket_placement() returned " << ret << dendl;
        return ret;
      }
    }
    bci.info.layout.current_index.layout.type = rule_info.index_type;
  } else {
    /* existing bucket, keep its placement */
    bci.info.bucket.explicit_placement = old_bci->info.bucket.explicit_placement;
    bci.info.placement_rule = old_bci->info.placement_rule;
  }

  //always keep bucket versioning enabled on archive zone
  if (bihandler->driver->get_zone()->get_tier_type() == "archive") {
    bci.info.flags = (bci.info.flags & ~BUCKET_VERSIONS_SUSPENDED) | BUCKET_VERSIONED;
  }

  /* record the read version (if any), store the new version */
  bci.info.objv_tracker.read_version = objv_tracker.read_version;
  bci.info.objv_tracker.write_version = objv_tracker.write_version;

  return 0;
}

int RGWMetadataHandlerPut_BucketInstance::put_checked(const DoutPrefixProvider *dpp)
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
							 y,
                                                         dpp);
}

int RGWMetadataHandlerPut_BucketInstance::put_post(const DoutPrefixProvider *dpp)
{
  RGWBucketCompleteInfo& bci = obj->get_bci();

  objv_tracker = bci.info.objv_tracker;

  int ret = bihandler->svc.bi->init_index(dpp, bci.info, bci.info.layout.current_index);
  if (ret < 0) {
    return ret;
  }

  /* update lifecyle policy */
  {
    auto bucket = bihandler->driver->get_bucket(bci.info);

    auto lc = bihandler->driver->get_rgwlc();

    auto lc_it = bci.attrs.find(RGW_ATTR_LC);
    if (lc_it != bci.attrs.end()) {
      ldpp_dout(dpp, 20) << "set lc config for " << bci.info.bucket.name << dendl;
      ret = lc->set_bucket_config(bucket.get(), bci.attrs, nullptr);
      if (ret < 0) {
	      ldpp_dout(dpp, 0) << __func__ << " failed to set lc config for "
			<< bci.info.bucket.name
			<< dendl;
	      return ret;
      }

    } else {
      ldpp_dout(dpp, 20) << "remove lc config for " << bci.info.bucket.name << dendl;
      ret = lc->remove_bucket_config(bucket.get(), bci.attrs, false /* cannot merge attrs */);
      if (ret < 0) {
	      ldpp_dout(dpp, 0) << __func__ << " failed to remove lc config for "
			<< bci.info.bucket.name
			<< dendl;
	      return ret;
      }
    }
  } /* update lc */

  return STATUS_APPLIED;
}

class RGWArchiveBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandler {
public:
  RGWArchiveBucketInstanceMetadataHandler(rgw::sal::Driver* driver)
    : RGWBucketInstanceMetadataHandler(driver) {}

  // N.B. replication of lifecycle policy relies on logic in RGWBucketInstanceMetadataHandler::do_put(...), override with caution

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y, const DoutPrefixProvider *dpp) override {
    ldpp_dout(dpp, 0) << "SKIP: bucket instance removal is not allowed on archive zone: bucket.instance:" << entry << dendl;
    return 0;
  }
};

RGWBucketCtl::RGWBucketCtl(RGWSI_Zone *zone_svc,
                           RGWSI_Bucket *bucket_svc,
                           RGWSI_Bucket_Sync *bucket_sync_svc,
                           RGWSI_BucketIndex *bi_svc,
                           RGWSI_User* user_svc)
  : cct(zone_svc->ctx())
{
  svc.zone = zone_svc;
  svc.bucket = bucket_svc;
  svc.bucket_sync = bucket_sync_svc;
  svc.bi = bi_svc;
  svc.user = user_svc;
}

void RGWBucketCtl::init(RGWUserCtl *user_ctl,
                        RGWBucketMetadataHandler *_bm_handler,
                        RGWBucketInstanceMetadataHandler *_bmi_handler,
                        RGWDataChangesLog *datalog,
                        const DoutPrefixProvider *dpp)
{
  ctl.user = user_ctl;

  bm_handler = _bm_handler;
  bmi_handler = _bmi_handler;

  bucket_be_handler = bm_handler->get_be_handler();
  bi_be_handler = bmi_handler->get_be_handler();

  datalog->set_bucket_filter(
    [this](const rgw_bucket& bucket, optional_yield y, const DoutPrefixProvider *dpp) {
      return bucket_exports_data(bucket, y, dpp);
    });
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
                                              optional_yield y, const DoutPrefixProvider *dpp,
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
                                                   dpp,
                                                   params.cache_info,
                                                   params.refresh_version);
  });
}

int RGWBucketCtl::store_bucket_entrypoint_info(const rgw_bucket& bucket,
                                               RGWBucketEntryPoint& info,
                                               optional_yield y,
                                               const DoutPrefixProvider *dpp,
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
                                                    y,
                                                    dpp);
  });
}

int RGWBucketCtl::remove_bucket_entrypoint_info(const rgw_bucket& bucket,
                                                optional_yield y,
                                                const DoutPrefixProvider *dpp,
                                                const Bucket::RemoveParams& params)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return svc.bucket->remove_bucket_entrypoint_info(ctx,
                                                     RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                     params.objv_tracker,
						     y,
                                                     dpp);
  });
}

int RGWBucketCtl::read_bucket_instance_info(const rgw_bucket& bucket,
                                            RGWBucketInfo *info,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp,
                                            const BucketInstance::GetParams& params)
{
  int ret = bmi_handler->call(params.bectx_params, [&](RGWSI_Bucket_BI_Ctx& ctx) {
    return svc.bucket->read_bucket_instance_info(ctx,
                                                 RGWSI_Bucket::get_bi_meta_key(bucket),
                                                 info,
                                                 params.mtime,
                                                 params.attrs,
						 y,
                                                 dpp,
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
                                   const DoutPrefixProvider *dpp,
                                   const BucketInstance::GetParams& params,
                                   RGWObjVersionTracker *ep_objv_tracker)
{
  const rgw_bucket *b = &bucket;

  std::optional<RGWBucketEntryPoint> ep;

  if (b->bucket_id.empty()) {
    ep.emplace();

    int r = read_bucket_entrypoint_info(*b, &(*ep), y, dpp, RGWBucketCtl::Bucket::GetParams()
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
						 y, dpp,
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
                                                const DoutPrefixProvider *dpp,
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
                                                y,
                                                dpp);
}

int RGWBucketCtl::store_bucket_instance_info(const rgw_bucket& bucket,
                                            RGWBucketInfo& info,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp,
                                            const BucketInstance::PutParams& params)
{
  return bmi_handler->call([&](RGWSI_Bucket_BI_Ctx& ctx) {
    return do_store_bucket_instance_info(ctx, bucket, info, y, dpp, params);
  });
}

int RGWBucketCtl::remove_bucket_instance_info(const rgw_bucket& bucket,
                                              RGWBucketInfo& info,
                                              optional_yield y,
                                              const DoutPrefixProvider *dpp,
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
                                                   y,
                                                   dpp);
  });
}

int RGWBucketCtl::do_store_linked_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                              RGWBucketInfo& info,
                                              RGWBucketInfo *orig_info,
                                              bool exclusive, real_time mtime,
                                              obj_version *pep_objv,
                                              map<string, bufferlist> *pattrs,
                                              bool create_entry_point,
					      optional_yield y, const DoutPrefixProvider *dpp)
{
  bool create_head = !info.has_instance_obj || create_entry_point;

  int ret = svc.bucket->store_bucket_instance_info(ctx.bi,
                                                   RGWSI_Bucket::get_bi_meta_key(info.bucket),
                                                   info,
                                                   orig_info,
                                                   exclusive,
                                                   mtime, pattrs,
						   y, dpp);
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
						 y,
                                                 dpp);
  if (ret < 0)
    return ret;

  return 0;
}
int RGWBucketCtl::convert_old_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                          const rgw_bucket& bucket,
                                          optional_yield y,
                                          const DoutPrefixProvider *dpp)
{
  RGWBucketEntryPoint entry_point;
  real_time ep_mtime;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  RGWBucketInfo info;
  auto cct = svc.bucket->ctx();

  ldpp_dout(dpp, 10) << "RGWRados::convert_old_bucket_info(): bucket=" << bucket << dendl;

  int ret = svc.bucket->read_bucket_entrypoint_info(ctx.ep,
                                                    RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                    &entry_point, &ot, &ep_mtime, &attrs, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: get_bucket_entrypoint_info() returned " << ret << " bucket=" << bucket << dendl;
    return ret;
  }

  if (!entry_point.has_bucket_info) {
    /* already converted! */
    return 0;
  }

  info = entry_point.old_bucket_info;

  ot.generate_new_write_ver(cct);

  ret = do_store_linked_bucket_info(ctx, info, nullptr, false, ep_mtime, &ot.write_version, &attrs, true, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to put_linked_bucket_info(): " << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWBucketCtl::set_bucket_instance_attrs(RGWBucketInfo& bucket_info,
                                            map<string, bufferlist>& attrs,
                                            RGWObjVersionTracker *objv_tracker,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    rgw_bucket& bucket = bucket_info.bucket;

    if (!bucket_info.has_instance_obj) {
      /* an old bucket object, need to convert it */
        int ret = convert_old_bucket_info(ctx, bucket, y, dpp);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: failed converting old bucket info: " << ret << dendl;
          return ret;
        }
    }

    return do_store_bucket_instance_info(ctx.bi,
                                         bucket,
                                         bucket_info,
                                         y,
                                         dpp,
                                         BucketInstance::PutParams().set_attrs(&attrs)
                                                                    .set_objv_tracker(objv_tracker)
                                                                    .set_orig_info(&bucket_info));
    });
}


int RGWBucketCtl::link_bucket(const rgw_user& user_id,
                              const rgw_bucket& bucket,
                              ceph::real_time creation_time,
			      optional_yield y,
                              const DoutPrefixProvider *dpp,
                              bool update_entrypoint,
                              rgw_ep_info *pinfo)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return do_link_bucket(ctx, user_id, bucket, creation_time,
                          update_entrypoint, pinfo, y, dpp);
  });
}

int RGWBucketCtl::do_link_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                                 const rgw_user& user_id,
                                 const rgw_bucket& bucket,
                                 ceph::real_time creation_time,
                                 bool update_entrypoint,
                                 rgw_ep_info *pinfo,
				 optional_yield y,
                                 const DoutPrefixProvider *dpp)
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
                                                    y, dpp);
      if (ret < 0 && ret != -ENOENT) {
        ldpp_dout(dpp, 0) << "ERROR: read_bucket_entrypoint_info() returned: "
                      << cpp_strerror(-ret) << dendl;
      }
      pattrs = &attrs;
    }
  }

  ret = svc.user->add_bucket(dpp, user_id, bucket, creation_time, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: error adding bucket to user directory:"
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
    ctx, meta_key, ep, false, real_time(), pattrs, &rot, y, dpp);
  if (ret < 0)
    goto done_err;

  return 0;

done_err:
  int r = do_unlink_bucket(ctx, user_id, bucket, true, y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed unlinking bucket on error cleanup: "
                           << cpp_strerror(-r) << dendl;
  }
  return ret;
}

int RGWBucketCtl::unlink_bucket(const rgw_user& user_id, const rgw_bucket& bucket, optional_yield y, const DoutPrefixProvider *dpp, bool update_entrypoint)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return do_unlink_bucket(ctx, user_id, bucket, update_entrypoint, y, dpp);
  });
}

int RGWBucketCtl::do_unlink_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                                   const rgw_user& user_id,
                                   const rgw_bucket& bucket,
                                   bool update_entrypoint,
				   optional_yield y,
                                   const DoutPrefixProvider *dpp)
{
  int ret = svc.user->remove_bucket(dpp, user_id, bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: error removing bucket from directory: "
        << cpp_strerror(-ret)<< dendl;
  }

  if (!update_entrypoint)
    return 0;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  string meta_key = RGWSI_Bucket::get_entrypoint_meta_key(bucket);
  ret = svc.bucket->read_bucket_entrypoint_info(ctx, meta_key, &ep, &ot, nullptr, &attrs, y, dpp);
  if (ret == -ENOENT)
    return 0;
  if (ret < 0)
    return ret;

  if (!ep.linked)
    return 0;

  if (ep.owner != user_id) {
    ldpp_dout(dpp, 0) << "bucket entry point user mismatch, can't unlink bucket: " << ep.owner << " != " << user_id << dendl;
    return -EINVAL;
  }

  ep.linked = false;
  return svc.bucket->store_bucket_entrypoint_info(ctx, meta_key, ep, false, real_time(), &attrs, &ot, y, dpp);
}

int RGWBucketCtl::read_bucket_stats(const rgw_bucket& bucket,
                                    RGWBucketEnt *result,
                                    optional_yield y,
                                    const DoutPrefixProvider *dpp)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket->read_bucket_stats(ctx, bucket, result, y, dpp);
  });
}

int RGWBucketCtl::read_buckets_stats(map<string, RGWBucketEnt>& m,
                                     optional_yield y, const DoutPrefixProvider *dpp)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket->read_buckets_stats(ctx, m, y, dpp);
  });
}

int RGWBucketCtl::sync_user_stats(const DoutPrefixProvider *dpp, 
                                  const rgw_user& user_id,
                                  const RGWBucketInfo& bucket_info,
				  optional_yield y,
                                  RGWBucketEnt* pent)
{
  RGWBucketEnt ent;
  if (!pent) {
    pent = &ent;
  }
  int r = svc.bi->read_stats(dpp, bucket_info, pent, y);
  if (r < 0) {
    ldpp_dout(dpp, 20) << __func__ << "(): failed to read bucket stats (r=" << r << ")" << dendl;
    return r;
  }

  return svc.user->flush_bucket_stats(dpp, user_id, *pent, y);
}

int RGWBucketCtl::get_sync_policy_handler(std::optional<rgw_zone_id> zone,
                                          std::optional<rgw_bucket> bucket,
                                          RGWBucketSyncPolicyHandlerRef *phandler,
                                          optional_yield y,
                                          const DoutPrefixProvider *dpp)
{
  int r = call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket_sync->get_policy_handler(ctx, zone, bucket, phandler, y, dpp);
  });
  if (r < 0) {
    ldpp_dout(dpp, 20) << __func__ << "(): failed to get policy handler for bucket=" << bucket << " (r=" << r << ")" << dendl;
    return r;
  }
  return 0;
}

int RGWBucketCtl::bucket_exports_data(const rgw_bucket& bucket,
                                      optional_yield y,
                                      const DoutPrefixProvider *dpp)
{

  RGWBucketSyncPolicyHandlerRef handler;

  int r = get_sync_policy_handler(std::nullopt, bucket, &handler, y, dpp);
  if (r < 0) {
    return r;
  }

  return handler->bucket_exports_data();
}

int RGWBucketCtl::bucket_imports_data(const rgw_bucket& bucket,
                                      optional_yield y, const DoutPrefixProvider *dpp)
{

  RGWBucketSyncPolicyHandlerRef handler;

  int r = get_sync_policy_handler(std::nullopt, bucket, &handler, y, dpp);
  if (r < 0) {
    return r;
  }

  return handler->bucket_imports_data();
}

RGWBucketMetadataHandlerBase* RGWBucketMetaHandlerAllocator::alloc()
{
  return new RGWBucketMetadataHandler();
}

RGWBucketInstanceMetadataHandlerBase* RGWBucketInstanceMetaHandlerAllocator::alloc(rgw::sal::Driver* driver)
{
  return new RGWBucketInstanceMetadataHandler(driver);
}

RGWBucketMetadataHandlerBase* RGWArchiveBucketMetaHandlerAllocator::alloc()
{
  return new RGWArchiveBucketMetadataHandler();
}

RGWBucketInstanceMetadataHandlerBase* RGWArchiveBucketInstanceMetaHandlerAllocator::alloc(rgw::sal::Driver* driver)
{
  return new RGWArchiveBucketInstanceMetadataHandler(driver);
}


void RGWBucketEntryPoint::generate_test_instances(list<RGWBucketEntryPoint*>& o)
{
  RGWBucketEntryPoint *bp = new RGWBucketEntryPoint();
  init_bucket(&bp->bucket, "tenant", "bucket", "pool", ".index.pool", "marker", "10");
  bp->owner = "owner";
  bp->creation_time = ceph::real_clock::from_ceph_timespec({ceph_le32(2), ceph_le32(3)});

  o.push_back(bp);
  o.push_back(new RGWBucketEntryPoint);
}

void RGWBucketEntryPoint::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("owner", owner, f);
  utime_t ut(creation_time);
  encode_json("creation_time", ut, f);
  encode_json("linked", linked, f);
  encode_json("has_bucket_info", has_bucket_info, f);
  if (has_bucket_info) {
    encode_json("old_bucket_info", old_bucket_info, f);
  }
}

void RGWBucketEntryPoint::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket", bucket, obj);
  JSONDecoder::decode_json("owner", owner, obj);
  utime_t ut;
  JSONDecoder::decode_json("creation_time", ut, obj);
  creation_time = ut.to_real_time();
  JSONDecoder::decode_json("linked", linked, obj);
  JSONDecoder::decode_json("has_bucket_info", has_bucket_info, obj);
  if (has_bucket_info) {
    JSONDecoder::decode_json("old_bucket_info", old_bucket_info, obj);
  }
}

