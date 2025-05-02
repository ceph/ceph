// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "include/function2.hpp"
#include "rgw_acl_s3.h"
#include "rgw_tag_s3.h"

#include "rgw_bucket.h"
#include "rgw_op.h"
#include "rgw_bucket_sync.h"

#include "services/svc_zone.h"
#include "services/svc_bucket.h"
#include "services/svc_user.h"

#include "account.h"
#include "buckets.h"
#include "rgw_metadata_lister.h"
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

static void dump_multipart_index_results(std::list<rgw_obj_index_key>& objs,
					 Formatter *f)
{
  for (const auto& o : objs) {
    f->dump_string("object",  o.name);
  }
}

void check_bad_owner_bucket_mapping(rgw::sal::Driver* driver,
                                    const rgw_owner& owner,
                                    const std::string& tenant,
                                    bool fix, optional_yield y,
                                    const DoutPrefixProvider *dpp)
{
  size_t max_entries = dpp->get_cct()->_conf->rgw_list_buckets_max_chunk;

  rgw::sal::BucketList listing;
  do {
    int ret = driver->list_buckets(dpp, owner, tenant, listing.next_marker,
                                   string(), max_entries, false, listing, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "failed to read user buckets: "
          << cpp_strerror(-ret) << dendl;
      return;
    }

    for (const auto& ent : listing.buckets) {
      std::unique_ptr<rgw::sal::Bucket> bucket;
      int r = driver->load_bucket(dpp, rgw_bucket(tenant, ent.bucket.name),
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
	  r = bucket->chown(dpp, owner, y);
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

int rgw_remove_object(const DoutPrefixProvider *dpp,
		      rgw::sal::Driver* driver,
		      rgw::sal::Bucket* bucket,
		      rgw_obj_key& key,
		      optional_yield y,
		      const bool force)
{

  std::unique_ptr<rgw::sal::Object> object = bucket->get_object(key);

  const uint32_t possible_force_flag = force ? rgw::sal::FLAG_FORCE_OP : 0;

  return object->delete_object(dpp, y, rgw::sal::FLAG_LOG_OP | possible_force_flag, nullptr, nullptr);
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

  auto bucket_name = op_state.get_bucket_name();
  auto bucket_id = op_state.get_bucket_id();

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

  int r = driver->load_bucket(dpp, rgw_bucket(tenant, bucket_name, bucket_id),
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


/**
 * Looks for incomplete and damaged multipart uploads on a single
 * shard. While the parts are being uploaded, entries are kept in the
 * bucket index to track the components of the upload. There is one
 * ".meta" entry and the part entries, all with the same prefix in
 * their keys. If we find the part entries but not their corresponding
 * shared ".meta" entry we can safely remove the part entries from the
 * index shard.
 *
 * We take advantage of the fact that since all entries for the same
 * upload have the same prefix, they're sequential in the index, with
 * the ".meta" coming last in the sequence. So we only need a small
 * window to track entries until either their ".meta" does or does not
 * come up.
 */
static int check_bad_index_multipart(rgw::sal::RadosStore* const rados_store,
				     rgw::sal::Bucket* const bucket,
				     const DoutPrefixProvider *dpp,
				     RGWBucketAdminOpState& op_state,
				     RGWFormatterFlusher& flusher,
				     const int shard,
				     optional_yield y)
{
  RGWRados* store = rados_store->getRados();
  RGWRados::BucketShard bs(store);

  const bool fix_index = op_state.will_fix_index();

  int ret = bs.init(dpp,
		    bucket->get_info(),
		    bucket->get_info().layout.current_index, shard, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR bs.init(bucket=" << bucket << "): " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  std::string marker = rgw_obj_key(std::string(),
				   std::string(),
				   RGW_OBJ_NS_MULTIPART).get_index_key_name();
  bool is_truncated = true;
  std::list<rgw_cls_bi_entry> entries_read;

  // holds part entries w/o ".meta"
  std::list<rgw_obj_index_key> entries_to_unlink;

  // holds entries pending finding of ".meta"
  std::list<rgw_obj_index_key> entries_window;

  // tracks whether on same multipart upload or not
  std::string prev_entry_prefix;
  do {
    entries_read.clear();
    ret = store->bi_list(bs, "", marker, -1,
			 &entries_read, &is_truncated, false, y);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR bi_list(): " << cpp_strerror(-ret) <<
	dendl;
      break;
    }

    for (const auto& entry : entries_read) {
      marker = entry.idx;

      rgw_obj_key obj_key;
      bool parsed = rgw_obj_key::parse_raw_oid(entry.idx, &obj_key);
      if (!parsed) {
	ldpp_dout(dpp, 0) <<
	  "WARNING: could not parse index entry; ignoring; key=" <<
	  entry.idx << dendl;
	continue;
      }
      const std::string& name = obj_key.name;
      const std::string& ns = obj_key.ns;

      // when we're out of the multipart namespace, we're done
      if (entry.type != BIIndexType::Plain || ns != RGW_OBJ_NS_MULTIPART) {
        is_truncated = false;
        break;
      }

      auto period = name.rfind(".");
      if (period == std::string::npos) {
	ldpp_dout(dpp, 0) <<
	  "WARNING: index entry in multipart namespace does not contain"
	  " suffix indicator ('.'); ignoring; key=" <<
	  entry.idx << dendl;
	continue;
      }

      const std::string entry_prefix = name.substr(0, period);
      const std::string entry_suffix = name.substr(1 + period);

      // the entries for a given multipart upload will appear
      // sequentially with the ".meta" will being the last. So we'll
      // cache the entries until we either find the "meta" entry or
      // switch to a different upload
      if (entry_suffix == "meta") {
	if (entry_prefix != prev_entry_prefix) {
	  entries_to_unlink.insert(entries_to_unlink.end(),
				   entries_window.cbegin(),
				   entries_window.cend());
	}

	// either way start over
	entries_window.clear();
	prev_entry_prefix.clear();
      } else {
	if (entry_prefix != prev_entry_prefix) {
	  entries_to_unlink.insert(entries_to_unlink.end(),
				   entries_window.cbegin(),
				   entries_window.cend());
	  entries_window.clear();
	  prev_entry_prefix = entry_prefix;
	}

	// create an rgw_obj_index_key to store in window
	rgw_obj_index_key obj_index_key;
	obj_key.get_index_key(&obj_index_key);
	entries_window.push_back(obj_index_key);
      }

      // check if this is a good point for intermediate index clean-up
      if (entries_to_unlink.size() >= listing_max_entries) {
	dump_multipart_index_results(entries_to_unlink,
				     flusher.get_formatter());
	if (fix_index) {
	  store->remove_objs_from_index(dpp, bucket->get_info(),
					entries_to_unlink);
	}
	entries_to_unlink.clear();
      }
    } // for
  } while (is_truncated);

  // any entries left over at the end can be unlinked
  entries_to_unlink.insert(entries_to_unlink.end(),
			   entries_window.cbegin(),
			   entries_window.cend());
  entries_window.clear();

  if (! entries_to_unlink.empty()) {
    dump_multipart_index_results(entries_to_unlink,
				 flusher.get_formatter());
    if (fix_index) {
      store->remove_objs_from_index(dpp, bucket->get_info(),
				    entries_to_unlink);
    }
    entries_to_unlink.clear();
  }

  flusher.flush();

  return 0;
} // static ::check_bad_index_multipart


/**
 * Checks for damaged incomplete multipart uploads in a bucket
 * index. Since all entries for a given multipart upload end up on the
 * same shard (by design), we spawn a set of co-routines, each one
 * working shard by shard until all work is complete.
 *
 * TODO: This function takes optional_yield so there's an expectation
 * that it can run asynchronously, but io_context::run() is
 * synchronous. This is fine for radosgw-admin, but we also serve this
 * 'bucket check' operation over the /admin/bucket api, so we'll want
 * to address this in the future.
 */
int RGWBucket::check_bad_index_multipart(rgw::sal::RadosStore* const rados_store,
					 RGWBucketAdminOpState& op_state,
					 RGWFormatterFlusher& flusher,
					 const DoutPrefixProvider *dpp,
					 optional_yield y,
					 std::string* err_msg)
{
  const RGWBucketInfo& bucket_info = get_bucket_info();

  Formatter* formatter = flusher.get_formatter();
  formatter->open_array_section("invalid_multipart_entries");

  const auto& index_layout = bucket_info.layout.current_index.layout;
  if (index_layout.type != rgw::BucketIndexType::Normal) {
    ldpp_dout(dpp, 0) << "ERROR: cannot check bucket indices with layouts of type " <<
      current_layout_desc(bucket_info.layout) <<
      " for bad multipart entries" << dendl;
    return -EINVAL;
  }
  const int num_shards = rgw::num_shards(index_layout.normal);
  int next_shard = 0;

  boost::asio::io_context context;
  const int max_aio = std::max(1, op_state.get_max_aio());
  int any_error = 0; // first error encountered if any
  for (int i = 0; i < max_aio; i++) {
    boost::asio::spawn(context, [&](boost::asio::yield_context yield) {
      while (true) {
        const int shard = next_shard++;
        if (shard >= num_shards) {
          return;
        }

        int r = ::check_bad_index_multipart(rados_store, &*bucket, dpp,
					    op_state, flusher, shard, yield);
        if (r < 0) {
          ldpp_dout(dpp, -1) << "WARNING: error processing shard " << shard <<
            " check_bad_index_multipart(): " << r << "; skipping" << dendl;
	  if (!any_error) {
	    // record first error encountered, but continue
	    any_error = r;
	  }
        }
      } // while
    }, [] (std::exception_ptr eptr) {
      if (eptr) std::rethrow_exception(eptr);
    });
  } // for

  try {
    context.run();
  } catch (const std::system_error& e) {
    formatter->close_section();
    *err_msg = e.what();
    return -e.code().value();
  }

  formatter->close_section();

  return any_error;
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
  bucket->set_tag_timeout(dpp, y, BUCKET_TAG_QUICK_TIMEOUT);

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
  bucket->set_tag_timeout(dpp, y, 0);

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
    ret = store->bi_list(bs, "", marker, -1, &entries, &is_truncated, false, y);
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
          ret = object->load_obj_state(dpp, y, false);
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR failed to load state for: " << olh_entry.key.name << " load_obj_state(): " << cpp_strerror(-ret) << dendl;
            continue;
          }
	  RGWObjState& state = static_cast<rgw::sal::RadosObject*>(object.get())->get_state();
          ret = store->update_olh(dpp, obj_ctx, &state, bucket->get_info(), obj, y);
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
 *
 * TODO: Currently this is synchronous as it uses
 * io_context::run(). Allow this to run asynchronously by receiving an
 * optional_yield parameter and making other adjustments.  Synchronous
 * is fine for radosgw-admin, but we also serve this 'bucket check'
 * operation over the /admin/bucket api, so we'll want to address
 * this.
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

  const auto& index_layout = bucket_info.layout.current_index.layout;
  if (index_layout.type != rgw::BucketIndexType::Normal) {
    ldpp_dout(dpp, 0) << "ERROR: cannot check bucket indices with layouts of type " <<
      current_layout_desc(bucket_info.layout) <<
      " for bad OLH entries" << dendl;
    return -EINVAL;
  }
  const int max_shards = rgw::num_shards(index_layout.normal);
  std::string verb = op_state.will_fix_index() ? "removed" : "found";
  uint64_t count_out = 0;
  
  boost::asio::io_context context;
  int next_shard = 0;
  
  const int max_aio = std::max(1, op_state.get_max_aio());

  for (int i=0; i<max_aio; i++) {
    boost::asio::spawn(context, [&](boost::asio::yield_context yield) {
      while (true) {
        int shard = next_shard;
        next_shard += 1;
        if (shard >= max_shards) {
          return;
        }
        optional_yield y(yield);
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
    }, [] (std::exception_ptr eptr) {
      if (eptr) std::rethrow_exception(eptr);
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
    int r = bs.bucket_obj.operate(dpp, std::move(op), &ibl, y);
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
    ret = store->bi_list(bs, "", marker, -1, &entries, &is_truncated, false, y);
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
    boost::asio::spawn(context, [&](boost::asio::yield_context yield) {
      while (true) {
        int shard = next_shard;
        next_shard += 1;
        if (shard >= max_shards) {
          return;
        }
        uint64_t shard_count = 0;
        int r = ::check_index_unlinked(rados_store, &*bucket, dpp, op_state, flusher, shard, &shard_count, yield);
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
    }, [] (std::exception_ptr eptr) {
      if (eptr) std::rethrow_exception(eptr);
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

int RGWBucket::check_index(const DoutPrefixProvider *dpp, optional_yield y,
        RGWBucketAdminOpState& op_state,
        map<RGWObjCategory, RGWStorageStats>& existing_stats,
        map<RGWObjCategory, RGWStorageStats>& calculated_stats,
        std::string *err_msg)
{
  bool fix_index = op_state.will_fix_index();

  int r = bucket->check_index(dpp, y, existing_stats, calculated_stats);
  if (r < 0) {
    set_err_msg(err_msg, "failed to check index error=" + cpp_strerror(-r));
    return r;
  }

  if (fix_index) {
    r = bucket->rebuild_index(dpp, y);
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

int RGWBucketAdminOp::unlink(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, string *err)
{
  rgw_owner owner;
  if (op_state.is_account_op()) {
    owner = op_state.get_account_id();
  } else if (op_state.is_user_op()) {
    owner = op_state.get_user_id();
  } else {
    set_err_msg(err, "requires user or account id");
    return -EINVAL;
  }

  auto radosdriver = dynamic_cast<rgw::sal::RadosStore*>(driver);
  if (!radosdriver) {
    set_err_msg(err, "rados store only");
    return -ENOTSUP;
  }

  RGWBucket bucket;
  int ret = bucket.init(driver, op_state, y, dpp);
  if (ret < 0)
    return ret;

  auto* rados = radosdriver->getRados()->get_rados_handle();
  return radosdriver->ctl()->bucket->unlink_bucket(*rados, owner, op_state.get_bucket()->get_info().bucket, y, dpp, true);
}

int RGWBucketAdminOp::link(rgw::sal::Driver* driver, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, optional_yield y, string *err)
{
  rgw_owner new_owner;
  if (op_state.is_account_op()) {
    new_owner = op_state.get_account_id();
  } else if (op_state.is_user_op()) {
    new_owner = op_state.get_user_id();
  } else {
    set_err_msg(err, "requires user or account id");
    return -EINVAL;
  }
  auto radosdriver = dynamic_cast<rgw::sal::RadosStore*>(driver);
  if (!radosdriver) {
    set_err_msg(err, "rados store only");
    return -ENOTSUP;
  }

  RGWBucket bucket;
  int ret = bucket.init(driver, op_state, y, dpp, err);
  if (ret < 0)
    return ret;

  std::string display_name;
  if (op_state.is_account_op()) {
    RGWAccountInfo info;
    rgw::sal::Attrs attrs;
    RGWObjVersionTracker objv;
    ret = driver->load_account_by_id(dpp, y, op_state.get_account_id(),
                                     info, attrs, objv);
    if (ret < 0) {
      set_err_msg(err, "failed to load account");
      return ret;
    }
    display_name = std::move(info.name);
  } else if (!bucket.get_user()->get_info().account_id.empty()) {
    set_err_msg(err, "account users cannot own buckets. use --account-id instead");
    return -EINVAL;
  } else {
    display_name = bucket.get_user()->get_display_name();
  }

  string bucket_id = op_state.get_bucket_id();
  std::unique_ptr<rgw::sal::Bucket> loc_bucket;
  std::unique_ptr<rgw::sal::Bucket> old_bucket;

  loc_bucket = op_state.get_bucket()->clone();

  if (!bucket_id.empty() && bucket_id != loc_bucket->get_bucket_id()) {
    set_err_msg(err,
	"specified bucket id does not match " + loc_bucket->get_bucket_id());
    return -EINVAL;
  }

  old_bucket = loc_bucket->clone();

  loc_bucket->get_key().tenant = op_state.get_tenant();

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

  auto* rados = radosdriver->getRados()->get_rados_handle();
  int r = radosdriver->ctl()->bucket->unlink_bucket(*rados, owner.id, old_bucket->get_info().bucket, y, dpp, false);
  if (r < 0) {
    set_err_msg(err, "could not unlink bucket from owner " + to_string(owner.id));
    return r;
  }

  // now update the user for the bucket...
  if (display_name.empty()) {
    ldpp_dout(dpp, 0) << "WARNING: user " << op_state.get_user_id() << " has no display name set" << dendl;
  }

  RGWAccessControlPolicy policy_instance;
  policy_instance.create_default(new_owner, display_name);
  owner = policy_instance.get_owner();

  aclbl.clear();
  policy_instance.encode(aclbl);

  bool exclusive = false;
  loc_bucket->get_info().owner = new_owner;
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
  ep.owner = new_owner;
  ep.creation_time = loc_bucket->get_info().creation_time;
  ep.linked = true;
  rgw::sal::Attrs ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  r = radosdriver->ctl()->bucket->link_bucket(*rados, new_owner, loc_bucket->get_info().bucket, loc_bucket->get_info().creation_time, y, dpp, true, &ep_data);
  if (r < 0) {
    set_err_msg(err, "failed to relink bucket");
    return r;
  }

  if (*loc_bucket != *old_bucket) {
    // like RGWRados::delete_bucket -- excepting no bucket_index work.
    r = radosdriver->ctl()->bucket->remove_bucket_entrypoint_info(
					old_bucket->get_key(), y, dpp,
					RGWBucketCtl::Bucket::RemoveParams()
					.set_objv_tracker(&ep_data.ep_objv));
    if (r < 0) {
      set_err_msg(err, "failed to unlink old bucket " + old_bucket->get_tenant() + "/" + old_bucket->get_name());
      return r;
    }
    r = radosdriver->ctl()->bucket->remove_bucket_instance_info(
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

int RGWBucketAdminOp::check_index(rgw::sal::Driver* driver,
				  RGWBucketAdminOpState& op_state,
				  RGWFormatterFlusher& flusher,
				  optional_yield y,
				  const DoutPrefixProvider* dpp)
{
  int ret;
  std::map<RGWObjCategory, RGWStorageStats> existing_stats;
  std::map<RGWObjCategory, RGWStorageStats> calculated_stats;

  RGWBucket bucket;
  ret = bucket.init(driver, op_state, y, dpp);
  if (ret < 0) {
    return ret;
  }

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  formatter->open_object_section("bucket_check");

  auto rados_store = dynamic_cast<rgw::sal::RadosStore*>(driver);
  if (!rados_store) {
    ldpp_dout(dpp, 0) << "WARNING: couldn't access a RadosStore, "
      "so skipping bad incomplete multipart check" << dendl;
  } else {
    ret = bucket.check_bad_index_multipart(rados_store, op_state, flusher, dpp, y);
    if (ret < 0) {
      return ret;
    }
  }

  if (op_state.will_check_objects()) {
    ret = bucket.check_object_index(dpp, op_state, flusher, y);
    if (ret < 0) {
      return ret;
    }
  }

  ret = bucket.check_index(dpp, y, op_state, existing_stats, calculated_stats);
  if (ret < 0) {
    return ret;
  }

  dump_index_check(existing_stats, calculated_stats, formatter);
  
  formatter->close_section();
  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::remove_bucket(rgw::sal::Driver* driver, const rgw::SiteConfig& site,
                                    RGWBucketAdminOpState& op_state,
				    optional_yield y, const DoutPrefixProvider *dpp, 
                                    bool bypass_gc, bool keep_index_consistent, bool forwarded_request)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;

  int ret = driver->load_bucket(dpp, rgw_bucket(op_state.get_tenant(),
                                                op_state.get_bucket_name()),
                                &bucket, y);
  if (ret < 0)
    return ret;

  // check if the bucket is owned by my zonegroup, if not, refuse to remove it
  if (!forwarded_request && bucket->get_info().zonegroup != driver->get_zone()->get_zonegroup().get_id()) {
    ldpp_dout(dpp, 0) << "ERROR: The bucket's zonegroup does not match. "
                      << "Removal is not allowed. Please execute this operation "
                      << "on the bucket's zonegroup: " << bucket->get_info().zonegroup << dendl;
    return -ERR_PERMANENT_REDIRECT;
  }

  if (bypass_gc)
    ret = bucket->remove_bypass_gc(op_state.get_max_aio(), keep_index_consistent, y, dpp);
  else
    ret = bucket->remove(dpp, op_state.will_delete_children(), y);
  if (ret < 0)
    return ret;

  // forward to master zonegroup
  const std::string delpath = "/admin/bucket";
  RGWEnv env;
  env.set("REQUEST_METHOD", "DELETE");
  env.set("SCRIPT_URI", delpath);
  env.set("REQUEST_URI", delpath);
  env.set("QUERY_STRING", fmt::format("bucket={}&tenant={}", bucket->get_name(), bucket->get_tenant()));
  req_info req(dpp->get_cct(), &env);
  rgw_err err; // unused

  ret = rgw_forward_request_to_master(dpp, site, bucket->get_owner(), nullptr, nullptr, req, err, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to forward request to master zonegroup: "
                      << ret << dendl;
    return ret;
  }

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

  const auto& index = bucket_info.get_current_index();
  if (is_layout_indexless(index)) {
    cerr << "error, indexless buckets do not maintain stats; bucket=" <<
      bucket->get_name() << std::endl;
    return -EINVAL;
  }

  std::string bucket_ver, master_ver;
  std::string max_marker;
  ret = bucket->read_stats(dpp, y, index, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0) {
    cerr << "error getting bucket stats bucket=" << bucket->get_name() << " ret=" << ret << std::endl;
    return ret;
  }

  utime_t ut(bucket->get_modification_time());
  utime_t ctime_ut(bucket->get_creation_time());
  utime_t logrecord_ut(bucket->get_info().layout.judge_reshard_lock_time);

  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket->get_name());
  formatter->dump_string("tenant", bucket->get_tenant());
  formatter->dump_string("versioning",
			 bucket->versioned()
			 ? (bucket->versioning_enabled() ? "enabled" : "suspended")
			 : "off");
  formatter->dump_string("zonegroup", bucket_info.zonegroup);
  formatter->dump_string("placement_rule", bucket_info.placement_rule.to_str());
  ::encode_json("explicit_placement", bucket->get_key().explicit_placement, formatter);
  formatter->dump_string("id", bucket->get_bucket_id());
  formatter->dump_string("marker", bucket->get_marker());
  formatter->dump_stream("index_type") << bucket_info.layout.current_index.layout.type;
  formatter->dump_int("index_generation", bucket_info.layout.current_index.gen);
  formatter->dump_int("num_shards",
		      bucket_info.layout.current_index.layout.normal.num_shards);
  formatter->dump_string("reshard_status", to_string(bucket_info.layout.resharding));
  logrecord_ut.gmtime(formatter->dump_stream("judge_reshard_lock_time"));
  formatter->dump_bool("object_lock_enabled", bucket_info.obj_lock_enabled());
  formatter->dump_bool("mfa_enabled", bucket_info.mfa_enabled());
  ::encode_json("owner", bucket_info.owner, formatter);
  formatter->dump_string("ver", bucket_ver);
  formatter->dump_string("master_ver", master_ver);
  ut.gmtime(formatter->dump_stream("mtime"));
  ctime_ut.gmtime(formatter->dump_stream("creation_time"));
  formatter->dump_string("max_marker", max_marker);
  dump_bucket_usage(stats, formatter);
  encode_json("bucket_quota", bucket_info.quota, formatter);

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

  formatter->dump_int("read_tracker", bucket_info.objv_tracker.read_version.ver);
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
    const auto user = rgw_user{user_id};

    formatter->open_object_section("user");
    formatter->dump_string("user_id", user_id);
    formatter->open_array_section("buckets");

    rgw::sal::BucketList listing;
    do {
      ret = driver->list_buckets(dpp, user, user.tenant, listing.next_marker,
                                 string(), max_entries, false, listing, y);
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
	ret = bucket->read_stats(dpp, y, index, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, nullptr);

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

static int list_owner_bucket_info(const DoutPrefixProvider* dpp,
                                  optional_yield y,
                                  rgw::sal::Driver* driver,
                                  const rgw_owner& owner,
                                  const std::string& tenant,
                                  const std::string& marker,
                                  bool show_stats,
                                  RGWFormatterFlusher& flusher)
{
  Formatter* formatter = flusher.get_formatter();
  formatter->open_array_section("buckets");

  const std::string empty_end_marker;
  const size_t max_entries = dpp->get_cct()->_conf->rgw_list_buckets_max_chunk;
  constexpr bool no_need_stats = false; // set need_stats to false

  rgw::sal::BucketList listing;
  listing.next_marker = marker;
  do {
    int ret = driver->list_buckets(dpp, owner, tenant, listing.next_marker,
                                   empty_end_marker, max_entries, no_need_stats,
                                   listing, y);
    if (ret < 0) {
      return ret;
    }

    for (const auto& ent : listing.buckets) {
      if (show_stats) {
        bucket_stats(driver, tenant, ent.bucket.name, formatter, dpp, y);
      } else {
        formatter->dump_string("bucket", ent.bucket.name);
      }
    } // for loop

    flusher.flush();
  } while (!listing.next_marker.empty());

  formatter->close_section();
  return 0;
}

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
    const rgw_user& uid = op_state.get_user_id();
    auto user = driver->get_user(uid);
    ret = user->load_user(dpp, y);
    if (ret < 0) {
      return ret;
    }
    const RGWUserInfo& info = user->get_info();
    if (!info.account_id.empty()) {
      ldpp_dout(dpp, 1) << "Listing buckets in user account "
          << info.account_id << dendl;
      ret = list_owner_bucket_info(dpp, y, driver, info.account_id, uid.tenant,
                                   op_state.marker, show_stats, flusher);
    } else {
      ret = list_owner_bucket_info(dpp, y, driver, uid, uid.tenant,
                                   op_state.marker, show_stats, flusher);
    }
    if (ret < 0) {
      return ret;
    }
  } else if (op_state.is_account_op()) {
    // look up the account's tenant
    const rgw_account_id& account_id = op_state.get_account_id();
    RGWAccountInfo info;
    rgw::sal::Attrs attrs; // ignored
    RGWObjVersionTracker objv; // ignored
    int ret = driver->load_account_by_id(dpp, y, account_id, info, attrs, objv);
    if (ret < 0) {
      ldpp_dout(dpp, 1) << "failed to load account " << account_id
          << ": " << cpp_strerror(ret) << dendl;
      return ret;
    }

    ret = list_owner_bucket_info(dpp, y, driver, account_id, info.tenant,
                                 op_state.marker, show_stats, flusher);
    if (ret < 0) {
      return ret;
    }
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

class RGWBucketMetadataHandler : public RGWMetadataHandler {
 protected:
  librados::Rados& rados;
  RGWSI_Bucket* svc_bucket{nullptr};
  RGWBucketCtl *ctl_bucket{nullptr};

 public:
  RGWBucketMetadataHandler(librados::Rados& rados,
                           RGWSI_Bucket* svc_bucket,
                           RGWBucketCtl* ctl_bucket)
    : rados(rados), svc_bucket(svc_bucket), ctl_bucket(ctl_bucket) {}

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

  int get(std::string& entry, RGWMetadataObject** obj, optional_yield y,
          const DoutPrefixProvider *dpp) override;
  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv_tracker,
          optional_yield y, const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override;
  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override;

  int mutate(const std::string& entry, const ceph::real_time& mtime,
             RGWObjVersionTracker* objv_tracker, optional_yield y,
             const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
             std::function<int()> f) override;

  int list_keys_init(const DoutPrefixProvider* dpp, const std::string& marker,
                     void** phandle) override;
  int list_keys_next(const DoutPrefixProvider* dpp, void* handle, int max,
                     std::list<std::string>& keys, bool* truncated) override;
  void list_keys_complete(void *handle) override;
  std::string get_marker(void *handle) override;
};

int RGWBucketMetadataHandler::get(std::string& entry, RGWMetadataObject** obj,
                                  optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWObjVersionTracker ot;
  RGWBucketEntryPoint be;
  real_time mtime;
  map<string, bufferlist> attrs;

  int ret = svc_bucket->read_bucket_entrypoint_info(entry, &be, &ot, &mtime,
                                                    &attrs, y, dpp);
  if (ret < 0) {
    return ret;
  }

  *obj = new RGWBucketEntryMetadataObject(be, ot.read_version, mtime,
                                          std::move(attrs));
  return 0;
}

int RGWBucketMetadataHandler::put(std::string& entry, RGWMetadataObject* obj,
                                  RGWObjVersionTracker& objv_tracker,
                                  optional_yield y, const DoutPrefixProvider* dpp,
                                  RGWMDLogSyncType type, bool from_remote_zone)
{
  std::optional old_be = RGWBucketEntryPoint{};
  int ret = svc_bucket->read_bucket_entrypoint_info(
      entry, &*old_be, &objv_tracker, nullptr, nullptr, y, dpp);
  if (ret == -ENOENT) {
    old_be = std::nullopt;
  } else if (ret < 0) {
    return ret;
  }

  auto* epobj = static_cast<RGWBucketEntryMetadataObject*>(obj);
  RGWBucketEntryPoint& new_be = epobj->get_ep();

  ret = svc_bucket->store_bucket_entrypoint_info(
      entry, new_be, false, obj->get_mtime(),
      obj->get_pattrs(), &objv_tracker, y, dpp);
  if (ret < 0) {
    return ret;
  }

  constexpr bool update_entrypoint = false;

  if (old_be && (old_be->owner != new_be.owner || // owner changed
      (old_be->linked && !new_be.linked))) { // linked -> false
    int ret = ctl_bucket->unlink_bucket(rados, old_be->owner, old_be->bucket,
                                        y, dpp, update_entrypoint);
    if (ret < 0) {
      return ret;
    }
  }

  if (new_be.linked && (!old_be || !old_be->linked || // linked -> true
      old_be->owner != new_be.owner)) { // owner changed
    int ret = ctl_bucket->link_bucket(rados, new_be.owner, new_be.bucket,
                                      new_be.creation_time, y, dpp,
                                      update_entrypoint);
    if (ret < 0) {
      return ret;
    }
  }

  return 0;
}

int update_bucket_topic_mappings(const DoutPrefixProvider* dpp,
                                 const RGWBucketCompleteInfo* orig_bci,
                                 const RGWBucketCompleteInfo* current_bci,
                                 rgw::sal::Driver* driver) {
  const auto decode_attrs = [](const rgw::sal::Attrs& attrs,
                               rgw_pubsub_bucket_topics& bucket_topics) -> int {
    auto iter = attrs.find(RGW_ATTR_BUCKET_NOTIFICATION);
    if (iter == attrs.end()) {
      return 0;
    }
    try {
      const auto& bl = iter->second;
      auto biter = bl.cbegin();
      bucket_topics.decode(biter);
    } catch (buffer::error& err) {
      return -EIO;
    }
    return 0;
  };
  std::string bucket_name;
  std::string bucket_tenant;
  rgw_pubsub_bucket_topics old_bucket_topics;
  if (orig_bci) {
    auto ret = decode_attrs(orig_bci->attrs, old_bucket_topics);
    if (ret < 0) {
      ldpp_dout(dpp, 1)
          << "ERROR: failed to decode OLD bucket topics for bucket: "
          << orig_bci->info.bucket.name << dendl;
      return ret;
    }
    bucket_name = orig_bci->info.bucket.name;
    bucket_tenant = orig_bci->info.bucket.tenant;
  }
  rgw_pubsub_bucket_topics current_bucket_topics;
  if (current_bci) {
    auto ret = decode_attrs(current_bci->attrs, current_bucket_topics);
    if (ret < 0) {
      ldpp_dout(dpp, 1)
          << "ERROR: failed to decode current bucket topics for bucket: "
          << current_bci->info.bucket.name << dendl;
      return ret;
    }
    bucket_name = current_bci->info.bucket.name;
    bucket_tenant = current_bci->info.bucket.tenant;
  }
  // fetch the list of subscribed topics stored inside old_bucket attrs.
  std::unordered_map<std::string, rgw_pubsub_topic> old_topics;
  for (const auto& [_, topic_filter] : old_bucket_topics.topics) {
    old_topics[topic_filter.topic.name] = topic_filter.topic;
  }
  // fetch the list of subscribed topics stored inside current_bucket attrs.
  std::unordered_map<std::string, rgw_pubsub_topic> current_topics;
  for (const auto& [_, topic_filter] : current_bucket_topics.topics) {
    current_topics[topic_filter.topic.name] = topic_filter.topic;
  }
  // traverse thru old topics and check if they are not in current, then delete
  // the mapping, if present in both current and old then delete from current
  // set as we do not need to update those mapping.
  int ret = 0;
  for (const auto& [topic_name, topic] : old_topics) {
    auto it = current_topics.find(topic_name);
    if (it == current_topics.end()) {
      const auto op_ret = driver->update_bucket_topic_mapping(
          topic, rgw_make_bucket_entry_name(bucket_tenant, bucket_name),
          /*add_mapping=*/false, null_yield, dpp);
      if (op_ret < 0) {
        ret = op_ret;
      }
    } else {
      // already that attr is present, so do not update the mapping.
      current_topics.erase(it);
    }
  }
  // traverse thru current topics and check if they are any present, then add
  // the mapping.
  for (const auto& [topic_name, topic] : current_topics) {
    const auto op_ret = driver->update_bucket_topic_mapping(
        topic, rgw_make_bucket_entry_name(bucket_tenant, bucket_name),
        /*add_mapping=*/true, null_yield, dpp);
    if (op_ret < 0) {
      ret = op_ret;
    }
  }
  return ret;
}

int RGWBucketMetadataHandler::remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
                                     optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWBucketEntryPoint be;
  int ret = svc_bucket->read_bucket_entrypoint_info(entry, &be, &objv_tracker,
                                                    nullptr, nullptr, y, dpp);
  if (ret < 0) {
    return ret;
  }

  constexpr bool update_ep = false; // removing anyway
  ret = ctl_bucket->unlink_bucket(rados, be.owner, be.bucket, y, dpp, update_ep);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
  }

  ret = svc_bucket->remove_bucket_entrypoint_info(entry, &objv_tracker, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "could not delete bucket=" << entry << dendl;
  }
  /* idempotent */
  return 0;
}

int RGWBucketMetadataHandler::mutate(const std::string& entry, const ceph::real_time& mtime,
                                     RGWObjVersionTracker* objv_tracker, optional_yield y,
                                     const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
                                     std::function<int()> f)
{
  return -ENOTSUP; // unused
}

int RGWBucketMetadataHandler::list_keys_init(const DoutPrefixProvider* dpp,
                                             const std::string& marker,
                                             void** phandle)
{
  std::unique_ptr<RGWMetadataLister> lister;
  int ret = svc_bucket->create_entrypoint_lister(dpp, marker, lister);
  if (ret < 0) {
    return ret;
  }
  *phandle = lister.release(); // release ownership
  return 0;
}

int RGWBucketMetadataHandler::list_keys_next(const DoutPrefixProvider* dpp,
                                             void* handle, int max,
                                             std::list<std::string>& keys,
                                             bool* truncated)
{
  auto lister = static_cast<RGWMetadataLister*>(handle);
  return lister->get_next(dpp, max, keys, truncated);
}

void RGWBucketMetadataHandler::list_keys_complete(void *handle)
{
  delete static_cast<RGWMetadataLister*>(handle);
}

std::string RGWBucketMetadataHandler::get_marker(void *handle)
{
  auto lister = static_cast<RGWMetadataLister*>(handle);
  return lister->get_marker();
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
  RGWArchiveBucketMetadataHandler(librados::Rados& rados,
                                  RGWSI_Bucket* svc_bucket,
                                  RGWBucketCtl* ctl_bucket)
    : RGWBucketMetadataHandler(rados, svc_bucket, ctl_bucket) {}

  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override
  {
    auto cct = svc_bucket->ctx();

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
    int ret = svc_bucket->read_bucket_entrypoint_info(entry, &be, &objv_tracker, &mtime, &attrs, y, dpp);
    if (ret < 0) {
        return ret;
    }

    string bi_meta_name = RGWSI_Bucket::get_bi_meta_key(be.bucket);

    /* read original bucket instance info */

    map<string, bufferlist> attrs_m;
    ceph::real_time orig_mtime;
    RGWBucketInfo old_bi;

    ret = ctl_bucket->read_bucket_instance_info(be.bucket, &old_bi, y, dpp, RGWBucketCtl::BucketInstance::GetParams()
                                                                    .set_mtime(&orig_mtime)
                                                                    .set_attrs(&attrs_m));
    if (ret < 0) {
        return ret;
    }

    archive_meta_info ami;

    if (!ami.from_attrs(svc_bucket->ctx(), attrs_m)) {
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

    ret = ctl_bucket->store_bucket_instance_info(new_be.bucket, new_bi, y, dpp, RGWBucketCtl::BucketInstance::PutParams()
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

    ret = svc_bucket->store_bucket_entrypoint_info(RGWSI_Bucket::get_entrypoint_meta_key(new_be.bucket),
                                                   new_be, true, mtime, &attrs, nullptr, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* link new bucket */

    ret = ctl_bucket->link_bucket(rados, new_be.owner, new_be.bucket, new_be.creation_time, y, dpp, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to link new bucket for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* clean up old stuff */

    ret = ctl_bucket->unlink_bucket(rados, be.owner, entry_bucket, y, dpp, false);
    if (ret < 0) {
        ldpp_dout(dpp, -1) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    // if (ret == -ECANCELED) it means that there was a race here, and someone
    // wrote to the bucket entrypoint just before we removed it. The question is
    // whether it was a newly created bucket entrypoint ...  in which case we
    // should ignore the error and move forward, or whether it is a higher version
    // of the same bucket instance ... in which we should retry
    ret = svc_bucket->remove_bucket_entrypoint_info(RGWSI_Bucket::get_entrypoint_meta_key(be.bucket),
                                                    &objv_tracker,
                                                    y,
                                                    dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    ret = ctl_bucket->remove_bucket_instance_info(be.bucket, old_bi, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "could not delete bucket=" << entry << dendl;
    }


    /* idempotent */

    return 0;
  }

  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv_tracker,
          optional_yield y, const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override
  {
    if (entry.find("-deleted-") != string::npos) {
      RGWObjVersionTracker ot;
      RGWMetadataObject *robj;
      int ret = get(entry, &robj, y, dpp);
      if (ret != -ENOENT) {
        if (ret < 0) {
          return ret;
        }
        ot.read_version = robj->get_version();
        delete robj;

        ret = remove(entry, ot, y, dpp);
        if (ret < 0) {
          return ret;
        }
      }
    }

    return RGWBucketMetadataHandler::put(entry, obj, objv_tracker, y,
                                         dpp, type, from_remote_zone);
  }
};

class RGWBucketInstanceMetadataHandler : public RGWMetadataHandler {
  rgw::sal::Driver* driver;
  RGWSI_Zone* svc_zone{nullptr};
  RGWSI_Bucket* svc_bucket{nullptr};
  RGWSI_BucketIndex* svc_bi{nullptr};
  RGWDataChangesLog *svc_datalog{nullptr};

  int put_prepare(const DoutPrefixProvider* dpp, optional_yield y,
                  const std::string& entry, RGWBucketCompleteInfo& bci,
                  const std::optional<RGWBucketCompleteInfo>& old_bci,
                  const RGWObjVersionTracker& objv_tracker,
                  bool from_remote_zone);
  int put_post(const DoutPrefixProvider* dpp, optional_yield y,
               const RGWBucketCompleteInfo& bci,
               const std::optional<RGWBucketCompleteInfo>& old_bci,
               RGWObjVersionTracker& objv_tracker);
 public:
  RGWBucketInstanceMetadataHandler(rgw::sal::Driver* driver,
                                   RGWSI_Zone* svc_zone,
                                   RGWSI_Bucket* svc_bucket,
                                   RGWSI_BucketIndex* svc_bi,
                                   RGWDataChangesLog *svc_datalog)
    : driver(driver), svc_zone(svc_zone),
      svc_bucket(svc_bucket), svc_bi(svc_bi), svc_datalog(svc_datalog) {}

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

  int get(std::string& entry, RGWMetadataObject** obj, optional_yield y,
          const DoutPrefixProvider *dpp) override;
  int put(std::string& entry, RGWMetadataObject* obj,
          RGWObjVersionTracker& objv_tracker,
          optional_yield y, const DoutPrefixProvider* dpp,
          RGWMDLogSyncType type, bool from_remote_zone) override;
  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override;

  int mutate(const std::string& entry, const ceph::real_time& mtime,
             RGWObjVersionTracker* objv_tracker, optional_yield y,
             const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
             std::function<int()> f) override;

  int list_keys_init(const DoutPrefixProvider* dpp, const std::string& marker,
                     void** phandle) override;
  int list_keys_next(const DoutPrefixProvider* dpp, void* handle, int max,
                     std::list<std::string>& keys, bool* truncated) override;
  void list_keys_complete(void *handle) override;
  std::string get_marker(void *handle) override;
};

int RGWBucketInstanceMetadataHandler::get(std::string& entry, RGWMetadataObject **obj,
                                          optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWBucketCompleteInfo bci;
  real_time mtime;

  int ret = svc_bucket->read_bucket_instance_info(entry, &bci.info, &mtime, &bci.attrs, y, dpp);
  if (ret < 0) {
    return ret;
  }

  *obj = new RGWBucketInstanceMetadataObject(bci, bci.info.objv_tracker.read_version, mtime);
  return 0;
}

int RGWBucketInstanceMetadataHandler::put(std::string& entry, RGWMetadataObject* obj,
                                          RGWObjVersionTracker& objv_tracker,
                                          optional_yield y, const DoutPrefixProvider *dpp,
                                          RGWMDLogSyncType sync_type, bool from_remote_zone)
{
  // read existing bucket instance
  std::optional old = RGWBucketCompleteInfo{};
  int ret = svc_bucket->read_bucket_instance_info(entry, &old->info, nullptr,
                                                  &old->attrs, y, dpp);
  if (ret == -ENOENT) {
    old = std::nullopt;
  } else if (ret < 0) {
    return ret;
  }

  auto newobj = static_cast<RGWBucketInstanceMetadataObject*>(obj);
  RGWBucketCompleteInfo& bci = newobj->get_bci();

  // initializate/update the bucket instance info
  ret = put_prepare(dpp, y, entry, bci, old, objv_tracker, from_remote_zone);
  if (ret < 0) {
    return ret;
  }

  // write updated instance
  RGWBucketInfo* old_info = (old ? &old->info : nullptr);
  auto mtime = obj->get_mtime();
  ret = svc_bucket->store_bucket_instance_info(entry, bci.info, old_info, false,
                                               mtime, &bci.attrs, y, dpp);
  if (ret < 0) {
    return ret;
  }

  // update related state on success
  return put_post(dpp, y, bci, old, objv_tracker);
}

void init_default_bucket_layout(CephContext *cct, rgw::BucketLayout& layout,
				const RGWZone& zone,
				std::optional<rgw::BucketIndexType> type,
				std::optional<uint32_t> shards) {
  layout.current_index.gen = 0;
  layout.current_index.layout.normal.hash_type = rgw::BucketHashType::Mod;

  layout.current_index.layout.type =
    type.value_or(rgw::BucketIndexType::Normal);

  if (shards) {
    layout.current_index.layout.normal.num_shards = *shards;
    layout.current_index.layout.normal.min_num_shards = *shards;
  } else if (cct->_conf->rgw_override_bucket_index_max_shards > 0) {
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

int RGWBucketInstanceMetadataHandler::put_prepare(
    const DoutPrefixProvider* dpp, optional_yield y,
    const std::string& entry, RGWBucketCompleteInfo& bci,
    const std::optional<RGWBucketCompleteInfo>& old_bci,
    const RGWObjVersionTracker& objv_tracker,
    bool from_remote_zone)
{
  if (from_remote_zone) {
    // bucket layout information is local. don't overwrite existing layout with
    // information from a remote zone
    if (old_bci) {
      bci.info.layout = old_bci->info.layout;
    } else {
      // replace peer's layout with default-constructed, then apply our defaults
      bci.info.layout = rgw::BucketLayout{};
      init_default_bucket_layout(dpp->get_cct(), bci.info.layout,
				 svc_zone->get_zone(),
				 std::nullopt, std::nullopt);
    }
  }

  if (!old_bci || old_bci->info.bucket.bucket_id != bci.info.bucket.bucket_id) {
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
    if (svc_zone->sync_module_supports_writes()) {
      int ret = svc_zone->select_bucket_location_by_rule(dpp, bci.info.placement_rule, &rule_info, y);
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

    //if the bucket is being deleted, create and store a special log type for
    //bucket instance cleanup in multisite setup
    const auto& log = bci.info.layout.logs.back();
    if (bci.info.bucket_deleted() && log.layout.type != rgw::BucketLogType::Deleted) {
      const auto index_log = bci.info.layout.logs.back();
      const int shards_num = rgw::num_shards(index_log.layout.in_index);
      bci.info.layout.logs.push_back({log.gen+1, {rgw::BucketLogType::Deleted}});
      ldpp_dout(dpp, 10) << "store log layout type: " <<  bci.info.layout.logs.back().layout.type << dendl;
      for (int i = 0; i < shards_num; ++i) {
        ldpp_dout(dpp, 10) << "adding to data_log shard_id: " << i << " of gen:" << index_log.gen << dendl;
        int ret = svc_datalog->add_entry(dpp, bci.info, index_log, i, y);
        if (ret < 0) {
          ldpp_dout(dpp, 1) << "WARNING: failed writing data log for bucket="
          << bci.info.bucket << ", shard_id=" << i << "of generation="
          << index_log.gen << dendl;
          } // datalog error is not fatal
      }
    }
  }

  //always keep bucket versioning enabled on archive zone
  if (driver->get_zone()->get_tier_type() == "archive") {
    bci.info.flags = (bci.info.flags & ~BUCKET_VERSIONS_SUSPENDED) | BUCKET_VERSIONED;
  }

  /* record the read version (if any), store the new version */
  bci.info.objv_tracker.read_version = objv_tracker.read_version;
  bci.info.objv_tracker.write_version = objv_tracker.write_version;
  
  return 0;
}

int RGWBucketInstanceMetadataHandler::put_post(
    const DoutPrefixProvider* dpp, optional_yield y,
    const RGWBucketCompleteInfo& bci,
    const std::optional<RGWBucketCompleteInfo>& old_bci,
    RGWObjVersionTracker& objv_tracker)
{
  int ret = svc_bi->init_index(dpp, y, bci.info, bci.info.layout.current_index);
  if (ret < 0) {
    return ret;
  }

  /* update lc list on changes to lifecyle policy */
  {
    auto bucket = driver->get_bucket(bci.info);
    auto lc = driver->get_rgwlc();

    auto lc_it = bci.attrs.find(RGW_ATTR_LC);
    if (lc_it != bci.attrs.end()) {
      ldpp_dout(dpp, 20) << "set lc config for " << bci.info.bucket.name << dendl;
      ret = lc->set_bucket_config(dpp, y, bucket.get(), bci.attrs, nullptr);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << __func__ << " failed to set lc config for "
            << bci.info.bucket.name
            << dendl;
        return ret;
      }

    } else if (old_bci) {
      lc_it = old_bci->attrs.find(RGW_ATTR_LC);
      if (lc_it != old_bci->attrs.end()) {
        ldpp_dout(dpp, 20) << "remove lc config for " << old_bci->info.bucket.name << dendl;
        constexpr bool update_attrs = false;
        ret = lc->remove_bucket_config(dpp, y, bucket.get(), update_attrs);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << __func__ << " failed to remove lc config for "
              << old_bci->info.bucket.name
              << dendl;
          return ret;
        }
      }
    }
  } /* update lc */

  /* update bucket topic mapping */
  {
    auto* orig_bci = (old_bci ? &*old_bci : nullptr);
    ret = update_bucket_topic_mappings(dpp, orig_bci, &bci, driver);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__
                        << " failed to apply bucket topic mapping for "
                        << bci.info.bucket.name << dendl;
      return ret;
    }
    ldpp_dout(dpp, 20) << __func__
                       << " successfully applied bucket topic mapping for "
                       << bci.info.bucket.name << dendl;
  }

  objv_tracker = bci.info.objv_tracker;

  return STATUS_APPLIED;
}

int RGWBucketInstanceMetadataHandler::remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
                                             optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWBucketCompleteInfo bci;
  int ret = svc_bucket->read_bucket_instance_info(entry, &bci.info, nullptr,
                                                  &bci.attrs, y, dpp);
  if (ret == -ENOENT) {
    return 0;
  }
  if (ret < 0) {
    return ret;
  }

  // skip bucket instance removal. each zone will handle it independently during trimming

  std::ignore = update_bucket_topic_mappings(dpp, &bci, /*current_bci=*/nullptr,
                                             driver);
  return 0;
}

int RGWBucketInstanceMetadataHandler::mutate(const std::string& entry, const ceph::real_time& mtime,
                                             RGWObjVersionTracker* objv_tracker, optional_yield y,
                                             const DoutPrefixProvider* dpp, RGWMDLogStatus op_type,
                                             std::function<int()> f)
{
  return -ENOTSUP; // unused
}

int RGWBucketInstanceMetadataHandler::list_keys_init(const DoutPrefixProvider* dpp,
                                                     const std::string& marker,
                                                     void** phandle)
{
  std::unique_ptr<RGWMetadataLister> lister;
  int ret = svc_bucket->create_instance_lister(dpp, marker, lister);
  if (ret < 0) {
    return ret;
  }
  *phandle = lister.release(); // release ownership
  return 0;
}

int RGWBucketInstanceMetadataHandler::list_keys_next(const DoutPrefixProvider* dpp,
                                                     void* handle, int max,
                                                     std::list<std::string>& keys,
                                                     bool* truncated)
{
  auto lister = static_cast<RGWMetadataLister*>(handle);
  return lister->get_next(dpp, max, keys, truncated);
}

void RGWBucketInstanceMetadataHandler::list_keys_complete(void *handle)
{
  delete static_cast<RGWMetadataLister*>(handle);
}

std::string RGWBucketInstanceMetadataHandler::get_marker(void *handle)
{
  auto lister = static_cast<RGWMetadataLister*>(handle);
  return lister->get_marker();
}


class RGWArchiveBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandler {
 public:
  using RGWBucketInstanceMetadataHandler::RGWBucketInstanceMetadataHandler;

  // N.B. replication of lifecycle policy relies on logic in RGWBucketInstanceMetadataHandler::put(...), override with caution

  int remove(std::string& entry, RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp) override
  {
    ldpp_dout(dpp, 0) << "SKIP: bucket instance removal is not allowed on archive zone: bucket.instance:" << entry << dendl;
    return 0;
  }
};

RGWBucketCtl::RGWBucketCtl(RGWSI_Zone *zone_svc,
                           RGWSI_Bucket *bucket_svc,
                           RGWSI_Bucket_Sync *bucket_sync_svc,
                           RGWSI_BucketIndex *bi_svc,
                           RGWSI_User* user_svc,
                           RGWDataChangesLog *datalog_svc)
  : cct(zone_svc->ctx())
{
  svc.zone = zone_svc;
  svc.bucket = bucket_svc;
  svc.bucket_sync = bucket_sync_svc;
  svc.bi = bi_svc;
  svc.user = user_svc;
  svc.datalog_rados = datalog_svc;
}

void RGWBucketCtl::init(RGWUserCtl *user_ctl,
                        RGWDataChangesLog *datalog,
                        const DoutPrefixProvider *dpp)
{
  ctl.user = user_ctl;

  datalog->set_bucket_filter(
    [this](const rgw_bucket& bucket, optional_yield y, const DoutPrefixProvider *dpp) {
      return bucket_exports_data(bucket, y, dpp);
    });
}

int RGWBucketCtl::read_bucket_entrypoint_info(const rgw_bucket& bucket,
                                              RGWBucketEntryPoint *info,
                                              optional_yield y, const DoutPrefixProvider *dpp,
                                              const Bucket::GetParams& params)
{
  return svc.bucket->read_bucket_entrypoint_info(RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                 info,
                                                 params.objv_tracker,
                                                 params.mtime,
                                                 params.attrs,
                                                 y,
                                                 dpp,
                                                 params.cache_info,
                                                 params.refresh_version);
}

int RGWBucketCtl::store_bucket_entrypoint_info(const rgw_bucket& bucket,
                                               RGWBucketEntryPoint& info,
                                               optional_yield y,
                                               const DoutPrefixProvider *dpp,
                                               const Bucket::PutParams& params)
{
  return svc.bucket->store_bucket_entrypoint_info(RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                  info,
                                                  params.exclusive,
                                                  params.mtime,
                                                  params.attrs,
                                                  params.objv_tracker,
                                                  y,
                                                  dpp);
}

int RGWBucketCtl::remove_bucket_entrypoint_info(const rgw_bucket& bucket,
                                                optional_yield y,
                                                const DoutPrefixProvider *dpp,
                                                const Bucket::RemoveParams& params)
{
  return svc.bucket->remove_bucket_entrypoint_info(RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                   params.objv_tracker,
                                                   y,
                                                   dpp);
}

int RGWBucketCtl::read_bucket_instance_info(const rgw_bucket& bucket,
                                            RGWBucketInfo *info,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp,
                                            const BucketInstance::GetParams& params)
{
  int ret = svc.bucket->read_bucket_instance_info(RGWSI_Bucket::get_bi_meta_key(bucket),
                                                  info,
                                                  params.mtime,
                                                  params.attrs,
                                                  y,
                                                  dpp,
                                                  params.cache_info,
                                                  params.refresh_version);
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
                                                    .set_objv_tracker(ep_objv_tracker));
    if (r < 0) {
      return r;
    }

    b = &ep->bucket;
  }

  int ret = svc.bucket->read_bucket_instance_info(RGWSI_Bucket::get_bi_meta_key(*b),
                                                  info,
                                                  params.mtime,
                                                  params.attrs,
                                                  y, dpp,
                                                  params.cache_info,
                                                  params.refresh_version);
  if (ret < 0) {
    return ret;
  }

  if (params.objv_tracker) {
    *params.objv_tracker = info->objv_tracker;
  }

  return 0;
}

int RGWBucketCtl::store_bucket_instance_info(const rgw_bucket& bucket,
                                            RGWBucketInfo& info,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp,
                                            const BucketInstance::PutParams& params)
{
  if (params.objv_tracker) {
    info.objv_tracker = *params.objv_tracker;
  }

  return svc.bucket->store_bucket_instance_info(RGWSI_Bucket::get_bi_meta_key(bucket),
                                                info,
                                                params.orig_info,
                                                params.exclusive,
                                                params.mtime,
                                                params.attrs,
                                                y,
                                                dpp);
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

  return svc.bucket->remove_bucket_instance_info(RGWSI_Bucket::get_bi_meta_key(bucket),
                                                 info,
                                                 &info.objv_tracker,
                                                 y,
                                                 dpp);
}

int RGWBucketCtl::do_store_linked_bucket_info(RGWBucketInfo& info,
                                              RGWBucketInfo *orig_info,
                                              bool exclusive, real_time mtime,
                                              obj_version *pep_objv,
                                              map<string, bufferlist> *pattrs,
                                              bool create_entry_point,
					      optional_yield y, const DoutPrefixProvider *dpp)
{
  bool create_head = !info.has_instance_obj || create_entry_point;

  int ret = svc.bucket->store_bucket_instance_info(RGWSI_Bucket::get_bi_meta_key(info.bucket),
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
  return svc.bucket->store_bucket_entrypoint_info(RGWSI_Bucket::get_entrypoint_meta_key(info.bucket),
                                                  entry_point,
                                                  exclusive,
                                                  mtime,
                                                  pattrs,
                                                  &ot,
                                                  y,
                                                  dpp);
}

int RGWBucketCtl::convert_old_bucket_info(const rgw_bucket& bucket,
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

  int ret = svc.bucket->read_bucket_entrypoint_info(RGWSI_Bucket::get_entrypoint_meta_key(bucket),
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

  ret = do_store_linked_bucket_info(info, nullptr, false, ep_mtime, &ot.write_version, &attrs, true, y, dpp);
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
  rgw_bucket& bucket = bucket_info.bucket;

  if (!bucket_info.has_instance_obj) {
    /* an old bucket object, need to convert it */
      int ret = convert_old_bucket_info(bucket, y, dpp);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: failed converting old bucket info: " << ret << dendl;
        return ret;
      }
  }

  return store_bucket_instance_info(bucket,
                                    bucket_info,
                                    y,
                                    dpp,
                                    BucketInstance::PutParams().set_attrs(&attrs)
                                                               .set_objv_tracker(objv_tracker)
                                                               .set_orig_info(&bucket_info));
}

static rgw_raw_obj get_owner_buckets_obj(RGWSI_User* svc_user,
                                         RGWSI_Zone* svc_zone,
                                         const rgw_owner& owner)
{
  return std::visit(fu2::overload(
      [&] (const rgw_user& uid) {
        return svc_user->get_buckets_obj(uid);
      },
      [&] (const rgw_account_id& account_id) {
        const RGWZoneParams& zone = svc_zone->get_zone_params();
        return rgwrados::account::get_buckets_obj(zone, account_id);
      }), owner);
}

int RGWBucketCtl::link_bucket(librados::Rados& rados,
                              const rgw_owner& owner,
                              const rgw_bucket& bucket,
                              ceph::real_time creation_time,
			      optional_yield y,
                              const DoutPrefixProvider *dpp,
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
      ret = svc.bucket->read_bucket_entrypoint_info(meta_key,
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

  const auto& buckets_obj = get_owner_buckets_obj(svc.user, svc.zone, owner);
  ret = rgwrados::buckets::add(dpp, y, rados, buckets_obj,
                               bucket, creation_time);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: error adding bucket to owner directory:"
		  << " owner=" << owner
                  << " bucket=" << bucket
		  << " err=" << cpp_strerror(-ret)
		  << dendl;
    goto done_err;
  }

  if (!update_entrypoint)
    return 0;

  ep.linked = true;
  ep.owner = owner;
  ep.bucket = bucket;
  ret = svc.bucket->store_bucket_entrypoint_info(
    meta_key, ep, false, real_time(), pattrs, &rot, y, dpp);
  if (ret < 0)
    goto done_err;

  return 0;

done_err:
  int r = unlink_bucket(rados, owner, bucket, y, dpp, true);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed unlinking bucket on error cleanup: "
                           << cpp_strerror(-r) << dendl;
  }
  return ret;
}

int RGWBucketCtl::unlink_bucket(librados::Rados& rados, const rgw_owner& owner,
                                const rgw_bucket& bucket, optional_yield y,
                                const DoutPrefixProvider *dpp, bool update_entrypoint)
{
  const auto& buckets_obj = get_owner_buckets_obj(svc.user, svc.zone, owner);
  int ret = rgwrados::buckets::remove(dpp, y, rados, buckets_obj, bucket);
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
  ret = svc.bucket->read_bucket_entrypoint_info(meta_key, &ep, &ot, nullptr, &attrs, y, dpp);
  if (ret == -ENOENT)
    return 0;
  if (ret < 0)
    return ret;

  if (!ep.linked)
    return 0;

  if (ep.owner != owner) {
    ldpp_dout(dpp, 0) << "bucket entry point owner mismatch, can't unlink bucket: " << ep.owner << " != " << owner << dendl;
    return -EINVAL;
  }

  ep.linked = false;
  return svc.bucket->store_bucket_entrypoint_info(meta_key, ep, false, real_time(), &attrs, &ot, y, dpp);
}

int RGWBucketCtl::read_bucket_stats(const rgw_bucket& bucket,
                                    RGWBucketEnt *result,
                                    optional_yield y,
                                    const DoutPrefixProvider *dpp)
{
  return svc.bucket->read_bucket_stats(bucket, result, y, dpp);
}

int RGWBucketCtl::read_buckets_stats(std::vector<RGWBucketEnt>& buckets,
                                     optional_yield y, const DoutPrefixProvider *dpp)
{
  return svc.bucket->read_buckets_stats(buckets, y, dpp);
}

int RGWBucketCtl::sync_owner_stats(const DoutPrefixProvider *dpp,
                                   librados::Rados& rados,
                                   const rgw_owner& owner,
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

  // flush stats to the user/account owner object
  const rgw_raw_obj& obj = std::visit(fu2::overload(
      [&] (const rgw_user& user) {
        return svc.user->get_buckets_obj(user);
      },
      [&] (const rgw_account_id& id) {
        const RGWZoneParams& zone = svc.zone->get_zone_params();
        return rgwrados::account::get_buckets_obj(zone, id);
      }), owner);
  return rgwrados::buckets::write_stats(dpp, y, rados, obj, *pent);
}

int RGWBucketCtl::get_sync_policy_handler(std::optional<rgw_zone_id> zone,
                                          std::optional<rgw_bucket> bucket,
                                          RGWBucketSyncPolicyHandlerRef *phandler,
                                          optional_yield y,
                                          const DoutPrefixProvider *dpp)
{
  int r = svc.bucket_sync->get_policy_handler(zone, bucket, phandler, y, dpp);
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

auto create_bucket_metadata_handler(librados::Rados& rados,
                                    RGWSI_Bucket* svc_bucket,
                                    RGWBucketCtl* ctl_bucket)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<RGWBucketMetadataHandler>(
      rados, svc_bucket, ctl_bucket);
}

auto create_bucket_instance_metadata_handler(rgw::sal::Driver* driver,
                                             RGWSI_Zone* svc_zone,
                                             RGWSI_Bucket* svc_bucket,
                                             RGWSI_BucketIndex* svc_bi,
                                             RGWDataChangesLog *svc_datalog)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<RGWBucketInstanceMetadataHandler>(driver, svc_zone,
                                                            svc_bucket, svc_bi,
                                                            svc_datalog);
}

auto create_archive_bucket_metadata_handler(librados::Rados& rados,
                                            RGWSI_Bucket* svc_bucket,
                                            RGWBucketCtl* ctl_bucket)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<RGWArchiveBucketMetadataHandler>(
      rados, svc_bucket, ctl_bucket);
}

auto create_archive_bucket_instance_metadata_handler(rgw::sal::Driver* driver,
                                                     RGWSI_Zone* svc_zone,
                                                     RGWSI_Bucket* svc_bucket,
                                                     RGWSI_BucketIndex* svc_bi,
                                                     RGWDataChangesLog *svc_datalog)
    -> std::unique_ptr<RGWMetadataHandler>
{
  return std::make_unique<RGWArchiveBucketInstanceMetadataHandler>(driver, svc_zone,
                                                                   svc_bucket, svc_bi,
                                                                   svc_datalog);
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

