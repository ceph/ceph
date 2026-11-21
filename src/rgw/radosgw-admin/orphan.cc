// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Copyright (C) 2024 IBM 
*/

#include "radosgw-admin/orphan.h"
#include <string>

#include "common/config.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "rgw_op.h"
#include "rgw_multi.h"
#include "rgw_zone.h"
#include "rgw_bucket.h"
#include "rgw_sal_rados.h"

#include "services/svc_zone.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

int RGWRadosList::handle_stat_result(const DoutPrefixProvider *dpp,
				     RGWRados::Object::Stat::Result& result,
				     const std::string& bucket_name,
				     const rgw_obj_key& obj_key,
				     std::set<string>& obj_oids)
{
  obj_oids.clear();

  rgw_bucket& bucket = result.obj.bucket;

  ldpp_dout(dpp, 20) << "RGWRadosList::" << __func__ <<
    " bucket=" << bucket <<
    ", has_manifest=" << result.manifest.has_value() <<
    dendl;

  // iterator to store result of dlo/slo attribute find
  decltype(result.attrs)::iterator attr_it = result.attrs.end();
  const std::string oid = bucket.marker + "_" + result.obj.get_oid();
  ldpp_dout(dpp, 20) << "radoslist processing object=\"" <<
      oid << "\"" << dendl;
  if (visited_oids.find(oid) != visited_oids.end()) {
    // apparently we hit a loop; don't continue with this oid
    ldpp_dout(dpp, 15) <<
      "radoslist stopped loop at already visited object=\"" <<
      oid << "\"" << dendl;
    return 0;
  }

  if (!result.manifest) {
    /* a very very old object, or part of a multipart upload during upload */
    obj_oids.insert(oid);

    /*
     * multipart parts don't have manifest on them, it's in the meta
     * object; we'll process them in
     * RGWRadosList::do_incomplete_multipart
     */
  } else if ((attr_it = result.attrs.find(RGW_ATTR_USER_MANIFEST)) !=
	     result.attrs.end()) {
    // *** handle DLO object ***

    obj_oids.insert(oid);
    visited_oids.insert(oid); // prevent dlo loops
    ldpp_dout(dpp, 15) << "radoslist added to visited list DLO=\"" <<
      oid << "\"" << dendl;

    char* prefix_path_c = attr_it->second.c_str();
    const std::string& prefix_path = prefix_path_c;

    const size_t sep_pos = prefix_path.find('/');
    if (string::npos == sep_pos) {
      return -EINVAL;
    }

    const std::string bucket_name = prefix_path.substr(0, sep_pos);
    const std::string prefix = prefix_path.substr(sep_pos + 1);

    add_bucket_prefix(bucket_name, prefix);
    ldpp_dout(dpp, 25) << "radoslist DLO oid=\"" << oid <<
      "\" added bucket=\"" << bucket_name << "\" prefix=\"" <<
      prefix << "\" to process list" << dendl;
  } else if ((attr_it = result.attrs.find(RGW_ATTR_SLO_MANIFEST)) !=
	     result.attrs.end()) {
    // *** handle SLO object ***

    obj_oids.insert(oid);
    visited_oids.insert(oid); // prevent slo loops
    ldpp_dout(dpp, 15) << "radoslist added to visited list SLO=\"" <<
      oid << "\"" << dendl;

    RGWSLOInfo slo_info;
    bufferlist::const_iterator bliter = attr_it->second.begin();
    try {
      ::decode(slo_info, bliter);
    } catch (buffer::error& err) {
      ldpp_dout(dpp, 0) <<
	"ERROR: failed to decode slo manifest for " << oid << dendl;
      return -EIO;
    }

    for (const auto& iter : slo_info.entries) {
      const string& path_str = iter.path;

      const size_t sep_pos = path_str.find('/', 1 /* skip initial slash */);
      if (string::npos == sep_pos) {
	return -EINVAL;
      }

      std::string bucket_name;
      std::string obj_name;

      bucket_name = url_decode(path_str.substr(1, sep_pos - 1));
      obj_name = url_decode(path_str.substr(sep_pos + 1));

      const rgw_obj_key obj_key(obj_name);
      add_bucket_filter(bucket_name, obj_key);
      ldpp_dout(dpp, 25) << "radoslist SLO oid=\"" << oid <<
	"\" added bucket=\"" << bucket_name << "\" obj_key=\"" <<
	obj_key << "\" to process list" << dendl;
    }
  } else {
    RGWObjManifest& manifest = *result.manifest;

    // in multipart, the head object contains no data and just has the
    // manifest AND empty objects have no manifest, but they're
    // realized as empty rados objects
    if (0 == manifest.get_max_head_size() ||
	manifest.obj_begin(dpp) == manifest.obj_end(dpp)) {
      obj_oids.insert(oid);
      // first_insert = true;
    }

    RGWObjManifest::obj_iterator miter;
    for (miter = manifest.obj_begin(dpp); miter != manifest.obj_end(dpp); ++miter) {
      const rgw_raw_obj& loc =
	miter.get_location().get_raw_obj(store->getRados());
      string s = loc.oid;
      obj_oids.insert(s);
    }
  }

  return 0;
} // RGWRadosList::handle_stat_result

int RGWRadosList::pop_and_handle_stat_op(
  const DoutPrefixProvider *dpp,
  RGWObjectCtx& obj_ctx,
  std::deque<RGWRados::Object::Stat>& ops)
{
  std::set<std::string> obj_oids;
  RGWRados::Object::Stat& front_op = ops.front();

  int ret = front_op.wait(dpp);
  // note: even if we get an error, front_op.result.obj will still be
  // populated with correct data
  const std::string bucket_name = front_op.result.obj.bucket.get_namespaced_name();
  const rgw_obj_key obj_key = front_op.result.obj.key;

  if (ret == -ENOENT) {
    const auto& result = front_op.result;
    const rgw_bucket& bucket = result.obj.bucket;
    const std::string oid = bucket.marker + "_" + result.obj.get_oid();
    obj_oids.insert(oid);

    ldpp_dout(dpp, -1) << "ERROR: " << __func__ <<
      ": stat of head object resulted in ENOENT; oid=" << oid << dendl;
  } else if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: " << __func__ <<
      ": stat_async() returned error: " <<
      cpp_strerror(-ret) << dendl;
    goto done;
  } else {
    ret = handle_stat_result(dpp, front_op.result, bucket_name, obj_key, obj_oids);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR: " << __func__ <<
	": handle_stat_result() returned error: " <<
	cpp_strerror(-ret) << dendl;
    }
  }

  // output results
  for (const auto& o : obj_oids) {
    if (include_rgw_obj_name) {
      std::cout << o <<
	field_separator << bucket_name <<
	field_separator << obj_key <<
	std::endl;
    } else {
      std::cout << o << std::endl;
    }
  }

done:

  // invalidate object context for this object to avoid memory leak
  // (see pr https://github.com/ceph/ceph/pull/30174)
  obj_ctx.invalidate(front_op.result.obj);

  ops.pop_front();

  return ret;
}


#if 0 // code that may be the basis for expansion
int RGWRadosList::build_buckets_instance_index()
{
  void *handle;
  int max = 1000;
  string section = "bucket.instance";
  int ret = store->meta_mgr->list_keys_init(section, &handle);
  if (ret < 0) {
    lderr(store->ctx()) << "ERROR: can't get key: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  map<int, list<string> > instances;

  bool truncated;

  RGWObjectCtx obj_ctx(store);

  int count = 0;
  uint64_t total = 0;

  do {
    list<string> keys;
    ret = store->meta_mgr->list_keys_next(handle, max, keys, &truncated);
    if (ret < 0) {
      lderr(store->ctx()) << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
      ++total;
      ldout(store->ctx(), 10) << "bucket_instance=" << *iter << " total=" << total << dendl;
      int shard = orphan_shard(*iter);
      instances[shard].push_back(*iter);

      if (++count >= COUNT_BEFORE_FLUSH) {
        ret = log_oids(buckets_instance_index, instances);
        if (ret < 0) {
          lderr(store->ctx()) << __func__ << ": ERROR: log_oids() returned ret=" << ret << dendl;
          return ret;
        }
        count = 0;
        instances.clear();
      }
    }
  } while (truncated);

  ret = log_oids(buckets_instance_index, instances);
  if (ret < 0) {
    lderr(store->ctx()) << __func__ << ": ERROR: log_oids() returned ret=" << ret << dendl;
    return ret;
  }
  store->meta_mgr->list_keys_complete(handle);

  return 0;
}
#endif


int RGWRadosList::process_bucket(
  const DoutPrefixProvider *dpp,
  const std::string& bucket_instance_id,
  const std::string& prefix,
  const std::set<rgw_obj_key>& entries_filter)
{
  ldpp_dout(dpp, 10) << "RGWRadosList::" << __func__ <<
    " bucket_instance_id=" << bucket_instance_id <<
    ", prefix=" << prefix <<
    ", entries_filter.size=" << entries_filter.size() << dendl;

  RGWBucketInfo bucket_info;
  int ret = store->getRados()->get_bucket_instance_info(bucket_instance_id,
							bucket_info,
							nullptr,
							nullptr,
							null_yield,
                                                        dpp);
  if (ret < 0) {
    if (ret == -ENOENT) {
      // probably raced with bucket removal
      return 0;
    }
    ldpp_dout(dpp, -1) << __func__ <<
      ": ERROR: RGWRados::get_bucket_instance_info() returned ret=" <<
      ret << dendl;
    return ret;
  }

  RGWRados::Bucket target(store->getRados(), bucket_info);
  RGWRados::Bucket::List list_op(&target);

  std::string marker;
  list_op.params.marker = rgw_obj_key(marker);
  list_op.params.list_versions = true;
  list_op.params.enforce_ns = false;
  list_op.params.allow_unordered = false;
  list_op.params.prefix = prefix;

  bool truncated;

  std::deque<RGWRados::Object::Stat> stat_ops;
  std::string prev_versioned_key_name = "";

  RGWObjectCtx obj_ctx(store);

  do {
    std::vector<rgw_bucket_dir_entry> result;
    constexpr int64_t LIST_OBJS_MAX_ENTRIES = 100;
    ret = list_op.list_objects(dpp, LIST_OBJS_MAX_ENTRIES, &result,
			       NULL, &truncated, null_yield);
    if (ret == -ENOENT) {
      // race with bucket delete?
      ret = 0;
      break;
    } else if (ret < 0) {
      std::cerr << "ERROR: store->list_objects(): " << cpp_strerror(-ret) <<
	std::endl;
      return ret;
    }

    for (std::vector<rgw_bucket_dir_entry>::iterator iter = result.begin();
	 iter != result.end();
	 ++iter) {
      rgw_bucket_dir_entry& entry = *iter;

      if (entry.key.instance.empty()) {
        ldpp_dout(dpp, 20) << "obj entry: " << entry.key.name << dendl;
      } else {
        ldpp_dout(dpp, 20) << "obj entry: " << entry.key.name <<
	  " [" << entry.key.instance << "]" << dendl;
      }

      ldpp_dout(dpp, 20) << __func__ << ": entry.key.name=" <<
	entry.key.name << " entry.key.instance=" << entry.key.instance <<
	dendl;

      // ignore entries that are not in the filter if there is a filter
      if (!entries_filter.empty() &&
	  entries_filter.find(entry.key) == entries_filter.cend()) {
	continue;
      }

      auto bucket = store->get_bucket(bucket_info);
      // we need to do this in two cases below, so use a lambda
      auto do_stat_key =
	[&](const rgw_obj_key& key) -> int {
	  int ret;

	  rgw_obj obj(bucket_info.bucket, key);
	  RGWRados::Object op_target(store->getRados(), bucket_info,
				     obj_ctx, obj);

	  stat_ops.push_back(RGWRados::Object::Stat(&op_target));
	  RGWRados::Object::Stat& op = stat_ops.back();

	  ret = op.stat_async(dpp);
	  if (ret < 0) {
	    ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " <<
	      cpp_strerror(-ret) << dendl;
	    return ret;
	  }

	  if (stat_ops.size() >= max_concurrent_ios) {
	    ret = pop_and_handle_stat_op(dpp, obj_ctx, stat_ops);
	    if (ret < 0) {
	      if (ret != -ENOENT) {
		ldpp_dout(dpp, -1) <<
		  "ERROR: pop_and_handle_stat_op() returned error: " <<
		  cpp_strerror(-ret) << dendl;
	      }

	      // clear error, so we'll continue processing directory
	      ret = 0;
	    }
	  }

	  return ret;
	}; // do_stat_key lambda

      // for versioned objects, make sure the head object is handled
      // as well by ignoring the instance identifier
      if (!entry.key.instance.empty() &&
	  entry.key.name != prev_versioned_key_name) {
	// don't do the same key twice; even though out bucket index
	// listing allows unordered, since all versions of an object
	// use the same bucket index key, they'll all end up together
	// and sorted
	prev_versioned_key_name = entry.key.name;

	rgw_obj_key uninstanced(entry.key);
	uninstanced.instance.clear();

	ret = do_stat_key(uninstanced);
	if (ret < 0) {
	  return ret;
	}
      }

      if (entry.is_delete_marker()) {
	continue;
      }

      ret = do_stat_key(entry.key);
      if (ret < 0) {
	return ret;
      }
    } // for iter loop
  } while (truncated);

  while (!stat_ops.empty()) {
    ret = pop_and_handle_stat_op(dpp, obj_ctx, stat_ops);
    if (ret < 0) {
      if (ret != -ENOENT) {
        ldpp_dout(dpp, -1) << "ERROR: stat_async() returned error: " <<
	  cpp_strerror(-ret) << dendl;
      }
    }
  }

  return 0;
}


int RGWRadosList::run(const DoutPrefixProvider *dpp,
		      const bool yes_i_really_mean_it)
{
  int ret;
  void* handle = nullptr;

  ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
      " ERROR: list_keys_init returned " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  constexpr int max_keys = 1000;
  bool truncated = true;
  bool warned_indexless = false;

  do {
    std::list<std::string> buckets;
    ret = store->meta_list_keys_next(dpp, handle, max_keys, buckets, &truncated);

    for (std::string& bucket_id : buckets) {
      ret = run(dpp, bucket_id, true);
      if (ret == -ENOENT) {
	continue;
      } else if (ret == -EINVAL) {
	if (! warned_indexless) {
	  if (yes_i_really_mean_it) {
	    std::cerr <<
	      "WARNING: because there is at least one indexless bucket (" <<
	      bucket_id <<
	      ") the results of radoslist are *incomplete*; continuing due to --yes-i-really-mean-it" <<
	      std::endl;
	    warned_indexless = true;
	  } else {
	    std::cerr << "ERROR: because there is at least one indexless bucket (" <<
	      bucket_id <<
	      ") the results of radoslist are *incomplete*; use --yes-i-really-mean-it to bypass error" <<
	      std::endl;
	    return ret;
	  }
	}
	continue;
      } else if (ret < 0) {
	return ret;
      }
    }
  } while (truncated);

  return 0;
} // RGWRadosList::run(DoutPrefixProvider, bool)


int RGWRadosList::run(const DoutPrefixProvider *dpp,
		      const std::string& start_bucket_name,
		      const bool silent_indexless)
{
  int ret;

  add_bucket_entire(start_bucket_name);

  while (! bucket_process_map.empty()) {
    // pop item from map and capture its key data
    auto front = bucket_process_map.begin();
    std::string bucket_name = front->first;
    process_t process;
    std::swap(process, front->second);
    bucket_process_map.erase(front);

    std::unique_ptr<rgw::sal::Bucket> bucket;
    ret = store->load_bucket(dpp, rgw_bucket(tenant_name, bucket_name),
                             &bucket, null_yield);
    if (ret == -ENOENT) {
      std::cerr << "WARNING: bucket " << bucket_name <<
	" does not exist; could it have been deleted very recently?" <<
	std::endl;
      continue;
    } else if (ret < 0) {
      std::cerr << "ERROR: could not get info for bucket " << bucket_name <<
	" -- " << cpp_strerror(-ret) << std::endl;
      return ret;
    } else if (bucket->get_info().is_indexless()) {
      if (! silent_indexless) {
	std::cerr << "ERROR: unable to run radoslist on indexless bucket " <<
	  bucket_name << std::endl;
      }
      return -EINVAL;
    }

    const std::string bucket_id = bucket->get_key().get_key();

    static const std::set<rgw_obj_key> empty_filter;
    static const std::string empty_prefix;

    auto do_process_bucket =
      [dpp, &bucket_id, this]
      (const std::string& prefix,
       const std::set<rgw_obj_key>& entries_filter) -> int {
	int ret = process_bucket(dpp, bucket_id, prefix, entries_filter);
	if (ret == -ENOENT) {
	  // bucket deletion race?
	  return 0;
	} if (ret < 0) {
	  ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
	    ": ERROR: process_bucket(); bucket_id=" <<
	    bucket_id << " returned ret=" << ret << dendl;
	}

	return ret;
      };

    // either process the whole bucket *or* process the filters and/or
    // the prefixes
    if (process.entire_container) {
      ret = do_process_bucket(empty_prefix, empty_filter);
      if (ret < 0) {
	return ret;
      }
    } else {
      if (! process.filter_keys.empty()) {
	ret = do_process_bucket(empty_prefix, process.filter_keys);
	if (ret < 0) {
	  return ret;
	}
      }
      for (const auto& p : process.prefixes) {
	ret = do_process_bucket(p, empty_filter);
	if (ret < 0) {
	  return ret;
	}
      }
    }
  } // while (! bucket_process_map.empty())

  // now handle incomplete multipart uploads by going back to the
  // initial bucket

  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = store->load_bucket(dpp, rgw_bucket(tenant_name, start_bucket_name),
                           &bucket, null_yield);
  if (ret == -ENOENT) {
    // bucket deletion race?
    return 0;
  } else if (ret < 0) {
    ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
      ": ERROR: get_bucket_info returned ret=" << ret << dendl;
    return ret;
  }

  ret = do_incomplete_multipart(dpp, bucket.get());
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
      ": ERROR: do_incomplete_multipart returned ret=" << ret << dendl;
    return ret;
  }

  return 0;
} // RGWRadosList::run(DoutPrefixProvider, string, bool)


int RGWRadosList::do_incomplete_multipart(const DoutPrefixProvider *dpp,
					  rgw::sal::Bucket* bucket)
{
  constexpr int max_uploads = 1000;
  constexpr int max_parts = 1000;
  std::string marker;
  vector<std::unique_ptr<rgw::sal::MultipartUpload>> uploads;
  bool is_truncated;
  int ret;

  // use empty strings for params.{prefix,delim}

  do {
    uploads.clear();
    ret = bucket->list_multiparts(dpp, string(), marker, string(), max_uploads, uploads, nullptr, &is_truncated, null_yield);
    if (ret == -ENOENT) {
      // could bucket have been removed while this is running?
      ldpp_dout(dpp, 5) << "RGWRadosList::" << __func__ <<
	": WARNING: call to list_objects of multipart namespace got ENOENT; "
	"assuming bucket removal race" << dendl;
      break;
    } else if (ret < 0) {
      ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
	": ERROR: list_objects op returned ret=" << ret << dendl;
      return ret;
    }

    if (!uploads.empty()) {
      // now process the uploads vector
      for (const auto& upload : uploads) {
	int parts_marker = 0;
	bool is_parts_truncated = false;

	do { // while (is_parts_truncated);
	  ret = upload->list_parts(dpp, store->ctx(), max_parts, parts_marker,
				   &parts_marker, &is_parts_truncated, null_yield);
	  if (ret == -ENOENT) {
	    ldpp_dout(dpp, 5) <<  "RGWRadosList::" << __func__ <<
	      ": WARNING: list_multipart_parts returned ret=-ENOENT "
	      "for " << upload->get_upload_id() << ", moving on" << dendl;
	    break;
	  } else if (ret < 0) {
	    ldpp_dout(dpp, -1) << "RGWRadosList::" << __func__ <<
	      ": ERROR: list_multipart_parts returned ret=" << ret <<
	      dendl;
	    return ret;
	  }

	  for (auto& p : upload->get_parts()) {
	    rgw::sal::RadosMultipartPart* part =
	      dynamic_cast<rgw::sal::RadosMultipartPart*>(p.second.get());
	    RGWObjManifest& manifest = part->get_manifest();
	    for (auto obj_it = manifest.obj_begin(dpp);
		 obj_it != manifest.obj_end(dpp);
		 ++obj_it) {
	      const rgw_raw_obj& loc =
		obj_it.get_location().get_raw_obj(store->getRados());
	      if (include_rgw_obj_name) {
		std::cout << loc.oid <<
		  field_separator << bucket->get_key().get_namespaced_name() <<
		  field_separator << manifest.get_prefix() << "." <<
		  part->get_num() << std::endl;
	      } else {
		std::cout << loc.oid << std::endl;
	      }
	    } // for (auto obj_it
	  } // for (auto& p
	} while (is_parts_truncated);
      } // for (const auto& upload
    } // if objs not empty
  } while (is_truncated);

  return 0;
} // RGWRadosList::do_incomplete_multipart
