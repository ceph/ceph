// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/bucket.h"

#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/Formatter.h"
#include "driver/rados/rgw_bucket.h"
#include "driver/rados/rgw_reshard.h"
#include "driver/rados/rgw_sal_rados.h"
#include "include/utime.h"
#include "rgw_common.h"
#include "rgw_formats.h"
#include "rgw_sal.h"
#include "rgw_user.h"
#include "rgw_zone.h"
#include "services/svc_zone.h"

#ifdef WITH_RADOSGW_RADOS
#include "driver/rados/rgw_rados.h"
#include "radosgw-admin/orphan.h"
#endif

using ceph::Formatter;
using namespace std;

inline int posix_errortrans(int r)
{
  return ERR_NO_SUCH_BUCKET == r ? ENOENT : r;
}

namespace {

int rgw_admin_init_bucket_impl(const DoutPrefixProvider* dpp,
                               rgw::sal::Driver* driver,
                               const rgw_bucket& b,
                               std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  return driver->load_bucket(dpp, b, bucket, null_yield);
}

int rgw_admin_init_bucket_impl(const DoutPrefixProvider* dpp,
                               rgw::sal::Driver* driver,
                               const std::string& tenant_name,
                               const string& bucket_name,
                               const string& bucket_id,
                               std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  rgw_bucket b{tenant_name, bucket_name, bucket_id};
  return rgw_admin_init_bucket_impl(dpp, driver, b, bucket);
}

static int rgw_admin_check_reshard_bucket_params_impl(const DoutPrefixProvider* dpp,
                                                      rgw::sal::Driver* driver,
                                                      const string& bucket_name,
                                                      const string& tenant,
                                                      const string& bucket_id,
                                                      bool num_shards_specified,
                                                      int num_shards,
                                                      int yes_i_really_mean_it,
                                                      std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  if (bucket_name.empty()) {
    cerr << "ERROR: bucket not specified" << std::endl;
    return -EINVAL;
  }

  if (!num_shards_specified) {
    cerr << "ERROR: --num-shards not specified" << std::endl;
    return -EINVAL;
  }

#ifdef WITH_RADOSGW_RADOS
  if (num_shards > (int)static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_max_bucket_shards()) {
    cerr << "ERROR: num_shards too high, max value: " << static_cast<rgw::sal::RadosStore*>(driver)->getRados()->get_max_bucket_shards() << std::endl;
    return -EINVAL;
  }
#endif

  if (num_shards < 0) {
    cerr << "ERROR: num_shards must be non-negative integer" << std::endl;
    return -EINVAL;
  }

  int ret = rgw_admin_init_bucket_impl(dpp, driver, tenant, bucket_name, bucket_id, bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  if (! is_layout_reshardable((*bucket)->get_info().layout)) {
    std::cerr << "Bucket '" << (*bucket)->get_name() <<
      "' currently has layout '" <<
      current_layout_desc((*bucket)->get_info().layout) <<
      "', which does not support resharding." << std::endl;
    return -EINVAL;
  }

  int num_source_shards = rgw::current_num_shards((*bucket)->get_info().layout);

  if (num_shards <= num_source_shards && !yes_i_really_mean_it) {
    cerr << "num shards is less or equal to current shards count" << std::endl
	 << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
    return -EINVAL;
  }
  return 0;
}

static int rgw_admin_check_min_obj_stripe_size_impl(const DoutPrefixProvider* dpp,
                                                    rgw::sal::Driver* driver,
                                                    rgw::sal::Object* obj,
                                                    uint64_t min_stripe_size,
                                                    bool *need_rewrite)
{
  int ret = obj->get_obj_attrs(null_yield, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to stat object, returned error: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  map<string, bufferlist>::iterator iter;
  iter = obj->get_attrs().find(RGW_ATTR_MANIFEST);
  if (iter == obj->get_attrs().end()) {
    *need_rewrite = (obj->get_size() >= min_stripe_size);
    return 0;
  }

  RGWObjManifest manifest;

  try {
    bufferlist& bl = iter->second;
    auto biter = bl.cbegin();
    decode(manifest, biter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode manifest" << dendl;
    return -EIO;
  }

  map<uint64_t, RGWObjManifestPart>& objs = manifest.get_explicit_objs();
  map<uint64_t, RGWObjManifestPart>::iterator oiter;
  for (oiter = objs.begin(); oiter != objs.end(); ++oiter) {
    RGWObjManifestPart& part = oiter->second;

    if (part.size >= min_stripe_size) {
      *need_rewrite = true;
      return 0;
    }
  }
  *need_rewrite = false;

  return 0;
}

#ifdef WITH_RADOSGW_RADOS
static int check_obj_locator_underscore(const DoutPrefixProvider* dpp,
                                        rgw::sal::Driver* driver,
                                        rgw::sal::Object* obj,
                                        bool fix,
                                        bool remove_bad,
                                        Formatter *f)
{
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "head");
  f->dump_string("name", obj->get_name());
  f->dump_string("instance", obj->get_instance());
  f->close_section();

  string oid;
  string locator;

  get_obj_bucket_and_oid_loc(obj->get_obj(), oid, locator);

  f->dump_string("oid", oid);
  f->dump_string("locator", locator);

  std::unique_ptr<rgw::sal::Object::ReadOp> read_op = obj->get_read_op();

  int ret = read_op->prepare(null_yield, dpp);
  bool needs_fixing = (ret == -ENOENT);

  f->dump_bool("needs_fixing", needs_fixing);

  string status = (needs_fixing ? "needs_fixing" : "ok");

  if ((needs_fixing || remove_bad) && fix) {
    ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->fix_head_obj_locator(dpp, obj->get_bucket()->get_info(), needs_fixing, remove_bad, obj->get_key(), null_yield);
    if (ret < 0) {
      cerr << "ERROR: fix_head_object_locator() returned ret=" << ret << std::endl;
      goto done;
    }
    status = "fixed";
  }

done:
  f->dump_string("status", status);

  f->close_section();

  return 0;
}

static int check_obj_tail_locator_underscore(const DoutPrefixProvider* dpp,
                                             rgw::sal::Driver* driver,
                                             RGWBucketInfo& bucket_info,
                                             rgw_obj_key& key,
                                             bool fix,
                                             Formatter *f)
{
  f->open_object_section("object");
  f->open_object_section("key");
  f->dump_string("type", "tail");
  f->dump_string("name", key.name);
  f->dump_string("instance", key.instance);
  f->close_section();

  bool needs_fixing;
  string status;

  int ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->fix_tail_obj_locator(dpp, bucket_info, key, fix, &needs_fixing, null_yield);
  if (ret < 0) {
    cerr << "ERROR: fix_tail_object_locator_underscore() returned ret=" << ret << std::endl;
    status = "failed";
  } else {
    status = (needs_fixing && !fix ? "needs_fixing" : "ok");
  }

  f->dump_bool("needs_fixing", needs_fixing);
  f->dump_string("status", status);

  f->close_section();

  return 0;
}

static int do_check_object_locator(const DoutPrefixProvider* dpp,
                                   rgw::sal::Driver* driver,
                                   const string& tenant_name,
                                   const string& bucket_name,
                                   bool fix,
                                   bool remove_bad,
                                   Formatter *f)
{
  if (remove_bad && !fix) {
    cerr << "ERROR: can't have remove_bad specified without fix" << std::endl;
    return -EINVAL;
  }

  std::unique_ptr<rgw::sal::Bucket> bucket;
  string bucket_id;

  f->open_object_section("bucket");
  f->dump_string("bucket", bucket_name);
  int ret = rgw_admin_init_bucket_impl(dpp, driver, tenant_name, bucket_name, bucket_id, &bucket);
  if (ret < 0) {
    cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }
  int count = 0;

  int max_entries = 1000;

  string prefix;
  string delim;
  string marker;
  string ns;

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.prefix = prefix;
  params.delim = delim;
  params.marker = rgw_obj_key(marker);
  params.ns = ns;
  params.enforce_ns = true;
  params.list_versions = true;

  f->open_array_section("check_objects");
  do {
    ret = bucket->list(dpp, params, max_entries - count, results, null_yield);
    if (ret < 0) {
      cerr << "ERROR: driver->list_objects(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    count += results.objs.size();

    for (vector<rgw_bucket_dir_entry>::iterator iter = results.objs.begin(); iter != results.objs.end(); ++iter) {
      std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(iter->key);

      if (obj->get_name()[0] == '_') {
        ret = check_obj_locator_underscore(dpp, driver, obj.get(), fix, remove_bad, f);

	if (ret >= 0) {
          ret = check_obj_tail_locator_underscore(dpp, driver, bucket->get_info(), obj->get_key(), fix, f);
          if (ret < 0) {
              cerr << "ERROR: check_obj_tail_locator_underscore(): " << cpp_strerror(-ret) << std::endl;
              return -ret;
          }
	}
      }
    }
    f->flush(cout);
  } while (results.is_truncated && count < max_entries);
  f->close_section();
  f->close_section();

  f->flush(cout);

  return 0;
}
#endif // WITH_RADOSGW_RADOS

} // anonymous namespace

int rgw_admin_init_bucket(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          const rgw_bucket& b,
                          std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  return rgw_admin_init_bucket_impl(dpp, driver, b, bucket);
}

int rgw_admin_init_bucket(const DoutPrefixProvider* dpp,
                          rgw::sal::Driver* driver,
                          const std::string& tenant_name,
                          const std::string& bucket_name,
                          const std::string& bucket_id,
                          std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  return rgw_admin_init_bucket_impl(dpp, driver, tenant_name, bucket_name, bucket_id, bucket);
}

int rgw_admin_check_reshard_bucket_params(const DoutPrefixProvider* dpp,
                                            rgw::sal::Driver* driver,
                                            const std::string& bucket_name,
                                            const std::string& tenant,
                                            const std::string& bucket_id,
                                            bool num_shards_specified,
                                            int num_shards,
                                            int yes_i_really_mean_it,
                                            std::unique_ptr<rgw::sal::Bucket>* bucket)
{
  return rgw_admin_check_reshard_bucket_params_impl(dpp, driver, bucket_name, tenant,
      bucket_id, num_shards_specified, num_shards, yes_i_really_mean_it, bucket);
}

int rgw_admin_check_min_obj_stripe_size(const DoutPrefixProvider* dpp,
                                          rgw::sal::Driver* driver,
                                          rgw::sal::Object* obj,
                                          uint64_t min_stripe_size,
                                          bool* need_rewrite)
{
  return rgw_admin_check_min_obj_stripe_size_impl(dpp, driver, obj, min_stripe_size, need_rewrite);
}

int rgw_admin_bucket(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     rgw::SiteConfig& site,
                     Formatter* formatter,
                     RGWFormatterFlusher& stream_flusher,
                     std::unique_ptr<rgw::sal::User>& user,
                     RGWUserAdminOpState& user_op,
                     RGWBucketAdminOpState& bucket_op,
                     std::unique_ptr<rgw::sal::Bucket>& bucket,
                     rgw_admin_bucket_options& opts)
{
  int& ret = *opts.ret;
  int max_entries = opts.max_entries.value_or(1000);
  int max_concurrent_ios = opts.max_concurrent_ios;
  int orphan_stale_secs = opts.orphan_stale_secs;
  int num_shards = opts.num_shards;
  int shard_id = opts.shard_id;
  uint64_t min_rewrite_size = opts.min_rewrite_size;
  uint64_t max_rewrite_size = opts.max_rewrite_size;
  uint64_t min_rewrite_stripe_size = opts.min_rewrite_stripe_size;

  const bool limit_specified = opts.max_entries.has_value();
  bool warnings_only = opts.warnings_only;
  bool allow_unordered = opts.allow_unordered;
  bool show_restore_stats = opts.show_restore_stats;
  bool yes_i_really_mean_it = opts.yes_i_really_mean_it;
  bool bypass_gc = opts.bypass_gc;
  bool inconsistent_index = opts.inconsistent_index;
  bool num_shards_specified = opts.num_shards_specified;
  bool specified_shard_id = opts.specified_shard_id;
  bool fix = opts.fix;
  bool verbose = opts.verbose;
  bool check_head_obj_locator = opts.check_head_obj_locator;
  bool remove_bad = opts.remove_bad;

  rgw_admin::OPT command = opts.command;

  (void)user_op;

  if (command == rgw_admin::OPT::NO_CMD) {
    return EINVAL;
  }

  if (command == rgw_admin::OPT::BUCKET_LIMIT_CHECK) {
    void *handle;
    std::list<std::string> user_ids;
    opts.metadata_key = "user";
    int max = 1000;

    bool truncated;

    if (!rgw::sal::User::empty(user)) {
      user_ids.push_back(user->get_id().id);
      ret =
	RGWBucketAdminOp::limit_check(driver, bucket_op, user_ids, stream_flusher,
				      null_yield, dpp, warnings_only);
    } else {
      /* list users in groups of max-keys, then perform user-bucket
       * limit-check on each group */
     ret = driver->meta_list_keys_init(dpp, opts.metadata_key, string(), &handle);
      if (ret < 0) {
	cerr << "ERROR: buckets limit check can't get user metadata_key: "
	     << cpp_strerror(-ret) << std::endl;
	return -ret;
      }

      do {
	ret = driver->meta_list_keys_next(dpp, handle, max, user_ids,
					      &truncated);
	if (ret < 0 && ret != -ENOENT) {
	  cerr << "ERROR: buckets limit check lists_keys_next(): "
	       << cpp_strerror(-ret) << std::endl;
	  break;
	} else {
	  /* ok, do the limit checks for this group */
	  ret =
	    RGWBucketAdminOp::limit_check(driver, bucket_op, user_ids, stream_flusher,
					  null_yield, dpp, warnings_only);
	  if (ret < 0)
	    break;
	}
	user_ids.clear();
      } while (truncated);
      driver->meta_list_keys_complete(handle);
    }
    return -ret;
  } /* rgw_admin::OPT::BUCKET_LIMIT_CHECK */

  if (command == rgw_admin::OPT::BUCKETS_LIST) {
    if (opts.bucket_name.empty()) {
      if (!rgw::sal::User::empty(user)) {
        if (!user_op.has_existing_user()) {
          cerr << "ERROR: could not find user: " << user << std::endl;
          return -ENOENT;
        }
      }
      bucket_op.marker = opts.marker;
      if (limit_specified)
        bucket_op.max_entries = *opts.max_entries;
      else
        bucket_op.max_entries = 0; /* for backward compatibility */
      RGWBucketAdminOp::info(driver, site, bucket_op, stream_flusher, null_yield, dpp);
    } else {
      int ret = rgw_admin_init_bucket(dpp, driver, opts.tenant, opts.bucket_name, opts.bucket_id, &bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      formatter->open_array_section("entries");

      int count = 0;

      static constexpr int MAX_PAGINATE_SIZE = 10000;
      static constexpr int DEFAULT_MAX_ENTRIES = 1000;

      if (max_entries < 0) {
	max_entries = DEFAULT_MAX_ENTRIES;
      }
      const int paginate_size = std::min(max_entries, MAX_PAGINATE_SIZE);

      string prefix;
      string delim;
      string ns;

      rgw::sal::Bucket::ListParams params;
      rgw::sal::Bucket::ListResults results;

      params.prefix = prefix;
      params.delim = delim;
      // Support pagination for versioned buckets using --opts.marker and --opts.object-version
      // For versioned buckets: use both --opts.marker (name) and --opts.object-version (instance)
      // For non-versioned buckets: use only --opts.marker (name)
      if (!opts.object_version.empty()) {
        params.marker = rgw_obj_key(opts.marker, opts.object_version);
      } else {
        params.marker = rgw_obj_key(opts.marker);
      }
      params.ns = ns;
      params.enforce_ns = false;
      params.list_versions = true;
      params.allow_unordered = bool(allow_unordered);

      do {
        const int remaining = max_entries - count;
	ret = bucket->list(dpp, params, std::min(remaining, paginate_size), results,
			   null_yield);
        if (ret < 0) {
          cerr << "ERROR: driver->list_objects(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }
	ldpp_dout(dpp, 20) << "INFO: " << __func__ <<
	  ": list() returned without error; results.objs.size()=" <<
	  results.objs.size() << ", results.is_truncated=" << results.is_truncated << ", marker=" <<
	  params.marker << dendl;

        count += results.objs.size();

        for (const auto& entry : results.objs) {
          encode_json("entry", entry, formatter);
        }
        formatter->flush(cout);
      } while (results.is_truncated && count < max_entries);
      ldpp_dout(dpp, 20) << "INFO: " << __func__ << ": done" << dendl;

      formatter->close_section();
      formatter->flush(cout);
    } /* have opts.bucket_name */
  } /* rgw_admin::OPT::BUCKETS_LIST */

#ifdef WITH_RADOSGW_RADOS
  if (command == rgw_admin::OPT::BUCKET_RADOS_LIST) {
    RGWRadosList lister(static_cast<rgw::sal::RadosStore*>(driver),
			max_concurrent_ios, orphan_stale_secs, opts.tenant);
    if (opts.rgw_obj_fs) {
      lister.set_field_separator(*opts.rgw_obj_fs);
    }

    if (opts.bucket_name.empty()) {
      // yes_i_really_mean_it means continue with listing even if
      // there are indexless buckets
      ret = lister.run(dpp, yes_i_really_mean_it);
    } else {
      ret = lister.run(dpp, opts.bucket_name);
    }

    if (ret < 0) {
      std::cerr <<
	"ERROR: bucket radoslist failed to finish before " <<
	"encountering error: " << cpp_strerror(-ret) << std::endl;
      std::cerr << "************************************"
	"************************************" << std::endl;
      std::cerr << "WARNING: THE RESULTS ARE NOT RELIABLE AND SHOULD NOT " <<
	"BE USED IN DELETING ORPHANS" << std::endl;
      std::cerr << "************************************"
	"************************************" << std::endl;
      return -ret;
    }
  }
#endif

  if (command == rgw_admin::OPT::BUCKET_LAYOUT) {
    if (opts.bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, opts.tenant, opts.bucket_name, opts.bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    const auto& bucket_info = bucket->get_info();
    formatter->open_object_section("layout");
    encode_json("layout", bucket_info.layout, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == rgw_admin::OPT::BUCKET_STATS) {
    if (opts.bucket_name.empty() && !opts.bucket_id.empty()) {
      rgw_bucket bucket_key;
      if (!rgw_find_bucket_by_id(dpp, driver->ctx(), driver, opts.marker, opts.bucket_id, &bucket_key)) {
        cerr << "failure: no such bucket id" << std::endl;
        return -ENOENT;
      }
      bucket_op.set_tenant(bucket_key.tenant);
      bucket_op.set_bucket_name(bucket_key.name);
    }
    bucket_op.set_fetch_stats(true);
      if (limit_specified)
      bucket_op.max_entries = *opts.max_entries;
    else
      bucket_op.max_entries = 0; /* for backward compatibility */
    bucket_op.set_restore_stats(bool(show_restore_stats));

    int r = RGWBucketAdminOp::info(driver, site, bucket_op, stream_flusher, null_yield, dpp);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << opts.err << std::endl;
      return posix_errortrans(-r);
    }
  }

#ifdef WITH_RADOSGW_RADOS
  if (command == rgw_admin::OPT::BUCKET_LINK) {
    bucket_op.set_bucket_id(opts.bucket_id);
    bucket_op.set_new_bucket_name(opts.new_bucket_name);
    string link_err;
    int r = RGWBucketAdminOp::link(driver, bucket_op, dpp, null_yield, &link_err);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << link_err << std::endl;
      return -r;
    }
  }

  if (command == rgw_admin::OPT::BUCKET_UNLINK) {
    int r = RGWBucketAdminOp::unlink(driver, bucket_op, dpp, null_yield);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << std::endl;
      return -r;
    }
  }
#endif

  if (command == rgw_admin::OPT::BUCKET_SHARD_OBJECTS) {
    const auto prefix = opts.opt_prefix ? *opts.opt_prefix : "obj"s;
    if (!num_shards_specified) {
      cerr << "ERROR: num-shards must be specified."
	   << std::endl;
      return EINVAL;
    }

    if (specified_shard_id) {
      if (shard_id >= num_shards) {
	cerr << "ERROR: shard-id must be less than num-shards."
	     << std::endl;
	return EINVAL;
      }
      std::string obj;
      uint64_t ctr = 0;
      int shard;
      do {
	obj = fmt::format("{}{:0>20}", prefix, ctr);
	shard = RGWSI_BucketIndex_RADOS::bucket_shard_index(obj, num_shards);
	++ctr;
      } while (shard != shard_id);

      formatter->open_object_section("shard_obj");
      encode_json("obj", obj, formatter);
      formatter->close_section();
      formatter->flush(cout);
    } else {
      std::vector<std::string> objs(num_shards);
      for (uint64_t ctr = 0, shardsleft = num_shards; shardsleft > 0; ++ctr) {
	auto key = fmt::format("{}{:0>20}", prefix, ctr);
	auto shard = RGWSI_BucketIndex_RADOS::bucket_shard_index(key, num_shards);
	if (objs[shard].empty()) {
	  objs[shard] = std::move(key);
	  --shardsleft;
	}
      }

      formatter->open_object_section("shard_objs");
      encode_json("objs", objs, formatter);
      formatter->close_section();
      formatter->flush(cout);
    }
  }

  if (command == rgw_admin::OPT::BUCKET_OBJECT_SHARD) {
    if (!num_shards_specified || opts.object.empty()) {
      cerr << "ERROR: num-shards and object must be specified."
	   << std::endl;
      return EINVAL;
    } else if (num_shards <= 0) {
      cerr << "ERROR: non-positive value supplied for num-shards: " <<
	num_shards << std::endl;
      return EINVAL;
    }
    auto shard =
      RGWSI_BucketIndex_RADOS::bucket_shard_index(opts.object, num_shards);
    formatter->open_object_section("obj_shard");
    encode_json("shard", shard, formatter);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == rgw_admin::OPT::BUCKET_CHOWN) {
    if (opts.bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }

    bucket_op.account_id = opts.account_id;
    bucket_op.set_bucket_name(opts.bucket_name);
    bucket_op.set_new_bucket_name(opts.new_bucket_name);
    string chown_err;

    int r = RGWBucketAdminOp::chown(driver, bucket_op, opts.marker, dpp, null_yield, &chown_err);
    if (r < 0) {
      cerr << "failure: " << cpp_strerror(-r) << ": " << chown_err << std::endl;
      return -r;
    }
  }

#ifdef WITH_RADOSGW_RADOS
  if (command == rgw_admin::OPT::BUCKET_REWRITE) {
    if (opts.bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    int ret = rgw_admin_init_bucket(dpp, driver, opts.tenant, opts.bucket_name, opts.bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    uint64_t start_epoch = 0;
    uint64_t end_epoch = 0;

    if (!opts.end_date.empty()) {
      int ret = utime_t::parse_date(opts.end_date, &end_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse end date" << std::endl;
        return EINVAL;
      }
    }
    if (!opts.start_date.empty()) {
      int ret = utime_t::parse_date(opts.start_date, &start_epoch, NULL);
      if (ret < 0) {
        cerr << "ERROR: failed to parse start date" << std::endl;
        return EINVAL;
      }
    }

    bool is_truncated = true;
    bool cls_filtered = true;

    rgw_obj_index_key list_marker;
    string empty_prefix;
    string empty_delimiter;

    formatter->open_object_section("result");
    formatter->dump_string("bucket", opts.bucket_name);
    formatter->open_array_section("objects");

    constexpr uint32_t NUM_ENTRIES = 1000;
    uint16_t expansion_factor = 1;
    while (is_truncated) {
      RGWRados::ent_map_t result;
      result.reserve(NUM_ENTRIES);

      const auto& current_index = bucket->get_info().layout.current_index;
      int r = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->cls_bucket_list_ordered(
	dpp, bucket->get_info(), current_index, RGW_NO_SHARD,
	list_marker, empty_prefix, empty_delimiter,
	NUM_ENTRIES, true, expansion_factor,
	result, &is_truncated, &cls_filtered, &list_marker,
	null_yield,
	rgw_bucket_object_check_filter);
      if (r < 0 && r != -ENOENT) {
        cerr << "ERROR: failed operation r=" << r << std::endl;
      } else if (r == -ENOENT) {
        break;
      }

      if (result.size() < NUM_ENTRIES / 8) {
	++expansion_factor;
      } else if (result.size() > NUM_ENTRIES * 7 / 8 &&
		 expansion_factor > 1) {
	--expansion_factor;
      }

      for (auto iter = result.begin(); iter != result.end(); ++iter) {
        rgw_obj_key key = iter->second.key;
        rgw_bucket_dir_entry& entry = iter->second;

        formatter->open_object_section("object");
        formatter->dump_string("name", key.name);
        formatter->dump_string("instance", key.instance);
        formatter->dump_int("size", entry.meta.size);
        utime_t ut(entry.meta.mtime);
        ut.gmtime(formatter->dump_stream("mtime"));

        if ((entry.meta.size < min_rewrite_size) ||
            (entry.meta.size > max_rewrite_size) ||
            (start_epoch > 0 && start_epoch > (uint64_t)ut.sec()) ||
            (end_epoch > 0 && end_epoch < (uint64_t)ut.sec())) {
          formatter->dump_string("status", "Skipped");
        } else {
	  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);

          bool need_rewrite = true;
          if (min_rewrite_stripe_size > 0) {
            r = rgw_admin_check_min_obj_stripe_size(dpp, driver, obj.get(), min_rewrite_stripe_size, &need_rewrite);
            if (r < 0) {
              ldpp_dout(dpp, 0) << "WARNING: check_min_obj_stripe_size failed, r=" << r << dendl;
            }
          }
          if (!need_rewrite) {
            formatter->dump_string("status", "Skipped");
          } else {
            RGWRados* store = static_cast<rgw::sal::RadosStore*>(driver)->getRados();
            r = store->rewrite_obj(bucket->get_info(), obj->get_obj(), dpp, null_yield);
            if (r == 0) {
              formatter->dump_string("status", "Success");
            } else {
              formatter->dump_string("status", cpp_strerror(-r));
            }
          }
        }
        formatter->dump_int("flags", entry.flags);

        formatter->close_section();
        formatter->flush(cout);
      }
    }
    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == rgw_admin::OPT::BUCKET_RESHARD) {
    int ret = rgw_admin_check_reshard_bucket_params_impl(dpp, driver,
					  opts.bucket_name,
					  opts.tenant,
					  opts.bucket_id,
					  num_shards_specified,
					  num_shards,
					  yes_i_really_mean_it,
					  &bucket);
    if (ret < 0) {
      return ret;
    }

    auto zone_svc = static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone;
    if (!zone_svc->can_reshard()) {
      const auto& zonegroup = zone_svc->get_zonegroup();
      std::cerr << "The zonegroup '" << zonegroup.get_name() << "' does not "
          "have the resharding feature enabled." << std::endl;
      return ENOTSUP;
    }

    if (!RGWBucketReshard::should_zone_reshard_now(bucket->get_info(), zone_svc) &&
        !yes_i_really_mean_it) {
      std::cerr << "Bucket '" << bucket->get_name() << "' already has too many "
          "log generations (" << bucket->get_info().layout.logs.size() << ") "
          "from previous reshards that peer zones haven't finished syncing. "
          "Resharding is not recommended until the old generations sync, but "
          "you can force a reshard with --yes-i-really-mean-it." << std::endl;
      return EINVAL;
    }

    RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(driver),
			bucket->get_info(), bucket->get_attrs(),
			nullptr /* no callback */);

#define DEFAULT_RESHARD_MAX_ENTRIES 1000
    if (max_entries < 1) {
      max_entries = DEFAULT_RESHARD_MAX_ENTRIES;
    }

    ReshardFaultInjector fault;
    if (opts.inject_error_at) {
      const int code = -(opts.inject_error_code.has_value() ?
                         *opts.inject_error_code : EIO);
      fault.inject(*opts.inject_error_at, InjectError{code, dpp});
    } else if (opts.inject_abort_at) {
      fault.inject(*opts.inject_abort_at, InjectAbort{});
    } else if (opts.inject_delay_at) {
      fault.inject(*opts.inject_delay_at, InjectDelay{opts.inject_delay, dpp});
    }
    ret = br.execute(num_shards, fault, max_entries,
		     cls_rgw_reshard_initiator::Admin,
		     dpp, null_yield,
                     verbose, &cout, formatter);
    return -ret;
  }
#endif

  if (command == rgw_admin::OPT::BUCKET_SET_MIN_SHARDS) {
    if (opts.bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return -EINVAL;
    }

    if (!num_shards_specified) {
      cerr << "ERROR: --num-shards not specified" << std::endl;
      return -EINVAL;
    }

    if (num_shards < 1) {
      cerr << "ERROR: --num-shards must be at least 1" << std::endl;
      return -EINVAL;
    }

    int ret = rgw_admin_init_bucket(dpp, driver, opts.tenant, opts.bucket_name, opts.bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    auto& bucket_info = bucket->get_info();

    const rgw::BucketIndexType type =
      bucket_info.layout.current_index.layout.type;
    if (type != rgw::BucketIndexType::Normal) {
      cerr << "ERROR: the bucket's layout is type " << type <<
	" instead of type " << rgw::BucketIndexType::Normal <<
	" and therefore does not have a "
	"minimum number of shards that can be altered" << std::endl;
      return EINVAL;
    }

    uint32_t& min_num_shards =
      bucket_info.layout.current_index.layout.normal.min_num_shards;
    min_num_shards = num_shards;

    ret = bucket->put_info(dpp, false, real_time(), null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed writing bucket instance info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    return 0;
  } // SET_MIN_SHARDS

#ifdef WITH_RADOSGW_RADOS
  if (command == rgw_admin::OPT::BUCKET_CHECK) {
    if (check_head_obj_locator) {
      if (opts.bucket_name.empty()) {
        cerr << "ERROR: need to specify bucket name" << std::endl;
        return EINVAL;
      }
      do_check_object_locator(dpp, driver, opts.tenant, opts.bucket_name, fix, remove_bad, formatter);
    } else {
      RGWBucketAdminOp::check_index(driver, bucket_op, stream_flusher, null_yield, dpp);
    }
  }

  if (command == rgw_admin::OPT::BUCKET_CHECK_OLH) {
    rgw::sal::RadosStore* store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      cerr <<
	      "WARNING: this command is only relevant when the cluster has a RADOS backing store." <<
	      std::endl;
      return 0;
    }
    RGWBucketAdminOp::check_index_olh(store, bucket_op, stream_flusher, dpp);
  }

  if (command == rgw_admin::OPT::BUCKET_CHECK_UNLINKED) {
    rgw::sal::RadosStore* store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      cerr <<
	      "WARNING: this command is only relevant when the cluster has a RADOS backing store." <<
	      std::endl;
      return 0;
    }
    RGWBucketAdminOp::check_index_unlinked(store, bucket_op, stream_flusher, dpp);
  }
#endif

  if (command == rgw_admin::OPT::BUCKET_RM) {
    if (!inconsistent_index) {
      RGWBucketAdminOp::remove_bucket(driver, site, bucket_op, null_yield, dpp, bypass_gc, true, false);
    } else {
      if (!yes_i_really_mean_it) {
	cerr << "using --inconsistent_index can corrupt the bucket index " << std::endl
	<< "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
	return 1;
      }
      RGWBucketAdminOp::remove_bucket(driver, site, bucket_op, null_yield, dpp, bypass_gc, false, false);
    }
  }

  if ((command == rgw_admin::OPT::BUCKET_SUSPEND) || (command == rgw_admin::OPT::BUCKET_UNSUSPEND)) {
    if (opts.bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    ret = rgw_admin_init_bucket(dpp, driver, opts.tenant, opts.bucket_name, opts.bucket_id, &bucket);
    if (ret < 0) {
      return -ret;
    }
    std::vector<rgw_bucket> buckets;
    buckets.push_back(bucket->get_key());
    const bool enabled = (command == rgw_admin::OPT::BUCKET_UNSUSPEND);
    ret = driver->set_buckets_enabled(dpp, buckets, enabled, null_yield);
    if (ret < 0) {
      cerr << "failed to " << (enabled ? "unsuspend" : "suspend")
           << " bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == rgw_admin::OPT::POLICY) {
    if (opts.format == "xml") {
      ret = RGWBucketAdminOp::dump_s3_policy(driver, bucket_op, std::cout, dpp, null_yield);
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    } else {
      ret = RGWBucketAdminOp::get_policy(driver, bucket_op, stream_flusher, dpp, null_yield);
      if (ret < 0) {
        cerr << "ERROR: failed to get policy: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }

  return 0;
}
