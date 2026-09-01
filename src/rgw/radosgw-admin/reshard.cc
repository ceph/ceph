// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/reshard.h"
#include <iostream>
#include "common/ceph_json.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"
#include "driver/rados/rgw_reshard.h"
#include "driver/rados/rgw_bucket.h"
#include "services/svc_zone.h"
#include "radosgw-admin/bucket.h"


using namespace rgw_admin;
using namespace std;

namespace {

static void show_reshard_status(
  const list<cls_rgw_bucket_instance_entry>& status, ceph::Formatter *formatter)
{
  formatter->open_array_section("status");
  for (const auto& entry : status) {
    formatter->open_object_section("entry");
    formatter->dump_string("reshard_status", to_string(entry.reshard_status));
    formatter->close_section();
  }
  formatter->close_section();
  formatter->flush(cout);
}


} // anonymous namespace

int rgw_admin_reshard(const DoutPrefixProvider* dpp,
                      rgw::sal::Driver* driver,
                      ceph::Formatter* formatter,
                      RGWStreamFlusher& stream_flusher,
                      RGWBucketAdminOpState& bucket_op,
                      std::unique_ptr<rgw::sal::Bucket>& bucket,
                      const rgw_admin_reshard_options& opts)
{
  auto& command = opts.command;
  auto& tenant = *opts.tenant;
  auto& bucket_name = *opts.bucket_name;
  auto& bucket_id = *opts.bucket_id;
  auto& marker = *opts.marker;
  int max_entries = opts.max_entries;
  int num_shards = opts.num_shards;
  int shard_id = opts.shard_id;
  bool num_shards_specified = opts.num_shards_specified;
  bool specified_shard_id = opts.specified_shard_id;
  bool yes_i_really_mean_it = opts.yes_i_really_mean_it;
  int ret = 0;

  if (command == OPT::RESHARD_ADD) {
    int ret = rgw_admin_check_reshard_bucket_params(dpp, driver,
					  bucket_name,
					  tenant,
					  bucket_id,
					  num_shards_specified,
					  num_shards,
					  yes_i_really_mean_it,
					  &bucket);
    if (ret < 0) {
      return ret;
    }

    int num_source_shards = rgw::current_num_shards(bucket->get_info().layout);

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(driver), dpp);
    cls_rgw_reshard_entry entry;
    entry.time = real_clock::now();
    entry.tenant = tenant;
    entry.bucket_name = bucket_name;
    entry.bucket_id = bucket->get_info().bucket.bucket_id;
    entry.old_num_shards = num_source_shards;
    entry.new_num_shards = num_shards;
    entry.initiator = cls_rgw_reshard_initiator::Admin;

    return reshard.add(dpp, entry, null_yield);
  }

  if (command == OPT::RESHARD_LIST) {
    int ret;
    int count = 0;
    if (max_entries < 0) {
      max_entries = 1000;
    }

    int num_logshards =
      driver->ctx()->_conf.get_val<uint64_t>("rgw_reshard_num_logs");

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(driver), dpp);

    formatter->open_array_section("reshard");
    for (int i = 0; i < num_logshards; i++) {
      bool is_truncated = true;
      std::string marker;
      do {
	std::list<cls_rgw_reshard_entry> entries;
        ret = reshard.list(dpp, i, marker, max_entries - count, entries, &is_truncated);
        if (ret < 0) {
          cerr << "Error listing resharding buckets: " << cpp_strerror(-ret) << std::endl;
          return ret;
        }
        for (const auto& entry : entries) {
          encode_json("entry", entry, formatter);
        }
	if (is_truncated) {
	  entries.crbegin()->get_key(&marker); // last entry's key becomes marker
	}
        count += entries.size();
        formatter->flush(cout);
      } while (is_truncated && count < max_entries);

      if (count >= max_entries) {
        break;
      }
    }

    formatter->close_section();
    formatter->flush(cout);

    return 0;
  }

  if (command == OPT::RESHARD_STATUS) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(driver),
			bucket->get_info(), bucket->get_attrs(),
			nullptr /* no callback */);
    list<cls_rgw_bucket_instance_entry> status;
    int r = br.get_status(dpp, null_yield, &status);
    if (r < 0) {
      cerr << "ERROR: could not get resharding status for bucket " <<
	bucket_name << std::endl;
      return -r;
    }

    show_reshard_status(status, formatter);
  }

  if (command == OPT::RESHARD_PROCESS) {
    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(driver), true, &cout);

    int ret = reshard.process_all_logshards(dpp, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed to process reshard logs, error=" << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (command == OPT::RESHARD_CANCEL) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    bool bucket_initable = true;
    ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      if (yes_i_really_mean_it) {
        bucket_initable = false;
      } else {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) <<
          "; if you want to cancel the reshard request nonetheless, please "
          "use the --yes-i-really-mean-it option" << std::endl;
        return -ret;
      }
    }

    bool resharding_underway = true;

    if (bucket_initable) {
      // we did not encounter an error, so let's work with the bucket
	RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(driver),
			    bucket->get_info(), bucket->get_attrs(),
			    nullptr /* no callback */);
      int ret = br.cancel(dpp, null_yield);
      if (ret < 0) {
        if (ret == -EBUSY) {
          cerr << "There is ongoing resharding, please retry after " <<
            driver->ctx()->_conf.get_val<uint64_t>("rgw_reshard_bucket_lock_duration") <<
            " seconds." << std::endl;
	  return -ret;
	} else if (ret == -EINVAL) {
	  resharding_underway = false;
	  // we can continue and try to unschedule
        } else {
          cerr << "Error cancelling bucket \"" << bucket_name <<
            "\" resharding: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
        }
      }
    }

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(driver), dpp);

    cls_rgw_reshard_entry entry;
    entry.tenant = tenant;
    entry.bucket_name = bucket_name;

    ret = reshard.remove(dpp, entry, null_yield);
    if (ret == -ENOENT) {
      if (!resharding_underway) {
	cerr << "Error, bucket \"" << bucket_name <<
	  "\" is neither undergoing resharding nor scheduled to undergo "
	  "resharding." << std::endl;
	return EINVAL;
      } else {
	// we cancelled underway resharding above, so we're good
	return 0;
      }
    } else if (ret < 0) {
      cerr << "Error in updating reshard log with bucket \"" <<
        bucket_name << "\": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  } // OPT_RESHARD_CANCEL
 if (command == OPT::RESHARD_STALE_INSTANCES_LIST) {
   if (!static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->can_reshard() && !yes_i_really_mean_it) {
     cerr << "Resharding disabled in a multisite env, stale instances unlikely from resharding" << std::endl;
     cerr << "These instances may not be safe to delete." << std::endl;
     cerr << "Use --yes-i-really-mean-it to force displaying these instances." << std::endl;
     return EINVAL;
   }

   ret = RGWBucketAdminOp::list_stale_instances(driver, bucket_op, stream_flusher, dpp, null_yield);
   if (ret < 0) {
     cerr << "ERROR: listing stale instances" << cpp_strerror(-ret) << std::endl;
   }
 }

 if (command == OPT::RESHARD_STALE_INSTANCES_DELETE) {
   if (!static_cast<rgw::sal::RadosStore*>(driver)->svc()->zone->can_reshard()) {
     cerr << "Resharding disabled in a multisite env. Stale instances are not safe to be deleted." << std::endl;
     return EINVAL;
   }

   ret = RGWBucketAdminOp::clear_stale_instances(driver, bucket_op, stream_flusher, dpp, null_yield);
   if (ret < 0) {
     cerr << "ERROR: deleting stale instances" << cpp_strerror(-ret) << std::endl;
   }
 }

  if (command == OPT::RESHARDLOG_LIST) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    list<rgw_cls_bi_entry> entries;
    bool is_truncated;
    if (max_entries < 0)
      max_entries = 1000;

    const auto& index = bucket->get_info().layout.current_index;
    if (index.layout.type == rgw::BucketIndexType::Indexless) {
      cerr << "ERROR: indexless bucket has no index to purge" << std::endl;
      return EINVAL;
    }

    int max_shards = rgw::num_shards(index);

    formatter->open_array_section("entries");
    int i = (specified_shard_id ? shard_id : 0);
    for (; i < max_shards; i++) {
      formatter->open_object_section("shard");
      encode_json("shard_id", i, formatter);
      formatter->open_array_section("shard_entries");
      RGWRados::BucketShard bs(static_cast<rgw::sal::RadosStore*>(driver)->getRados());
      int ret = bs.init(dpp, bucket->get_info(), index, i, null_yield);
      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << i << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      marker.clear();
      do {
        entries.clear();
        ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->bi_list(bs, "", marker, max_entries,
                                                                              &entries, &is_truncated,
                                                                              true, null_yield);
        if (ret < 0) {
          cerr << "ERROR: bi_list(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        list<rgw_cls_bi_entry>::iterator iter;
        for (iter = entries.begin(); iter != entries.end(); ++iter) {
          rgw_cls_bi_entry& entry = *iter;
          formatter->dump_string("idx", entry.idx);
          marker = entry.idx;
        }
        formatter->flush(cout);
      } while (is_truncated);
      formatter->close_section();
      formatter->close_section();
      formatter->flush(cout);

      if (specified_shard_id)
        break;
    }
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::RESHARDLOG_PURGE) {
    if (bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }
    int ret = rgw_admin_init_bucket(dpp, driver, tenant, bucket_name, bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    ret = static_cast<rgw::sal::RadosStore*>(driver)->svc()->bi_rados->trim_reshard_log(dpp, null_yield, bucket->get_info());
    if (ret < 0) {
      cerr << "ERROR: trim_reshard_log(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  return 0;
}

