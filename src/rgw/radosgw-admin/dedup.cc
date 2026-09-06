// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/dedup.h"
#include <iostream>
#include "driver/rados/rgw_dedup.h"
#include "driver/rados/rgw_sal_rados.h"

using namespace rgw_admin;
using namespace std;


namespace {



} // anonymous namespace

int rgw_admin_dedup(const DoutPrefixProvider* dpp,
                    rgw::sal::Driver* driver,
                    ceph::Formatter* formatter,
                    const rgw_admin_dedup_options& opts)
{
  if (opts.command == OPT::DEDUP_STATS    ||
      opts.command == OPT::DEDUP_ESTIMATE ||
      opts.command == OPT::DEDUP_ABORT    ||
      opts.command == OPT::DEDUP_PAUSE    ||
      opts.command == OPT::DEDUP_RESUME   ||
      opts.command == OPT::DEDUP_THROTTLE ||
      opts.command == OPT::DEDUP_EXEC) {

    using namespace rgw::dedup;
    rgw::sal::RadosStore *store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!store) {
      cerr << "ERROR: this command can only work when the cluster has a RADOS "
	   << "backing store." << std::endl;
      return EPERM;
    }

    if (opts.command == OPT::DEDUP_STATS) {
      int ret = cluster::collect_all_shard_stats(store, formatter, dpp);
      if (ret == 0) {
	formatter->flush(cout);
      }
      else {
	cerr << "ERROR: Failed reading stat counters" << std::endl;
      }
      return -ret;
    }

    if (opts.command == OPT::DEDUP_THROTTLE) {
      bufferlist urgent_msg_bl;
      urgent_msg_t urgent_msg = URGENT_MSG_THROTTLE;
      ceph::encode(urgent_msg, urgent_msg_bl);
      throttle_msg_t throttle_msg;

      if (!opts.throttle_stat) {
        if (unlikely(!opts.have_max_bucket_index_ops && !opts.have_max_metadata_ops)) {
          std::cerr << "dedup throttle must set either --max-bucket-index-ops or --max-metadata-ops" << std::endl;
          return EINVAL;
        }

        if (opts.have_max_bucket_index_ops) {
          throttle_action_t action = { .op_type = BUCKET_INDEX_OP,
                                       .limit = static_cast<uint32_t>(opts.max_bucket_index_ops)};
          throttle_msg.vec.push_back(action);
        }

        if (opts.have_max_metadata_ops) {
          throttle_action_t action = { .op_type = METADATA_ACCESS_OP,
                                       .limit = static_cast<uint32_t>(opts.max_metadata_ops)};
          throttle_msg.vec.push_back(action);
        }
      }

      encode(throttle_msg, urgent_msg_bl);
      int ret = cluster::dedup_control_bl(store, dpp, urgent_msg, urgent_msg_bl,
                                          formatter);
      if (ret == 0) {
        formatter->flush(cout);
      }
      else {
        cerr << "ERROR: Failed throttle command" << std::endl;
      }
      return -ret;
    }

    if (opts.command == OPT::DEDUP_ABORT  ||
	opts.command == OPT::DEDUP_PAUSE  ||
	opts.command == OPT::DEDUP_RESUME) {
      urgent_msg_t urgent_msg;
      if (opts.command == OPT::DEDUP_ABORT) {
	urgent_msg = URGENT_MSG_ABORT;
      }
      else if (opts.command == OPT::DEDUP_PAUSE) {
	urgent_msg = URGENT_MSG_PASUE;
      }
      else {
	urgent_msg = URGENT_MSG_RESUME;
      }
      return -cluster::dedup_control(store, dpp, urgent_msg);
    }

    if (opts.command == OPT::DEDUP_EXEC || opts.command == OPT::DEDUP_ESTIMATE) {
      dedup_req_type_t dedup_type = dedup_req_type_t::DEDUP_TYPE_NONE;
      if (opts.command == OPT::DEDUP_ESTIMATE) {
	dedup_type = dedup_req_type_t::DEDUP_TYPE_ESTIMATE;
      }
      else {
	if (!opts.yes_i_really_mean_it) {
	  cerr << "Full Dedup is dangerous and could lead to data loss!\n"
	       << "do you really mean it? (requires --yes-i-really-mean-it)"
	       << std::endl;
	  return EINVAL;
	}
	dedup_type = dedup_req_type_t::DEDUP_TYPE_EXEC;
#ifndef FULL_DEDUP_SUPPORT
	std::cerr << "Only dedup estimate is supported!" << std::endl;
	return EPERM;
#endif
      }

      // Build the dedup filter from the supplied file paths
      dedup_filter_t dedup_filter(opts.allow_bucket_list_file, opts.deny_bucket_list_file,
				  opts.allow_storage_class_list_file,
				  opts.deny_storage_class_list_file, dpp);
      int filter_err = dedup_filter.errcode();
      if (filter_err != 0) {
	cerr << "ERROR: failed to build dedup filter: "
             << cpp_strerror(-filter_err) << std::endl;
	return -filter_err;
      }

      int ret = cluster::dedup_restart_scan(store, dedup_type, dpp,
					    dedup_filter.is_active() ? &dedup_filter : nullptr);
      // reverse negative errno codes
      ret = -ret;
      if (ret == 0) {
	std::cout << "Dedup was restarted successfully" << std::endl;
      }
      else {
	std::cerr << "Dedup failed to restart" << std::endl;
	std::cerr << "Error is: " << ret << "::" << cpp_strerror(ret) << std::endl;
      }
      return ret;
    }
  }

  return 0;
}

