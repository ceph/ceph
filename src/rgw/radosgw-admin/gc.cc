// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/gc.h"
#include <iostream>
#include "common/ceph_json.h"
#include "driver/rados/rgw_rados.h"
#include "driver/rados/rgw_sal_rados.h"


using namespace rgw_admin;
using namespace std;

namespace {



} // anonymous namespace

int rgw_admin_gc(const DoutPrefixProvider* dpp,
                 rgw::sal::Driver* driver,
                 ceph::Formatter* formatter,
                 const rgw_admin_gc_options& opts)
{
  auto& command = opts.command;
  auto& marker = *opts.marker;
  int shard_id = opts.shard_id;
  bool specified_shard_id = opts.specified_shard_id;
  bool include_all = opts.include_all;
  int ret = 0;

  if (command == OPT::GC_LIST) {
    if (specified_shard_id) {
      int max_gc_shards = min(static_cast<int>(driver->ctx()->_conf->rgw_gc_max_objs), rgw_shards_max());
      if (shard_id < 0 || shard_id >= max_gc_shards) {
        cerr << "ERROR: shard-id must be in the range [0, " << max_gc_shards - 1 << "]" << std::endl;
        return EINVAL;
      }
    }

    int index = 0;
    bool truncated;
    bool processing_queue = false;
    formatter->open_array_section("entries");

    std::optional<int> gc_shard_id = specified_shard_id ? std::optional<int>(shard_id) : std::nullopt;

    do {
      list<cls_rgw_gc_obj_info> result;
      int ret = static_cast<rgw::sal::RadosStore*>(driver)->getRados()->list_gc_objs(index, marker, 1000, !include_all, result, truncated, processing_queue, gc_shard_id);
      if (ret < 0) {
	cerr << "ERROR: failed to list objs: " << cpp_strerror(-ret) << std::endl;
	return 1;
      }


      list<cls_rgw_gc_obj_info>::iterator iter;
      for (iter = result.begin(); iter != result.end(); ++iter) {
	cls_rgw_gc_obj_info& info = *iter;
	formatter->open_object_section("chain_info");
	formatter->dump_string("tag", info.tag);
	formatter->dump_stream("time") << info.time;
	formatter->open_array_section("objs");
	for (const auto& obj : info.chain.objs) {
          encode_json("obj", obj, formatter);
	}
	formatter->close_section(); // objs
	formatter->close_section(); // obj_chain
	formatter->flush(cout);
      }
    } while (truncated);
    formatter->close_section();
    formatter->flush(cout);
  }

  if (command == OPT::GC_PROCESS) {
    if (specified_shard_id) {
      int max_gc_shards = min(static_cast<int>(driver->ctx()->_conf->rgw_gc_max_objs), rgw_shards_max());
      if (shard_id < 0 || shard_id >= max_gc_shards) {
        cerr << "ERROR: shard-id must be in the range [0, " << max_gc_shards - 1 << "]" << std::endl;
        return EINVAL;
      }
    }

    rgw::sal::RadosStore* rados_store = dynamic_cast<rgw::sal::RadosStore*>(driver);
    if (!rados_store) {
      cerr <<
	"WARNING: this command can only work when the cluster has a RADOS backing store." <<
	std::endl;
      return 0;
    }
    RGWRados* store = rados_store->getRados();

    std::optional<int> gc_shard_id = specified_shard_id ? std::optional<int>(shard_id) : std::nullopt;
    int ret = store->process_gc(!include_all, null_yield, gc_shard_id);
    if (ret < 0) {
      cerr << "ERROR: gc processing returned error: " << cpp_strerror(-ret) << std::endl;
      return 1;
    }
  }

  return 0;
}

