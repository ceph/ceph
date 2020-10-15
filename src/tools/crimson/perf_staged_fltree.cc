// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include "crimson/os/seastore/onode_manager/staged-fltree/tree_utils.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction_manager.h"

using namespace crimson::os::seastore::onode;
namespace bpo = boost::program_options;
namespace seastore = crimson::os::seastore;
using seastore::lba_manager::create_lba_manager;
using seastore::segment_manager::create_ephemeral;
using seastore::segment_manager::DEFAULT_TEST_EPHEMERAL;
using sc_config_t = seastore::SegmentCleaner::config_t;

template <bool TRACK>
class PerfTree {
 public:
  PerfTree(bool is_dummy,
           const std::vector<size_t>& str_sizes,
           const std::vector<size_t>& onode_sizes,
           const std::pair<int, int>& range2,
           const std::pair<unsigned, unsigned>& range1,
           const std::pair<unsigned, unsigned>& range0)
    : is_dummy{is_dummy},
      segment_manager(create_ephemeral(DEFAULT_TEST_EPHEMERAL)),
      segment_cleaner(sc_config_t::default_from_segment_manager(*segment_manager)),
      journal(*segment_manager),
      cache(*segment_manager),
      lba_manager(create_lba_manager(*segment_manager, cache)),
      tm(*segment_manager, segment_cleaner, journal, cache, *lba_manager),
      kvs(str_sizes, onode_sizes, range2, range1, range0),
      tree{kvs, is_dummy ? NodeExtentManager::create_dummy(true)
                         : NodeExtentManager::create_seastore(tm)} {
    journal.set_segment_provider(&segment_cleaner);
    segment_cleaner.set_extent_callback(&tm);
  }

  seastar::future<> run() {
    return start().then([this] {
      return tree.run().handle_error(
        crimson::ct_error::all_same_way([] {
          ceph_abort("runtime error");
        })
      );
    }).then([this] {
      return stop();
    });
  }

 private:
  seastar::future<> start() {
    if (is_dummy) {
      return tree.bootstrap().handle_error(
        crimson::ct_error::all_same_way([] {
          ceph_abort("Unable to mkfs");
        })
      );
    } else {
      return segment_manager->init().safe_then([this] {
        return tm.mkfs();
      }).safe_then([this] {
        return tm.mount();
      }).safe_then([this] {
        return tree.bootstrap();
      }).handle_error(
        crimson::ct_error::all_same_way([] {
          ceph_abort("Unable to mkfs");
        })
      );
    }
  }

  seastar::future<> stop() {
    if (is_dummy) {
      return seastar::now();
    } else {
      return tm.close().handle_error(
        crimson::ct_error::all_same_way([] {
          ceph_abort("Unable to close");
        })
      );
    }
  }

  bool is_dummy;
  std::unique_ptr<seastore::SegmentManager> segment_manager;
  seastore::SegmentCleaner segment_cleaner;
  seastore::Journal journal;
  seastore::Cache cache;
  seastore::LBAManagerRef lba_manager;
  seastore::TransactionManager tm;
  KVPool kvs;
  TreeBuilder<TRACK> tree;
};

template <bool TRACK>
seastar::future<> run(const bpo::variables_map& config) {
  return seastar::async([&config]{
    auto backend = config["backend"].as<std::string>();
    bool is_dummy;
    if (backend == "dummy") {
      is_dummy = true;
    } else if (backend == "seastore") {
      is_dummy = false;
    } else {
      ceph_abort(false && "invalid backend");
    }
    auto str_sizes = config["str-sizes"].as<std::vector<size_t>>();
    auto onode_sizes = config["onode-sizes"].as<std::vector<size_t>>();
    auto range2 = config["range2"].as<std::vector<int>>();
    ceph_assert(range2.size() == 2);
    auto range1 = config["range1"].as<std::vector<unsigned>>();
    ceph_assert(range1.size() == 2);
    auto range0 = config["range0"].as<std::vector<unsigned>>();
    ceph_assert(range0.size() == 2);

    PerfTree<TRACK> perf{is_dummy, str_sizes, onode_sizes,
      {range2[0], range2[1]}, {range1[0], range1[1]}, {range0[0], range0[1]}};
    perf.run().get0();
  });
}


int main(int argc, char** argv)
{
  seastar::app_template app;
  app.add_options()
    ("backend", bpo::value<std::string>()->default_value("dummy"),
     "tree backend: dummy, seastore")
    ("tracked", bpo::value<bool>()->default_value(false),
     "track inserted cursors")
    ("str-sizes", bpo::value<std::vector<size_t>>()->default_value(
        {8, 11, 64, 256, 301, 320}),
     "sizes of ns/oid strings")
    ("onode-sizes", bpo::value<std::vector<size_t>>()->default_value(
        {8, 16, 128, 512, 576, 640}),
     "sizes of onode")
    ("range2", bpo::value<std::vector<int>>()->default_value(
        {0, 128}),
     "range of shard-pool-crush [a, b)")
    ("range1", bpo::value<std::vector<unsigned>>()->default_value(
        {0, 10}),
     "range of ns-oid strings [a, b)")
    ("range0", bpo::value<std::vector<unsigned>>()->default_value(
        {0, 4}),
     "range of snap-gen [a, b)");
  return app.run(argc, argv, [&app] {
    auto&& config = app.configuration();
    auto tracked = config["tracked"].as<bool>();
    if (tracked) {
      return run<true>(config);
    } else {
      return run<false>(config);
    }
  });
}
