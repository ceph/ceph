// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include "crimson/common/config_proxy.h"
#include "crimson/common/log.h"
#include "crimson/common/perf_counters_collection.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree_utils.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"

#include "test/crimson/seastore/onode_tree/test_value.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

using namespace crimson::os::seastore::onode;
namespace bpo = boost::program_options;

seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_test);
}

template <bool TRACK>
class PerfTree : public TMTestState {
 public:
  PerfTree(bool is_dummy) : is_dummy{is_dummy} {}

  seastar::future<> run(KVPool<test_item_t>& kvs, double erase_ratio) {
    return tm_setup().then([this, &kvs, erase_ratio] {
      return seastar::async([this, &kvs, erase_ratio] {
        auto tree = std::make_unique<TreeBuilder<TRACK, ExtendedValue>>(kvs,
            (is_dummy ? NodeExtentManager::create_dummy(true)
                      : NodeExtentManager::create_seastore(*tm)));
        {
          auto t = tm->create_transaction();
          tree->bootstrap(*t).unsafe_get();
          submit_transaction(std::move(t));
        }
        {
          auto t = tm->create_transaction();
          tree->insert(*t).unsafe_get();
          auto start_time = mono_clock::now();
          submit_transaction(std::move(t));
          std::chrono::duration<double> duration = mono_clock::now() - start_time;
          logger().warn("submit_transaction() done! {}s", duration.count());
        }
        {
          // Note: tm->create_weak_transaction() can also work, but too slow.
          auto t = tm->create_transaction();
          tree->get_stats(*t).unsafe_get();
          tree->validate(*t).unsafe_get();
        }
        {
          auto t = tm->create_transaction();
          tree->erase(*t, kvs.size() * erase_ratio).unsafe_get();
          submit_transaction(std::move(t));
        }
        {
          auto t = tm->create_transaction();
          tree->get_stats(*t).unsafe_get();
          tree->validate(*t).unsafe_get();
        }
        tree.reset();
      });
    }).then([this] {
      return tm_teardown();
    });
  }

 private:
  bool is_dummy;
};

template <bool TRACK>
seastar::future<> run(const bpo::variables_map& config) {
  return seastar::async([&config] {
    auto backend = config["backend"].as<std::string>();
    bool is_dummy;
    if (backend == "dummy") {
      is_dummy = true;
    } else if (backend == "seastore") {
      is_dummy = false;
    } else {
      ceph_abort(false && "invalid backend");
    }
    auto ns_sizes = config["ns-sizes"].as<std::vector<size_t>>();
    auto oid_sizes = config["oid-sizes"].as<std::vector<size_t>>();
    auto onode_sizes = config["onode-sizes"].as<std::vector<size_t>>();
    auto range2 = config["range2"].as<std::vector<int>>();
    ceph_assert(range2.size() == 2);
    auto range1 = config["range1"].as<std::vector<unsigned>>();
    ceph_assert(range1.size() == 2);
    auto range0 = config["range0"].as<std::vector<unsigned>>();
    ceph_assert(range0.size() == 2);
    auto erase_ratio = config["erase-ratio"].as<double>();
    ceph_assert(erase_ratio >= 0);
    ceph_assert(erase_ratio <= 1);

    using crimson::common::sharded_conf;
    sharded_conf().start(EntityName{}, string_view{"ceph"}).get();
    seastar::engine().at_exit([] {
      return sharded_conf().stop();
    });

    using crimson::common::sharded_perf_coll;
    sharded_perf_coll().start().get();
    seastar::engine().at_exit([] {
      return sharded_perf_coll().stop();
    });

    auto kvs = KVPool<test_item_t>::create_raw_range(
        ns_sizes, oid_sizes, onode_sizes,
        {range2[0], range2[1]},
        {range1[0], range1[1]},
        {range0[0], range0[1]});
    PerfTree<TRACK> perf{is_dummy};
    perf.run(kvs, erase_ratio).get0();
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
    ("ns-sizes", bpo::value<std::vector<size_t>>()->default_value(
        {8, 11, 64, 128, 255, 256}),
     "sizes of ns strings")
    ("oid-sizes", bpo::value<std::vector<size_t>>()->default_value(
        {8, 13, 64, 512, 2035, 2048}),
     "sizes of oid strings")
    ("onode-sizes", bpo::value<std::vector<size_t>>()->default_value(
        {8, 16, 128, 576, 992, 1200}),
     "sizes of onode")
    ("range2", bpo::value<std::vector<int>>()->default_value(
        {0, 128}),
     "range of shard-pool-crush [a, b)")
    ("range1", bpo::value<std::vector<unsigned>>()->default_value(
        {0, 10}),
     "range of ns-oid strings [a, b)")
    ("range0", bpo::value<std::vector<unsigned>>()->default_value(
        {0, 4}),
     "range of snap-gen [a, b)")
    ("erase-ratio", bpo::value<double>()->default_value(
        0.8),
     "erase-ratio of all the inserted onodes");
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
