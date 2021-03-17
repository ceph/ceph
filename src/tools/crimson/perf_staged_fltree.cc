// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include "crimson/common/log.h"
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

  seastar::future<> run(KVPool<test_item_t>& kvs) {
    return tm_setup().then([this, &kvs] {
      return seastar::async([this, &kvs] {
        auto tree = std::make_unique<TreeBuilder<TRACK, test_item_t>>(kvs,
            (is_dummy ? NodeExtentManager::create_dummy(true)
                      : NodeExtentManager::create_seastore(*tm)));
        {
          auto t = tm->create_transaction();
          tree->bootstrap(*t).unsafe_get();
          tm->submit_transaction(std::move(t)).unsafe_get();
        }
        {
          auto t = tm->create_transaction();
          tree->insert(*t).unsafe_get();
          auto start_time = mono_clock::now();
          tm->submit_transaction(std::move(t)).unsafe_get();
          std::chrono::duration<double> duration = mono_clock::now() - start_time;
          logger().warn("submit_transaction() done! {}s", duration.count());
        }
        {
          auto t = tm->create_transaction();
          tree->get_stats(*t).unsafe_get();
          tm->submit_transaction(std::move(t)).unsafe_get();
        }
        {
          // Note: tm->create_weak_transaction() can also work, but too slow.
          auto t = tm->create_transaction();
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
    auto str_sizes = config["str-sizes"].as<std::vector<size_t>>();
    auto onode_sizes = config["onode-sizes"].as<std::vector<size_t>>();
    auto range2 = config["range2"].as<std::vector<int>>();
    ceph_assert(range2.size() == 2);
    auto range1 = config["range1"].as<std::vector<unsigned>>();
    ceph_assert(range1.size() == 2);
    auto range0 = config["range0"].as<std::vector<unsigned>>();
    ceph_assert(range0.size() == 2);

    auto kvs = KVPool<test_item_t>::create_raw_range(
        str_sizes, onode_sizes,
        {range2[0], range2[1]},
        {range1[0], range1[1]},
        {range0[0], range0[1]});
    PerfTree<TRACK> perf{is_dummy};
    perf.run(kvs).get0();
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
