// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/program_options.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>

#include "crimson/os/seastore/onode_manager/staged-fltree/tree_utils.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

using namespace crimson::os::seastore::onode;
namespace bpo = boost::program_options;

template <bool TRACK>
class PerfTree : public TMTestState {
 public:
  PerfTree(bool is_dummy) : is_dummy{is_dummy} {}

  seastar::future<> run(KVPool& kvs) {
    return start(kvs).then([this] {
      return tree->run().handle_error(
        crimson::ct_error::all_same_way([] {
          ceph_abort("runtime error");
        })
      );
    }).then([this] {
      return stop();
    });
  }

 private:
  seastar::future<> start(KVPool& kvs) {
    if (is_dummy) {
      tree = std::make_unique<TreeBuilder<TRACK>>(
          kvs,  NodeExtentManager::create_dummy(true));
      return tree->bootstrap().handle_error(
        crimson::ct_error::all_same_way([] {
          ceph_abort("Unable to mkfs");
        })
      );
    } else {
      return tm_setup().then([this, &kvs] {
        tree = std::make_unique<TreeBuilder<TRACK>>(
            kvs,  NodeExtentManager::create_seastore(*tm));
        return tree->bootstrap();
      }).handle_error(
        crimson::ct_error::all_same_way([] {
          ceph_abort("Unable to mkfs");
        })
      );
    }
  }

  seastar::future<> stop() {
    tree.reset();
    if (is_dummy) {
      return seastar::now();
    } else {
      return tm_teardown();
    }
  }

  bool is_dummy;
  std::unique_ptr<TreeBuilder<TRACK>> tree;
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

    KVPool kvs{str_sizes, onode_sizes,
               {range2[0], range2[1]},
               {range1[0], range1[1]},
               {range0[0], range0[1]}};
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
