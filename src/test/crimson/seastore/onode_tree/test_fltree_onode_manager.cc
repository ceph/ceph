// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include <iostream>
#include <sstream>

#include "test/crimson/gtest_seastar.h"

#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/futurized_collection.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::onode;
using CTransaction = ceph::os::Transaction;
using namespace std;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

ghobject_t make_oid(unsigned i) {
  stringstream ss;
  ss << "object_" << i;
  auto ret = ghobject_t(
    hobject_t(
      sobject_t(ss.str(), CEPH_NOSNAP)));
  ret.hobj.nspace = "asdf";
  return ret;
}

struct fltree_onode_manager_test_t :
  public seastar_test_suite_t,
  TMTestState {
  FLTreeOnodeManagerRef manager;

  seastar::future<> set_up_fut() final {
    return tm_setup();
  }

  seastar::future<> tear_down_fut() final {
    return tm_teardown();
  }


  virtual void _init() final {
    TMTestState::_init();
    manager.reset(new FLTreeOnodeManager(*tm));
  }

  virtual void _destroy() final {
    manager.reset();
    TMTestState::_destroy();
  }

  virtual seastar::future<> _mkfs() final {
    return TMTestState::_mkfs(
    ).then([this] {
      return seastar::do_with(
	tm->create_transaction(),
	[this](auto &t) {
	  return manager->mkfs(*t
	  ).safe_then([this, &t] {
	    return tm->submit_transaction(std::move(t));
	  }).handle_error(
	    crimson::ct_error::assert_all{
	      "Invalid error in _mkfs"
	    }
	  );
	});
    });
  }

  template <typename F>
  void with_onodes_write(
    std::vector<ghobject_t> oids,
    F &&f) {
    auto t = tm->create_transaction();
    auto onodes = manager->get_or_create_onodes(
      *t,
      oids).unsafe_get0();
    std::invoke(f, *t, onodes);
    manager->write_dirty(
      *t,
      onodes
    ).unsafe_get0();
    tm->submit_transaction(std::move(t)).unsafe_get0();
  }

  template <typename F>
  void with_onodes_write_range(
    unsigned start,
    unsigned end,
    unsigned stride,
    F &&f) {
    std::vector<ghobject_t> oids;
    for (unsigned i = start; i < end; i += stride) {
      oids.emplace_back(make_oid(i));
    }
    with_onodes_write(std::move(oids), std::forward<F>(f));
  }

  template <typename F>
  void with_onode_write(unsigned i, F &&f) {
    with_onodes_write_range(
      i,
      i + 1,
      1, [this, f=std::forward<F>(f)](auto &t, auto &onodes) {
	assert(onodes.size() == 1);
	std::invoke(f, t, *onodes[0]);
      });
  }

  fltree_onode_manager_test_t() {}
};

TEST_F(fltree_onode_manager_test_t, touch)
{
  run_async([this] {
    with_onode_write(0, [](auto &t, auto &onode) {
      onode.get_mutable_layout(t);
    });
    /* Disabled pending fix
    with_onode_write(0, [](auto &t, auto &onode) {
      onode.get_mutable_layout(t);
    });
    */
  });
}
