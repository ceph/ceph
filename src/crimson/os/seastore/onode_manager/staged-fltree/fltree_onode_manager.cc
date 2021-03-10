// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

namespace {
[[maybe_unused]] seastar::logger& logger() {
  return crimson::get_logger(ceph_subsys_test);
}
}

namespace crimson::os::seastore::onode {

FLTreeOnodeManager::get_onode_ret FLTreeOnodeManager::get_onode(
  Transaction &trans,
  const ghobject_t &hoid) {
  return tree.find(
    trans, hoid
  ).safe_then([this, &trans, &hoid](auto cursor)
	      -> get_onode_ret {
    if (cursor == tree.end()) {
      logger().debug(
	"FLTreeOnodeManager::{}: no entry for {}",
	__func__,
	hoid);
      return crimson::ct_error::enoent::make();
    }
    auto val = OnodeRef(new FLTreeOnode(cursor.value()));
    return seastar::make_ready_future<OnodeRef>(
      val
    );
  }).handle_error(
    get_onode_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in FLTreeOnodeManager::get_onode"
    }
  );
}

FLTreeOnodeManager::get_or_create_onode_ret
FLTreeOnodeManager::get_or_create_onode(
  Transaction &trans,
  const ghobject_t &hoid) {
  return tree.insert(
    trans, hoid,
    OnodeTree::tree_value_config_t{sizeof(onode_layout_t)}
  ).safe_then([this, &trans, &hoid](auto p)
	      -> get_or_create_onode_ret {
    auto [cursor, created] = std::move(p);
    auto val = OnodeRef(new FLTreeOnode(cursor.value()));
    if (created) {
      logger().debug(
	"FLTreeOnodeManager::{}: created onode for entry for {}",
	__func__,
	hoid);
      val->get_mutable_layout(trans) = onode_layout_t{};
    }
    return seastar::make_ready_future<OnodeRef>(
      val
    );
  }).handle_error(
    get_or_create_onode_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in FLTreeOnodeManager::get_or_create_onode"
    }
  );
}

FLTreeOnodeManager::get_or_create_onodes_ret
FLTreeOnodeManager::get_or_create_onodes(
  Transaction &trans,
  const std::vector<ghobject_t> &hoids) {
  return seastar::do_with(
    std::vector<OnodeRef>(),
    [this, &hoids, &trans](auto &ret) {
      ret.reserve(hoids.size());
      return crimson::do_for_each(
	hoids,
	[this, &trans, &ret](auto &hoid) {
	  return get_or_create_onode(trans, hoid
	  ).safe_then([this, &ret](auto &&onoderef) {
	    ret.push_back(std::move(onoderef));
	  });
	}).safe_then([&ret] {
	  return std::move(ret);
	});
    });
}

FLTreeOnodeManager::write_dirty_ret FLTreeOnodeManager::write_dirty(
  Transaction &trans,
  const std::vector<OnodeRef> &onodes) {
  return crimson::do_for_each(
    onodes,
    [this, &trans](auto &onode) -> OnodeTree::btree_future<> {
      auto &flonode = static_cast<FLTreeOnode&>(*onode);
      switch (flonode.status) {
      case FLTreeOnode::status_t::MUTATED: {
	flonode.populate_recorder(trans);
	return seastar::now();
      }
      case FLTreeOnode::status_t::DELETED: {
	return tree.erase(trans, flonode).safe_then([](auto) {});
      }
      case FLTreeOnode::status_t::STABLE: {
	return seastar::now();
      }
      default:
	__builtin_unreachable();
      }
    }).handle_error(
      write_dirty_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in FLTreeOnodeManager::write_dirty"
      }
    );
}

FLTreeOnodeManager::~FLTreeOnodeManager() {}

}
