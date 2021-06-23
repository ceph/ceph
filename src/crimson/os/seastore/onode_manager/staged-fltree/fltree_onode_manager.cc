// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

namespace crimson::os::seastore::onode {

FLTreeOnodeManager::contains_onode_ret FLTreeOnodeManager::contains_onode(
  Transaction &trans,
  const ghobject_t &hoid)
{
  return tree.contains(trans, hoid);
}

FLTreeOnodeManager::get_onode_ret FLTreeOnodeManager::get_onode(
  Transaction &trans,
  const ghobject_t &hoid)
{
  LOG_PREFIX(FLTreeOnodeManager::get_onode);
  return tree.find(
    trans, hoid
  ).safe_then([this, &hoid, &trans, FNAME](auto cursor)
              -> get_onode_ret {
    if (cursor == tree.end()) {
      DEBUGT("no entry for {}", trans, hoid);
      return crimson::ct_error::enoent::make();
    }
    auto val = OnodeRef(new FLTreeOnode(cursor.value()));
    return seastar::make_ready_future<OnodeRef>(
      val
    );
  });
}

FLTreeOnodeManager::get_or_create_onode_ret
FLTreeOnodeManager::get_or_create_onode(
  Transaction &trans,
  const ghobject_t &hoid)
{
  LOG_PREFIX(FLTreeOnodeManager::get_or_create_onode);
  return tree.insert(
    trans, hoid,
    OnodeTree::tree_value_config_t{sizeof(onode_layout_t)}
  ).safe_then([&trans, &hoid, FNAME](auto p)
              -> get_or_create_onode_ret {
    auto [cursor, created] = std::move(p);
    auto val = OnodeRef(new FLTreeOnode(cursor.value()));
    if (created) {
      DEBUGT("created onode for entry for {}", trans, hoid);
      val->get_mutable_layout(trans) = onode_layout_t{};
    }
    return seastar::make_ready_future<OnodeRef>(
      val
    );
  });
}

FLTreeOnodeManager::get_or_create_onodes_ret
FLTreeOnodeManager::get_or_create_onodes(
  Transaction &trans,
  const std::vector<ghobject_t> &hoids)
{
  return seastar::do_with(
    std::vector<OnodeRef>(),
    [this, &hoids, &trans](auto &ret) {
      ret.reserve(hoids.size());
      return crimson::do_for_each(
        hoids,
        [this, &trans, &ret](auto &hoid) {
          return get_or_create_onode(trans, hoid
          ).safe_then([&ret](auto &&onoderef) {
            ret.push_back(std::move(onoderef));
          });
        }).safe_then([&ret] {
          return std::move(ret);
        });
    });
}

FLTreeOnodeManager::write_dirty_ret FLTreeOnodeManager::write_dirty(
  Transaction &trans,
  const std::vector<OnodeRef> &onodes)
{
  return crimson::do_for_each(
    onodes,
    [this, &trans](auto &onode) -> eagain_future<> {
      auto &flonode = static_cast<FLTreeOnode&>(*onode);
      switch (flonode.status) {
      case FLTreeOnode::status_t::MUTATED: {
        flonode.populate_recorder(trans);
        return seastar::now();
      }
      case FLTreeOnode::status_t::DELETED: {
        return tree.erase(trans, flonode);
      }
      case FLTreeOnode::status_t::STABLE: {
        return seastar::now();
      }
      default:
        __builtin_unreachable();
      }
    });
}

FLTreeOnodeManager::erase_onode_ret FLTreeOnodeManager::erase_onode(
  Transaction &trans,
  OnodeRef &onode)
{
  auto &flonode = static_cast<FLTreeOnode&>(*onode);
  flonode.mark_delete();
  return erase_onode_ertr::now();
}

FLTreeOnodeManager::list_onodes_ret FLTreeOnodeManager::list_onodes(
  Transaction &trans,
  const ghobject_t& start,
  const ghobject_t& end,
  uint64_t limit)
{
  return tree.lower_bound(trans, start
  ).safe_then([this, &trans, end, limit] (auto&& cursor) {
    using crimson::os::seastore::onode::full_key_t;
    return seastar::do_with(
        limit,
        std::move(cursor),
        list_onodes_bare_ret(),
        [this, &trans, end] (auto& to_list, auto& current_cursor, auto& ret) {
      return crimson::repeat(
          [this, &trans, end, &to_list, &current_cursor, &ret] () mutable
          -> eagain_future<seastar::stop_iteration> {
        if (current_cursor.is_end() ||
            current_cursor.get_ghobj() >= end) {
          std::get<1>(ret) = end;
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        if (to_list == 0) {
          std::get<1>(ret) = current_cursor.get_ghobj();
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        std::get<0>(ret).emplace_back(current_cursor.get_ghobj());
        return tree.get_next(trans, current_cursor
        ).safe_then([&to_list, &current_cursor] (auto&& next_cursor) mutable {
          // we intentionally hold the current_cursor during get_next() to
          // accelerate tree lookup.
          --to_list;
          current_cursor = next_cursor;
          return seastar::stop_iteration::no;
        });
      }).safe_then([&ret] () mutable {
        return seastar::make_ready_future<list_onodes_bare_ret>(
            std::move(ret));
      });
    });
  });
}

FLTreeOnodeManager::~FLTreeOnodeManager() {}

}
