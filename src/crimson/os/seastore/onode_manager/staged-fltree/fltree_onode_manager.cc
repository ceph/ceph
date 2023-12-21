// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

SET_SUBSYS(seastore_onode);

namespace crimson::os::seastore::onode {

void FLTreeOnode::Recorder::apply_value_delta(
  ceph::bufferlist::const_iterator &bliter,
  NodeExtentMutable &value,
  laddr_t value_addr)
{
  LOG_PREFIX(FLTreeOnode::Recorder::apply_value_delta);
  delta_op_t op;
  try {
    ceph::decode(op, bliter);
    auto &mlayout = *reinterpret_cast<onode_layout_t*>(value.get_write());
    switch (op) {
    case delta_op_t::UPDATE_ONODE_SIZE:
      DEBUG("update onode size");
      bliter.copy(sizeof(mlayout.size), (char *)&mlayout.size);
      break;
    case delta_op_t::UPDATE_OMAP_ROOT:
      DEBUG("update omap root");
      bliter.copy(sizeof(mlayout.omap_root), (char *)&mlayout.omap_root);
      break;
    case delta_op_t::UPDATE_XATTR_ROOT:
      DEBUG("update xattr root");
      bliter.copy(sizeof(mlayout.xattr_root), (char *)&mlayout.xattr_root);
      break;
    case delta_op_t::UPDATE_OBJECT_DATA:
      DEBUG("update object data");
      bliter.copy(sizeof(mlayout.object_data), (char *)&mlayout.object_data);
      break;
    case delta_op_t::UPDATE_OBJECT_INFO:
      DEBUG("update object info");
      bliter.copy(onode_layout_t::MAX_OI_LENGTH, (char *)&mlayout.oi[0]);
      ceph::decode(mlayout.oi_size, bliter);
      break;
    case delta_op_t::UPDATE_SNAPSET:
      DEBUG("update snapset");
      bliter.copy(onode_layout_t::MAX_SS_LENGTH, (char *)&mlayout.ss[0]);
      ceph::decode(mlayout.ss_size, bliter);
      break;
    case delta_op_t::CLEAR_OBJECT_INFO:
      DEBUG("clear object info");
      memset(&mlayout.oi[0], 0, mlayout.oi_size);
      mlayout.oi_size = 0;
      break;
    case delta_op_t::CLEAR_SNAPSET:
      DEBUG("clear snapset");
      memset(&mlayout.ss[0], 0, mlayout.ss_size);
      mlayout.ss_size = 0;
      break;
    case delta_op_t::CREATE_DEFAULT:
      mlayout = onode_layout_t{};
      break;
    default:
      ceph_abort();
    }
  } catch (buffer::error& e) {
    ceph_abort();
  }
}

void FLTreeOnode::Recorder::encode_update(
  NodeExtentMutable &payload_mut, delta_op_t op)
{
  LOG_PREFIX(FLTreeOnode::Recorder::encode_update);
  auto &layout = *reinterpret_cast<const onode_layout_t*>(
    payload_mut.get_read());
  auto &encoded = get_encoded(payload_mut);
  ceph::encode(op, encoded);
  switch(op) {
  case delta_op_t::UPDATE_ONODE_SIZE:
    DEBUG("update onode size");
    encoded.append(
      (const char *)&layout.size,
      sizeof(layout.size));
    break;
  case delta_op_t::UPDATE_OMAP_ROOT:
    DEBUG("update omap root");
    encoded.append(
      (const char *)&layout.omap_root,
      sizeof(layout.omap_root));
    break;
  case delta_op_t::UPDATE_XATTR_ROOT:
    DEBUG("update xattr root");
    encoded.append(
      (const char *)&layout.xattr_root,
      sizeof(layout.xattr_root));
    break;
  case delta_op_t::UPDATE_OBJECT_DATA:
    DEBUG("update object data");
    encoded.append(
      (const char *)&layout.object_data,
      sizeof(layout.object_data));
    break;
  case delta_op_t::UPDATE_OBJECT_INFO:
    DEBUG("update object info");
    encoded.append(
      (const char *)&layout.oi[0],
      onode_layout_t::MAX_OI_LENGTH);
    ceph::encode(layout.oi_size, encoded);
    break;
  case delta_op_t::UPDATE_SNAPSET:
    DEBUG("update snapset");
    encoded.append(
      (const char *)&layout.ss[0],
      onode_layout_t::MAX_SS_LENGTH);
    ceph::encode(layout.ss_size, encoded);
    break;
  case delta_op_t::CREATE_DEFAULT:
    DEBUG("create default layout");
    [[fallthrough]];
  case delta_op_t::CLEAR_OBJECT_INFO:
    DEBUG("clear object info");
    [[fallthrough]];
  case delta_op_t::CLEAR_SNAPSET:
    DEBUG("clear snapset");
    break;
  default:
    ceph_abort();
  }
}

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
  ).si_then([this, &hoid, &trans, FNAME](auto cursor)
              -> get_onode_ret {
    if (cursor == tree.end()) {
      DEBUGT("no entry for {}", trans, hoid);
      return crimson::ct_error::enoent::make();
    }
    auto val = OnodeRef(new FLTreeOnode(
	default_data_reservation,
	default_metadata_range,
	cursor.value()));
    return get_onode_iertr::make_ready_future<OnodeRef>(
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
  ).si_then([this, &trans, &hoid, FNAME](auto p)
              -> get_or_create_onode_ret {
    auto [cursor, created] = std::move(p);
    auto onode = new FLTreeOnode(
	default_data_reservation,
	default_metadata_range,
	cursor.value());
    if (created) {
      DEBUGT("created onode for entry for {}", trans, hoid);
      onode->create_default_layout(trans);
    }
    return get_or_create_onode_iertr::make_ready_future<OnodeRef>(onode);
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
      return trans_intr::do_for_each(
        hoids,
        [this, &trans, &ret](auto &hoid) {
          return get_or_create_onode(trans, hoid
          ).si_then([&ret](auto &&onoderef) {
            ret.push_back(std::move(onoderef));
          });
        }).si_then([&ret] {
          return std::move(ret);
        });
    });
}

FLTreeOnodeManager::erase_onode_ret FLTreeOnodeManager::erase_onode(
  Transaction &trans,
  OnodeRef &onode)
{
  auto &flonode = static_cast<FLTreeOnode&>(*onode);
  assert(flonode.is_alive());
  flonode.mark_delete();
  return tree.erase(trans, flonode);
}

FLTreeOnodeManager::list_onodes_ret FLTreeOnodeManager::list_onodes(
  Transaction &trans,
  const ghobject_t& start,
  const ghobject_t& end,
  uint64_t limit)
{
  LOG_PREFIX(FLTreeOnodeManager::list_onodes);
  DEBUGT("start {}, end {}, limit {}", trans, start, end, limit);
  return tree.lower_bound(trans, start
  ).si_then([this, &trans, end, limit] (auto&& cursor) {
    using crimson::os::seastore::onode::full_key_t;
    return seastar::do_with(
        limit,
        std::move(cursor),
        list_onodes_bare_ret(),
        [this, &trans, end] (auto& to_list, auto& current_cursor, auto& ret) {
      return trans_intr::repeat(
          [this, &trans, end, &to_list, &current_cursor, &ret] ()
          -> eagain_ifuture<seastar::stop_iteration> {
	LOG_PREFIX(FLTreeOnodeManager::list_onodes);
        if (current_cursor.is_end()) {
	  DEBUGT("reached the onode tree end", trans);
          std::get<1>(ret) = ghobject_t::get_max();
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        } else if (current_cursor.get_ghobj() >= end) {
	  DEBUGT("reached the end {} > {}",
	    trans, current_cursor.get_ghobj(), end);
          std::get<1>(ret) = end;
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        if (to_list == 0) {
	  DEBUGT("reached the limit", trans);
          std::get<1>(ret) = current_cursor.get_ghobj();
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
	auto ghobj = current_cursor.get_ghobj();
	DEBUGT("found onode for {}", trans, ghobj);
        std::get<0>(ret).emplace_back(std::move(ghobj));
        return tree.get_next(trans, current_cursor
        ).si_then([&to_list, &current_cursor] (auto&& next_cursor) mutable {
          // we intentionally hold the current_cursor during get_next() to
          // accelerate tree lookup.
          --to_list;
          current_cursor = next_cursor;
          return seastar::make_ready_future<seastar::stop_iteration>(
	        seastar::stop_iteration::no);
        });
      }).si_then([&ret] () mutable {
        return seastar::make_ready_future<list_onodes_bare_ret>(
            std::move(ret));
       //  return ret;
      });
    });
  });
}

FLTreeOnodeManager::~FLTreeOnodeManager() {}

}
