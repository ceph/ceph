// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"

SET_SUBSYS(seastore_onode);

namespace crimson::os::seastore::onode {

void FLTreeOnode::Recorder::apply_value_delta(
  ceph::bufferlist::const_iterator &bliter,
  NodeExtentMutable &value,
  laddr_offset_t value_addr_offset)
{
  LOG_PREFIX(FLTreeOnode::Recorder::apply_value_delta);
  delta_op_t op;
  try {
    ceph::decode(op, bliter);
    auto &mlayout = *reinterpret_cast<onode_layout_t*>(value.get_write());
    switch (op) {
    case delta_op_t::UNSET_NEED_COW:
      DEBUG("setting need_cow");
      bliter.copy(
	sizeof(mlayout.need_cow),
	(char *)&mlayout.need_cow);
      break;
    case delta_op_t::SET_NEED_COW:
      DEBUG("setting need_cow");
      bliter.copy(
	sizeof(mlayout.need_cow),
	(char *)&mlayout.need_cow);
      break;
    case delta_op_t::UPDATE_ONODE_SIZE:
      DEBUG("update onode size");
      bliter.copy(sizeof(mlayout.size), (char *)&mlayout.size);
      break;
    case delta_op_t::UPDATE_OMAP_ROOT:
      DEBUG("update omap root");
      bliter.copy(sizeof(mlayout.omap_root), (char *)&mlayout.omap_root);
      break;
    case delta_op_t::UPDATE_LOG_ROOT:
      DEBUG("update log root");
      bliter.copy(sizeof(mlayout.log_root), (char *)&mlayout.log_root);
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
  case delta_op_t::UNSET_NEED_COW:
    DEBUG("setting need_cow");
    encoded.append(
      (const char *)&layout.need_cow,
      sizeof(layout.need_cow));
    break;
  case delta_op_t::SET_NEED_COW:
    DEBUG("setting need_cow");
    encoded.append(
      (const char *)&layout.need_cow,
      sizeof(layout.need_cow));
    break;
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
  case delta_op_t::UPDATE_LOG_ROOT:
    DEBUG("update log root");
    encoded.append(
      (const char *)&layout.log_root,
      sizeof(layout.log_root));
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
    auto val = OnodeRef(new FLTreeOnode(hoid.hobj, cursor.value()));
    assert(val->get_clone_prefix());
    return get_onode_iertr::make_ready_future<OnodeRef>(
      val
    );
  });
}

namespace {
struct ghobj_cmp_t {
  bool same_object;
  bool equal;
};
ghobj_cmp_t compare_ghobj(ghobject_t &identity, const ghobject_t &base) {
  auto snap_bak = identity.hobj.snap;
  identity.hobj.snap = base.hobj.snap;
  bool same_object = (identity == base);
  bool equal = same_object && (snap_bak == base.hobj.snap);
  identity.hobj.snap = snap_bak;
  return {same_object, equal};
}
}

FLTreeOnodeManager::get_or_create_onode_ret
FLTreeOnodeManager::get_or_create_onode(
  Transaction &trans,
  const ghobject_t &hoid)
{
  LOG_PREFIX(FLTreeOnodeManager::get_or_create_onode);
  auto [cursor, created] = co_await tree.insert(
    trans, hoid,
    OnodeTree::tree_value_config_t{sizeof(onode_layout_t)});

  auto flonode = new FLTreeOnode(hoid.hobj, cursor.value());
  if (!created) {
    DEBUGT("onode exists for {}", trans, hoid);
    assert(flonode->get_clone_prefix());
    co_return flonode;
  }
  assert(!cursor.is_end());
  // new onode created
  DEBUGT("created onode for entry for {}", trans, hoid);
  flonode->create_default_layout(trans);

  // handle object id from sibling
  auto onode = OnodeRef(flonode);
  if (!hoid.hobj.is_head()) {
    // cur onode is not head, move to next onode to check if it's
    // the clone sibling under the same object.
    auto next_cursor = co_await tree.get_next(trans, cursor);
    if (!next_cursor.is_end()) {
      auto nxt_oid = next_cursor.get_ghobj();
      auto ret = compare_ghobj(nxt_oid, hoid);
      assert(!ret.equal);
      if (ret.same_object) {
        auto nxt_onode = next_cursor.value();
        auto prefix = nxt_onode.get_clone_prefix();
        auto object_id = prefix->get_local_object_id();
        onode->set_sibling_object_id(object_id);
        co_return onode;
      } // not same object, fallthrough to search prev onode
    } // is end, fallthrough to search prev onode
  } // is head, fallthrough to search prev onode

  auto hint = hoid;
  hint.hobj.snap = 0;
  auto prev_cursor = co_await tree.lower_bound(trans, hint);

  assert(!prev_cursor.is_end());
  auto prv_oid = prev_cursor.get_ghobj();
  auto ret = compare_ghobj(prv_oid, hoid);
  if (!ret.equal && ret.same_object) {
    // sibling clone onode exists
    auto prv_onode = prev_cursor.value();
    auto prefix = prv_onode.get_clone_prefix();
    auto object_id = prefix->get_local_object_id();
    onode->set_sibling_object_id(object_id);
  } // else sibling clone onode not found, hoid is the first clone
  co_return onode;
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
