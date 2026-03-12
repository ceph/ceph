// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/lba_overlay_manager.h"
#include "crimson/os/seastore/lba/btree_lba_manager.h"

SET_SUBSYS(seastore_lba);

namespace crimson::os::seastore {

LBAOverlayManager::LBAOverlayManager(LBAManagerRef lba_manager)
    : lba_manager(std::move(lba_manager)) {}

LBAOverlayManagerRef LBAOverlayManager::create_lba_overlay_manager(Cache &cache) {
    auto lba_manager = LBAManagerRef(new lba::BtreeLBAManager(cache));
    return LBAOverlayManagerRef(new LBAOverlayManager(std::move(lba_manager)));
}

void LBAOverlayManager::apply_transaction_overlay(
  LBAOverlayCursor overlay_cursor,
  Transaction &t) {
  t.overlaid_cursors[overlay_cursor.key] = overlay_cursor;
}

void LBAOverlayManager::apply_overlay_op(
  Transaction &t,
  overlay_entry entry) {
  overlaid_ops[t.get_trans_id()] = entry;
}

LBAOverlayManager::get_cursor_ret LBAOverlayManager::get_cursor(
  Transaction &t,
  laddr_t offset,
  bool search_containing) {

  LOG_PREFIX(LBAOverlayManager::get_cursor);
  DEBUGT("{} ... search_containing={}", t, offset, search_containing);
  if (t.overlaid_cursors.contains(offset)) {
    co_return t.overlaid_cursors.at(offset);
  }
  auto commited_cursor = co_await lba_manager->get_cursor(t, offset, search_containing);
  LBAOverlayCursor overlay_cursor;
  // todo: move to smarter ctor once time is right see ctor header.
  co_return LBAOverlayCursor{commited_cursor->get_laddr(), commited_cursor->iter.get_val()};
}

LBAOverlayManager::get_cursor_iertr::future<LBAOverlayCursor> LBAOverlayManager::update_mapping_refcount(
  Transaction &t,
  LBACursorRef cursor,
  int delta) {
  LOG_PREFIX(LBAOverlayManager::update_mapping_refcount);
  DEBUGT("{} ... delta={}", t, cursor->get_laddr(), delta);
  auto overlaid_refcount = cursor->get_refcount();
  ceph_assert((int)overlaid_refcount + delta >= 0);
  overlaid_refcount += delta;
  // todo: move to smarter ctor once time is right see ctor header.
  LBAOverlayCursor overlay_cursor(cursor->get_laddr(), cursor->iter.get_val());
  overlay_cursor.set_overlay(&LBAOverlayCursor::overlaid_refcount, overlaid_refcount);
  apply_transaction_overlay(overlay_cursor, t);
  // todo:
  // store all relevant params in the entry
  //apply_overlay_op(t, overlay_entry{op_type::update_refcount, cursor, delta})
  apply_overlay_op(t, overlay_entry{op_type::update_refcount});
  // todo: return references
  co_return overlay_cursor;
}

LBAOverlayManager::alloc_extents_ret LBAOverlayManager::alloc_extents(
  Transaction &t,
  LBACursorRef cursor,
  std::vector<LogicalChildNodeRef> ext) {
  LOG_PREFIX(LBAOverlayManager::alloc_extents);
  DEBUGT("{} ...", t, cursor->get_laddr());
  // todo:
  // store all relevant params in the entry
  //apply_overlay_op(t, overlay_entry{op_type::alloc_extents, cursor, ext})
  std::vector<LBAOverlayCursor> allocated_extents;
  for (auto &extent : ext) {
    assert(extent->has_laddr());
    auto key = extent->get_laddr();
    auto val = lba::lba_map_val_t{
      extent->get_length(),
      extent->get_paddr(),
      EXTENT_DEFAULT_REF_COUNT,
      extent->get_last_committed_crc(),
      extent->get_type()};

   auto& overlay_cursor = allocated_extents.emplace_back(key, val);
   apply_transaction_overlay(overlay_cursor, t);
  }
  co_return allocated_extents;
}

LBAOverlayManager::alloc_extent_ret LBAOverlayManager::alloc_extent(
  Transaction &t,
  laddr_t hint,
  LogicalChildNode &nextent,
  extent_ref_count_t refcount) {
  // todo:
  // store all relevant params in the entry
  //apply_overlay_op(t, overlay_entry{op_type::alloc_extent, hint, nextent, refcount})
  assert(!nextent.has_laddr());
  auto key = hint;
  auto val = lba::lba_map_val_t{
    nextent.get_length(),
    nextent.get_paddr(),
    refcount,
    nextent.get_last_committed_crc(),
    nextent.get_type()};
  LBAOverlayCursor overlay_cursor(key, val);
  apply_transaction_overlay(overlay_cursor, t);
  co_return overlay_cursor;
}

}
