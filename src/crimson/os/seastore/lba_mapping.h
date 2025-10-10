// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/btree/btree_types.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"
#include "crimson/os/seastore/logical_child_node.h"

namespace crimson::os::seastore {

namespace lba {
class BtreeLBAManager;
}

class LBAMapping {
  LBAMapping(LBACursorRef direct, LBACursorRef indirect)
    : direct_cursor(std::move(direct)),
      indirect_cursor(std::move(indirect))
  {
    assert(!is_linked_direct() || !direct_cursor->is_indirect());
    assert(!indirect_cursor || indirect_cursor->is_indirect());
    // if the mapping is indirect, it mustn't be at the end
    if (is_indirect() && is_linked_direct()) {
      assert((bool)direct_cursor->val
	    && direct_cursor->key != L_ADDR_NULL);
    }
  }

public:
  static LBAMapping create_indirect(
    LBACursorRef direct, LBACursorRef indirect) {
    return LBAMapping(std::move(direct), std::move(indirect));
  }

  static LBAMapping create_direct(LBACursorRef direct) {
    assert(!direct->is_indirect());
    return LBAMapping(std::move(direct), nullptr);
  }

  LBAMapping() = delete;
  LBAMapping(const LBAMapping &) = default;
  LBAMapping(LBAMapping &&) = default;
  LBAMapping &operator=(const LBAMapping &) = default;
  LBAMapping &operator=(LBAMapping &&) = default;
  ~LBAMapping() = default;

  // whether the removal of this mapping would cause
  // other mappings to be removed.
  //
  // Note that this should only be called on complete
  // indirect mappings
  bool would_cascade_remove() const {
    assert(is_indirect());
    assert(is_complete_indirect());
    return direct_cursor->get_refcount() == 1;
  }

  // whether the mapping corresponds to a pending extent
  bool is_pending() const {
    return !is_indirect() && !is_data_stable();
  }

  // whether the mapping corresponds to an initial pending extent
  bool is_initial_pending() const;

  bool is_linked_direct() const {
    return (bool)direct_cursor;
  }

  bool is_end() const {
    bool end = !is_indirect() && !direct_cursor->val;
    // if the mapping is at the end, it can't be indirect and
    // the physical cursor must be L_ADDR_NULL
    assert(end
      ? (!indirect_cursor && direct_cursor->key == L_ADDR_NULL)
      : true);
    return end;
  }

  bool is_indirect() const {
    assert(!is_null());
    return (bool)indirect_cursor;
  }

  bool is_viewable() const {
    assert(!is_null());
    if (is_complete_indirect()) {
      return indirect_cursor->is_viewable() && direct_cursor->is_viewable();
    }
    if (is_indirect()) {
      return indirect_cursor->is_viewable();
    }
    return direct_cursor->is_viewable();
  }

  // For reserved mappings, the return values are
  // undefined although it won't crash
  bool is_stable() const;
  bool is_data_stable() const;
  bool is_clone() const {
    assert(!is_null());
    if (is_indirect()) {
      return false;
    }
    assert(is_linked_direct());
    assert(!direct_cursor->is_end());
    return direct_cursor->get_refcount() > 1;
  }
  bool is_zero_reserved() const {
    return !is_indirect() && get_val().is_zero();
  }
  // true if the mapping corresponds to real data
  bool is_real() const {
    return !is_indirect() && !get_val().is_zero();
  }

  extent_len_t get_length() const {
    assert(!is_null());
    if (is_indirect()) {
      assert(!indirect_cursor->is_end());
      return indirect_cursor->get_length();
    }
    assert(!direct_cursor->is_end());
    return direct_cursor->get_length();
  }

  paddr_t get_val() const {
    assert(is_linked_direct());
    assert(!direct_cursor->is_end());
    return direct_cursor->get_paddr();
  }

  checksum_t get_checksum() const {
    assert(is_linked_direct());
    assert(!direct_cursor->is_end());
    return direct_cursor->get_checksum();
  }

  laddr_t get_key() const {
    assert(!is_null());
    if (is_indirect()) {
      assert(!indirect_cursor->is_end());
      return indirect_cursor->get_laddr();
    }
    assert(!direct_cursor->is_end());
    return direct_cursor->get_laddr();
  }

  laddr_t get_end() const {
    return (get_key() + get_length()).checked_to_laddr();
  }

   // An lba pin may be indirect, see comments in lba/btree_lba_manager.h
  laddr_t get_intermediate_key() const {
    assert(is_indirect());
    assert(!indirect_cursor->is_end());
    return indirect_cursor->get_intermediate_key();
  }
  laddr_t get_intermediate_base() const {
    assert(is_linked_direct());
    return direct_cursor->get_laddr();
  }
  extent_len_t get_intermediate_length() const {
    assert(is_linked_direct());
    assert(!direct_cursor->is_end());
    return direct_cursor->get_length();
  }
  // The start offset of the indirect cursor related to direct cursor
  extent_len_t get_intermediate_offset() const {
    assert(is_indirect());
    assert(get_intermediate_base() <= get_intermediate_key());
    assert(get_intermediate_key() + get_length() <=
	   get_intermediate_base() + get_intermediate_length());
    return get_intermediate_base().get_byte_distance<
      extent_len_t>(get_intermediate_key());
  }

  get_child_ret_t<lba::LBALeafNode, LogicalChildNode>
  get_logical_extent(Transaction &t) const;

  LogicalChildNodeRef peek_logical_extent(Transaction &t) const;

  //TODO: should be changed to return future<> once all calls
  //	  to refresh are through co_await. We return LBAMapping
  //	  for now to avoid mandating the callers to make sure
  //	  the life of the lba mapping survives the refresh.
  base_iertr::future<LBAMapping> refresh();

  base_iertr::future<LBAMapping> next();

private:
  friend class LBAManager;
  friend lba::BtreeLBAManager;
  friend class TransactionManager;
  friend std::ostream &operator<<(std::ostream&, const LBAMapping&);

  LBACursor& get_effective_cursor() {
    if (is_indirect()) {
      return *indirect_cursor;
    }
    return *direct_cursor;
  }

  LBACursorRef get_effective_cursor_ref() {
    if (is_indirect()) {
      return indirect_cursor;
    }
    return direct_cursor;
  }

  bool is_null() const {
    return !direct_cursor && !indirect_cursor;
  }

  bool is_complete_indirect() const {
    assert(!is_null());
    return (bool)indirect_cursor && (bool)direct_cursor;
  }

  bool is_complete() const {
    return !is_indirect() || is_complete_indirect();
  }

  // To support cloning, there are two kinds of lba mappings:
  //    1. direct lba mapping: the pladdr in the value of which is the paddr of
  //       the corresponding extent;
  //    2. indirect lba mapping: the pladdr in the value of which is an laddr pointing
  //       to the direct lba mapping that's pointing to the actual paddr of the
  //       extent being searched;
  //
  // Accordingly, LBAMapping may also work under two modes: indirect or direct
  //    1. LBAMappings that come from quering an indirect lba mapping in the lba tree
  //       are indirect;
  //    2. LBAMappings that come from quering a direct lba mapping in the lba tree
  //       are direct.
  //
  // For direct LBAMappings, there are two important properties:
  //    1. key: the laddr of the lba mapping being queried;
  //    2. paddr: the paddr recorded in the value of the lba mapping being queried.
  // For indirect LBAMappings, LBAMapping has three important properties:
  //    1. key: the laddr key of the lba entry being queried;
  //    2. intermediate_key: the laddr within the scope of the direct lba mapping
  //       that the current indirect lba mapping points to; although an indirect mapping
  //       points to the start of the direct lba mapping, it may change to other
  //       laddr after remap
  //    3. intermediate_base: the laddr key of the direct lba mapping, intermediate_key
  //       and intermediate_base should be the same when doing cloning
  //    4. intermediate_offset: intermediate_key - intermediate_base
  //    5. intermediate_length: the length of the actual direct lba mapping
  //    6. paddr: the paddr recorded in the direct lba mapping pointed to by the
  //       indirect lba mapping being queried;
  //
  // NOTE THAT, for direct LBAMappings, their intermediate_keys are the same as
  // their keys.
  LBACursorRef direct_cursor;
  LBACursorRef indirect_cursor;
};

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs);
using lba_mapping_list_t = std::list<LBAMapping>;

std::ostream &operator<<(std::ostream &out, const lba_mapping_list_t &rhs);

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LBAMapping> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_mapping_list_t> : fmt::ostream_formatter {};
#endif
