// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/btree/btree_types.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/logical_child_node.h"

namespace crimson::os::seastore {

namespace lba_manager::btree {
class BtreeLBAManager;
}

class LBAMapping {
public:
  LBAMapping() = default;
  LBAMapping(LBACursorRef physical, LBACursorRef indirect)
      : physical_cursor(std::move(physical)),
	indirect_cursor(std::move(indirect))
  {
    if (physical) {
      assert(physical->parent);
      auto parent = physical->parent;
      if (!parent->is_pending()) {
	using LBALeafNode = lba_manager::btree::LBALeafNode;
	child_pos = {parent->template cast<LBALeafNode>(), physical->pos};
      }
    }
    // if the mapping is indirect, it mustn't be at the end
    assert(indirect_cursor
      ? (!physical_cursor
	|| ((bool)physical_cursor->val
	    && physical_cursor->key != L_ADDR_NULL))
      : true);
  }

  static LBAMapping create_physical(LBACursorRef iter) {
    return LBAMapping(std::move(iter), nullptr);
  }

  static LBAMapping create_indirect(LBACursorRef iter) {
    return LBAMapping(nullptr, std::move(iter));
  }

  LBAMapping(const LBAMapping &) = delete;
  LBAMapping(LBAMapping &&) = default;

  LBAMapping &operator=(const LBAMapping &) = delete;
  LBAMapping &operator=(LBAMapping &&) = default;

  ~LBAMapping() = default;

  bool is_null() const {
    return !physical_cursor && !indirect_cursor;
  }

  bool is_end() const {
    bool end = (bool)physical_cursor && !physical_cursor->val;
    // if the mapping is at the end, it can't be indirect and
    // the physical cursor must be L_ADDR_NULL
    assert(end
      ? (!indirect_cursor && physical_cursor->key == L_ADDR_NULL)
      : true);
    return end;
  }

  bool is_indirect() const {
    assert(!is_null());
    return (bool)indirect_cursor;
  }

  bool is_valid() const {
    assert(!is_null());
    return is_indirect()
      ? indirect_cursor->is_valid()
      : physical_cursor->is_valid();
  }

  // For reserved mappings, the return values are
  // undefined although it won't crash
  bool is_stable() const;
  bool is_data_stable() const;
  bool is_clone() const {
    assert(!is_null());
    assert(physical_cursor->val);
    return physical_cursor->val->refcount > 1;
  }
  bool is_zero_reserved() const {
    assert(!is_null());
    return !get_val().is_real();
  }

  extent_len_t get_length() const {
    assert(!is_null());
    if (is_indirect()) {
      assert(indirect_cursor->val);
      return indirect_cursor->val->len;
    } else {
      assert(physical_cursor->val);
      return physical_cursor->val->len;
    }
  }

  paddr_t get_val() const {
    assert(!is_null());
    assert(physical_cursor->val);
    return physical_cursor->val->pladdr.get_paddr();
  }

  uint32_t get_checksum() const {
    assert(!is_null());
    assert(physical_cursor->val);
    return physical_cursor->val->checksum;
  }

  laddr_t get_key() const {
    assert(!is_null());
    if (is_indirect()) {
      assert(indirect_cursor->key != L_ADDR_NULL);
      return indirect_cursor->key;
    } else {
      assert(physical_cursor->key != L_ADDR_NULL);
      return physical_cursor->key;
    }
  }

  // An lba pin may be indirect, see comments in lba_manager/btree/btree_lba_manager.h
  laddr_t get_intermediate_key() const {
    assert(!is_null());
    assert(is_indirect());
    assert(indirect_cursor->val);
    return indirect_cursor->val->pladdr.get_laddr();
  }
  laddr_t get_intermediate_base() const {
    assert(!is_null());
    assert(is_indirect());
    return physical_cursor->key;
  }
  extent_len_t get_intermediate_length() const {
    assert(!is_null());
    assert(is_indirect());
    assert(physical_cursor->val);
    return physical_cursor->val->len;
  }
  // The start offset of the indirect cursor related to physical cursor
  extent_len_t get_intermediate_offset() const {
    assert(!is_null());
    assert(is_indirect());
    return get_intermediate_base().get_byte_distance<
      extent_len_t>(get_intermediate_key());
  }

  get_child_ret_t<lba_manager::btree::LBALeafNode, LogicalChildNode>
  get_logical_extent(Transaction &t);

  void link_child(LogicalChildNode &extent) {
    ceph_assert(child_pos);
    child_pos->link_child(&extent);
  }

  LBAMapping duplicate() const {
    assert(!is_null());
    auto dup_iter = [](const LBACursorRef &iter) -> LBACursorRef {
      if (iter) {
	return std::make_unique<LBACursor>(*iter);
      } else {
	return nullptr;
      }
    };
    return LBAMapping(dup_iter(physical_cursor), dup_iter(indirect_cursor));
  }
private:
  // Only allow BtreeLBAManager use these two methods
  // FIXME: remove them

  friend class lba_manager::btree::BtreeLBAManager;
  friend class TransactionManager;
  friend std::ostream &operator<<(std::ostream&, const LBAMapping&);

  LBACursor& get_effective_cursor() {
    if (is_indirect()) {
      return *indirect_cursor;
    }
    return *physical_cursor;
  }

  bool is_complete_indirect() const {
    assert(!is_null());
    return (bool)indirect_cursor && (bool)physical_cursor;
  }

  bool is_complete() const {
    return !is_indirect() || is_complete_indirect();
  }

  void make_indirect(LBACursorRef cursor) {
    assert(physical_cursor);
    assert(!indirect_cursor);
    indirect_cursor = std::move(cursor);
  }

  void link_physical(LBACursorRef cursor) {
    assert(!physical_cursor);
    assert(indirect_cursor);
    physical_cursor = std::move(cursor);
  }

private:
  // To support cloning, there are two kinds of lba mappings:
  //    1. physical lba mapping: the pladdr in the value of which is the paddr of
  //       the corresponding extent;
  //    2. indirect lba mapping: the pladdr in the value of which is an laddr pointing
  //       to the physical lba mapping that's pointing to the actual paddr of the
  //       extent being searched;
  //
  // Accordingly, LBAMapping may also work under two modes: indirect or direct
  //    1. BtreeLBAMappings that come from quering an indirect lba mapping in the lba tree
  //       are indirect;
  //    2. BtreeLBAMappings that come from quering a physical lba mapping in the lba tree
  //       are direct.
  //
  // For direct LBAMappings, there are two important properties:
  //    1. key: the laddr of the lba mapping being queried;
  //    2. paddr: the paddr recorded in the value of the lba mapping being queried.
  // For indirect LBAMappings, LBAMapping has three important properties:
  //    1. key: the laddr key of the lba entry being queried;
  //    2. intermediate_key: the laddr within the scope of the physical lba mapping
  //       that the current indirect lba mapping points to; although an indirect mapping
  //       points to the start of the physical lba mapping, it may change to other
  //       laddr after remap
  //    3. intermediate_base: the laddr key of the physical lba mapping, intermediate_key
  //       and intermediate_base should be the same when doing cloning
  //    4. intermediate_offset: intermediate_key - intermediate_base
  //    5. intermediate_length: the length of the actual physical lba mapping
  //    6. paddr: the paddr recorded in the physical lba mapping pointed to by the
  //       indirect lba mapping being queried;
  //
  // NOTE THAT, for direct LBAMappings, their intermediate_keys are the same as
  // their keys.
  LBACursorRef physical_cursor;
  LBACursorRef indirect_cursor;
  using LBALeafNode = lba_manager::btree::LBALeafNode;
  std::optional<child_pos_t<LBALeafNode>> child_pos;
};

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs);
using lba_mapping_list_t = std::list<LBAMapping>;

std::ostream &operator<<(std::ostream &out, const lba_mapping_list_t &rhs);

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LBAMapping> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_mapping_list_t> : fmt::ostream_formatter {};
#endif
