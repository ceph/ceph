// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <string>

#include "include/byteorder.h"
#include "include/denc.h"
#include "include/encoding.h"

#include "crimson/common/layout.h"
#include "crimson/common/fixed_kv_node_layout.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_types.h"

namespace crimson::os::seastore::omap_manager {
class StringKVInnerNodeLayout;
class StringKVLeafNodeLayout;

/**
 * copy_from_foreign
 *
 * Copy from another node entries to this node.
 * [from_src, to_src) is another node entry range.
 * tgt is this node entry to copy to.
 * tgt and from_src must be from different nodes.
 * from_src and to_src must be in the same node.
 */
template <typename iterator, typename const_iterator>
static void copy_from_foreign(
  iterator tgt,
  const_iterator from_src,
  const_iterator to_src) {
  assert(tgt->node != from_src->node);
  assert(to_src->node == from_src->node);
  if (from_src == to_src)
    return;

  auto to_copy = from_src->get_right_ptr_end() - to_src->get_right_ptr_end();
  assert(to_copy > 0);
  memcpy(
    tgt->get_right_ptr_end() - to_copy,
    to_src->get_right_ptr_end(),
    to_copy);
  memcpy(
    tgt->get_node_key_ptr(),
    from_src->get_node_key_ptr(),
    to_src->get_node_key_ptr() - from_src->get_node_key_ptr());

  auto offset_diff = tgt->get_right_offset_end() - from_src->get_right_offset_end();
  for (auto i = tgt; i != tgt + (to_src - from_src); ++i) {
    i->update_offset(offset_diff);
  }
}

/**
 * copy_from_local
 *
 * Copies entries from [from_src, to_src) to tgt.
 * tgt, from_src, and to_src must be from the same node.
 */
template <typename iterator>
static void copy_from_local(
  unsigned len,
  iterator tgt,
  iterator from_src,
  iterator to_src) {
  assert(tgt->node == from_src->node);
  assert(to_src->node == from_src->node);

  auto to_copy = from_src->get_right_ptr_end() - to_src->get_right_ptr_end();
  assert(to_copy > 0);
  int adjust_offset = tgt > from_src? -len : len;
  memmove(to_src->get_right_ptr_end() + adjust_offset,
          to_src->get_right_ptr_end(),
          to_copy);

  for ( auto ite = from_src; ite < to_src; ite++) {
      ite->update_offset(-adjust_offset);
  }
  memmove(tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
          to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
}

struct delta_inner_t {
  enum class op_t : uint_fast8_t {
    INSERT,
    UPDATE,
    REMOVE,
  } op;
  std::string key;
  laddr_t addr;

  DENC(delta_inner_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.key, p);
    denc(v.addr, p);
    DENC_FINISH(p);
  }

  void replay(StringKVInnerNodeLayout &l);
  bool operator==(const delta_inner_t &rhs) const {
    return op == rhs.op &&
           key == rhs.key &&
           addr == rhs.addr;
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::omap_manager::delta_inner_t)

namespace crimson::os::seastore::omap_manager {
struct delta_leaf_t {
  enum class op_t : uint_fast8_t {
    INSERT,
    UPDATE,
    REMOVE,
  } op;
  std::string key;
  ceph::bufferlist val;

  DENC(delta_leaf_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.key, p);
    denc(v.val, p);
    DENC_FINISH(p);
  }

  void replay(StringKVLeafNodeLayout &l);
  bool operator==(const delta_leaf_t &rhs) const {
    return op == rhs.op &&
      key == rhs.key &&
      val == rhs.val;
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::omap_manager::delta_leaf_t)

namespace crimson::os::seastore::omap_manager {
class delta_inner_buffer_t {
  std::vector<delta_inner_t> buffer;
public:
  bool empty() const {
    return buffer.empty();
  }
  void insert(
    const std::string &key,
    laddr_t addr) {
    buffer.push_back(
      delta_inner_t{
        delta_inner_t::op_t::INSERT,
        key,
        addr
      });
  }
  void update(
    const std::string &key,
    laddr_t addr) {
    buffer.push_back(
      delta_inner_t{
	delta_inner_t::op_t::UPDATE,
	key,
	addr
      });
  }
  void remove(const std::string &key) {
    buffer.push_back(
      delta_inner_t{
	delta_inner_t::op_t::REMOVE,
	key,
	L_ADDR_NULL
      });
  }

  void replay(StringKVInnerNodeLayout &node) {
    for (auto &i: buffer) {
      i.replay(node);
    }
  }

  void clear() {
    buffer.clear();
  }

  DENC(delta_inner_buffer_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.buffer, p);
    DENC_FINISH(p);
  }

  bool operator==(const delta_inner_buffer_t &rhs) const {
    return buffer == rhs.buffer;
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::omap_manager::delta_inner_buffer_t)

namespace crimson::os::seastore::omap_manager {
class delta_leaf_buffer_t {
  std::vector<delta_leaf_t> buffer;
public:
  bool empty() const {
    return buffer.empty();
  }
  void insert(
    const std::string &key,
    const ceph::bufferlist &val) {
    buffer.push_back(
      delta_leaf_t{
        delta_leaf_t::op_t::INSERT,
        key,
        val
      });
  }
  void update(
    const std::string &key,
    const ceph::bufferlist &val) {
    buffer.push_back(
      delta_leaf_t{
        delta_leaf_t::op_t::UPDATE,
	key,
	val
      });
  }
  void remove(const std::string &key) {
    buffer.push_back(
      delta_leaf_t{
	delta_leaf_t::op_t::REMOVE,
	key,
	bufferlist()
      });
  }

  void replay(StringKVLeafNodeLayout &node) {
    for (auto &i: buffer) {
      i.replay(node);
    }
  }

  void clear() {
    buffer.clear();
  }

  DENC(delta_leaf_buffer_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.buffer, p);
    DENC_FINISH(p);
  }

  bool operator==(const delta_leaf_buffer_t &rhs) const {
    return buffer == rhs.buffer;
  }
};
}
WRITE_CLASS_DENC(crimson::os::seastore::omap_manager::delta_leaf_buffer_t)

namespace crimson::os::seastore::omap_manager {
/**
 * StringKVInnerNodeLayout
 *
 * Uses absl::container_internal::Layout for the actual key memory layout.
 *
 * The primary interface exposed is centered on the iterator
 * and related methods.
 *
 * Also included are helpers for doing splits and merges as for a btree.
 *
 * layout diagram:
 *
 * # <----------------------------- node range --------------------------------------------> #
 * #                                                         #<~># free space                #
 * # <------------- left part -----------------------------> # <~# <----- right keys  -----> #
 * #               # <------------ left keys --------------> #~> #                           #
 * #               #                         keys [2, n) |<~>#   #<~>| right keys [2, n)     #
 * #               # <--- key 0 ----> | <--- key 1 ----> |   #   #   | <- k1 -> | <-- k0 --> #
 * #               #                  |                  |   #   #   |          |            #
 * #  num_ | meta  # key | key | val  | key | key | val  |   #   #   | key      | key        #
 * #  keys | depth # off | len | laddr| off | len | laddr|   #   #   | buff     | buff       #
 * #       |       # 0   | 0   | 0    | 1   | 1   | 1    |...#...#...| key 1    | key 0      #
 * #                 |                  |                            | <- off --+----------> #
 * #                 |                  |                                  ^    | <- off --> #
 *                   |                  |                                  |          ^
 *                   |                  +----------------------------------+          |
 *                   +----------------------------------------------------------------+
 */
class StringKVInnerNodeLayout {
  char *buf = nullptr;

  using L = absl::container_internal::Layout<ceph_le32, omap_node_meta_le_t, omap_inner_key_le_t>;
  static constexpr L layout{1, 1, 1}; // = L::Partial(1, 1, 1);
  friend class delta_inner_t;
public:
  template <bool is_const>
  class iter_t {
    friend class StringKVInnerNodeLayout;

    template <typename iterator, typename const_iterator>
    friend void copy_from_foreign(iterator, const_iterator, const_iterator);
    template <typename iterator>
    friend void copy_from_local(unsigned, iterator, iterator, iterator);

    using parent_t = typename crimson::common::maybe_const_t<StringKVInnerNodeLayout, is_const>::type;

    mutable parent_t node;
    uint16_t index;

    iter_t(
      parent_t parent,
      uint16_t index) : node(parent), index(index) {}

  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = StringKVInnerNodeLayout;
    using difference_type = std::ptrdiff_t;
    using pointer = StringKVInnerNodeLayout*;
    using reference = iter_t&;

    iter_t(const iter_t &) = default;
    iter_t(iter_t &&) = default;
    iter_t &operator=(const iter_t &) = default;
    iter_t &operator=(iter_t &&) = default;

    operator iter_t<!is_const>() const {
      static_assert(!is_const);
      return iter_t<!is_const>(node, index);
    }

    iter_t &operator*() { return *this; }
    iter_t *operator->() { return this; }

    iter_t operator++(int) {
      auto ret = *this;
      ++index;
      return ret;
    }

    iter_t &operator++() {
      ++index;
      return *this;
    }

    iter_t operator--(int) {
      auto ret = *this;
      assert(index > 0);
      --index;
      return ret;
    }

    iter_t &operator--() {
      assert(index > 0);
      --index;
      return *this;
    }

    uint16_t operator-(const iter_t &rhs) const {
      assert(rhs.node == node);
      return index - rhs.index;
    }

    iter_t operator+(uint16_t off) const {
      return iter_t(node, index + off);
    }
    iter_t operator-(uint16_t off) const {
      return iter_t(node, index - off);
    }

    uint16_t operator<(const iter_t &rhs) const {
      assert(rhs.node == node);
      return index < rhs.index;
    }

    uint16_t operator>(const iter_t &rhs) const {
      assert(rhs.node == node);
      return index > rhs.index;
    }

    friend bool operator==(const iter_t &lhs, const iter_t &rhs) {
      assert(lhs.node == rhs.node);
      return lhs.index == rhs.index;
    }

  private:
    omap_inner_key_t get_node_key() const {
      omap_inner_key_le_t kint = node->get_node_key_ptr()[index];
      return omap_inner_key_t(kint);
    }
    auto get_node_key_ptr() const {
      return reinterpret_cast<
	typename crimson::common::maybe_const_t<char, is_const>::type>(
	  node->get_node_key_ptr() + index);
    }

    uint32_t get_node_val_offset() const {
      return get_node_key().key_off;
    }
    auto get_node_val_ptr() const {
      auto tail = node->buf + OMAP_INNER_BLOCK_SIZE;
      if (*this == node->iter_end())
        return tail;
      else {
        return tail - get_node_val_offset();
      }
    }

    int get_right_offset_end() const {
      if (index == 0)
        return 0;
      else
        return (*this - 1)->get_node_val_offset();
    }
    auto get_right_ptr_end() const {
      return node->buf + OMAP_INNER_BLOCK_SIZE - get_right_offset_end();
    }

    void update_offset(int offset) {
      static_assert(!is_const);
      auto key = get_node_key();
      assert(offset + key.key_off >= 0);
      key.key_off += offset;
      set_node_key(key);
    }

    void set_node_key(omap_inner_key_t _lb) {
      static_assert(!is_const);
      omap_inner_key_le_t lb;
      lb = _lb;
      node->get_node_key_ptr()[index] = lb;
    }

    void set_node_val(const std::string &str) {
      static_assert(!is_const);
      assert(str.size() == get_node_key().key_len);
      assert(get_node_key().key_off >= str.size());
      assert(get_node_key().key_off < OMAP_INNER_BLOCK_SIZE);
      assert(str.size() < OMAP_INNER_BLOCK_SIZE);
      ::memcpy(get_node_val_ptr(), str.data(), str.size());
    }

  public:
    uint16_t get_index() const {
      return index;
    }

    std::string get_key() const {
      return std::string(
	get_node_val_ptr(),
	get_node_key().key_len);
    }

    laddr_t get_val() const {
      return get_node_key().laddr;
    }

    bool contains(std::string_view key) const {
      assert(*this != node->iter_end());
      auto next = *this + 1;
      if (next == node->iter_end()) {
        return get_key() <= key;
      } else {
	return (get_key() <= key) && (next->get_key() > key);
      }
    }
  };
  using const_iterator = iter_t<true>;
  using iterator = iter_t<false>;

public:
  void journal_inner_insert(
    const_iterator _iter,
    const laddr_t laddr,
    const std::string &key,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->insert(
	key,
	laddr);
    }
    inner_insert(iter, key, laddr);
  }

  void journal_inner_update(
    const_iterator _iter,
    const laddr_t laddr,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    auto key = iter->get_key();
    if (recorder) {
      recorder->update(key, laddr);
    }
    inner_update(iter, laddr);
  }

  void journal_inner_remove(
    const_iterator _iter,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->remove(iter->get_key());
    }
    inner_remove(iter);
  }

  StringKVInnerNodeLayout(char *buf) :
    buf(buf) {}

  uint32_t get_size() const {
    ceph_le32 &size = *layout.template Pointer<0>(buf);
    return uint32_t(size);
  }

  /**
   * set_size
   *
   * Set size representation to match size
   */
  void set_size(uint32_t size) {
    ceph_le32 s;
    s = size;
    *layout.template Pointer<0>(buf) = s;
  }

  const_iterator iter_cbegin() const {
    return const_iterator(
      this,
      0);
  }
  const_iterator iter_begin() const {
    return iter_cbegin();
  }

  const_iterator iter_cend() const {
    return const_iterator(
      this,
      get_size());
  }
  const_iterator iter_end() const {
    return iter_cend();
  }

  iterator iter_begin() {
    return iterator(
      this,
      0);
  }

  iterator iter_end() {
    return iterator(
      this,
      get_size());
  }

  const_iterator iter_idx(uint16_t off) const {
    return const_iterator(
      this,
      off);
  }

  const_iterator string_lower_bound(std::string_view str) const {
    auto it = std::lower_bound(boost::make_counting_iterator<uint16_t>(0),
                               boost::make_counting_iterator<uint16_t>(get_size()),
                               str,
                               [this](uint16_t i, std::string_view str) {
                                 const_iterator iter(this, i);
                                 return iter->get_key() < str;
                               });
    return const_iterator(this, *it);
  }

  iterator string_lower_bound(std::string_view str) {
    const auto &tref = *this;
    return iterator(this, tref.string_lower_bound(str).index);
  }

  const_iterator string_upper_bound(std::string_view str) const {
    auto it = std::upper_bound(boost::make_counting_iterator<uint16_t>(0),
                               boost::make_counting_iterator<uint16_t>(get_size()),
                               str,
                               [this](std::string_view str, uint16_t i) {
                                 const_iterator iter(this, i);
                                 return str < iter->get_key();
                               });
    return const_iterator(this, *it);
  }

  iterator string_upper_bound(std::string_view str) {
    const auto &tref = *this;
    return iterator(this, tref.string_upper_bound(str).index);
  }

  const_iterator find_string_key(std::string_view str) const {
    auto ret = iter_begin();
    for (; ret != iter_end(); ++ret) {
     std::string s = ret->get_key();
      if (s == str)
        break;
    }
    return ret;
  }

  iterator find_string_key(std::string_view str) {
    const auto &tref = *this;
    return iterator(this, tref.find_string_key(str).index);
  }

  const_iterator get_split_pivot() const {
    uint32_t total_size = omap_inner_key_t(
      get_node_key_ptr()[get_size()-1]).key_off;
    uint32_t pivot_size = total_size / 2;
    uint32_t size = 0;
    for (auto ite = iter_begin(); ite < iter_end(); ite++) {
      auto node_key = ite->get_node_key();
      size += node_key.key_len;
      if (size >= pivot_size){
        return ite;
      }
    }
    return iter_end();
  }


  /**
   * get_meta/set_meta
   *
   * Enables stashing a templated type within the layout.
   * Cannot be modified after initial write as it is not represented
   * in delta_t
   */
  omap_node_meta_t get_meta() const {
    omap_node_meta_le_t &metaint = *layout.template Pointer<1>(buf);
    return omap_node_meta_t(metaint);
  }
  void set_meta(const omap_node_meta_t &meta) {
    *layout.template Pointer<1>(buf) = omap_node_meta_le_t(meta);
  }

  uint32_t used_space() const {
    uint32_t count = get_size();
    if (count) {
      omap_inner_key_t last_key = omap_inner_key_t(get_node_key_ptr()[count-1]);
      return last_key.key_off + count * sizeof(omap_inner_key_le_t);
    } else {
      return 0;
    }
  }

  uint32_t free_space() const {
    return capacity() - used_space();
  }

  uint16_t capacity() const {
    return OMAP_INNER_BLOCK_SIZE
      - (reinterpret_cast<char*>(layout.template Pointer<2>(buf))
      - reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }

  bool is_overflow(size_t ksize) const {
    return free_space() < (sizeof(omap_inner_key_le_t) + ksize);
  }

  bool is_overflow(const StringKVInnerNodeLayout &rhs) const {
    return free_space() < rhs.used_space();
  }

  bool below_min() const {
    return free_space() > (capacity() / 2);
  }

  bool operator==(const StringKVInnerNodeLayout &rhs) const {
    if (get_size() != rhs.get_size()) {
      return false;
    }

    auto iter = iter_begin();
    auto iter2 = rhs.iter_begin();
    while (iter != iter_end()) {
      if (iter->get_key() != iter2->get_key() ||
          iter->get_val() != iter2->get_val()) {
          return false;
      }
      iter++;
      iter2++;
    }
    return true;
  }

  /**
   * split_into
   *
   * Takes *this and splits its contents into left and right.
   */
  std::string split_into(
    StringKVInnerNodeLayout &left,
    StringKVInnerNodeLayout &right) const {
    auto piviter = get_split_pivot();
    assert(piviter != iter_end());

    copy_from_foreign(left.iter_begin(), iter_begin(), piviter);
    left.set_size(piviter - iter_begin());

    copy_from_foreign(right.iter_begin(), piviter, iter_end());
    right.set_size(iter_end() - piviter);

    auto [lmeta, rmeta] = get_meta().split_into();
    left.set_meta(lmeta);
    right.set_meta(rmeta);

    return piviter->get_key();
  }

  /**
   * merge_from
   *
   * Takes two nodes and copies their contents into *this.
   *
   * precondition: left.size() + right.size() < CAPACITY
   */
  void merge_from(
    const StringKVInnerNodeLayout &left,
    const StringKVInnerNodeLayout &right) {
    copy_from_foreign(
      iter_end(),
      left.iter_begin(),
      left.iter_end());
    set_size(left.get_size());

    copy_from_foreign(
      iter_end(),
      right.iter_begin(),
      right.iter_end());
    set_size(left.get_size() + right.get_size());
    set_meta(omap_node_meta_t::merge_from(left.get_meta(), right.get_meta()));
  }

  /**
   * balance_into_new_nodes
   *
   * Takes the contents of left and right and copies them into
   * replacement_left and replacement_right such that
   * the size of replacement_left just >= 1/2 of (left + right)
   */
  static std::string balance_into_new_nodes(
    const StringKVInnerNodeLayout &left,
    const StringKVInnerNodeLayout &right,
    StringKVInnerNodeLayout &replacement_left,
    StringKVInnerNodeLayout &replacement_right)
  {
    uint32_t left_size = omap_inner_key_t(left.get_node_key_ptr()[left.get_size()-1]).key_off;
    uint32_t right_size = omap_inner_key_t(right.get_node_key_ptr()[right.get_size()-1]).key_off;
    uint32_t total = left_size + right_size;
    uint32_t pivot_size = total / 2;
    uint32_t pivot_idx = 0;
    if (pivot_size < left_size) {
      uint32_t size = 0;
      for (auto ite = left.iter_begin(); ite < left.iter_end(); ite++) {
        auto node_key = ite->get_node_key();
        size += node_key.key_len;
        if (size >= pivot_size){
          pivot_idx = ite.get_index();
          break;
        }
      }
    } else {
      uint32_t more_size = pivot_size - left_size;
      uint32_t size = 0;
      for (auto ite = right.iter_begin(); ite < right.iter_end(); ite++) {
        auto node_key = ite->get_node_key();
        size += node_key.key_len;
        if (size >= more_size){
          pivot_idx = ite.get_index() + left.get_size();
          break;
        }
      }
    }

    auto replacement_pivot = pivot_idx >= left.get_size() ?
      right.iter_idx(pivot_idx - left.get_size())->get_key() :
      left.iter_idx(pivot_idx)->get_key();

    if (pivot_size < left_size) {
      copy_from_foreign(
        replacement_left.iter_end(),
        left.iter_begin(),
        left.iter_idx(pivot_idx));
      replacement_left.set_size(pivot_idx);

      copy_from_foreign(
        replacement_right.iter_end(),
        left.iter_idx(pivot_idx),
        left.iter_end());
      replacement_right.set_size(left.get_size() - pivot_idx);

      copy_from_foreign(
        replacement_right.iter_end(),
        right.iter_begin(),
        right.iter_end());
      replacement_right.set_size(right.get_size() + left.get_size()- pivot_idx);
    } else {
      copy_from_foreign(
        replacement_left.iter_end(),
        left.iter_begin(),
        left.iter_end());
      replacement_left.set_size(left.get_size());

      copy_from_foreign(
        replacement_left.iter_end(),
        right.iter_begin(),
        right.iter_idx(pivot_idx - left.get_size()));
      replacement_left.set_size(pivot_idx);

      copy_from_foreign(
        replacement_right.iter_end(),
        right.iter_idx(pivot_idx - left.get_size()),
        right.iter_end());
      replacement_right.set_size(right.get_size() + left.get_size() - pivot_idx);
    }

    auto [lmeta, rmeta] = omap_node_meta_t::rebalance(
      left.get_meta(), right.get_meta());
    replacement_left.set_meta(lmeta);
    replacement_right.set_meta(rmeta);
    return replacement_pivot;
  }

private:
  void inner_insert(
    iterator iter,
    const std::string &key,
    laddr_t val) {
    if (iter != iter_begin()) {
      assert((iter - 1)->get_key() < key);
    }
    if (iter != iter_end()) {
      assert(iter->get_key() > key);
    }
    assert(!is_overflow(key.size()));

    if (iter != iter_end()) {
      copy_from_local(key.size(), iter + 1, iter, iter_end());
    }

    omap_inner_key_t nkey;
    nkey.key_len = key.size();
    nkey.laddr = val;
    if (iter != iter_begin()) {
      auto pkey = (iter - 1).get_node_key();
      nkey.key_off = nkey.key_len + pkey.key_off;
    } else {
      nkey.key_off = nkey.key_len;
    }

    iter->set_node_key(nkey);
    set_size(get_size() + 1);
    iter->set_node_val(key);
  }

  void inner_update(
    iterator iter,
    laddr_t addr) {
    assert(iter != iter_end());
    auto node_key = iter->get_node_key();
    node_key.laddr = addr;
    iter->set_node_key(node_key);
  }

  void inner_remove(iterator iter) {
    assert(iter != iter_end());
    if ((iter + 1) != iter_end())
      copy_from_local(iter->get_node_key().key_len, iter, iter + 1, iter_end());
    set_size(get_size() - 1);
  }

  /**
   * get_key_ptr
   *
   * Get pointer to start of key array
   */
  omap_inner_key_le_t *get_node_key_ptr() {
    return L::Partial(1, 1, get_size()).template Pointer<2>(buf);
  }
  const omap_inner_key_le_t *get_node_key_ptr() const {
    return L::Partial(1, 1, get_size()).template Pointer<2>(buf);
  }

};

/**
 * StringKVLeafNodeLayout
 *
 * layout diagram:
 *
 * # <----------------------------- node range -------------------------------------------------> #
 * #                                                       #<~># free space                       #
 * # <------------- left part ---------------------------> # <~# <----- right key-value pairs --> #
 * #               # <------------ left keys ------------> #~> #                                  #
 * #               #                       keys [2, n) |<~>#   #<~>| right kvs [2, n)             #
 * #               # <--- key 0 ---> | <--- key 1 ---> |   #   #   | <-- kv 1 --> | <-- kv 0 -->  #
 * #               #                 |                 |   #   #   |              |               #
 * # num_ | meta   # key | key | val | key | key | val |   #   #   | key   | val  | key   | val   #
 * # keys | depth  # off | len | len | off | len | len |   #   #   | buff  | buff | buff  | buff  #
 * #               # 0   | 0   | 0   | 1   | 1   | 1   |...#...#...| key 1 | val 1| key 0 | val 0 #
 * #                 |                 |                           | <--- off ----+-------------> #
 * #                 |                 |                                   ^      | <--- off ---> #
 *                   |                 |                                   |             ^
 *                   |                 +-----------------------------------+             |
 *                   +-------------------------------------------------------------------+
 */
class StringKVLeafNodeLayout {
  char *buf = nullptr;

  using L = absl::container_internal::Layout<ceph_le32, omap_node_meta_le_t, omap_leaf_key_le_t>;
  static constexpr L layout{1, 1, 1}; // = L::Partial(1, 1, 1);
  friend class delta_leaf_t;

public:
  template <bool is_const>
  class iter_t {
    friend class StringKVLeafNodeLayout;
    using parent_t = typename crimson::common::maybe_const_t<StringKVLeafNodeLayout, is_const>::type;

    template <typename iterator, typename const_iterator>
    friend void copy_from_foreign(iterator, const_iterator, const_iterator);
    template <typename iterator>
    friend void copy_from_local(unsigned, iterator, iterator, iterator);

    parent_t node;
    uint16_t index;

    iter_t(
      parent_t parent,
      uint16_t index) : node(parent), index(index) {}

  public:
    iter_t(const iter_t &) = default;
    iter_t(iter_t &&) = default;
    iter_t &operator=(const iter_t &) = default;
    iter_t &operator=(iter_t &&) = default;

    operator iter_t<!is_const>() const {
      static_assert(!is_const);
      return iter_t<!is_const>(node, index);
    }

    iter_t &operator*() { return *this; }
    iter_t *operator->() { return this; }

    iter_t operator++(int) {
      auto ret = *this;
      ++index;
      return ret;
    }

    iter_t &operator++() {
      ++index;
      return *this;
    }

    uint16_t operator-(const iter_t &rhs) const {
      assert(rhs.node == node);
      return index - rhs.index;
    }

    iter_t operator+(uint16_t off) const {
      return iter_t(
             node,
             index + off);
    }
    iter_t operator-(uint16_t off) const {
      return iter_t(
             node,
             index - off);
    }

    uint16_t operator<(const iter_t &rhs) const {
      assert(rhs.node == node);
      return index < rhs.index;
    }

    uint16_t operator>(const iter_t &rhs) const {
      assert(rhs.node == node);
      return index > rhs.index;
    }

    bool operator==(const iter_t &rhs) const {
      assert(node == rhs.node);
      return rhs.index == index;
    }

    bool operator!=(const iter_t &rhs) const {
      assert(node == rhs.node);
      return index != rhs.index;
    }

  private:
    omap_leaf_key_t get_node_key() const {
      omap_leaf_key_le_t kint = node->get_node_key_ptr()[index];
      return omap_leaf_key_t(kint);
    }
    auto get_node_key_ptr() const {
      return reinterpret_cast<
	typename crimson::common::maybe_const_t<char, is_const>::type>(
	  node->get_node_key_ptr() + index);
    }

    uint32_t get_node_val_offset() const {
      return get_node_key().key_off;
    }
    auto get_node_val_ptr() const {
      auto tail = node->buf + OMAP_LEAF_BLOCK_SIZE;
      if (*this == node->iter_end())
        return tail;
      else {
        return tail - get_node_val_offset();
      }
    }

    int get_right_offset_end() const {
      if (index == 0)
        return 0;
      else
        return (*this - 1)->get_node_val_offset();
    }
    auto get_right_ptr_end() const {
      return node->buf + OMAP_LEAF_BLOCK_SIZE - get_right_offset_end();
    }

    void update_offset(int offset) {
      auto key = get_node_key();
      assert(offset + key.key_off >= 0);
      key.key_off += offset;
      set_node_key(key);
    }

    void set_node_key(omap_leaf_key_t _lb) const {
      static_assert(!is_const);
      omap_leaf_key_le_t lb;
      lb = _lb;
      node->get_node_key_ptr()[index] = lb;
    }

    void set_node_val(const std::string &key, const ceph::bufferlist &val) {
      static_assert(!is_const);
      auto node_key = get_node_key();
      assert(key.size() == node_key.key_len);
      assert(val.length() == node_key.val_len);
      ::memcpy(get_node_val_ptr(), key.data(), key.size());
      auto bliter = val.begin();
      bliter.copy(node_key.val_len, get_node_val_ptr() + node_key.key_len);
    }

  public:
    uint16_t get_index() const {
      return index;
    }

    std::string get_key() const {
      return std::string(
	get_node_val_ptr(),
	get_node_key().key_len);
    }

    std::string get_str_val() const {
      auto node_key = get_node_key();
      return std::string(
	get_node_val_ptr() + node_key.key_len,
	get_node_key().val_len);
    }

    ceph::bufferlist get_val() const {
      auto node_key = get_node_key();
      ceph::bufferlist bl;
      ceph::bufferptr bptr(
	get_node_val_ptr() + node_key.key_len,
	get_node_key().val_len);
      bl.append(bptr);
      return bl;
    }
  };
  using const_iterator = iter_t<true>;
  using iterator = iter_t<false>;

public:
  void journal_leaf_insert(
    const_iterator _iter,
    const std::string &key,
    const ceph::bufferlist &val,
    delta_leaf_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->insert(
        key,
        val);
    }
    leaf_insert(iter, key, val);
  }

  void journal_leaf_update(
    const_iterator _iter,
    const std::string &key,
    const ceph::bufferlist &val,
    delta_leaf_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->remove(iter->get_key());
      recorder->insert(key, val);
    }
    leaf_update(iter, key, val);
  }

  void journal_leaf_remove(
    const_iterator _iter,
    delta_leaf_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->remove(iter->get_key());
    }
    leaf_remove(iter);
  }

  StringKVLeafNodeLayout(char *buf) :
    buf(buf) {}

  const_iterator iter_begin() const {
    return const_iterator(
      this,
      0);
  }

  const_iterator iter_end() const {
    return const_iterator(
      this,
      get_size());
  }

  iterator iter_begin() {
    return iterator(
      this,
      0);
  }

  iterator iter_end() {
    return iterator(
      this,
      get_size());
  }

  const_iterator iter_idx(uint16_t off) const {
    return const_iterator(
      this,
      off);
  }

  const_iterator string_lower_bound(std::string_view str) const {
    uint16_t start = 0, end = get_size();
    while (start != end) {
      unsigned mid = (start + end) / 2;
      const_iterator iter(this, mid);
      std::string s = iter->get_key();
      if (s < str) {
        start = ++mid;
      } else if (s > str) {
        end = mid;
      } else {
        return iter;
      }
    }
    return const_iterator(this, start);
  }

  iterator string_lower_bound(std::string_view str) {
    const auto &tref = *this;
    return iterator(this, tref.string_lower_bound(str).index);
  }

  const_iterator string_upper_bound(std::string_view str) const {
    auto ret = iter_begin();
    for (; ret != iter_end(); ++ret) {
      std::string s = ret->get_key();
      if (s > str)
        break;
    }
    return ret;
  }

  iterator string_upper_bound(std::string_view str) {
    const auto &tref = *this;
    return iterator(this, tref.string_upper_bound(str).index);
  }

  const_iterator find_string_key(std::string_view str) const {
    auto ret = iter_begin();
    for (; ret != iter_end(); ++ret) {
      std::string s = ret->get_key();
      if (s == str)
        break;
    }
    return ret;
  }
  iterator find_string_key(std::string_view str) {
    const auto &tref = *this;
    return iterator(this, tref.find_string_key(str).index);
  }

  const_iterator get_split_pivot() const {
    uint32_t total_size = omap_leaf_key_t(get_node_key_ptr()[get_size()-1]).key_off;
    uint32_t pivot_size = total_size / 2;
    uint32_t size = 0;
    for (auto ite = iter_begin(); ite < iter_end(); ite++) {
      auto node_key = ite->get_node_key();
      size += node_key.key_len + node_key.val_len;
      if (size >= pivot_size){
        return ite;
      }
    }
    return iter_end();
  }

  uint32_t get_size() const {
    ceph_le32 &size = *layout.template Pointer<0>(buf);
    return uint32_t(size);
  }

  /**
   * set_size
   *
   * Set size representation to match size
   */
  void set_size(uint32_t size) {
    ceph_le32 s;
    s = size;
    *layout.template Pointer<0>(buf) = s;
  }

  /**
   * get_meta/set_meta
   *
   * Enables stashing a templated type within the layout.
   * Cannot be modified after initial write as it is not represented
   * in delta_t
   */
  omap_node_meta_t get_meta() const {
    omap_node_meta_le_t &metaint = *layout.template Pointer<1>(buf);
    return omap_node_meta_t(metaint);
  }
  void set_meta(const omap_node_meta_t &meta) {
    *layout.template Pointer<1>(buf) = omap_node_meta_le_t(meta);
  }

  uint32_t used_space() const {
    uint32_t count = get_size();
    if (count) {
      omap_leaf_key_t last_key = omap_leaf_key_t(get_node_key_ptr()[count-1]);
      return last_key.key_off + count * sizeof(omap_leaf_key_le_t);
    } else {
      return 0;
    }
  }

  uint32_t free_space() const {
    return capacity() - used_space();
  }

  uint32_t capacity() const {
    return OMAP_LEAF_BLOCK_SIZE
      - (reinterpret_cast<char*>(layout.template Pointer<2>(buf))
      - reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }

  bool is_overflow(size_t ksize, size_t vsize) const {
    return free_space() < (sizeof(omap_leaf_key_le_t) + ksize + vsize);
  }

  bool is_overflow(const StringKVLeafNodeLayout &rhs) const {
    return free_space() < rhs.used_space();
  }

  bool below_min() const {
    return free_space() > (capacity() / 2);
  }

  bool operator==(const StringKVLeafNodeLayout &rhs) const {
    if (get_size() != rhs.get_size()) {
      return false;
    }

    auto iter = iter_begin();
    auto iter2 = rhs.iter_begin();
    while (iter != iter_end()) {
      if(iter->get_key() != iter2->get_key() ||
         iter->get_val() != iter2->get_val()) {
	return false;
      }
      iter++;
      iter2++;
    }
    return true;
  }

  /**
   * split_into
   *
   * Takes *this and splits its contents into left and right.
   */
  std::string split_into(
    StringKVLeafNodeLayout &left,
    StringKVLeafNodeLayout &right) const {
    auto piviter = get_split_pivot();
    assert (piviter != iter_end());

    copy_from_foreign(left.iter_begin(), iter_begin(), piviter);
    left.set_size(piviter - iter_begin());

    copy_from_foreign(right.iter_begin(), piviter, iter_end());
    right.set_size(iter_end() - piviter);

    auto [lmeta, rmeta] = get_meta().split_into();
    left.set_meta(lmeta);
    right.set_meta(rmeta);

    return piviter->get_key();
  }

  /**
   * merge_from
   *
   * Takes two nodes and copies their contents into *this.
   *
   * precondition: left.size() + right.size() < CAPACITY
   */
  void merge_from(
    const StringKVLeafNodeLayout &left,
    const StringKVLeafNodeLayout &right)
  {
    copy_from_foreign(
      iter_end(),
      left.iter_begin(),
      left.iter_end());
    set_size(left.get_size());
    copy_from_foreign(
      iter_end(),
      right.iter_begin(),
      right.iter_end());
    set_size(left.get_size() + right.get_size());
    set_meta(omap_node_meta_t::merge_from(left.get_meta(), right.get_meta()));
  }

  /**
   * balance_into_new_nodes
   *
   * Takes the contents of left and right and copies them into
   * replacement_left and replacement_right such that
   * the size of replacement_left side just >= 1/2 of the total size (left + right).
   */
  static std::string balance_into_new_nodes(
    const StringKVLeafNodeLayout &left,
    const StringKVLeafNodeLayout &right,
    StringKVLeafNodeLayout &replacement_left,
    StringKVLeafNodeLayout &replacement_right)
  {
    uint32_t left_size = omap_leaf_key_t(left.get_node_key_ptr()[left.get_size()-1]).key_off;
    uint32_t right_size = omap_leaf_key_t(right.get_node_key_ptr()[right.get_size()-1]).key_off;
    uint32_t total = left_size + right_size;
    uint32_t pivot_size = total / 2;
    uint32_t pivot_idx = 0;
    if (pivot_size < left_size) {
      uint32_t size = 0;
      for (auto ite = left.iter_begin(); ite < left.iter_end(); ite++) {
        auto node_key = ite->get_node_key();
        size += node_key.key_len + node_key.val_len;
        if (size >= pivot_size){
          pivot_idx = ite.get_index();
          break;
        }
      }
    } else {
      uint32_t more_size = pivot_size - left_size;
      uint32_t size = 0;
      for (auto ite = right.iter_begin(); ite < right.iter_end(); ite++) {
        auto node_key = ite->get_node_key();
        size += node_key.key_len + node_key.val_len;
        if (size >= more_size){
          pivot_idx = ite.get_index() + left.get_size();
          break;
        }
      }
    }

    auto replacement_pivot = pivot_idx >= left.get_size() ?
      right.iter_idx(pivot_idx - left.get_size())->get_key() :
      left.iter_idx(pivot_idx)->get_key();

    if (pivot_size < left_size) {
      copy_from_foreign(
        replacement_left.iter_end(),
        left.iter_begin(),
        left.iter_idx(pivot_idx));
      replacement_left.set_size(pivot_idx);

      copy_from_foreign(
        replacement_right.iter_end(),
        left.iter_idx(pivot_idx),
        left.iter_end());
      replacement_right.set_size(left.get_size() - pivot_idx);

      copy_from_foreign(
        replacement_right.iter_end(),
        right.iter_begin(),
        right.iter_end());
      replacement_right.set_size(right.get_size() + left.get_size() - pivot_idx);
    } else {
      copy_from_foreign(
        replacement_left.iter_end(),
        left.iter_begin(),
        left.iter_end());
      replacement_left.set_size(left.get_size());

      copy_from_foreign(
        replacement_left.iter_end(),
        right.iter_begin(),
        right.iter_idx(pivot_idx - left.get_size()));
      replacement_left.set_size(pivot_idx);

      copy_from_foreign(
        replacement_right.iter_end(),
        right.iter_idx(pivot_idx - left.get_size()),
        right.iter_end());
      replacement_right.set_size(right.get_size() + left.get_size() - pivot_idx);
    }

    auto [lmeta, rmeta] = omap_node_meta_t::rebalance(
      left.get_meta(), right.get_meta());
    replacement_left.set_meta(lmeta);
    replacement_right.set_meta(rmeta);
    return replacement_pivot;
  }

private:
  void leaf_insert(
    iterator iter,
    const std::string &key,
    const bufferlist &val) {
    if (iter != iter_begin()) {
      assert((iter - 1)->get_key() < key);
    }
    if (iter != iter_end()) {
      assert(iter->get_key() > key);
    }
    assert(!is_overflow(key.size(), val.length()));
    omap_leaf_key_t node_key;
    if (iter == iter_begin()) {
      node_key.key_off = key.size() + val.length();
      node_key.key_len = key.size();
      node_key.val_len = val.length();
    } else {
      node_key.key_off = (iter - 1)->get_node_key().key_off +
	(key.size() + val.length());
      node_key.key_len = key.size();
      node_key.val_len = val.length();
    }
    if (get_size() != 0 && iter != iter_end())
      copy_from_local(node_key.key_len + node_key.val_len, iter + 1, iter, iter_end());

    iter->set_node_key(node_key);
    set_size(get_size() + 1);
    iter->set_node_val(key, val);
  }

  void leaf_update(
    iterator iter,
    const std::string &key,
    const ceph::bufferlist &val) {
    assert(iter != iter_end());
    leaf_remove(iter);
    assert(!is_overflow(key.size(), val.length()));
    leaf_insert(iter, key, val);
  }

  void leaf_remove(iterator iter) {
    assert(iter != iter_end());
    if ((iter + 1) != iter_end()) {
      omap_leaf_key_t key = iter->get_node_key();
      copy_from_local(key.key_len + key.val_len, iter, iter + 1, iter_end());
    }
    set_size(get_size() - 1);
  }

  /**
   * get_key_ptr
   *
   * Get pointer to start of key array
   */
  omap_leaf_key_le_t *get_node_key_ptr() {
    return L::Partial(1, 1, get_size()).template Pointer<2>(buf);
  }
  const omap_leaf_key_le_t *get_node_key_ptr() const {
    return L::Partial(1, 1, get_size()).template Pointer<2>(buf);
  }

};

inline void delta_inner_t::replay(StringKVInnerNodeLayout &l) {
  switch (op) {
    case op_t::INSERT: {
      l.inner_insert(l.string_lower_bound(key), key, addr);
      break;
    }
    case op_t::UPDATE: {
      auto iter = l.find_string_key(key);
      assert(iter != l.iter_end());
      l.inner_update(iter, addr);
      break;
    }
    case op_t::REMOVE: {
      auto iter = l.find_string_key(key);
      assert(iter != l.iter_end());
      l.inner_remove(iter);
      break;
    }
    default:
      assert(0 == "Impossible");
  }
}

inline void delta_leaf_t::replay(StringKVLeafNodeLayout &l) {
  switch (op) {
    case op_t::INSERT: {
      l.leaf_insert(l.string_lower_bound(key), key, val);
      break;
    }
    case op_t::UPDATE: {
      auto iter = l.find_string_key(key);
      assert(iter != l.iter_end());
      l.leaf_update(iter, key, val);
      break;
    }
    case op_t::REMOVE: {
      auto iter = l.find_string_key(key);
      assert(iter != l.iter_end());
      l.leaf_remove(iter);
      break;
    }
    default:
      assert(0 == "Impossible");
  }
}

}
