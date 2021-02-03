// -*- mode:C++; tab-width:8; c-basic-index:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <string>

#include "include/byteorder.h"
#include "include/denc.h"

#include "crimson/common/layout.h"
#include "crimson/common/fixed_kv_node_layout.h"
#include "crimson/os/seastore/omap_manager/btree/omap_types.h"

#define BLOCK_SIZE 4096
namespace crimson::os::seastore::omap_manager {

template <
  typename Meta,
  typename MetaInt,
  bool VALIDATE_INVARIANTS=true> class StringKVInnerNodeLayout;

template <
  typename Meta,
  typename MetaInt,
  bool VALIDATE_INVARIANTS=true> class StringKVLeafNodeLayout;


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
/* copy_from_local
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
   memmove(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
  }

/**
 * StringKVInnerNodeLayout
 *
 * Reusable implementation of a fixed size key mapping
 * omap_inner_key_t(fixed) -> V(string) with internal representations omap_inner_key_le_t.
 *
 * Uses absl::container_internal::Layout for the actual key memory layout.
 *
 * The primary interface exposed is centered on the iterator
 * and related methods.
 *
 * Also included are helpers for doing splits and merges as for a btree.
 */
template <
  typename Meta,
  typename MetaInt,
  bool VALIDATE_INVARIANTS>
class StringKVInnerNodeLayout {
  char *buf = nullptr;

  using L = absl::container_internal::Layout<ceph_le32, MetaInt, omap_inner_key_le_t>;
  static constexpr L layout{1, 1, 1}; // = L::Partial(1, 1, 1);

public:
  template <bool is_const>
  struct iter_t : public std::iterator<std::input_iterator_tag, StringKVInnerNodeLayout> {
    friend class StringKVInnerNodeLayout;

    template <typename iterator, typename const_iterator>
    friend void copy_from_foreign(iterator, const_iterator, const_iterator);
    template <typename iterator>
    friend void copy_from_local(unsigned, iterator, iterator, iterator);

    using parent_t = typename crimson::common::maybe_const_t<StringKVInnerNodeLayout, is_const>::type;

    parent_t node;
    uint16_t index;

    iter_t(
      parent_t parent,
      uint16_t index) : node(parent), index(index) {}

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
      return !(*this == rhs);
    }

    omap_inner_key_t get_node_key() const {
      omap_inner_key_le_t kint = node->get_node_key_ptr()[index];
      return omap_inner_key_t(kint);
    }

    char *get_node_val_ptr() {
      auto tail = node->buf + BLOCK_SIZE;
      if (*this == node->iter_end())
        return tail;
      else {
        return tail - static_cast<uint32_t>(get_node_key().key_off);
      }
    }

    const char *get_node_val_ptr() const {
      auto tail = node->buf + BLOCK_SIZE;
      if ( *this == node->iter_end())
        return tail;
      else {
        return tail - static_cast<uint32_t>(get_node_key().key_off);
      }
    }

    void set_node_val(const std::string &val) {
      static_assert(!is_const);
      std::strcpy((char*)get_node_val_ptr(), val.c_str()); //copy char* to char* include "\0"
    }

    std::string get_node_val(){
     std::string s(get_node_val_ptr());
     return s;
    }
    std::string get_node_val() const{
      std::string s(get_node_val_ptr());
      return s;
    }

    bool contains(std::string_view key) const {
      auto next = *this + 1;
      if (next == node->iter_end())
        return get_node_val() <= key;

      return (get_node_val() <= key) && (next->get_node_val() > key);
    }

    uint16_t get_index() const {
      return index;
    }

  private:
    int get_right_offset() const {
      return get_node_key().key_off;
    }

    int get_right_offset_end() const {
      if (index == 0)
        return 0;
      else
        return (*this - 1)->get_right_offset();
    }

    char *get_right_ptr() {
      return node->buf + BLOCK_SIZE - get_right_offset();
    }

    const char *get_right_ptr() const {
      static_assert(!is_const);
      return node->buf + BLOCK_SIZE - get_right_offset();
    }

    char *get_right_ptr_end() {
      if (index == 0)
        return node->buf + BLOCK_SIZE;
      else
        return (*this - 1)->get_right_ptr();
    }

    const char *get_right_ptr_end() const {
      if (index == 0)
        return node->buf + BLOCK_SIZE;
      else
        return (*this - 1)->get_right_ptr();
    }

    void update_offset(int offset) {
      auto key = get_node_key();
      assert(offset + key.key_off >= 0);
      key.key_off += offset;
      set_node_key(key);
    }

    void set_node_key(omap_inner_key_t _lb) const {
      static_assert(!is_const);
      omap_inner_key_le_t lb;
      lb = _lb;
      node->get_node_key_ptr()[index] = lb;
    }

    typename crimson::common::maybe_const_t<char, is_const>::type get_node_key_ptr() const {
      return reinterpret_cast<
        typename crimson::common::maybe_const_t<char, is_const>::type>(
	        node->get_node_key_ptr() + index);
    }

  };
  using const_iterator = iter_t<true>;
  using iterator = iter_t<false>;

  struct delta_inner_t {
    enum class op_t : uint_fast8_t {
      INSERT,
      UPDATE,
      REMOVE,
    } op;
    omap_inner_key_t key;
    std::string val;

    DENC(delta_inner_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.op, p);
      denc(v.key, p);
      denc(v.val, p);
      DENC_FINISH(p);
    }

    void replay(StringKVInnerNodeLayout &l) {
      switch (op) {
      case op_t::INSERT: {
        l.inner_insert(l.string_lower_bound(val), key, val);
        break;
      }
      case op_t::UPDATE: {
        auto iter = l.find_string_key(val);
        assert(iter != l.iter_end());
        l.inner_update(iter, key);
        break;
      }
      case op_t::REMOVE: {
        auto iter = l.find_string_key(val);
        assert(iter != l.iter_end());
        l.inner_remove(iter);
        break;
      }
      default:
        assert(0 == "Impossible");
      }
    }

    bool operator==(const delta_inner_t &rhs) const {
      return op == rhs.op &&
             key == rhs.key &&
             val == rhs.val;
    }
  };

public:
  class delta_inner_buffer_t {
    std::vector<delta_inner_t> buffer;
  public:
    bool empty() const {
      return buffer.empty();
    }
    void insert(
      const omap_inner_key_t key,
      std::string_view val) {
      omap_inner_key_le_t k;
      k = key;
      buffer.push_back(
       delta_inner_t{
         delta_inner_t::op_t::INSERT,
         k,
         val.data()
       });
    }
    void update(
      const omap_inner_key_t key,
      std::string_view val) {
      omap_inner_key_le_t k;
      k = key;
      buffer.push_back(
       delta_inner_t{
         delta_inner_t::op_t::UPDATE,
         k,
         val.data()
       });
    }
    void remove(std::string_view val) {
      buffer.push_back(
        delta_inner_t{
          delta_inner_t::op_t::REMOVE,
          omap_inner_key_le_t(),
          val.data()
        });
    }

    void replay(StringKVInnerNodeLayout &node) {
      for (auto &i: buffer) {
        i.replay(node);
      }
    }
    size_t get_bytes() const {
      size_t size = 0;
      for (auto &i: buffer) {
        size += sizeof(i.op_t) + sizeof(i.key) + i.val.size();
      }
      return size;
    }
    //copy out
    void encode(ceph::bufferlist &bl) {
      uint32_t num = buffer.size();
      ::encode(num, bl);
      for (auto &&i: buffer) {
        ::encode(i, bl);
      }
      buffer.clear();
    }
    //copy in
    void decode(const ceph::bufferlist &bl) {
      auto p = bl.cbegin();
      uint32_t num;
      ::decode(num, p);
      while (num--) {
        delta_inner_t delta;
        ::decode(delta, p);
        buffer.push_back(delta);
      }
    }

    bool operator==(const delta_inner_buffer_t &rhs) const {
      return buffer == rhs.buffer;
    }
  };

  void journal_inner_insert(
    const_iterator _iter,
    const laddr_t laddr,
    const std::string &key,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    omap_inner_key_t node_key;
    node_key.laddr = laddr;
    node_key.key_len = key.size() + 1;
    node_key.key_off = iter.get_index() == 0 ?
                       node_key.key_len :
                       (iter - 1).get_node_key().key_off + node_key.key_len;
    if (recorder) {
      recorder->insert(
        node_key,
        key);
    }
    inner_insert(iter, node_key, key);
  }

  void journal_inner_update(
    const_iterator _iter,
    const laddr_t laddr,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    auto node_key = iter.get_node_key();
    node_key.laddr = laddr;
    if (recorder) {
      recorder->update(node_key, iter->get_node_val());
    }
    inner_update(iter, node_key);
  }

  void journal_inner_replace(
    const_iterator _iter,
    const laddr_t laddr,
    const std::string &key,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    omap_inner_key_t node_key;
    node_key.laddr = laddr;
    node_key.key_len = key.size() + 1;
    node_key.key_off = iter.get_index() == 0?
                       node_key.key_len :
                       (iter - 1).get_node_key().key_off + node_key.key_len;
    if (recorder) {
      recorder->remove(iter->get_node_val());
      recorder->insert(node_key, key);
    }
    inner_replace(iter, node_key, key);
  }

  void journal_inner_remove(
    const_iterator _iter,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->remove(iter->get_node_val());
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
      std::string s = iter->get_node_val();
      if (s < str) {
        start = ++mid;
      } else if ( s > str) {
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
     std::string s = ret->get_node_val();
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
     std::string s = ret->get_node_val();
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
    uint32_t total_size = omap_inner_key_t(get_node_key_ptr()[get_size()-1]).key_off;
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
  Meta get_meta() const {
    MetaInt &metaint = *layout.template Pointer<1>(buf);
    return Meta(metaint);
  }
  void set_meta(const Meta &meta) {
    *layout.template Pointer<1>(buf) = MetaInt(meta);
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
    return BLOCK_SIZE - (reinterpret_cast<char*>(layout.template Pointer<2>(buf))-
                        reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }

  bool is_overflow(size_t ksize) const {
    return free_space() < (sizeof(omap_inner_key_le_t) + ksize);
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
      if (iter->get_node_key() != iter2->get_node_key() ||
          iter->get_node_val() != iter2->get_node_val()) {
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

    return piviter->get_node_val();
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
    set_meta(Meta::merge_from(left.get_meta(), right.get_meta()));
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
      right.iter_idx(pivot_idx - left.get_size())->get_node_val() :
      left.iter_idx(pivot_idx)->get_node_val();

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

    auto [lmeta, rmeta] = Meta::rebalance(
      left.get_meta(), right.get_meta());
    replacement_left.set_meta(lmeta);
    replacement_right.set_meta(rmeta);
    return replacement_pivot;
  }

private:
  void inner_insert(
    iterator iter,
    const omap_inner_key_t key,
    const std::string &val) {
    if (VALIDATE_INVARIANTS) {
      if (iter != iter_begin()) {
        assert((iter - 1)->get_node_val() < val);
      }
      if (iter != iter_end()) {
        assert(iter->get_node_val() > val);
      }
      assert(is_overflow(val.size() + 1) == false);
    }
    if (get_size() != 0 && iter != iter_end())
      copy_from_local(key.key_len, iter + 1, iter, iter_end());

    iter->set_node_key(key);
    set_size(get_size() + 1);
    iter->set_node_val(val);
  }

  void inner_update(
    iterator iter,
    omap_inner_key_t key ) {
    assert(iter != iter_end());
    iter->set_node_key(key);
  }

  void inner_replace(
    iterator iter,
    const omap_inner_key_t key,
    const std::string &val) {
    assert(iter != iter_end());
    if (VALIDATE_INVARIANTS) {
      if (iter != iter_begin()) {
        assert((iter - 1)->get_node_val() < val);
      }
      if ((iter + 1) != iter_end()) {
        assert((iter + 1)->get_node_val() > val);
      }
      assert(is_overflow(val.size() + 1) == false);
    }
    inner_remove(iter);
    inner_insert(iter, key, val);
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

template <
  typename Meta,
  typename MetaInt,
  bool VALIDATE_INVARIANTS>
class StringKVLeafNodeLayout {
  char *buf = nullptr;

  using L = absl::container_internal::Layout<ceph_le32, MetaInt, omap_leaf_key_le_t>;
  static constexpr L layout{1, 1, 1}; // = L::Partial(1, 1, 1);

public:
  template <bool is_const>
  struct iter_t {
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

    omap_leaf_key_t get_node_key() const {
      omap_leaf_key_le_t kint = node->get_node_key_ptr()[index];
      return omap_leaf_key_t(kint);
    }

    char *get_node_val_ptr() {
      auto tail = node->buf + BLOCK_SIZE;
      if ( *this == node->iter_end())
        return tail;
      else
        return tail - static_cast<int>(get_node_key().key_off);
    }

    const char *get_node_val_ptr() const {
      auto tail = node->buf + BLOCK_SIZE;
      if ( *this == node->iter_end())
        return tail;
      else
        return tail - static_cast<int>(get_node_key().key_off);
    }

    char *get_string_val_ptr() {
      auto tail = node->buf + BLOCK_SIZE;
      return tail - static_cast<int>(get_node_key().val_off);
    }

    const char *get_string_val_ptr() const {
      auto tail = node->buf + BLOCK_SIZE;
      return tail - static_cast<int>(get_node_key().val_off);
    }

    void set_node_val(const std::string &val) const {
      static_assert(!is_const);
      std::strcpy((char*)get_node_val_ptr(), val.c_str()); //copy char* to char* include "\0"
    }

    std::string get_node_val() {
      std::string s(get_node_val_ptr());
      return s;
    }
    std::string get_node_val() const{
      std::string s(get_node_val_ptr());
      return s;
    }

    void set_string_val(const std::string &val) {
      static_assert(!is_const);
      std::strcpy((char*)get_string_val_ptr(), val.c_str()); //copy char* to char* include "\0"
    }

    std::string get_string_val() const {
      std::string s(get_string_val_ptr());
      return s;
    }

    bool contains(std::string_view key) const {
      auto next = *this + 1;
      if (*this == node->iter_begin()){
        if (next->get_node_val() > key)
          return true;
        else
          return false;
      }
      if (next == node->iter_end())
        return get_node_val() <= key;

      return (get_node_val() <= key) && (next->get_node_val() > key);
    }

    uint16_t get_index() const {
      return index;
    }

  private:
    int get_right_offset() const {
      return get_node_key().key_off;
    }

    int get_right_offset_end() const {
      if (index == 0)
	      return 0;
      else
	      return (*this - 1)->get_right_offset();
    }

    char *get_right_ptr() {
      return node->buf + BLOCK_SIZE - get_right_offset();
    }

    const char *get_right_ptr() const {
      static_assert(!is_const);
      return node->buf + BLOCK_SIZE - get_right_offset();
    }

    char *get_right_ptr_end() {
      if (index == 0)
	      return node->buf + BLOCK_SIZE;
      else
	      return (*this - 1)->get_right_ptr();
    }

    const char *get_right_ptr_end() const {
      if (index == 0)
	      return node->buf + BLOCK_SIZE;
      else
	      return (*this - 1)->get_right_ptr();
    }

    void update_offset(int offset) {
      auto key = get_node_key();
      assert(offset + key.key_off >= 0);
      assert(offset + key.val_off >= 0);
      key.key_off += offset;
      key.val_off += offset;
      set_node_key(key);
    }

    void set_node_key(omap_leaf_key_t _lb) const {
      static_assert(!is_const);
      omap_leaf_key_le_t lb;
      lb = _lb;
      node->get_node_key_ptr()[index] = lb;
    }

    typename crimson::common::maybe_const_t<char, is_const>::type get_node_key_ptr() const {
      return reinterpret_cast<
        typename crimson::common::maybe_const_t<char, is_const>::type>(
        node->get_node_key_ptr() + index);
    }
  };
  using const_iterator = iter_t<true>;
  using iterator = iter_t<false>;

  struct delta_leaf_t {
    enum class op_t : uint_fast8_t {
      INSERT,
      UPDATE,
      REMOVE,
    } op;
    std::string key;
    std::string val;

    DENC(delta_leaf_t, v, p) {
      DENC_START(1, 1, p);
      denc(v.op, p);
      denc(v.key, p);
      denc(v.val, p);
      DENC_FINISH(p);
    }

    void replay(StringKVLeafNodeLayout &l) {
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

    bool operator==(const delta_leaf_t &rhs) const {
      return op == rhs.op &&
        key == rhs.key &&
        val == rhs.val;
    }
  };

public:
  class delta_leaf_buffer_t {
    std::vector<delta_leaf_t> buffer;
  public:
    bool empty() const {
      return buffer.empty();
    }
    void insert(
      std::string_view key,
      std::string_view val) {
      buffer.push_back(
       delta_leaf_t{
         delta_leaf_t::op_t::INSERT,
         key.data(),
         val.data()
       });
    }
    void update(
      std::string_view key,
      std::string_view val) {
      buffer.push_back(
       delta_leaf_t{
         delta_leaf_t::op_t::UPDATE,
         key.data(),
         val.data()
       });
    }
    void remove(std::string_view key) {
      buffer.push_back(
       delta_leaf_t{
         delta_leaf_t::op_t::REMOVE,
         key.data(),
         ""
       });
    }

    void replay(StringKVLeafNodeLayout &node) {
      for (auto &i: buffer) {
        i.replay(node);
      }
    }
    size_t get_bytes() const {
      size_t size = 0;
      for (auto &i: buffer) {
        size += sizeof(i.op_t) + i.key.size() + i.val.size();
      }
      return size;
    }
    //copy out
    void encode(ceph::bufferlist &bl) {
      uint32_t num = buffer.size();
      ::encode(num, bl);
      for (auto &&i: buffer) {
        ::encode(i, bl);
      }
      buffer.clear();
    }
    //copy in
    void decode(const ceph::bufferlist &bl) {
      auto p = bl.cbegin();
      uint32_t num;
      ::decode(num, p);
      while (num--) {
        delta_leaf_t delta;
        ::decode(delta, p);
        buffer.push_back(delta);
      }
    }

    bool operator==(const delta_leaf_buffer_t &rhs) const {
      return buffer == rhs.buffer;
    }
  };

  void journal_leaf_insert(
    const_iterator _iter,
    const std::string &key,
    const std::string &val,
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
    const std::string &val,
    delta_leaf_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->remove(iter->get_node_val());
      recorder->insert(key, val);
    }
    leaf_update(iter, key, val);
  }


  void journal_leaf_remove(
    const_iterator _iter,
    delta_leaf_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    if (recorder) {
      recorder->remove(iter->get_node_val());
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
      std::string s = iter->get_node_val();
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
      std::string s = ret->get_node_val();
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
      std::string s = ret->get_node_val();
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
  Meta get_meta() const {
    MetaInt &metaint = *layout.template Pointer<1>(buf);
    return Meta(metaint);
  }
  void set_meta(const Meta &meta) {
    *layout.template Pointer<1>(buf) = MetaInt(meta);
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
    return BLOCK_SIZE - (reinterpret_cast<char*>(layout.template Pointer<2>(buf))-
                        reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }

  bool is_overflow(size_t ksize, size_t vsize) const {
    return free_space() < (sizeof(omap_leaf_key_le_t) + ksize + vsize);
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
      if(iter->get_node_key() != iter2->get_node_key() ||
	       iter->get_node_val() != iter2->get_node_val() ||
         iter->get_string_val() != iter2->get_string_val()){
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

    return piviter->get_node_val();
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
    set_meta(Meta::merge_from(left.get_meta(), right.get_meta()));
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
      right.iter_idx(pivot_idx - left.get_size())->get_node_val() :
      left.iter_idx(pivot_idx)->get_node_val();

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

    auto [lmeta, rmeta] = Meta::rebalance(
      left.get_meta(), right.get_meta());
    replacement_left.set_meta(lmeta);
    replacement_right.set_meta(rmeta);
    return replacement_pivot;
  }

private:
  void leaf_insert(
    iterator iter,
    const std::string &key,
    const std::string &val) {
    if (VALIDATE_INVARIANTS) {
      if (iter != iter_begin()) {
        assert((iter - 1)->get_node_val() < key);
      }
      if (iter != iter_end()) {
        assert(iter->get_node_val() > key);
      }
      assert(is_overflow(key.size() + 1, val.size() + 1) == false);
    }
    omap_leaf_key_t node_key;
    if (iter == iter_begin()) {
      node_key.key_off = key.size() + 1 + val.size() + 1;
      node_key.key_len = key.size() + 1;
      node_key.val_off = val.size() + 1;
      node_key.val_len = val.size() + 1;
    } else {
      node_key.key_off = (iter - 1)->get_node_key().key_off + (key.size() + 1 + val.size() + 1);
      node_key.key_len = key.size() + 1;
      node_key.val_off = (iter - 1)->get_node_key().key_off + (val.size() + 1);
      node_key.val_len = val.size() + 1;
    }
    if (get_size() != 0 && iter != iter_end())
      copy_from_local(node_key.key_len + node_key.val_len, iter + 1, iter, iter_end());

    iter->set_node_key(node_key);
    set_size(get_size() + 1);
    iter->set_node_val(key);
    iter->set_string_val(val);
  }

  void leaf_update(
    iterator iter,
    const std::string &key,
    const std::string &val) {
    assert(iter != iter_end());
    if (VALIDATE_INVARIANTS) {
      assert(is_overflow(0, val.size() + 1) == false);
    }
    leaf_remove(iter);
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

}
using namespace crimson::os::seastore::omap_manager;
using InnerNodeLayout = StringKVInnerNodeLayout<omap_node_meta_t, omap_node_meta_le_t>;
WRITE_CLASS_DENC_BOUNDED(InnerNodeLayout::delta_inner_t)
using LeafNodeLayout = StringKVLeafNodeLayout<omap_node_meta_t, omap_node_meta_le_t>;
WRITE_CLASS_DENC_BOUNDED(LeafNodeLayout::delta_leaf_t)
