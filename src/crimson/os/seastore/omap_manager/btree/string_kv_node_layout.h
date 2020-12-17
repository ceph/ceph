// -*- mode:C++; tab-width:8; c-basic-index:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <string>

#include "include/byteorder.h"

#include "crimson/common/layout.h"
#include "crimson/common/fixed_kv_node_layout.h"
#include "crimson/os/seastore/omap_manager/btree/omap_types.h"

#define BlockSize 4096
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
  struct iter_t {
    friend class StringKVInnerNodeLayout;
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

    // Work nicely with for loops without requiring a nested type.
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
      auto tail = node->buf + BlockSize;
      if (*this == node->iter_end())
        return tail;
      else {
        return tail - static_cast<uint32_t>(get_node_key().key_off);
      }
    }

    const char *get_node_val_ptr() const {
      auto tail = node->buf + BlockSize;
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

    bool contains(const std::string &key) const {
      auto next = *this + 1;
      if (next == node->iter_end())
        return get_node_val() <= key;

      return (get_node_val() <= key) && (next->get_node_val() > key);
    }

    uint16_t get_index() const {
      return index;
    }

  private:
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
    enum class op_t : uint8_t {
      INSERT,
      UPDATE,
      REMOVE,
    } op;
    omap_inner_key_le_t key;
    std::string val;

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
      const omap_inner_key_t &key,
      const std::string val) {
      omap_inner_key_le_t k;
      k = key;
      buffer.push_back(
       delta_inner_t{
         delta_inner_t::op_t::INSERT,
         k,
         val
       });
    }
    void update(
      const omap_inner_key_t &key,
      const std::string &val) {
      omap_inner_key_le_t k;
      k = key;
      buffer.push_back(
       delta_inner_t{
         delta_inner_t::op_t::UPDATE,
         k,
         val
       });
    }
    void remove(std::string val) {
      buffer.push_back(
       delta_inner_t{
         delta_inner_t::op_t::REMOVE,
         omap_inner_key_le_t(),
         val
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
      using ceph::encode;
      uint32_t num = buffer.size();
      encode(num, bl);
      for (auto &&i: buffer) {
        encode(i.op, bl);
        bl.append((char*)&(i.key), sizeof(i.key));
        encode(i.val, bl);
      }
      buffer.clear();
    }
    //copy in
    void decode(const ceph::bufferlist &bl) {
      using ceph::decode;
      auto p = bl.cbegin();
      uint32_t num;
      decode (num, p);
      while (num--) {
        delta_inner_t delta;
        decode(delta.op, p);
        omap_inner_key_le_t key;
        p.copy(sizeof(key), (char*)&(key));
        delta.key = key;
        decode(delta.val, p);
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
    const std::string val,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    omap_inner_key_t node_key;
    node_key.laddr = laddr;
    node_key.key_len = val.size() + 1;
    node_key.key_off = iter.get_index() == 0 ?
                       node_key.key_len :
                       (iter - 1).get_node_key().key_off + node_key.key_len;
    if (recorder) {
      recorder->insert(
        node_key,
        val);
    }
    inner_insert(iter, node_key, val);
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
    const std::string val,
    delta_inner_buffer_t *recorder) {
    auto iter = iterator(this, _iter.index);
    omap_inner_key_t node_key;
    node_key.laddr = laddr;
    node_key.key_len = val.size() + 1;
    node_key.key_off = iter.get_index() == 0?
                       node_key.key_len :
                       (iter - 1).get_node_key().key_off + node_key.key_len;
    if (recorder) {
      recorder->remove(iter->get_node_val());
      recorder->insert(node_key, val);
    }
    inner_replace(iter, node_key, val);
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

  const_iterator string_lower_bound(std::string str) const {
    uint16_t start = 0, end = get_size();
    while (start != end) {
      unsigned mid = (start + end) / 2;
      const_iterator iter(this, mid);
      std::string s = iter->get_node_val();
      if (s < str)
        start = ++mid;
      if ( s > str)
        end = mid;
      if (s == str)
        return iter;
    }
    return const_iterator(this, start);
  }

  iterator string_lower_bound(std::string str) {
    const auto &tref = *this;
    return iterator(this, tref.string_lower_bound(str).index);
  }

  const_iterator string_upper_bound(std::string str) const {
    auto ret = iter_begin();
    for (; ret != iter_end(); ++ret) {
     std::string s = ret->get_node_val();
      if (s > str)
        break;
    }
      return ret;
  }

  iterator string_upper_bound(std::string str) {
    const auto &tref = *this;
    return iterator(this, tref.string_upper_bound(str).index);
  }

  const_iterator find_string_key(const std::string &str) const {
    auto ret = iter_begin();
    for (; ret != iter_end(); ++ret) {
     std::string s = ret->get_node_val();
      if (s == str)
        break;
    }
    return ret;
  }
  iterator find_string_key(const std::string &str) {
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
    return BlockSize - (reinterpret_cast<char*>(layout.template Pointer<2>(buf))-
                        reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }

  char* from_end(int off) {
    return  buf + (BlockSize - off);
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

    left.copy_from_foreign_head(left.iter_begin(), iter_begin(), piviter);
    left.set_size(piviter - iter_begin());

    right.copy_from_foreign_back(right.iter_begin(), piviter, iter_end());
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
    copy_from_foreign_head(
      iter_end(),
      left.iter_begin(),
      left.iter_end());
    set_size(left.get_size());

    append_copy_from_foreign_head(
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
      replacement_left.copy_from_foreign_head(
        replacement_left.iter_end(),
        left.iter_begin(),
        left.iter_idx(pivot_idx));
      replacement_left.set_size(pivot_idx);

      replacement_right.copy_from_foreign_back(
        replacement_right.iter_end(),
        left.iter_idx(pivot_idx),
        left.iter_end());
      replacement_right.set_size(left.get_size() - pivot_idx);

      replacement_right.append_copy_from_foreign_head(
        replacement_right.iter_end(),
        right.iter_begin(),
        right.iter_end());
      replacement_right.set_size(right.get_size() + left.get_size()- pivot_idx);
    } else {
      replacement_left.copy_from_foreign_head(
        replacement_left.iter_end(),
        left.iter_begin(),
        left.iter_end());
      replacement_left.set_size(left.get_size());

      replacement_left.append_copy_from_foreign_head(
        replacement_left.iter_end(),
        right.iter_begin(),
        right.iter_idx(pivot_idx - left.get_size()));
      replacement_left.set_size(pivot_idx);

      replacement_right.copy_from_foreign_back(
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
      local_move_back(key, iter + 1, iter, iter_end());

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
    const omap_inner_key_t &key,
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
      local_move_ahead(iter, iter + 1, iter_end());
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

  /**
   * copy_from_foreign_head
   *
   * Copy from another node begin entries to this node.
   * [from_src, to_src) is another node entry range.
   * tgt is this node entry to copy to.
   * tgt and from_src must be from different nodes.
   * from_src and to_src must be in the same node.
   */
  static void copy_from_foreign_head(
    iterator tgt,
    const_iterator from_src,
    const_iterator to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    void* des = tgt.node->from_end((to_src -1)->get_node_key().key_off);
    void* src = (to_src - 1)->get_node_val_ptr();
    size_t len = (to_src -1)->get_node_key().key_off;
    memcpy(des, src, len);
    memcpy(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
  }

  /**
   * copy_from_foreign_back
   *
   * Copy from another node back entries to this node.
   * [from_src, to_src) is another node entry range.
   * tgt is this node entry to copy to.
   * tgt and from_src must be from different nodes.
   * from_src and to_src must be in the same node.
   */
  void copy_from_foreign_back(
    iterator tgt,
    const_iterator from_src,
    const_iterator to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    auto offset = from_src.get_index() == 0? 0: (from_src-1)->get_node_key().key_off;
    void* des = tgt.node->from_end((to_src -1)->get_node_key().key_off - offset);
    void* src = (to_src - 1)->get_node_val_ptr();
    size_t len = from_src.get_index() == 0? (to_src -1)->get_node_key().key_off:
                 (from_src-1)->get_node_val_ptr() - (to_src -1)->get_node_val_ptr();
    memcpy(des, src, len);
    memcpy(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
    if ( from_src.get_index() == 0)
      return;

    omap_inner_key_t key = (from_src - 1)->get_node_key();
    auto end_idx = tgt.get_index() + to_src.get_index() - from_src.get_index();
    for (auto ite = tgt; ite.get_index() != end_idx; ite++) {
       omap_inner_key_t node_key = ite->get_node_key();
       node_key.key_off -= key.key_off;
       ite->set_node_key(node_key);
    }
  }

  /**
   * append copy_from_foreign_ahead
   *
   * append another node head entries to this node back.
   * [from_src, to_src) is another node entry range.
   * tgt is this node entry to copy to.
   * tgt and from_src must be from different nodes.
   * from_src and to_src must be in the same node.
   */
  void append_copy_from_foreign_head(
    iterator tgt,
    const_iterator from_src,
    const_iterator to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    if (from_src == to_src)
      return;

    void* des = tgt.node->from_end((to_src -1)->get_node_key().key_off + (tgt - 1)->get_node_key().key_off);
    void* src = (to_src - 1)->get_node_val_ptr();
    size_t len = (to_src -1)->get_node_key().key_off;
    memcpy(des, src, len);
    memcpy(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
    omap_inner_key_t key = (tgt - 1)->get_node_key();
    auto end_idx = tgt.get_index() + to_src.get_index() - from_src.get_index();
    for (auto ite = tgt; ite.get_index() != end_idx; ite++) {
       omap_inner_key_t node_key = ite->get_node_key();
       node_key.key_off += key.key_off;
       ite->set_node_key(node_key);
    }
  }

  /**
   * local_move_back
   *
   * move this node entries range [from_src, to_src) back to tgt position.
   *
   * tgt, from_src, and to_src must be from the same node.
   */
  static void local_move_back(
    omap_inner_key_t key,
    iterator tgt,
    iterator from_src,
    iterator to_src) {
    assert(tgt->node == from_src->node);
    assert(to_src->node == from_src->node);
    void* des = (to_src-1)->get_node_val_ptr() - key.key_len;
    void* src = (to_src-1)->get_node_val_ptr();
    size_t len = from_src.get_index() == 0?
                 from_src->node->buf + BlockSize - (to_src-1)->get_node_val_ptr():
                 (from_src-1)->get_node_val_ptr() - (to_src-1)->get_node_val_ptr();

    memmove(des, src, len);
    for ( auto ite = from_src; ite < to_src; ite++) {
      omap_inner_key_t node_key = ite->get_node_key();
      node_key.key_off += key.key_len;
      ite->set_node_key(node_key);
    }
    memmove(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
  }

  /**
   * local_move_ahead
   *
   * move this node entries range [from_src, to_src) ahead to tgt position.
   *
   * tgt, from_src, and to_src must be from the same node.
   */
  static void local_move_ahead(
    iterator tgt,
    iterator from_src,
    iterator to_src) {
    assert(tgt->node == from_src->node);
    assert(to_src->node == from_src->node);
    assert(from_src.get_index() != 0);
    omap_inner_key_t key = tgt->get_node_key();
    void* des = (to_src-1)->get_node_val_ptr() + key.key_len;
    void* src = (to_src-1)->get_node_val_ptr();
    size_t len = (from_src-1)->get_node_val_ptr() - (to_src-1)->get_node_val_ptr();
    memmove(des, src, len);
    for ( auto ite = from_src; ite < to_src; ite++) {
      omap_inner_key_t node_key = ite->get_node_key();
      node_key.key_off -= key.key_len;
      ite->set_node_key(node_key);
    }
    memmove(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
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

    // Work nicely with for loops without requiring a nested type.
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
      auto tail = node->buf + BlockSize;
      if ( *this == node->iter_end())
        return tail;
      else
        return tail - static_cast<int>(get_node_key().key_off);
    }

    const char *get_node_val_ptr() const {
      auto tail = node->buf + BlockSize;
      if ( *this == node->iter_end())
        return tail;
      else
        return tail - static_cast<int>(get_node_key().key_off);
    }

    char *get_string_val_ptr() {
      auto tail = node->buf + BlockSize;
      return tail - static_cast<int>(get_node_key().val_off);
    }

    const char *get_string_val_ptr() const {
      auto tail = node->buf + BlockSize;
      return tail - static_cast<int>(get_node_key().val_off);
    }

    void set_node_val(std::string val) const {
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

    void set_string_val(std::string val) {
      static_assert(!is_const);
      std::strcpy((char*)get_string_val_ptr(), val.c_str()); //copy char* to char* include "\0"
    }

    std::string get_string_val() const {
      std::string s(get_string_val_ptr());
      return s;
    }

    bool contains(const std::string &key) const {
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
    enum class op_t : uint8_t {
      INSERT,
      UPDATE,
      REMOVE,
    } op;
    std::string key;
    std::string val;

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
      const std::string &key,
      const std::string &val) {
      buffer.push_back(
       delta_leaf_t{
         delta_leaf_t::op_t::INSERT,
         key,
         val
       });
    }
    void update(
      const std::string &key,
      const std::string &val) {
      buffer.push_back(
       delta_leaf_t{
         delta_leaf_t::op_t::UPDATE,
         key,
         val
       });
    }
    void remove(std::string key) {
      buffer.push_back(
       delta_leaf_t{
         delta_leaf_t::op_t::REMOVE,
         key,
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
      using ceph::encode;
      uint32_t num = buffer.size();
      encode(num, bl);
      for (auto &&i: buffer) {
        encode(i.op, bl);
        encode(i.key, bl);
        //bl.append((char*)&(i.key), sizeof(i.key));
        encode(i.val, bl);
      }
      buffer.clear();
    }
    //copy in
    void decode(const ceph::bufferlist &bl) {
      using ceph::decode;
      auto p = bl.cbegin();
      uint32_t num;
      decode (num, p);
      while (num--) {
        delta_leaf_t delta;
        decode(delta.op, p);
        decode(delta.key, p);
        decode(delta.val, p);
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

  const_iterator string_lower_bound(std::string str) const {
    uint16_t start = 0, end = get_size();
    while (start != end) {
      unsigned mid = (start + end) / 2;
      const_iterator iter(this, mid);
      std::string s = iter->get_node_val();
      if (s < str)
        start = ++mid;
      if (s > str)
        end = mid;
      if (s == str)
        return iter;
    }
    return const_iterator(this, start);
  }

  iterator string_lower_bound(std::string str) {
    const auto &tref = *this;
    return iterator(this, tref.string_lower_bound(str).index);
  }

  const_iterator string_upper_bound(std::string str) const {
    auto ret = iter_begin();
    for (; ret != iter_end(); ++ret) {
     std::string s = ret->get_node_val();
      if (s > str)
        break;
    }
    return ret;
  }

  iterator string_upper_bound(std::string str) {
    const auto &tref = *this;
    return iterator(this, tref.string_upper_bound(str).index);
  }

  const_iterator find_string_key(const std::string &str) const {
    auto ret = iter_begin();
    for (; ret != iter_end(); ++ret) {
     std::string s = ret->get_node_val();
      if (s == str)
        break;
    }
    return ret;
  }
  iterator find_string_key(const std::string &str) {
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
    return BlockSize - (reinterpret_cast<char*>(layout.template Pointer<2>(buf))-
                        reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }
  char* from_end(int off) {
    return buf + (BlockSize - off);
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
      if (iter->get_node_key() != iter2->get_node_key() ||
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

    left.copy_from_foreign_head(left.iter_begin(), iter_begin(), piviter);
    left.set_size(piviter - iter_begin());

    right.copy_from_foreign_back(right.iter_begin(), piviter, iter_end());
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
    copy_from_foreign_head(
      iter_end(),
      left.iter_begin(),
      left.iter_end());
    set_size(left.get_size());
    append_copy_from_foreign_head(
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
      replacement_left.copy_from_foreign_head(
        replacement_left.iter_end(),
        left.iter_begin(),
        left.iter_idx(pivot_idx));
      replacement_left.set_size(pivot_idx);

      replacement_right.copy_from_foreign_back(
        replacement_right.iter_end(),
        left.iter_idx(pivot_idx),
        left.iter_end());
      replacement_right.set_size(left.get_size() - pivot_idx);

      replacement_right.append_copy_from_foreign_head(
        replacement_right.iter_end(),
        right.iter_begin(),
        right.iter_end());
      replacement_right.set_size(right.get_size() + left.get_size() - pivot_idx);
    } else {
      replacement_left.copy_from_foreign_head(
        replacement_left.iter_end(),
        left.iter_begin(),
        left.iter_end());
      replacement_left.set_size(left.get_size());

      replacement_left.append_copy_from_foreign_head(
        replacement_left.iter_end(),
        right.iter_begin(),
        right.iter_idx(pivot_idx - left.get_size()));
      replacement_left.set_size(pivot_idx);

      replacement_right.copy_from_foreign_back(
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
      local_move_back(node_key, iter + 1, iter, iter_end());

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
    if ((iter + 1) != iter_end())
      local_move_ahead(iter, iter + 1, iter_end());
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

  /**
   * copy_from_foreign_head
   *
   * Copy from another node begin entries to this node.
   * [from_src, to_src) is another node entry range.
   * tgt is this node entry to copy to.
   * tgt and from_src must be from different nodes.
   * from_src and to_src must be in the same node.
   */
  static void copy_from_foreign_head(
    iterator tgt,
    const_iterator from_src,
    const_iterator to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    void* des = tgt.node->from_end((to_src -1)->get_node_key().key_off);
    void* src = (to_src - 1)->get_node_val_ptr();
    size_t len = (to_src -1)->get_node_key().key_off;
    memcpy(des, src, len);
    memcpy(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
  }

  /**
   * copy_from_foreign_back
   *
   * Copy from another node back entries to this node.
   * [from_src, to_src) is another node entry range.
   * tgt is this node entry to copy to.
   * tgt and from_src must be from different nodes.
   * from_src and to_src must be in the same node.
   */
  void copy_from_foreign_back(
    iterator tgt,
    const_iterator from_src,
    const_iterator to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    auto offset = from_src.get_index() == 0? 0: (from_src-1)->get_node_key().key_off;

    void* des = tgt.node->from_end((to_src -1)->get_node_key().key_off - offset);
    void* src = (to_src - 1)->get_node_val_ptr();
    size_t len = from_src.get_index() == 0? (to_src -1)->get_node_key().key_off:
                 (from_src-1)->get_node_val_ptr() - (to_src -1)->get_node_val_ptr();
    memcpy(des, src, len);
    memcpy(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
    if ( from_src.get_index() == 0)
      return;

    omap_leaf_key_t key = (from_src - 1)->get_node_key();
    for (auto ite = tgt; ite.get_index() < (tgt.get_index() + to_src.get_index() - from_src.get_index()); ite++) {
       omap_leaf_key_t node_key = ite->get_node_key();
       node_key.key_off -= key.key_off;
       node_key.val_off -= key.key_off;
       ite->set_node_key(node_key);
    }
  }

  /**
   * append copy_from_foreign_ahead
   *
   * append another node head entries to this node back.
   * [from_src, to_src) is another node entry range.
   * tgt is this node entry to copy to.
   * tgt and from_src must be from different nodes.
   * from_src and to_src must be in the same node.
   */
  void append_copy_from_foreign_head(
    iterator tgt,
    const_iterator from_src,
    const_iterator to_src) {
    assert(tgt->node != from_src->node);
    assert(to_src->node == from_src->node);
    if (from_src == to_src)
      return;

    void* des = tgt.node->from_end((to_src -1)->get_node_key().key_off + (tgt - 1)->get_node_key().key_off);
    void* src = (to_src - 1)->get_node_val_ptr();
    size_t len = (to_src -1)->get_node_key().key_off;
    memcpy(des, src, len);
    memcpy(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
    omap_leaf_key_t key = (tgt - 1)->get_node_key();
    auto end_idx = tgt.get_index() + to_src.get_index() - from_src.get_index();
    for (auto ite = tgt; ite.get_index() != end_idx; ite++) {
       omap_leaf_key_t node_key = ite->get_node_key();
       node_key.key_off += key.key_off;
       node_key.val_off += key.key_off;
       ite->set_node_key(node_key);
    }
  }

  /**
   * local_move_back
   *
   * move this node entries range [from_src, to_src) back to tgt position.
   *
   * tgt, from_src, and to_src must be from the same node.
   */
  static void local_move_back(
    omap_leaf_key_t key,
    iterator tgt,
    iterator from_src,
    iterator to_src) {
    assert(tgt->node == from_src->node);
    assert(to_src->node == from_src->node);
    void* des = (to_src-1)->get_node_val_ptr() - (key.key_len + key.val_len);
    void* src = (to_src-1)->get_node_val_ptr();
    size_t len = from_src.get_index() == 0?
                 from_src->node->buf + BlockSize - (to_src-1)->get_node_val_ptr():
                 (from_src-1)->get_node_val_ptr() - (to_src-1)->get_node_val_ptr();
    memmove(des, src, len);
    for ( auto ite = from_src; ite < to_src; ite++) {
      omap_leaf_key_t node_key = ite->get_node_key();
      node_key.key_off += (key.key_len + key.val_len);
      node_key.val_off += (key.key_len + key.val_len);
      ite->set_node_key(node_key);
    }
    memmove(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
  }

  /**
   * local_move_ahead
   *
   * move this node entries range [from_src, to_src) ahead to tgt position.
   *
   * tgt, from_src, and to_src must be from the same node.
   */
  static void local_move_ahead(
    iterator tgt,
    iterator from_src,
    iterator to_src) {
    assert(tgt->node == from_src->node);
    assert(to_src->node == from_src->node);
    assert(from_src.get_index() != 0);
    omap_leaf_key_t key = tgt->get_node_key();
    void* des = (to_src - 1)->get_node_val_ptr() + key.key_len + key.val_len;
    void* src = (to_src - 1)->get_node_val_ptr();
    size_t len = (from_src - 1)->get_node_val_ptr() - (to_src - 1)->get_node_val_ptr();
    memmove(des, src, len);
    for ( auto ite = from_src; ite < to_src; ite++) {
      omap_leaf_key_t node_key = ite->get_node_key();
      node_key.key_off -= (key.key_len + key.val_len);
      node_key.val_off -= (key.key_len + key.val_len);
      ite->set_node_key(node_key);
    }
    memmove(
      tgt->get_node_key_ptr(), from_src->get_node_key_ptr(),
      to_src->get_node_key_ptr() - from_src->get_node_key_ptr());
  }

};

}
