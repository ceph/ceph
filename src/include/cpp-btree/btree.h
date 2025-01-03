// Copyright 2018 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A btree implementation of the STL set and map interfaces. A btree is smaller
// and generally also faster than STL set/map (refer to the benchmarks below).
// The red-black tree implementation of STL set/map has an overhead of 3
// pointers (left, right and parent) plus the node color information for each
// stored value. So a set<int32_t> consumes 40 bytes for each value stored in
// 64-bit mode. This btree implementation stores multiple values on fixed
// size nodes (usually 256 bytes) and doesn't store child pointers for leaf
// nodes. The result is that a btree_set<int32_t> may use much less memory per
// stored value. For the random insertion benchmark in btree_bench.cc, a
// btree_set<int32_t> with node-size of 256 uses 5.1 bytes per stored value.
//
// The packing of multiple values on to each node of a btree has another effect
// besides better space utilization: better cache locality due to fewer cache
// lines being accessed. Better cache locality translates into faster
// operations.
//
// CAVEATS
//
// Insertions and deletions on a btree can cause splitting, merging or
// rebalancing of btree nodes. And even without these operations, insertions
// and deletions on a btree will move values around within a node. In both
// cases, the result is that insertions and deletions can invalidate iterators
// pointing to values other than the one being inserted/deleted. Therefore, this
// container does not provide pointer stability. This is notably different from
// STL set/map which takes care to not invalidate iterators on insert/erase
// except, of course, for iterators pointing to the value being erased.  A
// partial workaround when erasing is available: erase() returns an iterator
// pointing to the item just after the one that was erased (or end() if none
// exists).

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <experimental/type_traits>
#include <functional>
#include <iterator>
#include <limits>
#include <new>
#include <type_traits>
#include <utility>

namespace btree::internal {

template <typename Compare, typename T>
using btree_is_key_compare_to =
  std::is_signed<std::invoke_result_t<Compare, T, T>>;

template<typename T>
using compare_to_t = decltype(std::declval<T&>().compare(std::declval<const T&>()));
template<typename T>
inline constexpr bool has_compare_to = std::experimental::is_detected_v<compare_to_t, T>;
// A helper class to convert a boolean comparison into a three-way "compare-to"
// comparison that returns a negative value to indicate less-than, zero to
// indicate equality and a positive value to indicate greater-than. This helper
// class is specialized for less<std::string>, greater<std::string>,
// less<string_view>, and greater<string_view>.
//
// key_compare_to_adapter is provided so that btree users
// automatically get the more efficient compare-to code when using common
// google string types with common comparison functors.
// These string-like specializations also turn on heterogeneous lookup by
// default.
template <typename Compare, typename=void>
struct key_compare_to_adapter {
  using type = Compare;
};

template <typename K>
struct key_compare_to_adapter<std::less<K>, std::enable_if_t<has_compare_to<K>>>
{
  struct type {
    inline int operator()(const K& lhs, const K& rhs) const noexcept {
      return lhs.compare(rhs);
    }
  };
};

template <typename K>
struct key_compare_to_adapter<std::less<K>, std::enable_if_t<std::is_signed_v<K>>>
{
  struct type {
    inline K operator()(const K& lhs, const K& rhs) const noexcept {
      return lhs - rhs;
    }
  };
};

template <typename K>
struct key_compare_to_adapter<std::less<K>, std::enable_if_t<std::is_unsigned_v<K>>>
{
  struct type {
    inline int operator()(const K& lhs, const K& rhs) const noexcept  {
      if (lhs < rhs) {
        return -1;
      } else if (lhs > rhs) {
        return 1;
      } else {
        return 0;
      }
    }
  };
};

template <typename Key, typename Compare, typename Alloc,
          int TargetNodeSize, int ValueSize,
          bool Multi>
struct common_params {
  // If Compare is a common comparator for a std::string-like type, then we adapt it
  // to use heterogeneous lookup and to be a key-compare-to comparator.
  using key_compare = typename key_compare_to_adapter<Compare>::type;
  // A type which indicates if we have a key-compare-to functor or a plain old
  // key-compare functor.
  using is_key_compare_to = btree_is_key_compare_to<key_compare, Key>;

  using allocator_type = Alloc;
  using key_type = Key;
  using size_type = std::make_signed<size_t>::type;
  using difference_type = ptrdiff_t;

  // True if this is a multiset or multimap.
  using is_multi_container = std::integral_constant<bool, Multi>;

  constexpr static int kTargetNodeSize = TargetNodeSize;
  constexpr static int kValueSize = ValueSize;
  // Upper bound for the available space for values. This is largest for leaf
  // nodes, which have overhead of at least a pointer + 3 bytes (for storing
  // 3 field_types) + paddings. if alignof(key_type) is 1, the size of padding
  // would be 0.
  constexpr static int kNodeValueSpace =
        TargetNodeSize - /*minimum overhead=*/(sizeof(void *) + 4);

  // This is an integral type large enough to hold as many
  // ValueSize-values as will fit a node of TargetNodeSize bytes.
  using node_count_type =
      std::conditional_t<(kNodeValueSpace / ValueSize >
                          (std::numeric_limits<uint8_t>::max)()),
                         uint16_t,
                         uint8_t>;
};

// The internal storage type
//
// It is convenient for the value_type of a btree_map<K, V> to be
// pair<const K, V>; the "const K" prevents accidental modification of the key
// when dealing with the reference returned from find() and similar methods.
// However, this creates other problems; we want to be able to emplace(K, V)
// efficiently with move operations, and similarly be able to move a
// pair<K, V> in insert().
//
// The solution is this union, which aliases the const and non-const versions
// of the pair. This also allows flat_hash_map<const K, V> to work, even though
// that has the same efficiency issues with move in emplace() and insert() -
// but people do it anyway.
template <class K, class V>
union map_slot_type {
  map_slot_type() {}
  ~map_slot_type() = delete;
  map_slot_type& operator=(const map_slot_type& slot) {
    mutable_value = slot.mutable_value;
    return *this;
  }
  map_slot_type& operator=(map_slot_type&& slot) {
    mutable_value = std::move(slot.mutable_value);
    return *this;
  }
  using value_type = std::pair<const K, V>;
  using mutable_value_type = std::pair<K, V>;

  value_type value;
  mutable_value_type mutable_value;
  K key;
};

template <class K, class V>
void swap(map_slot_type<K, V>& lhs, map_slot_type<K, V>& rhs) {
  std::swap(lhs.mutable_value, rhs.mutable_value);
}

// A parameters structure for holding the type parameters for a btree_map.
// Compare and Alloc should be nothrow copy-constructible.
template <typename Key, typename Data, typename Compare, typename Alloc,
          int TargetNodeSize, bool Multi>
struct map_params : common_params<Key, Compare, Alloc, TargetNodeSize,
                                  sizeof(Key) + sizeof(Data), Multi> {
  using super_type = typename map_params::common_params;
  using mapped_type = Data;
  using value_type = std::pair<const Key, mapped_type>;
  using mutable_value_type = std::pair<Key, mapped_type>;
  using slot_type = map_slot_type<Key, mapped_type>;
  using pointer = value_type*;
  using const_pointer = const value_type *;
  using reference = value_type &;
  using const_reference = const value_type &;
  using key_compare = typename super_type::key_compare;
  using init_type = mutable_value_type;

  static constexpr size_t kValueSize = sizeof(Key) + sizeof(mapped_type);

  // Inherit from key_compare for empty base class optimization.
  struct value_compare : private key_compare {
    value_compare() = default;
    explicit value_compare(const key_compare &cmp) : key_compare(cmp) {}

    template <typename T, typename U>
    auto operator()(const T &left, const U &right) const
        -> decltype(std::declval<key_compare>()(left.first, right.first)) {
      return key_compare::operator()(left.first, right.first);
    }
  };
  using is_map_container = std::true_type;

  static const Key &key(const value_type &value) { return value.first; }
  static mapped_type &value(value_type *value) { return value->second; }
  static const Key &key(const slot_type *slot) { return slot->key; }
  static value_type& element(slot_type* slot) { return slot->value; }
  static const value_type& element(const slot_type* slot) { return slot->value; }
  template <class... Args>
  static void construct(Alloc *alloc, slot_type *slot, Args &&... args) {
    std::allocator_traits<Alloc>::construct(*alloc,
                                            &slot->mutable_value,
                                            std::forward<Args>(args)...);
  }
  // Construct this slot by moving from another slot.
  static void construct(Alloc* alloc, slot_type* slot, slot_type* other) {
    emplace(slot);
    std::allocator_traits<Alloc>::construct(*alloc, &slot->value,
                                            std::move(other->value));
  }
  static void move(Alloc *alloc, slot_type *src, slot_type *dest) {
    dest->mutable_value = std::move(src->mutable_value);
  }
  static void destroy(Alloc *alloc, slot_type *slot) {
    std::allocator_traits<Alloc>::destroy(*alloc, &slot->mutable_value);
  }

private:
  static void emplace(slot_type* slot) {
    // The construction of union doesn't do anything at runtime but it allows us
    // to access its members without violating aliasing rules.
    new (slot) slot_type;
  }
};

// A parameters structure for holding the type parameters for a btree_set.
template <typename Key, typename Compare, typename Alloc, int TargetNodeSize, bool Multi>
struct set_params
    : public common_params<Key, Compare, Alloc, TargetNodeSize,
                           sizeof(Key), Multi> {
  using value_type = Key;
  using mutable_value_type = value_type;
  using slot_type = Key;
  using pointer = value_type *;
  using const_pointer = const value_type *;
  using value_compare = typename set_params::common_params::key_compare;
  using reference = value_type &;
  using const_reference = const value_type &;
  using is_map_container = std::false_type;
  using init_type = mutable_value_type;

  template <class... Args>
  static void construct(Alloc *alloc, slot_type *slot, Args &&... args) {
    std::allocator_traits<Alloc>::construct(*alloc,
                                            slot,
                                            std::forward<Args>(args)...);
  }
  static void construct(Alloc *alloc, slot_type *slot, slot_type *other) {
    std::allocator_traits<Alloc>::construct(*alloc, slot, std::move(*other));
  }
  static void move(Alloc *alloc, slot_type *src, slot_type *dest) {
    *dest = std::move(*src);
  }
  static void destroy(Alloc *alloc, slot_type *slot) {
    std::allocator_traits<Alloc>::destroy(*alloc, slot);
  }
  static const Key &key(const value_type &x) { return x; }
  static const Key &key(const slot_type *slot) { return *slot; }
  static value_type &element(slot_type *slot) { return *slot; }
  static const value_type &element(const slot_type *slot) { return *slot; }
};

// Helper functions to do a boolean comparison of two keys given a boolean
// or three-way comparator.
// SFINAE prevents implicit conversions to bool (such as from int).
template <typename Result>
constexpr bool compare_result_as_less_than(const Result r) {
  if constexpr (std::is_signed_v<Result>) {
    return r < 0;
  } else {
    return r;
  }
}
// An adapter class that converts a lower-bound compare into an upper-bound
// compare. Note: there is no need to make a version of this adapter specialized
// for key-compare-to functors because the upper-bound (the first value greater
// than the input) is never an exact match.
template <typename Compare>
struct upper_bound_adapter {
  explicit upper_bound_adapter(const Compare &c) : comp(c) {}
  template <typename K, typename LK>
  bool operator()(const K &a, const LK &b) const {
    // Returns true when a is not greater than b.
    return !compare_result_as_less_than(comp(b, a));
  }
private:
  const Compare& comp;
};

enum class MatchKind : uint8_t { kEq, kNe };

template <typename V, bool IsCompareTo>
struct SearchResult {
  V value;
  MatchKind match;

  static constexpr bool has_match = true;
  bool IsEq() const { return match == MatchKind::kEq; }
};

// When we don't use CompareTo, `match` is not present.
// This ensures that callers can't use it accidentally when it provides no
// useful information.
template <typename V>
struct SearchResult<V, false> {
  V value;

  static constexpr bool has_match = false;
  static constexpr bool IsEq() { return false; }
};

// A node in the btree holding. The same node type is used for both internal
// and leaf nodes in the btree, though the nodes are allocated in such a way
// that the children array is only valid in internal nodes.
template <typename Params>
class btree_node {
  using is_key_compare_to = typename Params::is_key_compare_to;
  using is_multi_container = typename Params::is_multi_container;
  using field_type = typename Params::node_count_type;
  using allocator_type = typename Params::allocator_type;
  using slot_type = typename Params::slot_type;

 public:
  using params_type = Params;
  using key_type = typename Params::key_type;
  using value_type = typename Params::value_type;
  using mutable_value_type = typename Params::mutable_value_type;
  using pointer = typename Params::pointer;
  using const_pointer = typename Params::const_pointer;
  using reference = typename Params::reference;
  using const_reference = typename Params::const_reference;
  using key_compare = typename Params::key_compare;
  using size_type = typename Params::size_type;
  using difference_type = typename Params::difference_type;

  // Btree decides whether to use linear node search as follows:
  //   - If the key is arithmetic and the comparator is std::less or
  //     std::greater, choose linear.
  //   - Otherwise, choose binary.
  // TODO(ezb): Might make sense to add condition(s) based on node-size.
  using use_linear_search = std::integral_constant<
      bool,
      std::is_arithmetic_v<key_type> &&
      (std::is_same_v<std::less<key_type>, key_compare> ||
       std::is_same_v<std::greater<key_type>, key_compare>)>;

  ~btree_node() = default;
  btree_node(const btree_node&) = delete;
  btree_node& operator=(const btree_node&) = delete;

 protected:
  btree_node() = default;

 private:
  constexpr static size_type SizeWithNValues(size_type n) {
    return sizeof(base_fields) + n * sizeof(value_type);;
  }
  // A lower bound for the overhead of fields other than values in a leaf node.
  constexpr static size_type MinimumOverhead() {
    return SizeWithNValues(1) - sizeof(value_type);
  }

  // Compute how many values we can fit onto a leaf node taking into account
  // padding.
  constexpr static size_type NodeTargetValues(const int begin, const int end) {
    return begin == end ? begin
                        : SizeWithNValues((begin + end) / 2 + 1) >
                                  params_type::kTargetNodeSize
                              ? NodeTargetValues(begin, (begin + end) / 2)
                              : NodeTargetValues((begin + end) / 2 + 1, end);
  }

  constexpr static int kValueSize = params_type::kValueSize;
  constexpr static int kTargetNodeSize = params_type::kTargetNodeSize;
  constexpr static int kNodeTargetValues = NodeTargetValues(0, kTargetNodeSize);

  // We need a minimum of 3 values per internal node in order to perform
  // splitting (1 value for the two nodes involved in the split and 1 value
  // propagated to the parent as the delimiter for the split).
  constexpr static size_type kNodeValues = std::max(kNodeTargetValues, 3);

  // The node is internal (i.e. is not a leaf node) if and only if `max_count`
  // has this value.
  constexpr static size_type kInternalNodeMaxCount = 0;

  struct base_fields {
    // A pointer to the node's parent.
    btree_node *parent;
    // The position of the node in the node's parent.
    field_type position;
    // The count of the number of values in the node.
    field_type count;
    // The maximum number of values the node can hold.
    field_type max_count;
  };

  struct leaf_fields : public base_fields {
    // The array of values. Only the first count of these values have been
    // constructed and are valid.
    slot_type values[kNodeValues];
  };

  struct internal_fields : public leaf_fields {
    // The array of child pointers. The keys in children_[i] are all less than
    // key(i). The keys in children_[i + 1] are all greater than key(i). There
    // are always count + 1 children.
    btree_node *children[kNodeValues + 1];
  };

  constexpr static size_type LeafSize(const int max_values = kNodeValues) {
    return SizeWithNValues(max_values);
  }
  constexpr static size_type InternalSize() {
    return sizeof(internal_fields);
  }

  template<auto MemPtr>
  auto& GetField() {
    return reinterpret_cast<internal_fields*>(this)->*MemPtr;
  }

  template<auto MemPtr>
  auto& GetField() const {
    return reinterpret_cast<const internal_fields*>(this)->*MemPtr;
  }

  void set_parent(btree_node *p) { GetField<&base_fields::parent>() = p; }
  field_type &mutable_count() { return GetField<&base_fields::count>(); }
  slot_type *slot(int i) { return &GetField<&leaf_fields::values>()[i]; }
  const slot_type *slot(int i) const { return &GetField<&leaf_fields::values>()[i]; }
  void set_position(field_type v) { GetField<&base_fields::position>() = v; }
  void set_count(field_type v) { GetField<&base_fields::count>() = v; }
  // This method is only called by the node init methods.
  void set_max_count(field_type v) { GetField<&base_fields::max_count>() = v; }

public:
  constexpr static size_type Alignment() {
    static_assert(alignof(leaf_fields) == alignof(internal_fields),
                  "Alignment of all nodes must be equal.");
    return alignof(internal_fields);
  }

  // Getter/setter for whether this is a leaf node or not. This value doesn't
  // change after the node is created.
  bool leaf() const { return GetField<&base_fields::max_count>() != kInternalNodeMaxCount; }

  // Getter for the position of this node in its parent.
  field_type position() const { return GetField<&base_fields::position>(); }

  // Getter for the number of values stored in this node.
  field_type count() const { return GetField<&base_fields::count>(); }
  field_type max_count() const {
    // Internal nodes have max_count==kInternalNodeMaxCount.
    // Leaf nodes have max_count in [1, kNodeValues].
    const field_type max_count = GetField<&base_fields::max_count>();
    return max_count == field_type{kInternalNodeMaxCount}
               ? field_type{kNodeValues}
               : max_count;
  }

  // Getter for the parent of this node.
  btree_node* parent() const { return GetField<&base_fields::parent>(); }
  // Getter for whether the node is the root of the tree. The parent of the
  // root of the tree is the leftmost node in the tree which is guaranteed to
  // be a leaf.
  bool is_root() const { return parent()->leaf(); }
  void make_root() {
    assert(parent()->is_root());
    set_parent(parent()->parent());
  }

  // Getters for the key/value at position i in the node.
  const key_type& key(int i) const { return params_type::key(slot(i)); }
  reference value(int i) { return params_type::element(slot(i)); }
  const_reference value(int i) const { return params_type::element(slot(i)); }

  // Getters/setter for the child at position i in the node.
  btree_node* child(int i) const { return GetField<&internal_fields::children>()[i]; }
  btree_node*& mutable_child(int i) { return GetField<&internal_fields::children>()[i]; }
  void clear_child(int i) {
#ifndef NDEBUG
    memset(&mutable_child(i), 0, sizeof(btree_node*));
#endif
  }
  void set_child(int i, btree_node *c) {
    mutable_child(i) = c;
    c->set_position(i);
  }
  void init_child(int i, btree_node *c) {
    set_child(i, c);
    c->set_parent(this);
  }
  // Returns the position of the first value whose key is not less than k.
  template <typename K>
  SearchResult<int, is_key_compare_to::value> lower_bound(
      const K &k, const key_compare &comp) const {
    return use_linear_search::value ? linear_search(k, comp)
                                    : binary_search(k, comp);
  }
  // Returns the position of the first value whose key is greater than k.
  template <typename K>
  int upper_bound(const K &k, const key_compare &comp) const {
    auto upper_compare = upper_bound_adapter<key_compare>(comp);
    return use_linear_search::value ? linear_search(k, upper_compare).value
                                    : binary_search(k, upper_compare).value;
  }

  template <typename K, typename Compare>
  SearchResult<int, btree_is_key_compare_to<Compare, key_type>::value>
  linear_search(const K &k, const Compare &comp) const {
    return linear_search_impl(k, 0, count(), comp,
                              btree_is_key_compare_to<Compare, key_type>());
  }

  template <typename K, typename Compare>
  SearchResult<int, btree_is_key_compare_to<Compare, key_type>::value>
  binary_search(const K &k, const Compare &comp) const {
    return binary_search_impl(k, 0, count(), comp,
                              btree_is_key_compare_to<Compare, key_type>());
  }
  // Returns the position of the first value whose key is not less than k using
  // linear search performed using plain compare.
  template <typename K, typename Compare>
  SearchResult<int, false> linear_search_impl(
      const K &k, int s, const int e, const Compare &comp,
      std::false_type /* IsCompareTo */) const {
    while (s < e) {
      if (!comp(key(s), k)) {
        break;
      }
      ++s;
    }
    return {s};
  }

  // Returns the position of the first value whose key is not less than k using
  // linear search performed using compare-to.
  template <typename K, typename Compare>
  SearchResult<int, true> linear_search_impl(
      const K &k, int s, const int e, const Compare &comp,
      std::true_type /* IsCompareTo */) const {
    while (s < e) {
      const auto c = comp(key(s), k);
      if (c == 0) {
        return {s, MatchKind::kEq};
      } else if (c > 0) {
        break;
      }
      ++s;
    }
    return {s, MatchKind::kNe};
  }

  // Returns the position of the first value whose key is not less than k using
  // binary search performed using plain compare.
  template <typename K, typename Compare>
  SearchResult<int, false> binary_search_impl(
      const K &k, int s, int e, const Compare &comp,
      std::false_type /* IsCompareTo */) const {
    while (s != e) {
      const int mid = (s + e) >> 1;
      if (comp(key(mid), k)) {
        s = mid + 1;
      } else {
        e = mid;
      }
    }
    return {s};
  }

  // Returns the position of the first value whose key is not less than k using
  // binary search performed using compare-to.
  template <typename K, typename CompareTo>
  SearchResult<int, true> binary_search_impl(
      const K &k, int s, int e, const CompareTo &comp,
      std::true_type /* IsCompareTo */) const {
    if constexpr (is_multi_container::value) {
      MatchKind exact_match = MatchKind::kNe;
      while (s != e) {
        const int mid = (s + e) >> 1;
        const auto c = comp(key(mid), k);
        if (c < 0) {
          s = mid + 1;
        } else {
          e = mid;
          if (c == 0) {
            // Need to return the first value whose key is not less than k,
            // which requires continuing the binary search if this is a
            // multi-container.
            exact_match = MatchKind::kEq;
          }
        }
      }
      return {s, exact_match};
    } else {  // Not a multi-container.
      while (s != e) {
        const int mid = (s + e) >> 1;
        const auto c = comp(key(mid), k);
        if (c < 0) {
          s = mid + 1;
        } else if (c > 0) {
          e = mid;
        } else {
          return {mid, MatchKind::kEq};
        }
      }
      return {s, MatchKind::kNe};
    }
  }

  // Emplaces a value at position i, shifting all existing values and
  // children at positions >= i to the right by 1.
  template <typename... Args>
  void emplace_value(size_type i, allocator_type *alloc, Args &&... args);

  // Removes the value at position i, shifting all existing values and children
  // at positions > i to the left by 1.
  void remove_value(const int i, allocator_type *alloc);

  // Removes the values at positions [i, i + to_erase), shifting all values
  // after that range to the left by to_erase. Does not change children at all.
  void remove_values_ignore_children(int i, int to_erase,
                                     allocator_type *alloc);

  // Rebalances a node with its right sibling.
  void rebalance_right_to_left(const int to_move, btree_node *right,
                               allocator_type *alloc);
  void rebalance_left_to_right(const int to_move, btree_node *right,
                               allocator_type *alloc);

  // Splits a node, moving a portion of the node's values to its right sibling.
  void split(const int insert_position, btree_node *dest, allocator_type *alloc);

  // Merges a node with its right sibling, moving all of the values and the
  // delimiting key in the parent node onto itself.
  void merge(btree_node *sibling, allocator_type *alloc);

  // Swap the contents of "this" and "src".
  void swap(btree_node *src, allocator_type *alloc);

  // Node allocation/deletion routines.
  static btree_node *init_leaf(btree_node *n, btree_node *parent,
                               int max_count) {
    n->set_parent(parent);
    n->set_position(0);
    n->set_count(0);
    n->set_max_count(max_count);
    return n;
  }
  static btree_node *init_internal(btree_node *n, btree_node *parent) {
    init_leaf(n, parent, kNodeValues);
    // Set `max_count` to a sentinel value to indicate that this node is
    // internal.
    n->set_max_count(kInternalNodeMaxCount);
    return n;
  }
  void destroy(allocator_type *alloc) {
    for (int i = 0; i < count(); ++i) {
      value_destroy(i, alloc);
    }
  }

 private:
  template <typename... Args>
  void value_init(const size_type i, allocator_type *alloc, Args &&... args) {
    params_type::construct(alloc, slot(i), std::forward<Args>(args)...);
  }
  void value_destroy(const size_type i, allocator_type *alloc) {
    params_type::destroy(alloc, slot(i));
  }

  // Move n values starting at value i in this node into the values starting at
  // value j in node x.
  void uninitialized_move_n(const size_type n, const size_type i,
                            const size_type j, btree_node *x,
                            allocator_type *alloc) {
    for (slot_type *src = slot(i), *end = src + n, *dest = x->slot(j);
         src != end; ++src, ++dest) {
      params_type::construct(alloc, dest, src);
    }
  }

  // Destroys a range of n values, starting at index i.
  void value_destroy_n(const size_type i, const size_type n,
                       allocator_type *alloc) {
    for (int j = 0; j < n; ++j) {
      value_destroy(i + j, alloc);
    }
  }

private:
  template <typename P>
  friend class btree;
  template <typename N, typename R, typename P>
  friend struct btree_iterator;
};

template <typename Node, typename Reference, typename Pointer>
struct btree_iterator {
 private:
  using key_type = typename Node::key_type;
  using size_type = typename Node::size_type;
  using params_type = typename Node::params_type;

  using node_type = Node;
  using normal_node = typename std::remove_const<Node>::type;
  using const_node = const Node;
  using normal_pointer = typename params_type::pointer;
  using normal_reference = typename params_type::reference;
  using const_pointer = typename params_type::const_pointer;
  using const_reference = typename params_type::const_reference;
  using slot_type = typename params_type::slot_type;

  using iterator =
      btree_iterator<normal_node, normal_reference, normal_pointer>;
  using const_iterator =
      btree_iterator<const_node, const_reference, const_pointer>;

 public:
  // These aliases are public for std::iterator_traits.
  using difference_type = typename Node::difference_type;
  using value_type = typename params_type::value_type;
  using pointer = Pointer;
  using reference = Reference;
  using iterator_category = std::bidirectional_iterator_tag;

  btree_iterator() = default;
  btree_iterator(Node *n, int p) : node(n), position(p) {}

  // NOTE: this SFINAE allows for implicit conversions from iterator to
  // const_iterator, but it specifically avoids defining copy constructors so
  // that btree_iterator can be trivially copyable. This is for performance and
  // binary size reasons.
  template<typename N, typename R, typename P,
           std::enable_if_t<
             std::is_same_v<btree_iterator<N, R, P>, iterator> &&
             std::is_same_v<btree_iterator, const_iterator>,
             int> = 0>
  btree_iterator(const btree_iterator<N, R, P> &x)
      : node(x.node), position(x.position) {}

 private:
  // This SFINAE allows explicit conversions from const_iterator to
  // iterator, but also avoids defining a copy constructor.
  // NOTE: the const_cast is safe because this constructor is only called by
  // non-const methods and the container owns the nodes.
  template <typename N, typename R, typename P,
            std::enable_if_t<
              std::is_same_v<btree_iterator<N, R, P>, const_iterator> &&
              std::is_same_v<btree_iterator, iterator>,
              int> = 0>
  explicit btree_iterator(const btree_iterator<N, R, P> &x)
      : node(const_cast<node_type *>(x.node)), position(x.position) {}

  // Increment/decrement the iterator.
  void increment() {
    if (node->leaf() && ++position < node->count()) {
      return;
    }
    increment_slow();
  }
  void increment_slow();

  void decrement() {
    if (node->leaf() && --position >= 0) {
      return;
    }
    decrement_slow();
  }
  void decrement_slow();

 public:
  bool operator==(const const_iterator &x) const {
    return node == x.node && position == x.position;
  }
  bool operator!=(const const_iterator &x) const {
    return node != x.node || position != x.position;
  }
  bool operator==(const iterator& x) const {
    return node == x.node && position == x.position;
  }
  bool operator!=(const iterator& x) const {
    return node != x.node || position != x.position;
  }

  // Accessors for the key/value the iterator is pointing at.
  reference operator*() const {
    return node->value(position);
  }
  pointer operator->() const {
    return &node->value(position);
  }

  btree_iterator& operator++() {
    increment();
    return *this;
  }
  btree_iterator& operator--() {
    decrement();
    return *this;
  }
  btree_iterator operator++(int) {
    btree_iterator tmp = *this;
    ++*this;
    return tmp;
  }
  btree_iterator operator--(int) {
    btree_iterator tmp = *this;
    --*this;
    return tmp;
  }

 private:
  template <typename Params>
  friend class btree;
  template <typename Tree>
  friend class btree_container;
  template <typename Tree>
  friend class btree_set_container;
  template <typename Tree>
  friend class btree_map_container;
  template <typename Tree>
  friend class btree_multiset_container;
  template <typename N, typename R, typename P>
  friend struct btree_iterator;

  const key_type &key() const { return node->key(position); }
  slot_type *slot() { return node->slot(position); }

  // The node in the tree the iterator is pointing at.
  Node *node = nullptr;
  // The position within the node of the tree the iterator is pointing at.
  int position = -1;
};

template <size_t Alignment, class Alloc>
class AlignedAlloc {
  struct alignas(Alignment) M {};
  using alloc_t =
    typename std::allocator_traits<Alloc>::template rebind_alloc<M>;
  using traits_t =
    typename std::allocator_traits<Alloc>::template rebind_traits<M>;
  static constexpr size_t num_aligned_objects(size_t size) {
    return (size + sizeof(M) - 1) / sizeof(M);
  }
public:
  static void* allocate(Alloc* alloc, size_t size) {
    alloc_t aligned_alloc(*alloc);
    void* p = traits_t::allocate(aligned_alloc,
                                 num_aligned_objects(size));
    assert(reinterpret_cast<uintptr_t>(p) % Alignment == 0 &&
         "allocator does not respect alignment");
    return p;
  }
  static void deallocate(Alloc* alloc, void* p, size_t size) {
    alloc_t aligned_alloc(*alloc);
    traits_t::deallocate(aligned_alloc, static_cast<M*>(p),
                         num_aligned_objects(size));
  }
};

template <typename Params>
class btree {
  using node_type = btree_node<Params>;
  using is_key_compare_to = typename Params::is_key_compare_to;

  // We use a static empty node for the root/leftmost/rightmost of empty btrees
  // in order to avoid branching in begin()/end().
  struct alignas(node_type::Alignment()) EmptyNodeType : node_type {
    using field_type = typename node_type::field_type;
    node_type *parent;
    field_type position = 0;
    field_type count = 0;
    // max_count must be != kInternalNodeMaxCount (so that this node is regarded
    // as a leaf node). max_count() is never called when the tree is empty.
    field_type max_count = node_type::kInternalNodeMaxCount + 1;

    constexpr EmptyNodeType(node_type *p) : parent(p) {}
  };

  static node_type *EmptyNode() {
    static constexpr EmptyNodeType empty_node(
        const_cast<EmptyNodeType *>(&empty_node));
    return const_cast<EmptyNodeType *>(&empty_node);
  }

  constexpr static int kNodeValues = node_type::kNodeValues;
  constexpr static int kMinNodeValues = kNodeValues / 2;
  constexpr static int kValueSize = node_type::kValueSize;

  // A helper class to get the empty base class optimization for 0-size
  // allocators. Base is allocator_type.
  // (e.g. empty_base_handle<key_compare, allocator_type, node_type*>). If Base is
  // 0-size, the compiler doesn't have to reserve any space for it and
  // sizeof(empty_base_handle) will simply be sizeof(Data). Google [empty base
  // class optimization] for more details.
  template <typename Base1, typename Base2, typename Data>
  struct empty_base_handle : public Base1, Base2 {
    empty_base_handle(const Base1 &b1, const Base2 &b2, const Data &d)
        : Base1(b1),
          Base2(b2),
          data(d) {}
    Data data;
  };

  struct node_stats {
    using size_type = typename Params::size_type;

    node_stats(size_type l, size_type i)
        : leaf_nodes(l),
          internal_nodes(i) {
    }

    node_stats& operator+=(const node_stats &x) {
      leaf_nodes += x.leaf_nodes;
      internal_nodes += x.internal_nodes;
      return *this;
    }

    size_type leaf_nodes;
    size_type internal_nodes;
  };

 public:
  using key_type = typename Params::key_type;
  using value_type = typename Params::value_type;
  using size_type = typename Params::size_type;
  using difference_type = typename Params::difference_type;
  using key_compare = typename Params::key_compare;
  using value_compare = typename Params::value_compare;
  using allocator_type = typename Params::allocator_type;
  using reference = typename Params::reference;
  using const_reference = typename Params::const_reference;
  using pointer = typename Params::pointer;
  using const_pointer = typename Params::const_pointer;
  using iterator = btree_iterator<node_type, reference, pointer>;
  using const_iterator = typename iterator::const_iterator;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  // Internal types made public for use by btree_container types.
  using params_type = Params;

 private:
  // For use in copy_or_move_values_in_order.
  const value_type &maybe_move_from_iterator(const_iterator x) { return *x; }
  value_type &&maybe_move_from_iterator(iterator x) { return std::move(*x); }

  // Copies or moves (depending on the template parameter) the values in
  // x into this btree in their order in x. This btree must be empty before this
  // method is called. This method is used in copy construction, copy
  // assignment, and move assignment.
  template <typename Btree>
  void copy_or_move_values_in_order(Btree *x);

  // Validates that various assumptions/requirements are true at compile time.
  constexpr static bool static_assert_validation();

 public:
  btree(const key_compare &comp, const allocator_type &alloc);

  btree(const btree &x);
  btree(btree &&x) noexcept
      : root_(std::move(x.root_)),
        rightmost_(std::exchange(x.rightmost_, EmptyNode())),
        size_(std::exchange(x.size_, 0)) {
    x.mutable_root() = EmptyNode();
  }

  ~btree() {
    // Put static_asserts in destructor to avoid triggering them before the type
    // is complete.
    static_assert(static_assert_validation(), "This call must be elided.");
    clear();
  }

  // Assign the contents of x to *this.
  btree &operator=(const btree &x);
  btree &operator=(btree &&x) noexcept;

  iterator begin() {
    return iterator(leftmost(), 0);
  }
  const_iterator begin() const {
    return const_iterator(leftmost(), 0);
  }
  iterator end() {
    return iterator(rightmost_, rightmost_->count());
  }
  const_iterator end() const {
    return const_iterator(rightmost_, rightmost_->count());
  }
  reverse_iterator rbegin() {
    return reverse_iterator(end());
  }
  const_reverse_iterator rbegin() const {
    return const_reverse_iterator(end());
  }
  reverse_iterator rend() {
    return reverse_iterator(begin());
  }
  const_reverse_iterator rend() const {
    return const_reverse_iterator(begin());
  }

  // Finds the first element whose key is not less than key.
  template <typename K>
  iterator lower_bound(const K &key) {
    return internal_end(internal_lower_bound(key));
  }
  template <typename K>
  const_iterator lower_bound(const K &key) const {
    return internal_end(internal_lower_bound(key));
  }

  // Finds the first element whose key is greater than key.
  template <typename K>
  iterator upper_bound(const K &key) {
    return internal_end(internal_upper_bound(key));
  }
  template <typename K>
  const_iterator upper_bound(const K &key) const {
    return internal_end(internal_upper_bound(key));
  }

  // Finds the range of values which compare equal to key. The first member of
  // the returned pair is equal to lower_bound(key). The second member pair of
  // the pair is equal to upper_bound(key).
  template <typename K>
  std::pair<iterator, iterator> equal_range(const K &key) {
    return {lower_bound(key), upper_bound(key)};
  }
  template <typename K>
  std::pair<const_iterator, const_iterator> equal_range(const K &key) const {
    return {lower_bound(key), upper_bound(key)};
  }

  // Inserts a value into the btree only if it does not already exist. The
  // boolean return value indicates whether insertion succeeded or failed.
  // Requirement: if `key` already exists in the btree, does not consume `args`.
  // Requirement: `key` is never referenced after consuming `args`.
  template <typename... Args>
  std::pair<iterator, bool> insert_unique(const key_type &key, Args &&... args);

  // Inserts with hint. Checks to see if the value should be placed immediately
  // before `position` in the tree. If so, then the insertion will take
  // amortized constant time. If not, the insertion will take amortized
  // logarithmic time as if a call to insert_unique() were made.
  // Requirement: if `key` already exists in the btree, does not consume `args`.
  // Requirement: `key` is never referenced after consuming `args`.
  template <typename... Args>
  std::pair<iterator, bool> insert_hint_unique(iterator position,
                                               const key_type &key,
                                               Args &&... args);

  // Insert a range of values into the btree.
  template <typename InputIterator>
  void insert_iterator_unique(InputIterator b, InputIterator e);

  // Inserts a value into the btree.
  template <typename ValueType>
  iterator insert_multi(const key_type &key, ValueType &&v);

  // Inserts a value into the btree.
  template <typename ValueType>
  iterator insert_multi(ValueType &&v) {
    return insert_multi(params_type::key(v), std::forward<ValueType>(v));
  }

  // Insert with hint. Check to see if the value should be placed immediately
  // before position in the tree. If it does, then the insertion will take
  // amortized constant time. If not, the insertion will take amortized
  // logarithmic time as if a call to insert_multi(v) were made.
  template <typename ValueType>
  iterator insert_hint_multi(iterator position, ValueType &&v);

  // Insert a range of values into the btree.
  template <typename InputIterator>
  void insert_iterator_multi(InputIterator b, InputIterator e);

  // Erase the specified iterator from the btree. The iterator must be valid
  // (i.e. not equal to end()).  Return an iterator pointing to the node after
  // the one that was erased (or end() if none exists).
  // Requirement: does not read the value at `*iter`.
  iterator erase(iterator iter);

  // Erases range. Returns the number of keys erased and an iterator pointing
  // to the element after the last erased element.
  std::pair<size_type, iterator> erase(iterator begin, iterator end);

  // Erases the specified key from the btree. Returns 1 if an element was
  // erased and 0 otherwise.
  template <typename K>
  size_type erase_unique(const K &key);

  // Erases all of the entries matching the specified key from the
  // btree. Returns the number of elements erased.
  template <typename K>
  size_type erase_multi(const K &key);

  // Finds the iterator corresponding to a key or returns end() if the key is
  // not present.
  template <typename K>
  iterator find(const K &key) {
    return internal_end(internal_find(key));
  }
  template <typename K>
  const_iterator find(const K &key) const {
    return internal_end(internal_find(key));
  }

  // Returns a count of the number of times the key appears in the btree.
  template <typename K>
  size_type count_unique(const K &key) const {
    const iterator begin = internal_find(key);
    if (begin.node == nullptr) {
      // The key doesn't exist in the tree.
      return 0;
    }
    return 1;
  }
  // Returns a count of the number of times the key appears in the btree.
  template <typename K>
  size_type count_multi(const K &key) const {
    const auto range = equal_range(key);
    return std::distance(range.first, range.second);
  }

  // Clear the btree, deleting all of the values it contains.
  void clear();

  // Swap the contents of *this and x.
  void swap(btree &x);

  const key_compare &key_comp() const noexcept {
    return *static_cast<const key_compare*>(&root_);
  }
  template <typename K, typename LK>
  bool compare_keys(const K &x, const LK &y) const {
    return compare_result_as_less_than(key_comp()(x, y));
  }

  // Verifies the structure of the btree.
  void verify() const;

  // Size routines.
  size_type size() const { return size_; }
  size_type max_size() const { return std::numeric_limits<size_type>::max(); }
  bool empty() const { return size_ == 0; }

  // The height of the btree. An empty tree will have height 0.
  size_type height() const {
    size_type h = 0;
    if (!empty()) {
      // Count the length of the chain from the leftmost node up to the
      // root. We actually count from the root back around to the level below
      // the root, but the calculation is the same because of the circularity
      // of that traversal.
      const node_type *n = root();
      do {
        ++h;
        n = n->parent();
      } while (n != root());
    }
    return h;
  }

  // The number of internal, leaf and total nodes used by the btree.
  size_type leaf_nodes() const {
    return internal_stats(root()).leaf_nodes;
  }
  size_type internal_nodes() const {
    return internal_stats(root()).internal_nodes;
  }
  size_type nodes() const {
    node_stats stats = internal_stats(root());
    return stats.leaf_nodes + stats.internal_nodes;
  }

  // The total number of bytes used by the btree.
  size_type bytes_used() const {
    node_stats stats = internal_stats(root());
    if (stats.leaf_nodes == 1 && stats.internal_nodes == 0) {
      return sizeof(*this) +
             node_type::LeafSize(root()->max_count());
    } else {
      return sizeof(*this) +
             stats.leaf_nodes * node_type::LeafSize() +
             stats.internal_nodes * node_type::InternalSize();
    }
  }

  // The average number of bytes used per value stored in the btree.
  static double average_bytes_per_value() {
    // Returns the number of bytes per value on a leaf node that is 75%
    // full. Experimentally, this matches up nicely with the computed number of
    // bytes per value in trees that had their values inserted in random order.
    return node_type::LeafSize() / (kNodeValues * 0.75);
  }

  // The fullness of the btree. Computed as the number of elements in the btree
  // divided by the maximum number of elements a tree with the current number
  // of nodes could hold. A value of 1 indicates perfect space
  // utilization. Smaller values indicate space wastage.
  // Returns 0 for empty trees.
  double fullness() const {
    if (empty()) return 0.0;
    return static_cast<double>(size()) / (nodes() * kNodeValues);
  }
  // The overhead of the btree structure in bytes per node. Computed as the
  // total number of bytes used by the btree minus the number of bytes used for
  // storing elements divided by the number of elements.
  // Returns 0 for empty trees.
  double overhead() const {
    if (empty()) return 0.0;
    return (bytes_used() - size() * sizeof(value_type)) /
           static_cast<double>(size());
  }

  // The allocator used by the btree.
  allocator_type get_allocator() const {
    return allocator();
  }

 private:
  // Internal accessor routines.
  node_type *root() { return root_.data; }
  const node_type *root() const { return root_.data; }
  node_type *&mutable_root() { return root_.data; }
  key_compare *mutable_key_comp() noexcept {
    return static_cast<key_compare*>(&root_);
  }

  node_type* rightmost() {
    return rightmost_;
  }
  const node_type* rightmost() const {
    return rightmost_;
  }
  // The leftmost node is stored as the parent of the root node.
  node_type* leftmost() { return root() ? root()->parent() : NULL; }
  const node_type* leftmost() const { return root() ? root()->parent() : NULL; }

  // The size of the tree is stored in the root node.
  size_type* mutable_size() { return root()->mutable_size(); }

  // Allocator routines.
  allocator_type* mutable_allocator() noexcept {
    return static_cast<allocator_type*>(&root_);
  }
  const allocator_type& allocator() const noexcept {
    return *static_cast<const allocator_type*>(&root_);
  }

  node_type *allocate(const size_type size) {
    using aligned_alloc_t =
      AlignedAlloc<node_type::Alignment(), allocator_type>;
    return static_cast<node_type*>(
      aligned_alloc_t::allocate(mutable_allocator(), size));
  }

  // Node creation/deletion routines.
  node_type* new_internal_node(node_type *parent) {
    node_type *p = allocate(node_type::InternalSize());
    return node_type::init_internal(p, parent);
  }
  node_type* new_leaf_node(node_type *parent) {
    node_type *p = allocate(node_type::LeafSize());
    return node_type::init_leaf(p, parent, kNodeValues);
  }
  node_type *new_leaf_root_node(const int max_count) {
    node_type *p = allocate(node_type::LeafSize(max_count));
    return node_type::init_leaf(p, p, max_count);
  }

  // Deletion helper routines.
  void erase_same_node(iterator begin, iterator end);
  iterator erase_from_leaf_node(iterator begin, size_type to_erase);
  iterator rebalance_after_delete(iterator iter);

  // Deallocates a node of a certain size in bytes using the allocator.
  void deallocate(const size_type size, node_type *node) {
    using aligned_alloc_t =
      AlignedAlloc<node_type::Alignment(), allocator_type>;
    aligned_alloc_t::deallocate(mutable_allocator(), node, size);
  }

  void delete_internal_node(node_type *node) {
    node->destroy(mutable_allocator());
    deallocate(node_type::InternalSize(), node);
  }
  void delete_leaf_node(node_type *node) {
    node->destroy(mutable_allocator());
    deallocate(node_type::LeafSize(node->max_count()), node);
  }

  // Rebalances or splits the node iter points to.
  void rebalance_or_split(iterator *iter);

  // Merges the values of left, right and the delimiting key on their parent
  // onto left, removing the delimiting key and deleting right.
  void merge_nodes(node_type *left, node_type *right);

  // Tries to merge node with its left or right sibling, and failing that,
  // rebalance with its left or right sibling. Returns true if a merge
  // occurred, at which point it is no longer valid to access node. Returns
  // false if no merging took place.
  bool try_merge_or_rebalance(iterator *iter);

  // Tries to shrink the height of the tree by 1.
  void try_shrink();

  iterator internal_end(iterator iter) {
    return iter.node != nullptr ? iter : end();
  }
  const_iterator internal_end(const_iterator iter) const {
    return iter.node != nullptr ? iter : end();
  }

  // Emplaces a value into the btree immediately before iter. Requires that
  // key(v) <= iter.key() and (--iter).key() <= key(v).
  template <typename... Args>
  iterator internal_emplace(iterator iter, Args &&... args);

  // Returns an iterator pointing to the first value >= the value "iter" is
  // pointing at. Note that "iter" might be pointing to an invalid location as
  // iter.position == iter.node->count(). This routine simply moves iter up in
  // the tree to a valid location.
  // Requires: iter.node is non-null.
  template <typename IterType>
  static IterType internal_last(IterType iter);

  // Returns an iterator pointing to the leaf position at which key would
  // reside in the tree. We provide 2 versions of internal_locate. The first
  // version uses a less-than comparator and is incapable of distinguishing when
  // there is an exact match. The second version is for the key-compare-to
  // specialization and distinguishes exact matches. The key-compare-to
  // specialization allows the caller to avoid a subsequent comparison to
  // determine if an exact match was made, which is important for keys with
  // expensive comparison, such as strings.
  template <typename K>
  SearchResult<iterator, is_key_compare_to::value> internal_locate(
      const K &key) const;

  template <typename K>
  SearchResult<iterator, false> internal_locate_impl(
      const K &key, std::false_type /* IsCompareTo */) const;

  template <typename K>
  SearchResult<iterator, true> internal_locate_impl(
      const K &key, std::true_type /* IsCompareTo */) const;

  // Internal routine which implements lower_bound().
  template <typename K>
  iterator internal_lower_bound(const K &key) const;

  // Internal routine which implements upper_bound().
  template <typename K>
  iterator internal_upper_bound(const K &key) const;

  // Internal routine which implements find().
  template <typename K>
  iterator internal_find(const K &key) const;

  // Deletes a node and all of its children.
  void internal_clear(node_type *node);

  // Verifies the tree structure of node.
  int internal_verify(const node_type *node,
                      const key_type *lo, const key_type *hi) const;

  node_stats internal_stats(const node_type *node) const {
    // The root can be a static empty node.
    if (node == nullptr || (node == root() && empty())) {
      return node_stats(0, 0);
    }
    if (node->leaf()) {
      return node_stats(1, 0);
    }
    node_stats res(0, 1);
    for (int i = 0; i <= node->count(); ++i) {
      res += internal_stats(node->child(i));
    }
    return res;
  }

 private:
  empty_base_handle<key_compare, allocator_type, node_type*> root_;

  // A pointer to the rightmost node. Note that the leftmost node is stored as
  // the root's parent.
  node_type *rightmost_;

  // Number of values.
  size_type size_;
};

////
// btree_node methods
template <typename P>
template <typename... Args>
inline void btree_node<P>::emplace_value(const size_type i,
                                         allocator_type *alloc,
                                         Args &&... args) {
  assert(i <= count());
  // Shift old values to create space for new value and then construct it in
  // place.
  if (i < count()) {
    value_init(count(), alloc, slot(count() - 1));
    std::copy_backward(std::make_move_iterator(slot(i)),
                       std::make_move_iterator(slot(count() - 1)),
                       slot(count()));
    value_destroy(i, alloc);
  }
  value_init(i, alloc, std::forward<Args>(args)...);
  set_count(count() + 1);

  if (!leaf() && count() > i + 1) {
    for (int j = count(); j > i + 1; --j) {
      set_child(j, child(j - 1));
    }
    clear_child(i + 1);
  }
}

template <typename P>
inline void btree_node<P>::remove_value(const int i, allocator_type *alloc) {
  if (!leaf() && count() > i + 1) {
    assert(child(i + 1)->count() == 0);
    for (size_type j = i + 1; j < count(); ++j) {
      set_child(j, child(j + 1));
    }
    clear_child(count());
  }

  remove_values_ignore_children(i, /*to_erase=*/1, alloc);
}

template <typename P>
inline void btree_node<P>::remove_values_ignore_children(
    const int i, const int to_erase, allocator_type *alloc) {
  assert(to_erase >= 0);
  std::copy(std::make_move_iterator(slot(i + to_erase)),
            std::make_move_iterator(slot(count())),
            slot(i));
  value_destroy_n(count() - to_erase, to_erase, alloc);
  set_count(count() - to_erase);
}

template <typename P>
void btree_node<P>::rebalance_right_to_left(const int to_move,
                                            btree_node *right,
                                            allocator_type *alloc) {
  assert(parent() == right->parent());
  assert(position() + 1 == right->position());
  assert(right->count() >= count());
  assert(to_move >= 1);
  assert(to_move <= right->count());

  // 1) Move the delimiting value in the parent to the left node.
  value_init(count(), alloc, parent()->slot(position()));

  // 2) Move the (to_move - 1) values from the right node to the left node.
  right->uninitialized_move_n(to_move - 1, 0, count() + 1, this, alloc);

  // 3) Move the new delimiting value to the parent from the right node.
  params_type::move(alloc, right->slot(to_move - 1),
                    parent()->slot(position()));

  // 4) Shift the values in the right node to their correct position.
  std::copy(std::make_move_iterator(right->slot(to_move)),
            std::make_move_iterator(right->slot(right->count())),
            right->slot(0));

  // 5) Destroy the now-empty to_move entries in the right node.
  right->value_destroy_n(right->count() - to_move, to_move, alloc);

  if (!leaf()) {
    // Move the child pointers from the right to the left node.
    for (int i = 0; i < to_move; ++i) {
      init_child(count() + i + 1, right->child(i));
    }
    for (int i = 0; i <= right->count() - to_move; ++i) {
      assert(i + to_move <= right->max_count());
      right->init_child(i, right->child(i + to_move));
      right->clear_child(i + to_move);
    }
  }

  // Fixup the counts on the left and right nodes.
  set_count(count() + to_move);
  right->set_count(right->count() - to_move);
}

template <typename P>
void btree_node<P>::rebalance_left_to_right(const int to_move,
                                            btree_node *right,
                                            allocator_type *alloc) {
  assert(parent() == right->parent());
  assert(position() + 1 == right->position());
  assert(count() >= right->count());
  assert(to_move >= 1);
  assert(to_move <= count());

  // Values in the right node are shifted to the right to make room for the
  // new to_move values. Then, the delimiting value in the parent and the
  // other (to_move - 1) values in the left node are moved into the right node.
  // Lastly, a new delimiting value is moved from the left node into the
  // parent, and the remaining empty left node entries are destroyed.

  if (right->count() >= to_move) {
    // The original location of the right->count() values are sufficient to hold
    // the new to_move entries from the parent and left node.

    // 1) Shift existing values in the right node to their correct positions.
    right->uninitialized_move_n(to_move, right->count() - to_move,
                                right->count(), right, alloc);
    std::copy_backward(std::make_move_iterator(right->slot(0)),
                       std::make_move_iterator(right->slot(right->count() - to_move)),
                       right->slot(right->count()));

    // 2) Move the delimiting value in the parent to the right node.
    params_type::move(alloc, parent()->slot(position()),
                      right->slot(to_move - 1));

    // 3) Move the (to_move - 1) values from the left node to the right node.
    std::copy(std::make_move_iterator(slot(count() - (to_move - 1))),
              std::make_move_iterator(slot(count())),
              right->slot(0));
  } else {
    // The right node does not have enough initialized space to hold the new
    // to_move entries, so part of them will move to uninitialized space.

    // 1) Shift existing values in the right node to their correct positions.
    right->uninitialized_move_n(right->count(), 0, to_move, right, alloc);

    // 2) Move the delimiting value in the parent to the right node.
    right->value_init(to_move - 1, alloc, parent()->slot(position()));

    // 3) Move the (to_move - 1) values from the left node to the right node.
    const size_type uninitialized_remaining = to_move - right->count() - 1;
    uninitialized_move_n(uninitialized_remaining,
                         count() - uninitialized_remaining, right->count(),
                         right, alloc);
    std::copy(std::make_move_iterator(slot(count() - (to_move - 1))),
              std::make_move_iterator(slot(count() - uninitialized_remaining)),
              right->slot(0));
  }

  // 4) Move the new delimiting value to the parent from the left node.
  params_type::move(alloc, slot(count() - to_move), parent()->slot(position()));

  // 5) Destroy the now-empty to_move entries in the left node.
  value_destroy_n(count() - to_move, to_move, alloc);

  if (!leaf()) {
    // Move the child pointers from the left to the right node.
    for (int i = right->count(); i >= 0; --i) {
      right->init_child(i + to_move, right->child(i));
      right->clear_child(i);
    }
    for (int i = 1; i <= to_move; ++i) {
      right->init_child(i - 1, child(count() - to_move + i));
      clear_child(count() - to_move + i);
    }
  }

  // Fixup the counts on the left and right nodes.
  set_count(count() - to_move);
  right->set_count(right->count() + to_move);
}

template <typename P>
void btree_node<P>::split(const int insert_position, btree_node *dest,
                          allocator_type *alloc) {
  assert(dest->count() == 0);
  assert(max_count() == kNodeValues);

  // We bias the split based on the position being inserted. If we're
  // inserting at the beginning of the left node then bias the split to put
  // more values on the right node. If we're inserting at the end of the
  // right node then bias the split to put more values on the left node.
  if (insert_position == 0) {
    dest->set_count(count() - 1);
  } else if (insert_position == kNodeValues) {
    dest->set_count(0);
  } else {
    dest->set_count(count() / 2);
  }
  set_count(count() - dest->count());
  assert(count() >= 1);

  // Move values from the left sibling to the right sibling.
  uninitialized_move_n(dest->count(), count(), 0, dest, alloc);

  // Destroy the now-empty entries in the left node.
  value_destroy_n(count(), dest->count(), alloc);

  // The split key is the largest value in the left sibling.
  set_count(count() - 1);
  parent()->emplace_value(position(), alloc, slot(count()));
  value_destroy(count(), alloc);
  parent()->init_child(position() + 1, dest);

  if (!leaf()) {
    for (int i = 0; i <= dest->count(); ++i) {
      assert(child(count() + i + 1) != nullptr);
      dest->init_child(i, child(count() + i + 1));
      clear_child(count() + i + 1);
    }
  }
}

template <typename P>
void btree_node<P>::merge(btree_node *src, allocator_type *alloc) {
  assert(parent() == src->parent());
  assert(position() + 1 == src->position());

  // Move the delimiting value to the left node.
  value_init(count(), alloc, parent()->slot(position()));

    // Move the values from the right to the left node.
  src->uninitialized_move_n(src->count(), 0, count() + 1, this, alloc);

  // Destroy the now-empty entries in the right node.
  src->value_destroy_n(0, src->count(), alloc);

  if (!leaf()) {
    // Move the child pointers from the right to the left node.
    for (int i = 0; i <= src->count(); ++i) {
      init_child(count() + i + 1, src->child(i));
      src->clear_child(i);
    }
  }

  // Fixup the counts on the src and dest nodes.
  set_count(1 + count() + src->count());
  src->set_count(0);

  // Remove the value on the parent node.
  parent()->remove_value(position(), alloc);
}

template <typename P>
void btree_node<P>::swap(btree_node *x, allocator_type *alloc) {
  using std::swap;
  assert(leaf() == x->leaf());

  // Determine which is the smaller/larger node.
  btree_node *smaller = this, *larger = x;
  if (smaller->count() > larger->count()) {
    swap(smaller, larger);
  }

  // Swap the values.
  std::swap_ranges(smaller->slot(0), smaller->slot(smaller->count()),
                   larger->slot(0));

  // Move values that can't be swapped.
  const size_type to_move = larger->count() - smaller->count();
  larger->uninitialized_move_n(to_move, smaller->count(), smaller->count(),
                               smaller, alloc);
  larger->value_destroy_n(smaller->count(), to_move, alloc);

  if (!leaf()) {
    // Swap the child pointers.
    std::swap_ranges(&smaller->mutable_child(0),
                     &smaller->mutable_child(smaller->count() + 1),
                     &larger->mutable_child(0));
    // Update swapped children's parent pointers.
    int i = 0;
    for (; i <= smaller->count(); ++i) {
      smaller->child(i)->set_parent(smaller);
      larger->child(i)->set_parent(larger);
    }
    // Move the child pointers that couldn't be swapped.
    for (; i <= larger->count(); ++i) {
      smaller->init_child(i, larger->child(i));
      larger->clear_child(i);
    }
  }

  // Swap the counts.
  swap(mutable_count(), x->mutable_count());
}

////
// btree_iterator methods
template <typename N, typename R, typename P>
void btree_iterator<N, R, P>::increment_slow() {
  if (node->leaf()) {
    assert(position >= node->count());
    btree_iterator save(*this);
    while (position == node->count() && !node->is_root()) {
      assert(node->parent()->child(node->position()) == node);
      position = node->position();
      node = node->parent();
    }
    if (position == node->count()) {
      *this = save;
    }
  } else {
    assert(position < node->count());
    node = node->child(position + 1);
    while (!node->leaf()) {
      node = node->child(0);
    }
    position = 0;
  }
}

template <typename N, typename R, typename P>
void btree_iterator<N, R, P>::decrement_slow() {
  if (node->leaf()) {
    assert(position <= -1);
    btree_iterator save(*this);
    while (position < 0 && !node->is_root()) {
      assert(node->parent()->child(node->position()) == node);
      position = node->position() - 1;
      node = node->parent();
    }
    if (position < 0) {
      *this = save;
    }
  } else {
    assert(position >= 0);
    node = node->child(position);
    while (!node->leaf()) {
      node = node->child(node->count());
    }
    position = node->count() - 1;
  }
}

////
// btree methods
template <typename P>
template <typename Btree>
void btree<P>::copy_or_move_values_in_order(Btree *x) {
  static_assert(std::is_same_v<btree, Btree>||
                std::is_same_v<const btree, Btree>,
                "Btree type must be same or const.");
  assert(empty());

  // We can avoid key comparisons because we know the order of the
  // values is the same order we'll store them in.
  auto iter = x->begin();
  if (iter == x->end()) return;
  insert_multi(maybe_move_from_iterator(iter));
  ++iter;
  for (; iter != x->end(); ++iter) {
    // If the btree is not empty, we can just insert the new value at the end
    // of the tree.
    internal_emplace(end(), maybe_move_from_iterator(iter));
  }
}

template <typename P>
constexpr bool btree<P>::static_assert_validation() {
  static_assert(std::is_nothrow_copy_constructible_v<key_compare>,
                "Key comparison must be nothrow copy constructible");
  static_assert(std::is_nothrow_copy_constructible_v<allocator_type>,
                "Allocator must be nothrow copy constructible");
  static_assert(std::is_trivially_copyable_v<iterator>,
                "iterator not trivially copyable.");

  // Note: We assert that kTargetValues, which is computed from
  // Params::kTargetNodeSize, must fit the base_fields::field_type.
  static_assert(
      kNodeValues < (1 << (8 * sizeof(typename node_type::field_type))),
      "target node size too large");

  // Verify that key_compare returns an absl::{weak,strong}_ordering or bool.
  using compare_result_type =
    std::invoke_result_t<key_compare, key_type, key_type>;
  static_assert(
      std::is_same_v<compare_result_type, bool> ||
      std::is_signed_v<compare_result_type>,
      "key comparison function must return a signed value or "
      "bool.");

  // Test the assumption made in setting kNodeValueSpace.
  static_assert(node_type::MinimumOverhead() >= sizeof(void *) + 4,
                "node space assumption incorrect");

  return true;
}

template <typename P>
btree<P>::btree(const key_compare &comp, const allocator_type &alloc)
  : root_(comp, alloc, EmptyNode()), rightmost_(EmptyNode()), size_(0) {}

template <typename P>
btree<P>::btree(const btree &x) : btree(x.key_comp(), x.allocator()) {
  copy_or_move_values_in_order(&x);
}

template <typename P>
template <typename... Args>
auto btree<P>::insert_unique(const key_type &key, Args &&... args)
    -> std::pair<iterator, bool> {
  if (empty()) {
    mutable_root() = rightmost_ = new_leaf_root_node(1);
  }

  auto res = internal_locate(key);
  iterator &iter = res.value;

  if constexpr (res.has_match) {
    if (res.IsEq()) {
      // The key already exists in the tree, do nothing.
      return {iter, false};
    }
  } else {
    iterator last = internal_last(iter);
    if (last.node && !compare_keys(key, last.key())) {
      // The key already exists in the tree, do nothing.
      return {last, false};
    }
  }
  return {internal_emplace(iter, std::forward<Args>(args)...), true};
}

template <typename P>
template <typename... Args>
inline auto btree<P>::insert_hint_unique(iterator position, const key_type &key,
                                         Args &&... args)
    -> std::pair<iterator, bool> {
  if (!empty()) {
    if (position == end() || compare_keys(key, position.key())) {
      iterator prev = position;
      if (position == begin() || compare_keys((--prev).key(), key)) {
        // prev.key() < key < position.key()
        return {internal_emplace(position, std::forward<Args>(args)...), true};
      }
    } else if (compare_keys(position.key(), key)) {
      ++position;
      if (position == end() || compare_keys(key, position.key())) {
        // {original `position`}.key() < key < {current `position`}.key()
        return {internal_emplace(position, std::forward<Args>(args)...), true};
      }
    } else {
      // position.key() == key
      return {position, false};
    }
  }
  return insert_unique(key, std::forward<Args>(args)...);
}

template <typename P>
template <typename InputIterator>
void btree<P>::insert_iterator_unique(InputIterator b, InputIterator e) {
  for (; b != e; ++b) {
    insert_hint_unique(end(), params_type::key(*b), *b);
  }
}

template <typename P>
template <typename ValueType>
auto btree<P>::insert_multi(const key_type &key, ValueType&& v) -> iterator {
  if (empty()) {
    mutable_root() = rightmost_ = new_leaf_root_node(1);
  }

  iterator iter = internal_upper_bound(key);
  if (iter.node == nullptr) {
    iter = end();
  }
  return internal_emplace(iter, std::forward<ValueType>(v));
}

template <typename P>
template <typename ValueType>
auto btree<P>::insert_hint_multi(iterator position, ValueType &&v) -> iterator {
  if (!empty()) {
    const key_type &key = params_type::key(v);
    if (position == end() || !compare_keys(position.key(), key)) {
      iterator prev = position;
      if (position == begin() || !compare_keys(key, (--prev).key())) {
        // prev.key() <= key <= position.key()
        return internal_emplace(position, std::forward<ValueType>(v));
      }
    } else {
      iterator next = position;
      ++next;
      if (next == end() || !compare_keys(next.key(), key)) {
        // position.key() < key <= next.key()
        return internal_emplace(next, std::forward<ValueType>(v));
      }
    }
  }
  return insert_multi(std::forward<ValueType>(v));
}

template <typename P>
template <typename InputIterator>
void btree<P>::insert_iterator_multi(InputIterator b, InputIterator e) {
  for (; b != e; ++b) {
    insert_hint_multi(end(), *b);
  }
}

template <typename P>
auto btree<P>::operator=(const btree &x) -> btree & {
  if (this != &x) {
    clear();

    *mutable_key_comp() = x.key_comp();
    if constexpr (std::allocator_traits<
                  allocator_type>::propagate_on_container_copy_assignment::value) {
      *mutable_allocator() = x.allocator();
    }

    copy_or_move_values_in_order(&x);
  }
  return *this;
}

template <typename P>
auto btree<P>::operator=(btree &&x) noexcept -> btree & {
  if (this != &x) {
    clear();

    using std::swap;
    if constexpr (std::allocator_traits<
                  allocator_type>::propagate_on_container_copy_assignment::value) {
      // Note: `root_` also contains the allocator and the key comparator.
      swap(root_, x.root_);
      swap(rightmost_, x.rightmost_);
      swap(size_, x.size_);
    } else {
      if (allocator() == x.allocator()) {
        swap(mutable_root(), x.mutable_root());
        swap(*mutable_key_comp(), *x.mutable_key_comp());
        swap(rightmost_, x.rightmost_);
        swap(size_, x.size_);
      } else {
        // We aren't allowed to propagate the allocator and the allocator is
        // different so we can't take over its memory. We must move each element
        // individually. We need both `x` and `this` to have `x`s key comparator
        // while moving the values so we can't swap the key comparators.
        *mutable_key_comp() = x.key_comp();
        copy_or_move_values_in_order(&x);
      }
    }
  }
  return *this;
}

template <typename P>
auto btree<P>::erase(iterator iter) -> iterator {
  bool internal_delete = false;
  if (!iter.node->leaf()) {
    // Deletion of a value on an internal node. First, move the largest value
    // from our left child here, then delete that position (in remove_value()
    // below). We can get to the largest value from our left child by
    // decrementing iter.
    iterator internal_iter(iter);
    --iter;
    assert(iter.node->leaf());
    params_type::move(mutable_allocator(), iter.node->slot(iter.position),
                      internal_iter.node->slot(internal_iter.position));
    internal_delete = true;
  }

  // Delete the key from the leaf.
  iter.node->remove_value(iter.position, mutable_allocator());
  --size_;

  // We want to return the next value after the one we just erased. If we
  // erased from an internal node (internal_delete == true), then the next
  // value is ++(++iter). If we erased from a leaf node (internal_delete ==
  // false) then the next value is ++iter. Note that ++iter may point to an
  // internal node and the value in the internal node may move to a leaf node
  // (iter.node) when rebalancing is performed at the leaf level.

  iterator res = rebalance_after_delete(iter);

  // If we erased from an internal node, advance the iterator.
  if (internal_delete) {
    ++res;
  }
  return res;
}

template <typename P>
auto btree<P>::rebalance_after_delete(iterator iter) -> iterator {
  // Merge/rebalance as we walk back up the tree.
  iterator res(iter);
  bool first_iteration = true;
  for (;;) {
    if (iter.node == root()) {
      try_shrink();
      if (empty()) {
        return end();
      }
      break;
    }
    if (iter.node->count() >= kMinNodeValues) {
      break;
    }
    bool merged = try_merge_or_rebalance(&iter);
    // On the first iteration, we should update `res` with `iter` because `res`
    // may have been invalidated.
    if (first_iteration) {
      res = iter;
      first_iteration = false;
    }
    if (!merged) {
      break;
    }
    iter.position = iter.node->position();
    iter.node = iter.node->parent();
  }

  // Adjust our return value. If we're pointing at the end of a node, advance
  // the iterator.
  if (res.position == res.node->count()) {
    res.position = res.node->count() - 1;
    ++res;
  }

  return res;
}

template <typename P>
auto btree<P>::erase(iterator begin, iterator end)
    -> std::pair<size_type, iterator> {
  difference_type count = std::distance(begin, end);
  assert(count >= 0);

  if (count == 0) {
    return {0, begin};
  }

  if (count == size_) {
    clear();
    return {count, this->end()};
  }

  if (begin.node == end.node) {
    erase_same_node(begin, end);
    size_ -= count;
    return {count, rebalance_after_delete(begin)};
  }

  const size_type target_size = size_ - count;
  while (size_ > target_size) {
    if (begin.node->leaf()) {
      const size_type remaining_to_erase = size_ - target_size;
      const size_type remaining_in_node = begin.node->count() - begin.position;
      begin = erase_from_leaf_node(
          begin, std::min(remaining_to_erase, remaining_in_node));
    } else {
      begin = erase(begin);
    }
  }
  return {count, begin};
}

template <typename P>
void btree<P>::erase_same_node(iterator begin, iterator end) {
  assert(begin.node == end.node);
  assert(end.position > begin.position);

  node_type *node = begin.node;
  size_type to_erase = end.position - begin.position;
  if (!node->leaf()) {
    // Delete all children between begin and end.
    for (size_type i = 0; i < to_erase; ++i) {
      internal_clear(node->child(begin.position + i + 1));
    }
    // Rotate children after end into new positions.
    for (size_type i = begin.position + to_erase + 1; i <= node->count(); ++i) {
      node->set_child(i - to_erase, node->child(i));
      node->clear_child(i);
    }
  }
  node->remove_values_ignore_children(begin.position, to_erase,
                                      mutable_allocator());

  // Do not need to update rightmost_, because
  // * either end == this->end(), and therefore node == rightmost_, and still
  //   exists
  // * or end != this->end(), and therefore rightmost_ hasn't been erased, since
  //   it wasn't covered in [begin, end)
}

template <typename P>
auto btree<P>::erase_from_leaf_node(iterator begin, size_type to_erase)
    -> iterator {
  node_type *node = begin.node;
  assert(node->leaf());
  assert(node->count() > begin.position);
  assert(begin.position + to_erase <= node->count());

  node->remove_values_ignore_children(begin.position, to_erase,
                                      mutable_allocator());

  size_ -= to_erase;

  return rebalance_after_delete(begin);
}

template <typename P>
template <typename K>
auto btree<P>::erase_unique(const K &key) -> size_type {
  const iterator iter = internal_find(key);
  if (iter.node == nullptr) {
    // The key doesn't exist in the tree, return nothing done.
    return 0;
  }
  erase(iter);
  return 1;
}

template <typename P>
template <typename K>
auto btree<P>::erase_multi(const K &key) -> size_type {
  const iterator begin = internal_lower_bound(key);
  if (begin.node == nullptr) {
    // The key doesn't exist in the tree, return nothing done.
    return 0;
  }
  // Delete all of the keys between begin and upper_bound(key).
  const iterator end = internal_end(internal_upper_bound(key));
  return erase(begin, end).first;
}

template <typename P>
void btree<P>::clear() {
  if (!empty()) {
    internal_clear(root());
  }
  mutable_root() = EmptyNode();
  rightmost_ = EmptyNode();
  size_ = 0;
}

template <typename P>
void btree<P>::swap(btree &x) {
  using std::swap;
  if (std::allocator_traits<
          allocator_type>::propagate_on_container_swap::value) {
    // Note: `root_` also contains the allocator and the key comparator.
    swap(root_, x.root_);
  } else {
    // It's undefined behavior if the allocators are unequal here.
    assert(allocator() == x.allocator());
    swap(mutable_root(), x.mutable_root());
    swap(*mutable_key_comp(), *x.mutable_key_comp());
  }
  swap(rightmost_, x.rightmost_);
  swap(size_, x.size_);
}

template <typename P>
void btree<P>::verify() const {
  assert(root() != nullptr);
  assert(leftmost() != nullptr);
  assert(rightmost_ != nullptr);
  assert(empty() || size() == internal_verify(root(), nullptr, nullptr));
  assert(leftmost() == (++const_iterator(root(), -1)).node);
  assert(rightmost_ == (--const_iterator(root(), root()->count())).node);
  assert(leftmost()->leaf());
  assert(rightmost_->leaf());
}

template <typename P>
void btree<P>::rebalance_or_split(iterator *iter) {
  node_type *&node = iter->node;
  int &insert_position = iter->position;
  assert(node->count() == node->max_count());
  assert(kNodeValues == node->max_count());

  // First try to make room on the node by rebalancing.
  node_type *parent = node->parent();
  if (node != root()) {
    if (node->position() > 0) {
      // Try rebalancing with our left sibling.
      node_type *left = parent->child(node->position() - 1);
      assert(left->max_count() == kNodeValues);
      if (left->count() < kNodeValues) {
        // We bias rebalancing based on the position being inserted. If we're
        // inserting at the end of the right node then we bias rebalancing to
        // fill up the left node.
        int to_move = (kNodeValues - left->count()) /
                      (1 + (insert_position < kNodeValues));
        to_move = std::max(1, to_move);

        if (((insert_position - to_move) >= 0) ||
            ((left->count() + to_move) < kNodeValues)) {
          left->rebalance_right_to_left(to_move, node, mutable_allocator());

          assert(node->max_count() - node->count() == to_move);
          insert_position = insert_position - to_move;
          if (insert_position < 0) {
            insert_position = insert_position + left->count() + 1;
            node = left;
          }

          assert(node->count() < node->max_count());
          return;
        }
      }
    }

    if (node->position() < parent->count()) {
      // Try rebalancing with our right sibling.
      node_type *right = parent->child(node->position() + 1);
      assert(right->max_count() == kNodeValues);
      if (right->count() < kNodeValues) {
        // We bias rebalancing based on the position being inserted. If we're
        // inserting at the beginning of the left node then we bias rebalancing
        // to fill up the right node.
        int to_move =
            (kNodeValues - right->count()) / (1 + (insert_position > 0));
        to_move = (std::max)(1, to_move);

        if ((insert_position <= (node->count() - to_move)) ||
            ((right->count() + to_move) < kNodeValues)) {
          node->rebalance_left_to_right(to_move, right, mutable_allocator());

          if (insert_position > node->count()) {
            insert_position = insert_position - node->count() - 1;
            node = right;
          }

          assert(node->count() < node->max_count());
          return;
        }
      }
    }

    // Rebalancing failed, make sure there is room on the parent node for a new
    // value.
    assert(parent->max_count() == kNodeValues);
    if (parent->count() == kNodeValues) {
      iterator parent_iter(node->parent(), node->position());
      rebalance_or_split(&parent_iter);
    }
  } else {
    // Rebalancing not possible because this is the root node.
    // Create a new root node and set the current root node as the child of the
    // new root.
    parent = new_internal_node(parent);
    parent->init_child(0, root());
    mutable_root() = parent;
    // If the former root was a leaf node, then it's now the rightmost node.
    assert(!parent->child(0)->leaf() || parent->child(0) == rightmost_);
  }

  // Split the node.
  node_type *split_node;
  if (node->leaf()) {
    split_node = new_leaf_node(parent);
    node->split(insert_position, split_node, mutable_allocator());
    if (rightmost_ == node) rightmost_ = split_node;
  } else {
    split_node = new_internal_node(parent);
    node->split(insert_position, split_node, mutable_allocator());
  }

  if (insert_position > node->count()) {
    insert_position = insert_position - node->count() - 1;
    node = split_node;
  }
}

template <typename P>
void btree<P>::merge_nodes(node_type *left, node_type *right) {
  left->merge(right, mutable_allocator());
  if (right->leaf()) {
    if (rightmost_ == right) rightmost_ = left;
    delete_leaf_node(right);
  } else {
    delete_internal_node(right);
  }
}

template <typename P>
bool btree<P>::try_merge_or_rebalance(iterator *iter) {
  node_type *parent = iter->node->parent();
  if (iter->node->position() > 0) {
    // Try merging with our left sibling.
    node_type *left = parent->child(iter->node->position() - 1);
    assert(left->max_count() == kNodeValues);
    if ((1 + left->count() + iter->node->count()) <= kNodeValues) {
      iter->position += 1 + left->count();
      merge_nodes(left, iter->node);
      iter->node = left;
      return true;
    }
  }
  if (iter->node->position() < parent->count()) {
    // Try merging with our right sibling.
    node_type *right = parent->child(iter->node->position() + 1);
    assert(right->max_count() == kNodeValues);
    if ((1 + iter->node->count() + right->count()) <= kNodeValues) {
      merge_nodes(iter->node, right);
      return true;
    }
    // Try rebalancing with our right sibling. We don't perform rebalancing if
    // we deleted the first element from iter->node and the node is not
    // empty. This is a small optimization for the common pattern of deleting
    // from the front of the tree.
    if ((right->count() > kMinNodeValues) &&
        ((iter->node->count() == 0) ||
         (iter->position > 0))) {
      int to_move = (right->count() - iter->node->count()) / 2;
      to_move = std::min(to_move, right->count() - 1);
      iter->node->rebalance_right_to_left(to_move, right, mutable_allocator());
      return false;
    }
  }
  if (iter->node->position() > 0) {
    // Try rebalancing with our left sibling. We don't perform rebalancing if
    // we deleted the last element from iter->node and the node is not
    // empty. This is a small optimization for the common pattern of deleting
    // from the back of the tree.
    node_type *left = parent->child(iter->node->position() - 1);
    if ((left->count() > kMinNodeValues) &&
        ((iter->node->count() == 0) ||
         (iter->position < iter->node->count()))) {
      int to_move = (left->count() - iter->node->count()) / 2;
      to_move = std::min(to_move, left->count() - 1);
      left->rebalance_left_to_right(to_move, iter->node, mutable_allocator());
      iter->position += to_move;
      return false;
    }
  }
  return false;
}

template <typename P>
void btree<P>::try_shrink() {
  if (root()->count() > 0) {
    return;
  }
  // Deleted the last item on the root node, shrink the height of the tree.
  if (root()->leaf()) {
    assert(size() == 0);
    delete_leaf_node(root());
    mutable_root() = EmptyNode();
    rightmost_ = EmptyNode();
  } else {
    node_type *child = root()->child(0);
    child->make_root();
    delete_internal_node(root());
    mutable_root() = child;
  }
}

template <typename P>
template <typename IterType>
inline IterType btree<P>::internal_last(IterType iter) {
  assert(iter.node != nullptr);
  while (iter.position == iter.node->count()) {
    iter.position = iter.node->position();
    iter.node = iter.node->parent();
    if (iter.node->leaf()) {
      iter.node = nullptr;
      break;
    }
  }
  return iter;
}

template <typename P>
template <typename... Args>
inline auto btree<P>::internal_emplace(iterator iter, Args &&... args)
    -> iterator {
  if (!iter.node->leaf()) {
    // We can't insert on an internal node. Instead, we'll insert after the
    // previous value which is guaranteed to be on a leaf node.
    --iter;
    ++iter.position;
  }
  const int max_count = iter.node->max_count();
  if (iter.node->count() == max_count) {
    // Make room in the leaf for the new item.
    if (max_count < kNodeValues) {
      // Insertion into the root where the root is smaller than the full node
      // size. Simply grow the size of the root node.
      assert(iter.node == root());
      iter.node =
          new_leaf_root_node(std::min(kNodeValues, 2 * max_count));
      iter.node->swap(root(), mutable_allocator());
      delete_leaf_node(root());
      mutable_root() = iter.node;
      rightmost_ = iter.node;
    } else {
      rebalance_or_split(&iter);
    }
  }
  iter.node->emplace_value(iter.position, mutable_allocator(),
                           std::forward<Args>(args)...);
  ++size_;
  return iter;
}

template <typename P>
template <typename K>
inline auto btree<P>::internal_locate(const K &key) const
    -> SearchResult<iterator, is_key_compare_to::value> {
  return internal_locate_impl(key, is_key_compare_to());
}

template <typename P>
template <typename K>
inline auto btree<P>::internal_locate_impl(
    const K &key, std::false_type /* IsCompareTo */) const
    -> SearchResult<iterator, false> {
  iterator iter(const_cast<node_type *>(root()), 0);
  for (;;) {
    iter.position = iter.node->lower_bound(key, key_comp()).value;
    // NOTE: we don't need to walk all the way down the tree if the keys are
    // equal, but determining equality would require doing an extra comparison
    // on each node on the way down, and we will need to go all the way to the
    // leaf node in the expected case.
    if (iter.node->leaf()) {
      break;
    }
    iter.node = iter.node->child(iter.position);
  }
  return {iter};
}

template <typename P>
template <typename K>
inline auto btree<P>::internal_locate_impl(
    const K &key, std::true_type /* IsCompareTo */) const
    -> SearchResult<iterator, true> {
  iterator iter(const_cast<node_type *>(root()), 0);
  for (;;) {
    SearchResult<int, true> res = iter.node->lower_bound(key, key_comp());
    iter.position = res.value;
    if (res.match == MatchKind::kEq) {
      return {iter, MatchKind::kEq};
    }
    if (iter.node->leaf()) {
      break;
    }
    iter.node = iter.node->child(iter.position);
  }
  return {iter, MatchKind::kNe};
}

template <typename P>
template <typename K>
auto btree<P>::internal_lower_bound(const K &key) const -> iterator {
  iterator iter(const_cast<node_type *>(root()), 0);
  for (;;) {
    iter.position = iter.node->lower_bound(key, key_comp()).value;
    if (iter.node->leaf()) {
      break;
    }
    iter.node = iter.node->child(iter.position);
  }
  return internal_last(iter);
}

template <typename P>
template <typename K>
auto btree<P>::internal_upper_bound(const K &key) const -> iterator {
  iterator iter(const_cast<node_type *>(root()), 0);
  for (;;) {
    iter.position = iter.node->upper_bound(key, key_comp());
    if (iter.node->leaf()) {
      break;
    }
    iter.node = iter.node->child(iter.position);
  }
  return internal_last(iter);
}

template <typename P>
template <typename K>
auto btree<P>::internal_find(const K &key) const -> iterator {
  auto res = internal_locate(key);
  if constexpr (res.has_match) {
    if (res.IsEq()) {
      return res.value;
    }
  } else {
    const iterator iter = internal_last(res.value);
    if (iter.node != nullptr && !compare_keys(key, iter.key())) {
      return iter;
    }
  }
  return {nullptr, 0};
}

template <typename P>
void btree<P>::internal_clear(node_type *node) {
  if (!node->leaf()) {
    for (int i = 0; i <= node->count(); ++i) {
      internal_clear(node->child(i));
    }
    delete_internal_node(node);
  } else {
    delete_leaf_node(node);
  }
}

template <typename P>
int btree<P>::internal_verify(
    const node_type *node, const key_type *lo, const key_type *hi) const {
  assert(node->count() > 0);
  assert(node->count() <= node->max_count());
  if (lo) {
    assert(!compare_keys(node->key(0), *lo));
  }
  if (hi) {
    assert(!compare_keys(*hi, node->key(node->count() - 1)));
  }
  for (int i = 1; i < node->count(); ++i) {
    assert(!compare_keys(node->key(i), node->key(i - 1)));
  }
  int count = node->count();
  if (!node->leaf()) {
    for (int i = 0; i <= node->count(); ++i) {
      assert(node->child(i) != nullptr);
      assert(node->child(i)->parent() == node);
      assert(node->child(i)->position() == i);
      count += internal_verify(
          node->child(i),
          (i == 0) ? lo : &node->key(i - 1),
          (i == node->count()) ? hi : &node->key(i));
    }
  }
  return count;
}

} // namespace btree::internal
