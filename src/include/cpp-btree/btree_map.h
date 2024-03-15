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
//
// -----------------------------------------------------------------------------
// File: btree_map.h
// -----------------------------------------------------------------------------
//
// This header file defines B-tree maps: sorted associative containers mapping
// keys to values.
//
//     * `btree::btree_map<>`
//     * `btree::btree_multimap<>`
//
// These B-tree types are similar to the corresponding types in the STL
// (`std::map` and `std::multimap`) and generally conform to the STL interfaces
// of those types. However, because they are implemented using B-trees, they
// are more efficient in most situations.
//
// Unlike `std::map` and `std::multimap`, which are commonly implemented using
// red-black tree nodes, B-tree maps use more generic B-tree nodes able to hold
// multiple values per node. Holding multiple values per node often makes
// B-tree maps perform better than their `std::map` counterparts, because
// multiple entries can be checked within the same cache hit.
//
// However, these types should not be considered drop-in replacements for
// `std::map` and `std::multimap` as there are some API differences, which are
// noted in this header file.
//
// Importantly, insertions and deletions may invalidate outstanding iterators,
// pointers, and references to elements. Such invalidations are typically only
// an issue if insertion and deletion operations are interleaved with the use of
// more than one iterator, pointer, or reference simultaneously. For this
// reason, `insert()` and `erase()` return a valid iterator at the current
// position.

#pragma once

#include "btree.h"
#include "btree_container.h"

namespace btree {

// btree::btree_map<>
//
// A `btree::btree_map<K, V>` is an ordered associative container of
// unique keys and associated values designed to be a more efficient replacement
// for `std::map` (in most cases).
//
// Keys are sorted using an (optional) comparison function, which defaults to
// `std::less<K>`.
//
// A `btree::btree_map<K, V>` uses a default allocator of
// `std::allocator<std::pair<const K, V>>` to allocate (and deallocate)
// nodes, and construct and destruct values within those nodes. You may
// instead specify a custom allocator `A` (which in turn requires specifying a
// custom comparator `C`) as in `btree::btree_map<K, V, C, A>`.
//
template <typename Key, typename Value, typename Compare = std::less<Key>,
          typename Alloc = std::allocator<std::pair<const Key, Value>>>
class btree_map
    : public internal::btree_map_container<
          internal::btree<internal::map_params<
              Key, Value, Compare, Alloc, /*TargetNodeSize=*/256,
              /*Multi=*/false>>> {

  using Base = typename btree_map::btree_map_container;

 public:
  // Default constructor.
  btree_map() = default;
  using Base::Base;
};

// btree::swap(btree::btree_map<>, btree::btree_map<>)
//
// Swaps the contents of two `btree::btree_map` containers.
template <typename K, typename V, typename C, typename A>
void swap(btree_map<K, V, C, A> &x, btree_map<K, V, C, A> &y) {
  return x.swap(y);
}

// btree::erase_if(btree::btree_map<>, Pred)
//
// Erases all elements that satisfy the predicate pred from the container.
template <typename K, typename V, typename C, typename A, typename Pred>
void erase_if(btree_map<K, V, C, A> &map, Pred pred) {
  for (auto it = map.begin(); it != map.end();) {
    if (pred(*it)) {
      it = map.erase(it);
    } else {
      ++it;
    }
  }
}

// btree::btree_multimap
//
// A `btree::btree_multimap<K, V>` is an ordered associative container of
// keys and associated values designed to be a more efficient replacement for
// `std::multimap` (in most cases). Unlike `btree::btree_map`, a B-tree multimap
// allows multiple elements with equivalent keys.
//
// Keys are sorted using an (optional) comparison function, which defaults to
// `std::less<K>`.
//
// A `btree::btree_multimap<K, V>` uses a default allocator of
// `std::allocator<std::pair<const K, V>>` to allocate (and deallocate)
// nodes, and construct and destruct values within those nodes. You may
// instead specify a custom allocator `A` (which in turn requires specifying a
// custom comparator `C`) as in `btree::btree_multimap<K, V, C, A>`.
//
template <typename Key, typename Value, typename Compare = std::less<Key>,
          typename Alloc = std::allocator<std::pair<const Key, Value>>>
class btree_multimap
    : public internal::btree_multimap_container<
          internal::btree<internal::map_params<
              Key, Value, Compare, Alloc, /*TargetNodeSize=*/256,
              /*Multi=*/true>>> {
  using Base = typename btree_multimap::btree_multimap_container;

 public:
  btree_multimap() = default;
  using Base::Base;
};

// btree::swap(btree::btree_multimap<>, btree::btree_multimap<>)
//
// Swaps the contents of two `btree::btree_multimap` containers.
template <typename K, typename V, typename C, typename A>
void swap(btree_multimap<K, V, C, A> &x, btree_multimap<K, V, C, A> &y) {
  return x.swap(y);
}

// btree::erase_if(btree::btree_multimap<>, Pred)
//
// Erases all elements that satisfy the predicate pred from the container.
template <typename K, typename V, typename C, typename A, typename Pred>
void erase_if(btree_multimap<K, V, C, A> &map, Pred pred) {
  for (auto it = map.begin(); it != map.end();) {
    if (pred(*it)) {
      it = map.erase(it);
    } else {
      ++it;
    }
  }
}

} // namespace btree
