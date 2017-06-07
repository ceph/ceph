// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// A btree_map<> implements the STL unique sorted associative container
// interface and the pair associative container interface (a.k.a map<>) using a
// btree. A btree_multimap<> implements the STL multiple sorted associative
// container interface and the pair associtive container interface (a.k.a
// multimap<>) using a btree. See btree.h for details of the btree
// implementation and caveats.

#ifndef UTIL_BTREE_BTREE_MAP_H__
#define UTIL_BTREE_BTREE_MAP_H__

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "btree.h"
#include "btree_container.h"

namespace btree {

// The btree_map class is needed mainly for its constructors.
template <typename Key, typename Value,
          typename Compare = std::less<Key>,
          typename Alloc = std::allocator<std::pair<const Key, Value> >,
          int TargetNodeSize = 256>
class btree_map : public btree_map_container<
  btree<btree_map_params<Key, Value, Compare, Alloc, TargetNodeSize> > > {

  typedef btree_map<Key, Value, Compare, Alloc, TargetNodeSize> self_type;
  typedef btree_map_params<
    Key, Value, Compare, Alloc, TargetNodeSize> params_type;
  typedef btree<params_type> btree_type;
  typedef btree_map_container<btree_type> super_type;

 public:
  typedef typename btree_type::key_compare key_compare;
  typedef typename btree_type::allocator_type allocator_type;

 public:
  // Default constructor.
  btree_map(const key_compare &comp = key_compare(),
            const allocator_type &alloc = allocator_type())
      : super_type(comp, alloc) {
  }

  // Copy constructor.
  btree_map(const self_type &x)
      : super_type(x) {
  }

  // Range constructor.
  template <class InputIterator>
  btree_map(InputIterator b, InputIterator e,
            const key_compare &comp = key_compare(),
            const allocator_type &alloc = allocator_type())
      : super_type(b, e, comp, alloc) {
  }
};

template <typename K, typename V, typename C, typename A, int N>
inline void swap(btree_map<K, V, C, A, N> &x,
                 btree_map<K, V, C, A, N> &y) {
  x.swap(y);
}

// The btree_multimap class is needed mainly for its constructors.
template <typename Key, typename Value,
          typename Compare = std::less<Key>,
          typename Alloc = std::allocator<std::pair<const Key, Value> >,
          int TargetNodeSize = 256>
class btree_multimap : public btree_multi_container<
  btree<btree_map_params<Key, Value, Compare, Alloc, TargetNodeSize> > > {

  typedef btree_multimap<Key, Value, Compare, Alloc, TargetNodeSize> self_type;
  typedef btree_map_params<
    Key, Value, Compare, Alloc, TargetNodeSize> params_type;
  typedef btree<params_type> btree_type;
  typedef btree_multi_container<btree_type> super_type;

 public:
  typedef typename btree_type::key_compare key_compare;
  typedef typename btree_type::allocator_type allocator_type;
  typedef typename btree_type::data_type data_type;
  typedef typename btree_type::mapped_type mapped_type;

 public:
  // Default constructor.
  btree_multimap(const key_compare &comp = key_compare(),
                 const allocator_type &alloc = allocator_type())
      : super_type(comp, alloc) {
  }

  // Copy constructor.
  btree_multimap(const self_type &x)
      : super_type(x) {
  }

  // Range constructor.
  template <class InputIterator>
  btree_multimap(InputIterator b, InputIterator e,
                 const key_compare &comp = key_compare(),
                 const allocator_type &alloc = allocator_type())
      : super_type(b, e, comp, alloc) {
  }
};

template <typename K, typename V, typename C, typename A, int N>
inline void swap(btree_multimap<K, V, C, A, N> &x,
                 btree_multimap<K, V, C, A, N> &y) {
  x.swap(y);
}

} // namespace btree

#endif  // UTIL_BTREE_BTREE_MAP_H__
