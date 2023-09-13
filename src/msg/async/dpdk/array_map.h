// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CEPH_ARRAY_MAP_HH_
#define CEPH_ARRAY_MAP_HH_

#include <array>

// unordered_map implemented as a simple array

template <typename Value, size_t Max>
class array_map {
  std::array<Value, Max> _a {};
 public:
  array_map(std::initializer_list<std::pair<size_t, Value>> i) {
    for (auto kv : i) {
      _a[kv.first] = kv.second;
    }
  }
  Value& operator[](size_t key) { return _a[key]; }
  const Value& operator[](size_t key) const { return _a[key]; }

  Value& at(size_t key) {
    if (key >= Max) {
      throw std::out_of_range(std::to_string(key) + " >= " + std::to_string(Max));
    }
    return _a[key];
  }
};

#endif /* ARRAY_MAP_HH_ */
