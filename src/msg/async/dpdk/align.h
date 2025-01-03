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

#ifndef CEPH_MSG_DPDK_ALIGN_HH_
#define CEPH_MSG_DPDK_ALIGN_HH_

#include <cstdint>
#include <cstdlib>

template <typename T>
inline constexpr T align_up(T v, T align) {
  return (v + align - 1) & ~(align - 1);
}

template <typename T>
inline constexpr T* align_up(T* v, size_t align) {
  static_assert(sizeof(T) == 1, "align byte pointers only");
  return reinterpret_cast<T*>(align_up(reinterpret_cast<uintptr_t>(v), align));
}

template <typename T>
inline constexpr T align_down(T v, T align) {
  return v & ~(align - 1);
}

template <typename T>
inline constexpr T* align_down(T* v, size_t align) {
  static_assert(sizeof(T) == 1, "align byte pointers only");
  return reinterpret_cast<T*>(align_down(reinterpret_cast<uintptr_t>(v), align));
}

#endif /* CEPH_MSG_DPDK_ALIGN_HH_ */
