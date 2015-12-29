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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

// The following unaligned_cast<T*>(p) is a portable replacement for
// reinterpret_cast<T*>(p) which should be used every time address p
// is not guaranteed to be properly aligned to alignof(T).
//
// On architectures like x86 and ARM, where unaligned access is allowed,
// unaligned_cast will behave the same as reinterpret_cast and will generate
// the same code.
//
// Certain architectures (e.g., MIPS) make it extremely slow or outright
// forbidden to use ordinary machine instructions on a primitive type at an
// unaligned addresses - e.g., access a uint32_t at an address which is not
// a multiple of 4. Gcc's "undefined behavior sanitizer" (enabled in our debug
// build) also catches such unaligned accesses and reports them as errors,
// even when running on x86.
//
// Therefore, reinterpret_cast<int32_t*> on an address which is not guaranteed
// to be a multiple of 4 may generate extremely slow code or runtime errors,
// and must be avoided. The compiler needs to be told about the unaligned
// access, so it can generate reasonably-efficient code for the access
// (in MIPS, this means generating two instructions "lwl" and "lwr", instead
// of the one instruction "lw" which faults on unaligned/ access). The way to
// tell the compiler this is with __attribute__((packed)). This will also
// cause the sanitizer not to generate runtime alignment checks for this
// access.

template <typename T>
struct unaligned {
  T raw;
  unaligned() = default;
  unaligned(T x) : raw(x) {}
  unaligned& operator=(const T& x) { raw = x; return *this; }
  operator T() const { return raw; }
} __attribute__((packed));

template <typename T, typename F>
inline auto unaligned_cast(F* p) {
  return reinterpret_cast<unaligned<std::remove_pointer_t<T>>*>(p);
}

template <typename T, typename F>
inline auto unaligned_cast(const F* p) {
  return reinterpret_cast<const unaligned<std::remove_pointer_t<T>>*>(p);
}
