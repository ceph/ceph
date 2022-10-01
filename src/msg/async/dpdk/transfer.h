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

#ifndef CEPH_TRANSFER_H_
#define CEPH_TRANSFER_H_

// Helper functions for copying or moving multiple objects in an exception
// safe manner, then destroying the sources.
//
// To transfer, call transfer_pass1(allocator, &from, &to) on all object pairs,
// (this copies the object from @from to @to).  If no exceptions are encountered,
// call transfer_pass2(allocator, &from, &to).  This destroys the object at the
// origin.  If exceptions were encountered, simply destroy all copied objects.
//
// As an optimization, if the objects are moveable without throwing (noexcept)
// transfer_pass1() simply moves the objects and destroys the source, and
// transfer_pass2() does nothing.

#include <type_traits>
#include <utility>

template <typename T, typename Alloc>
inline void transfer_pass1(Alloc& a, T* from, T* to,
                           typename std::enable_if<std::is_nothrow_move_constructible<T>::value>::type* = nullptr) {
    a.construct(to, std::move(*from));
    a.destroy(from);
}

template <typename T, typename Alloc>
inline void transfer_pass2(Alloc& a, T* from, T* to,
                           typename std::enable_if<std::is_nothrow_move_constructible<T>::value>::type* = nullptr) {
}

template <typename T, typename Alloc>
inline void transfer_pass1(Alloc& a, T* from, T* to,
               typename std::enable_if<!std::is_nothrow_move_constructible<T>::value>::type* = nullptr) {
    a.construct(to, *from);
}

template <typename T, typename Alloc>
inline void transfer_pass2(Alloc& a, T* from, T* to,
               typename std::enable_if<!std::is_nothrow_move_constructible<T>::value>::type* = nullptr) {
    a.destroy(from);
}

#endif /* CEPH_TRANSFER_H_ */
