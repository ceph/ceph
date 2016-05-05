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

#ifndef CEPH_COMMON_DELETER_H
#define CEPH_COMMON_DELETER_H

#include <atomic>
#include <memory>
#include <cstdlib>
#include <type_traits>

#include "include/assert.h"

/// \addtogroup memory-module
/// @{

/// Provides a mechanism for managing the lifetime of a buffer.
///
/// A \c deleter is an object that is used to inform the consumer
/// of some buffer (not referenced by the deleter itself) how to
/// delete the buffer.  This can be by calling an arbitrary function
/// or destroying an object carried by the deleter.  Examples of
/// a deleter's encapsulated actions are:
///
///  - calling \c std::free(p) on some captured pointer, p
///  - calling \c delete \c p on some captured pointer, p
///  - decrementing a reference count somewhere
///
/// A deleter performs its action from its destructor.
class deleter final {
 public:
  /// \cond internal
  struct impl;
  struct raw_object_tag {};
  /// \endcond
 private:
  // if bit 0 set, point to object to be freed directly.
  impl* _impl = nullptr;
 public:
  /// Constructs an empty deleter that does nothing in its destructor.
  deleter() = default;
  deleter(const deleter&) = delete;
  /// Moves a deleter.
  deleter(deleter&& x) noexcept : _impl(x._impl) { x._impl = nullptr; }
  /// \cond internal
  explicit deleter(impl* i) : _impl(i) {}
  deleter(raw_object_tag tag, void* object)
          : _impl(from_raw_object(object)) {}
  /// \endcond
  /// Destroys the deleter and carries out the encapsulated action.
  ~deleter();
  deleter& operator=(deleter&& x);
  deleter& operator=(deleter&) = delete;
  /// Performs a sharing operation.  The encapsulated action will only
  /// be carried out after both the original deleter and the returned
  /// deleter are both destroyed.
  ///
  /// \return a deleter with the same encapsulated action as this one.
  deleter share();
  /// Checks whether the deleter has an associated action.
  explicit operator bool() const { return bool(_impl); }
  /// \cond internal
  void reset(impl* i) {
    this->~deleter();
    new (this) deleter(i);
  }
  /// \endcond
  /// Appends another deleter to this deleter.  When this deleter is
  /// destroyed, both encapsulated actions will be carried out.
  void append(deleter d);
 private:
  static bool is_raw_object(impl* i) {
    auto x = reinterpret_cast<uintptr_t>(i);
    return x & 1;
  }
  bool is_raw_object() const {
    return is_raw_object(_impl);
  }
  static void* to_raw_object(impl* i) {
    auto x = reinterpret_cast<uintptr_t>(i);
    return reinterpret_cast<void*>(x & ~uintptr_t(1));
  }
  void* to_raw_object() const {
    return to_raw_object(_impl);
  }
  impl* from_raw_object(void* object) {
    auto x = reinterpret_cast<uintptr_t>(object);
    return reinterpret_cast<impl*>(x | 1);
  }
};

/// \cond internal
struct deleter::impl {
  std::atomic_uint refs;
  deleter next;
  impl(deleter next) : refs(1), next(std::move(next)) {}
  virtual ~impl() {}
};
/// \endcond

inline deleter::~deleter() {
  if (is_raw_object()) {
    std::free(to_raw_object());
    return;
  }
  if (_impl && --_impl->refs == 0) {
    delete _impl;
  }
}

inline deleter& deleter::operator=(deleter&& x) {
  if (this != &x) {
    this->~deleter();
    new (this) deleter(std::move(x));
  }
  return *this;
}

/// \cond internal
template <typename Deleter>
struct lambda_deleter_impl final : deleter::impl {
  Deleter del;
  lambda_deleter_impl(deleter next, Deleter&& del)
          : impl(std::move(next)), del(std::move(del)) {}
  virtual ~lambda_deleter_impl() override { del(); }
};

template <typename Object>
struct object_deleter_impl final : deleter::impl {
  Object obj;
  object_deleter_impl(deleter next, Object&& obj)
          : impl(std::move(next)), obj(std::move(obj)) {}
};

template <typename Object>
inline
object_deleter_impl<Object>* make_object_deleter_impl(deleter next, Object obj) {
  return new object_deleter_impl<Object>(std::move(next), std::move(obj));
}
/// \endcond

/// Makes a \ref deleter that encapsulates the action of
/// destroying an object, as well as running another deleter.  The input
/// object is moved to the deleter, and destroyed when the deleter is destroyed.
///
/// \param d deleter that will become part of the new deleter's encapsulated action
/// \param o object whose destructor becomes part of the new deleter's encapsulated action
/// \related deleter
template <typename Object>
deleter make_deleter(deleter next, Object o) {
  return deleter(new lambda_deleter_impl<Object>(std::move(next), std::move(o)));
}

/// Makes a \ref deleter that encapsulates the action of destroying an object.  The input
/// object is moved to the deleter, and destroyed when the deleter is destroyed.
///
/// \param o object whose destructor becomes the new deleter's encapsulated action
/// \related deleter
template <typename Object>
deleter make_deleter(Object o) {
  return make_deleter(deleter(), std::move(o));
}

/// \cond internal
struct free_deleter_impl final : deleter::impl {
  void* obj;
  free_deleter_impl(void* obj) : impl(deleter()), obj(obj) {}
  virtual ~free_deleter_impl() override { std::free(obj); }
};
/// \endcond

inline deleter deleter::share() {
  if (!_impl) {
    return deleter();
  }
  if (is_raw_object()) {
    _impl = new free_deleter_impl(to_raw_object());
  }
  ++_impl->refs;
  return deleter(_impl);
}

// Appends 'd' to the chain of deleters. Avoids allocation if possible. For
// performance reasons the current chain should be shorter and 'd' should be
// longer.
inline void deleter::append(deleter d) {
  if (!d._impl) {
    return;
  }
  impl* next_impl = _impl;
  deleter* next_d = this;
  while (next_impl) {
    if (next_impl == d._impl)
      return ;
    if (is_raw_object(next_impl)) {
      next_d->_impl = next_impl = new free_deleter_impl(to_raw_object(next_impl));
    }
    if (next_impl->refs != 1) {
      next_d->_impl = next_impl = make_object_deleter_impl(std::move(next_impl->next), deleter(next_impl));
    }
    next_d = &next_impl->next;
    next_impl = next_d->_impl;
  }
  next_d->_impl = d._impl;
  d._impl = nullptr;
}

/// Makes a deleter that calls \c std::free() when it is destroyed.
///
/// \param obj object to free.
/// \related deleter
inline deleter make_free_deleter(void* obj) {
  if (!obj) {
    return deleter();
  }
  return deleter(deleter::raw_object_tag(), obj);
}

/// Makes a deleter that calls \c std::free() when it is destroyed, as well
/// as invoking the encapsulated action of another deleter.
///
/// \param d deleter to invoke.
/// \param obj object to free.
/// \related deleter
inline deleter make_free_deleter(deleter next, void* obj) {
  return make_deleter(std::move(next), [obj] () mutable { std::free(obj); });
}

/// \see make_deleter(Object)
/// \related deleter
template <typename T>
inline deleter make_object_deleter(T&& obj) {
  return deleter{make_object_deleter_impl(deleter(), std::move(obj))};
}

/// \see make_deleter(deleter, Object)
/// \related deleter
template <typename T>
inline deleter make_object_deleter(deleter d, T&& obj) {
  return deleter{make_object_deleter_impl(std::move(d), std::move(obj))};
}

/// @}

#endif /* CEPH_COMMON_DELETER_H */
