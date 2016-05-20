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

#ifndef CEPH_LW_SHARED_PTR_H_
#define CEPH_LW_SHARED_PTR_H_

#include <utility>
#include <type_traits>
#include <functional>
#include <iostream>

// This header defines two shared pointer facilities, lw_shared_ptr<>
// modeled after std::shared_ptr<>.
//
// Unlike std::shared_ptr<>, neither of these implementations are thread
// safe, and two pointers sharing the same object must not be used in
// different threads.
//
// lw_shared_ptr<> is the more lightweight variant, with a lw_shared_ptr<>
// occupying just one machine word, and adding just one word to the shared
// object.  However, it does not support polymorphism.
//
// Both variants support shared_from_this() via enable_shared_from_this<>
// and lw_enable_shared_from_this<>().
//

template< class T >
using remove_const_t = typename remove_const<T>::type;

template <typename T>
class lw_shared_ptr;

template <typename T>
class enable_lw_shared_from_this;

template <typename T, typename... A>
lw_shared_ptr<T> make_lw_shared(A&&... a);

template <typename T>
lw_shared_ptr<T> make_lw_shared(T&& a);

template <typename T>
lw_shared_ptr<T> make_lw_shared(T& a);

// CRTP from this to enable shared_from_this:
template <typename T>
class enable_lw_shared_from_this {
  long _count = 0;
  using ctor = T;
  T* to_value() { return static_cast<T*>(this); }
  T* to_internal_object() { return static_cast<T*>(this); }
 protected:
  enable_lw_shared_from_this() noexcept {}
  enable_lw_shared_from_this(enable_lw_shared_from_this&&) noexcept {}
  enable_lw_shared_from_this(const enable_lw_shared_from_this&) noexcept {}
  enable_lw_shared_from_this& operator=(const enable_lw_shared_from_this&) noexcept { return *this; }
  enable_lw_shared_from_this& operator=(enable_lw_shared_from_this&&) noexcept { return *this; }
 public:
  lw_shared_ptr<T> shared_from_this();
  lw_shared_ptr<const T> shared_from_this() const;
  template <typename X>
  friend class lw_shared_ptr;
};

template <typename T>
struct shared_ptr_no_esft {
  T _value;
  using ctor = shared_ptr_no_esft;

  T* to_value() { return &_value; }
  shared_ptr_no_esft* to_internal_object() { return this; }
  shared_ptr_no_esft() = default;
  shared_ptr_no_esft(const T& x) : _value(x) {}
  shared_ptr_no_esft(T&& x) : _value(std::move(x)) {}
  template <typename... A>
  shared_ptr_no_esft(A&&... a) : _value(std::forward<A>(a)...) {}
  template <typename X>
  friend class lw_shared_ptr;
};

template <typename T>
using shared_ptr_impl = typename std::conditional<
        std::is_base_of<enable_lw_shared_from_this<remove_const_t<T>>, T>::value,
        enable_lw_shared_from_this<remove_const_t<T>>,
        shared_ptr_no_esft<remove_const_t<T>> >::type;

template <typename T>
class lw_shared_ptr {
  mutable shared_ptr_impl<T>* _p = nullptr;
 private:
  lw_shared_ptr(shared_ptr_impl<T>* p) noexcept : _p(p) {
    if (_p) {
      ++_p->_count;
    }
  }
  template <typename... A>
  static lw_shared_ptr make(A&&... a) {
      return lw_shared_ptr(new typename shared_ptr_impl<T>::ctor(std::forward<A>(a)...));
  }
 public:
  using element_type = T;

  lw_shared_ptr() noexcept = default;
  lw_shared_ptr(std::nullptr_t) noexcept : lw_shared_ptr() {}
  lw_shared_ptr(const lw_shared_ptr& x) noexcept : _p(x._p) {
    if (_p) {
      ++_p->_count;
    }
  }
  lw_shared_ptr(lw_shared_ptr&& x) noexcept  : _p(x._p) {
      x._p = nullptr;
  }
  [[gnu::always_inline]]
  ~lw_shared_ptr() {
    if (_p && !--_p->_count) {
      delete _p->to_internal_object();
    }
  }
  lw_shared_ptr& operator=(const lw_shared_ptr& x) noexcept {
      if (_p != x._p) {
          this->~lw_shared_ptr();
          new (this) lw_shared_ptr(x);
      }
      return *this;
  }
  lw_shared_ptr& operator=(lw_shared_ptr&& x) noexcept {
      if (_p != x._p) {
          this->~lw_shared_ptr();
          new (this) lw_shared_ptr(std::move(x));
      }
      return *this;
  }
  lw_shared_ptr& operator=(std::nullptr_t) noexcept {
      return *this = lw_shared_ptr();
  }
  lw_shared_ptr& operator=(T&& x) noexcept {
      this->~lw_shared_ptr();
      new (this) lw_shared_ptr(make_lw_shared<T>(std::move(x)));
      return *this;
  }

  T& operator*() const noexcept { return *_p->to_value(); }
  T* operator->() const noexcept { return _p->to_value(); }
  T* get() const noexcept { return _p->to_value(); }

  long int use_count() const noexcept {
    if (_p) {
      return _p->_count;
    } else {
      return 0;
    }
  }

  operator lw_shared_ptr<const T>() const noexcept {
      return lw_shared_ptr<const T>(_p);
  }

  explicit operator bool() const noexcept {
      return _p;
  }

  bool owned() const noexcept {
    return _p->_count == 1;
  }

  bool operator==(const lw_shared_ptr<const T>& x) const {
      return _p == x._p;
  }

  bool operator!=(const lw_shared_ptr<const T>& x) const {
      return !operator==(x);
  }

  bool operator==(const lw_shared_ptr<remove_const_t<T>>& x) const {
      return _p == x._p;
  }

  bool operator!=(const lw_shared_ptr<remove_const_t<T>>& x) const {
      return !operator==(x);
  }

  template <typename U>
  friend class lw_shared_ptr;

  template <typename X, typename... A>
  friend lw_shared_ptr<X> make_lw_shared(A&&...);

  template <typename U>
  friend lw_shared_ptr<U> make_lw_shared(U&&);

  template <typename U>
  friend lw_shared_ptr<U> make_lw_shared(U&);

  template <typename U>
  friend class enable_lw_shared_from_this;
};

template <typename T, typename... A>
inline
lw_shared_ptr<T> make_lw_shared(A&&... a) {
    return lw_shared_ptr<T>::make(std::forward<A>(a)...);
}

template <typename T>
inline
lw_shared_ptr<T> make_lw_shared(T&& a) {
    return lw_shared_ptr<T>::make(std::move(a));
}

template <typename T>
inline
lw_shared_ptr<T> make_lw_shared(T& a) {
    return lw_shared_ptr<T>::make(a);
}

template <typename T>
inline
lw_shared_ptr<T>
enable_lw_shared_from_this<T>::shared_from_this() {
    return lw_shared_ptr<T>(this);
}

template <typename T>
inline
lw_shared_ptr<const T>
enable_lw_shared_from_this<T>::shared_from_this() const {
    return lw_shared_ptr<const T>(const_cast<enable_lw_shared_from_this*>(this));
}

template <typename T>
static inline
std::ostream& operator<<(std::ostream& out, const lw_shared_ptr<T>& p) {
    if (!p) {
        return out << "null";
    }
    return out << *p;
}

namespace std {

  template <typename T>
  struct hash<lw_shared_ptr<T>> : private hash<T*> {
    size_t operator()(const lw_shared_ptr<T>& p) const {
        return hash<T*>::operator()(p.get());
    }
  };

}

#endif /* CEPH_LW_SHARED_PTR_H_ */
