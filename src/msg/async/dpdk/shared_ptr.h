// -*- mode:C++; tab-width:8; c-basic-offset:4; indent-tabs-mode:nil -*-
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

// This header defines a shared pointer facility, lw_shared_ptr<>,
// modeled after std::shared_ptr<>.
//
// Unlike std::shared_ptr<>, this implementation is thread
// safe, and two pointers sharing the same object must not be used in
// different threads.
//
// lw_shared_ptr<> is the more lightweight variant, with a lw_shared_ptr<>
// occupying just one machine word, and adding just one word to the shared
// object.  However, it does not support polymorphism.
//
// It supports shared_from_this() via enable_shared_from_this<>
// and lw_enable_shared_from_this<>().
//

template <typename T>
class lw_shared_ptr;

template <typename T>
class enable_lw_shared_from_this;

template <typename T>
class enable_shared_from_this;

template <typename T, typename... A>
lw_shared_ptr<T> make_lw_shared(A&&... a);

template <typename T>
lw_shared_ptr<T> make_lw_shared(T&& a);

template <typename T>
lw_shared_ptr<T> make_lw_shared(T& a);

struct lw_shared_ptr_counter_base {
    long _count = 0;
};


namespace internal {

template <class T, class U>
struct lw_shared_ptr_accessors;

template <class T>
struct lw_shared_ptr_accessors_esft;

template <class T>
struct lw_shared_ptr_accessors_no_esft;

}


// We want to support two use cases for shared_ptr<T>:
//
//   1. T is any type (primitive or class type)
//
//   2. T is a class type that inherits from enable_shared_from_this<T>.
//
// In the first case, we must wrap T in an object containing the counter,
// since T may be a primitive type and cannot be a base class.
//
// In the second case, we want T to reach the counter through its
// enable_shared_from_this<> base class, so that we can implement
// shared_from_this().
//
// To implement those two conflicting requirements (T alongside its counter;
// T inherits from an object containing the counter) we use std::conditional<>
// and some accessor functions to select between two implementations.


// CRTP from this to enable shared_from_this:
template <typename T>
class enable_lw_shared_from_this : private lw_shared_ptr_counter_base {
    using ctor = T;
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
    template <typename X>
    friend class internal::lw_shared_ptr_accessors_esft;
    template <typename X, class Y>
    friend class internal::lw_shared_ptr_accessors;
};

template <typename T>
struct shared_ptr_no_esft : private lw_shared_ptr_counter_base {
    T _value;

    shared_ptr_no_esft() = default;
    shared_ptr_no_esft(const T& x) : _value(x) {}
    shared_ptr_no_esft(T&& x) : _value(std::move(x)) {}
    template <typename... A>
    shared_ptr_no_esft(A&&... a) : _value(std::forward<A>(a)...) {}

    template <typename X>
    friend class lw_shared_ptr;
    template <typename X>
    friend class internal::lw_shared_ptr_accessors_no_esft;
    template <typename X, class Y>
    friend class internal::lw_shared_ptr_accessors;
};


/// Extension point: the user may override this to change how \ref lw_shared_ptr objects are destroyed,
/// primarily so that incomplete classes can be used.
///
/// Customizing the deleter requires that \c T be derived from \c enable_lw_shared_from_this<T>.
/// The specialization must be visible for all uses of \c lw_shared_ptr<T>.
///
/// To customize, the template must have a `static void dispose(T*)` operator that disposes of
/// the object.
template <typename T>
struct lw_shared_ptr_deleter;  // No generic implementation

namespace internal {

template <typename T>
struct lw_shared_ptr_accessors_esft {
    using concrete_type = std::remove_const_t<T>;
    static T* to_value(lw_shared_ptr_counter_base* counter) {
        return static_cast<T*>(counter);
    }
    static void dispose(lw_shared_ptr_counter_base* counter) {
        delete static_cast<T*>(counter);
    }
    static void instantiate_to_value(lw_shared_ptr_counter_base* p) {
        // since to_value() is defined above, we don't need to do anything special
        // to force-instantiate it
    }
};

template <typename T>
struct lw_shared_ptr_accessors_no_esft {
    using concrete_type = shared_ptr_no_esft<T>;
    static T* to_value(lw_shared_ptr_counter_base* counter) {
        return &static_cast<concrete_type*>(counter)->_value;
    }
    static void dispose(lw_shared_ptr_counter_base* counter) {
        delete static_cast<concrete_type*>(counter);
    }
    static void instantiate_to_value(lw_shared_ptr_counter_base* p) {
        // since to_value() is defined above, we don't need to do anything special
        // to force-instantiate it
    }
};

// Generic case: lw_shared_ptr_deleter<T> is not specialized, select
// implementation based on whether T inherits from enable_lw_shared_from_this<T>.
template <typename T, typename U = void>
struct lw_shared_ptr_accessors : std::conditional_t<
         std::is_base_of<enable_lw_shared_from_this<T>, T>::value,
         lw_shared_ptr_accessors_esft<T>,
         lw_shared_ptr_accessors_no_esft<T>> {
};

// Overload when lw_shared_ptr_deleter<T> specialized
template <typename T>
struct lw_shared_ptr_accessors<T, std::void_t<decltype(lw_shared_ptr_deleter<T>{})>> {
    using concrete_type = T;
    static T* to_value(lw_shared_ptr_counter_base* counter);
    static void dispose(lw_shared_ptr_counter_base* counter) {
        lw_shared_ptr_deleter<T>::dispose(to_value(counter));
    }
    static void instantiate_to_value(lw_shared_ptr_counter_base* p) {
        // instantiate to_value(); must be defined by shared_ptr_incomplete.hh
        to_value(p);
    }
};

}

template <typename T>
class lw_shared_ptr {
    using accessors = internal::lw_shared_ptr_accessors<std::remove_const_t<T>>;
    using concrete_type = typename accessors::concrete_type;
    mutable lw_shared_ptr_counter_base* _p = nullptr;
private:
    lw_shared_ptr(lw_shared_ptr_counter_base* p) noexcept : _p(p) {
        if (_p) {
            ++_p->_count;
        }
    }
    template <typename... A>
    static lw_shared_ptr make(A&&... a) {
        auto p = new concrete_type(std::forward<A>(a)...);
        accessors::instantiate_to_value(p);
        return lw_shared_ptr(p);
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
            accessors::dispose(_p);
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

    T& operator*() const noexcept { return *accessors::to_value(_p); }
    T* operator->() const noexcept { return accessors::to_value(_p); }
    T* get() const noexcept {
        if (_p) {
            return accessors::to_value(_p);
        } else {
            return nullptr;
        }
    }

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

    bool operator==(const lw_shared_ptr<std::remove_const_t<T>>& x) const {
        return _p == x._p;
    }

    bool operator!=(const lw_shared_ptr<std::remove_const_t<T>>& x) const {
        return !operator==(x);
    }

    bool operator<(const lw_shared_ptr<const T>& x) const {
        return _p < x._p;
    }

    bool operator<(const lw_shared_ptr<std::remove_const_t<T>>& x) const {
        return _p < x._p;
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
