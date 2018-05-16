// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef INCLUDE_STATIC_ANY
#define INCLUDE_STATIC_ANY

#include <any>
#include <cstddef>
#include <initializer_list>
#include <memory>
#include <typeinfo>
#include <type_traits>

#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>

namespace ceph {

namespace _any {

// Shared Functionality
// --------------------
//
// Common implementation details. Most functionality is here. We
// assume that destructors do not throw. Some of them might and
// they'll invoke terminate and that's fine.
//
// We are using the Curiously Recurring Template Pattern! We require
// that all classes inheriting from us provide:
//
//   - `static constexpr size_t capacity`: Maximum capacity. No object
//                                         larger than this may be
//                                         stored. `dynamic` for dynamic.
//   - `void* ptr() const noexcept`: returns a pointer to storage.
//                                   (`alloc_storage` must have been called.
//                                   `free_storage` must not have been called
//                                   since.)
//   - `void* alloc_storage(const std::size_t)`: allocate storage
//   - `void free_storage() noexcept`: free storage. Must be idempotent.
//
// We provide most of the public interface, as well as the operator function,
// cast_helper, and the type() call.

// Set `capacity` to this value to indicate that there is no fixed
// capacity.
//
inline constexpr std::size_t dynamic = ~0;

// Driver Function
// ---------------
//
// The usual type-erasure control function trick. This one is simpler
// than usual since we punt on moving and copying. We could dispense
// with this and just store a deleter and a pointer to a typeinfo, but
// that would be twice the space.
//
// Moved out here so the type of `func_t` isn't dependent on the
// enclosing class.
//
enum class op { type, destroy };
template<typename T>
inline void op_func(const op o, void* p) noexcept {
  static const std::type_info& type = typeid(T);
  switch (o) {
  case op::type:
    *(reinterpret_cast<const std::type_info**>(p)) = &type;
    break;
  case op::destroy:
    reinterpret_cast<T*>(p)->~T();
    break;
  }
}
using func_t = void (*)(const op, void* p) noexcept;

// The base class 
// --------------
//
// The `storage_t` parameter gives the type of the value that manages
// storage and allocation. We use it to create a protected data member
// (named `storage`). This allows us to sidestep the problem in
// initialization order where, where exposed constructors were using
// trying to allocate or free storage *before* the data members of the
// derived class were initialized.
//
// Making storage_t a member type of the derived class won't work, due
// to C++'s rules for nested types being *horrible*. Just downright
// *horrible*.
//
template<typename D, typename storage_t>
class base {
  // Make definitions from our superclass visible
  // --------------------------------------------
  //
  // And check that they fit the requirements. At least those that are
  // statically checkable.
  //
  static constexpr std::size_t capacity = D::capacity;

  void* ptr() const noexcept {
    static_assert(
      noexcept(static_cast<const D*>(this)->ptr()) &&
      std::is_same_v<decltype(static_cast<const D*>(this)->ptr()), void*>,
      "‘void* ptr() const noexcept’ missing from superclass");
    return static_cast<const D*>(this)->ptr();
  }

  void* alloc_storage(const std::size_t z) {
    static_assert(
      std::is_same_v<decltype(static_cast<D*>(this)->alloc_storage(z)), void*>,
      "‘void* alloc_storage(const size_t)’ missing from superclass.");
    return static_cast<D*>(this)->alloc_storage(z);
  }

  void free_storage() noexcept {
    static_assert(
      noexcept(static_cast<D*>(this)->free_storage()) &&
      std::is_void_v<decltype(static_cast<D*>(this)->free_storage())>,
      "‘void free_storage() noexcept’ missing from superclass.");
    static_cast<D*>(this)->free_storage();
  }


  // Pile O' Templates
  // -----------------
  //
  // These are just verbose and better typed once than twice. They're
  // used for SFINAE and declaring noexcept.
  //
  template<class T>
  struct is_in_place_type_helper : std::false_type {};
  template<class T>
  struct is_in_place_type_helper<std::in_place_type_t<T>> : std::true_type {};

  template<class T>
  static constexpr bool is_in_place_type_v =
    is_in_place_type_helper<std::decay_t<T>>::value;

  // SFINAE condition for value initialized
  // constructors/assigners. This is analogous to the standard's
  // requirement that this overload only participate in overload
  // resolution if std::decay_t<T> is not the same type as the
  // any-type, nor a specialization of std::in_place_type_t
  //
  template<typename T>
  using value_condition_t = std::enable_if_t<
    !std::is_same_v<std::decay_t<T>, D> &&
    !is_in_place_type_v<std::decay_t<T>>>;

  // This `noexcept` condition for value construction lets
  // `immobile_any`'s value constructor/assigner be noexcept, so long
  // as the type's copy or move constructor cooperates.
  //
  template<typename T>
  static constexpr bool value_noexcept_v =
    std::is_nothrow_constructible_v<std::decay_t<T>, T> && capacity != dynamic;

  // SFINAE condition for in-place constructors/assigners
  //
  template<typename T, typename... Args>
  using in_place_condition_t = std::enable_if_t<std::is_constructible_v<
						  std::decay_t<T>, Args...>>;

  // Analogous to the above. Give noexcept to immobile_any::emplace
  // when possible.
  //
  template<typename T, typename... Args>
  static constexpr bool in_place_noexcept_v =
    std::is_nothrow_constructible_v<std::decay_t<T>, Args...> &&
    capacity != dynamic;

private:

  // Functionality!
  // --------------

  // The driver function for the currently stored object. Whether this
  // is null is the canonical way to know whether an instance has a
  // value.
  //
  func_t func = nullptr;

  // Construct an object within ourselves. As you can see we give the
  // weak exception safety guarantee.
  //
  template<typename T, typename ...Args>
  std::decay_t<T>& construct(Args&& ...args) {
    using Td = std::decay_t<T>;
    static_assert(capacity == dynamic || sizeof(Td) <= capacity,
		  "Supplied type is too large for this specialization.");
    try {
      func = &op_func<Td>;
      return *new (reinterpret_cast<Td*>(alloc_storage(sizeof(Td))))
	Td(std::forward<Args>(args)...);
    } catch (...) {
      reset();
      throw;
    }
  }

protected:

  // We hold the storage, even if the superclass class manipulates it,
  // so that its default initialization comes soon enough for us to
  // use it in our constructors.
  //
  storage_t storage;

public:

  base() noexcept = default;
  ~base() noexcept {
    reset();
  }

protected:
  // Since some of our derived classes /can/ be copied or moved.
  //
  base(const base& rhs) noexcept : func(rhs.func) {
    if constexpr (std::is_copy_assignable_v<storage_t>) {
      storage = rhs.storage;
    }
  }
  base& operator =(const base& rhs) noexcept {
    reset();
    func = rhs.func;
    if constexpr (std::is_copy_assignable_v<storage_t>) {
      storage = rhs.storage;
    }
    return *this;
  }

  base(base&& rhs) noexcept : func(std::move(rhs.func)) {
    if constexpr (std::is_move_assignable_v<storage_t>) {
      storage = std::move(rhs.storage);
    }
    rhs.func = nullptr;
  }
  base& operator =(base&& rhs) noexcept {
    reset();
    func = rhs.func;
    if constexpr (std::is_move_assignable_v<storage_t>) {
      storage = std::move(rhs.storage);
    }
    rhs.func = nullptr;
    return *this;
  }

public:

  // Value construct/assign
  // ----------------------
  //
  template<typename T,
	   typename = value_condition_t<T>>
  base(T&& t) noexcept(value_noexcept_v<T>) {
    construct<T>(std::forward<T>(t));
  }

  // On exception, *this is set to empty.
  //
  template<typename T,
           typename = value_condition_t<T>>
  base& operator =(T&& t) noexcept(value_noexcept_v<T>) {
    reset();
    construct<T>(std::forward<T>(t));
    return *this;
  }

  // In-place construct/assign
  // -------------------------
  //
  // I really hate the way the C++ standard library treats references
  // as if they were stepchildren in a Charles Dickens novel. I am
  // quite upset that std::optional lacks a specialization for
  // references. There's no legitimate reason for it. The whole
  // 're-seat or refuse' debate is simply a canard. The optional is
  // effectively a container, so of course it can be emptied or
  // reassigned. No, pointers are not an acceptable substitute. A
  // pointer gives an address in memory which may be null and which
  // may represent an object or may a location in which an object is
  // to be created. An optional reference, on the other hand, is a
  // reference to an initialized, live object or /empty/. This is an
  // obvious difference that should be communicable to any programmer
  // reading the code through the type system.
  //
  // `std::any`, even in the case of in-place construction,
  // only stores the decayed type. I suspect this was to get around
  // the question of whether, for a std::any holding a T&,
  // std::any_cast<T> should return a copy or throw
  // std::bad_any_cast.
  //
  // I think the appropriate response in that case would be to make a
  // copy if the type supports it and fail otherwise. Once a concrete
  // type is known the problem solves itself.
  //
  // If one were inclined, one could easily load the driver function
  // with a heavy subset of the type traits (those that depend only on
  // the type in question) and simply /ask/ whether it's a reference.
  //
  // At the moment, I'm maintaining compatibility with the standard
  // library except for copy/move semantics.
  //
  template<typename T,
           typename... Args,
           typename = in_place_condition_t<T, Args...>>
  base(std::in_place_type_t<T>,
       Args&& ...args) noexcept(in_place_noexcept_v<T, Args...>) {
    construct<T>(std::forward<Args>(args)...);
  }

  // On exception, *this is set to empty.
  //
  template<typename T,
           typename... Args,
           typename = in_place_condition_t<T>>
  std::decay_t<T>& emplace(Args&& ...args) noexcept(in_place_noexcept_v<
						    T, Args...>) {
    reset();
    return construct<T>(std::forward<Args>(args)...);
  }

  template<typename T,
           typename U,
           typename... Args,
           typename = in_place_condition_t<T, std::initializer_list<U>,
					   Args...>>
  base(std::in_place_type_t<T>,
       std::initializer_list<U> i,
       Args&& ...args) noexcept(in_place_noexcept_v<T, std::initializer_list<U>,
				Args...>) {
    construct<T>(i, std::forward<Args>(args)...);
  }

  // On exception, *this is set to empty.
  //
  template<typename T,
           typename U,
           typename... Args,
           typename = in_place_condition_t<T, std::initializer_list<U>,
					   Args...>>
  std::decay_t<T>& emplace(std::initializer_list<U> i,
                           Args&& ...args) noexcept(in_place_noexcept_v<T,
						    std::initializer_list<U>,
						    Args...>) {
    reset();
    return construct<T>(i,std::forward<Args>(args)...);
  }

  // Empty ourselves, using the subclass to free any storage.
  //
  void reset() noexcept {
    if (has_value()) {
      func(op::destroy, ptr());
      func = nullptr;
    }
    free_storage();
  }

  template<typename U = storage_t,
	   typename = std::enable_if<std::is_swappable_v<storage_t>>>
  void swap(base& rhs) {
    using std::swap;
    swap(func, rhs.func);
    swap(storage, rhs.storage);
  }

  // All other functions should use this function to test emptiness
  // rather than examining `func` directly.
  //
  bool has_value() const noexcept {
    return !!func;
  }

  // Returns the type of the value stored, if any.
  //
  const std::type_info& type() const noexcept {
    if (has_value()) {
      const std::type_info* t;
      func(op::type, reinterpret_cast<void*>(&t));
      return *t;
    } else {
      return typeid(void);
    }
  }

  template<typename T, typename U, typename V>
  friend inline void* cast_helper(const base<U, V>& b) noexcept;
};

// Function used by all `any_cast` functions
//
// Returns a void* to the contents if they exist and match the
// requested type, otherwise `nullptr`.
//
template<typename T, typename U, typename V>
inline void* cast_helper(const base<U, V>& b) noexcept {
  if (b.func && ((&op_func<T> == b.func) ||
		 (b.type() == typeid(T)))) {
    return b.ptr();
  } else {
    return nullptr;
  }
}
}

// `any_cast`
// ==========
//
// Just the usual gamut of `any_cast` overloads. These get a bit
// repetitive and it would be nice to think of a way to collapse them
// down a bit.
//

// The pointer pair!
//
template<typename T, typename U, typename V>
inline T* any_cast(_any::base<U, V>* a) noexcept {
  if (a) {
    return static_cast<T*>(_any::cast_helper<std::decay_t<T>>(*a));
  }
  return nullptr;
}

template<typename T, typename U, typename V>
inline const T* any_cast(const _any::base<U, V>* a) noexcept {
  if (a) {
    return static_cast<T*>(_any::cast_helper<std::decay_t<T>>(*a));
  }
  return nullptr;
}

// While we disallow copying the immobile any itself, we can allow
// anything with an extracted value that the type supports.
//
template<typename T, typename U, typename V>
inline T any_cast(_any::base<U, V>& a) {
  static_assert(std::is_reference_v<T> ||
                std::is_copy_constructible_v<T>,
                "The supplied type must be either a reference or "
                "copy constructible.");
  auto p = any_cast<std::decay_t<T>>(&a);
  if (p) {
    return static_cast<T>(*p);
  }
  throw std::bad_any_cast();
}

template<typename T, typename U, typename V>
inline T any_cast(const _any::base<U, V>& a) {
  static_assert(std::is_reference_v<T> ||
                std::is_copy_constructible_v<T>,
                "The supplied type must be either a reference or "
                "copy constructible.");
  auto p = any_cast<std::decay_t<T>>(&a);
  if (p) {
    return static_cast<T>(*p);
  }
  throw std::bad_any_cast();
}

template<typename T, typename U, typename V>
inline std::enable_if_t<(std::is_move_constructible_v<T> ||
			 std::is_copy_constructible_v<T>) &&
			!std::is_rvalue_reference_v<T>, T>
any_cast(_any::base<U, V>&& a) {
  auto p = any_cast<std::decay_t<T>>(&a);
  if (p) {
    return std::move((*p));
  }
  throw std::bad_any_cast();
}

template<typename T, typename U, typename V>
inline std::enable_if_t<std::is_rvalue_reference_v<T>, T>
any_cast(_any::base<U, V>&& a) {
  auto p = any_cast<std::decay_t<T>>(&a);
  if (p) {
    return static_cast<T>(*p);
  }
  throw std::bad_any_cast();
}

// `immobile_any`
// ==============
//
// Sometimes, uncopyable objects exist and I want to do things with
// them. The C++ standard library is really quite keen on insisting
// things be copyable before it deigns to work. I find this annoying.
//
// Also, the allocator, while useful, is really not considerate of
// other people's time. Every time we go to visit it, it takes us
// quite an awfully long time to get away again. As such, I've been
// trying to avoid its company whenever it is convenient and seemly.
//
// We accept any type that will fit in the declared capacity. You may
// store types with throwing destructors, but terminate will be
// invoked when they throw.
//
template<std::size_t S>
class immobile_any : public _any::base<immobile_any<S>,
				       std::aligned_storage_t<S>> {
  using base = _any::base<immobile_any<S>, std::aligned_storage_t<S>>;
  friend base;

  using _any::base<immobile_any<S>, std::aligned_storage_t<S>>::storage;

  // Superclass requirements!
  // ------------------------
  //
  // Simple as anything. We have a buffer of fixed size and return the
  // pointer to it when asked.
  //
  static constexpr std::size_t capacity = S;
  void* ptr() const noexcept {
    return const_cast<void*>(static_cast<const void*>(&storage));
  }
  void* alloc_storage(std::size_t) noexcept {
    return ptr();
  }
  void free_storage() noexcept {}

  static_assert(capacity != _any::dynamic,
		"That is not a valid size for an immobile_any.");

public:

  immobile_any() noexcept = default;

  immobile_any(const immobile_any&) = delete;
  immobile_any& operator =(const immobile_any&) = delete;
  immobile_any(immobile_any&&) = delete;
  immobile_any& operator =(immobile_any&&) = delete;

  using base::base;
  using base::operator =;

  void swap(immobile_any&) = delete;
};

template<typename T, std::size_t S, typename... Args>
inline immobile_any<S> make_immobile_any(Args&& ...args) {
  return immobile_any<S>(std::in_place_type<T>, std::forward<Args>(args)...);
}

template<typename T, std::size_t S, typename U, typename... Args>
inline immobile_any<S> make_immobile_any(std::initializer_list<U> i, Args&& ...args) {
  return immobile_any<S>(std::in_place_type<T>, i, std::forward<Args>(args)...);
}

// `unique_any`
// ============
//
// Oh dear. Now we're getting back into allocation. You don't think
// the allocator noticed all those mean things we said about it, do
// you?
//
// Well. Okay, allocator. Sometimes when it's the middle of the night
// and you're writing template code you say things you don't exactly
// mean. If it weren't for you, we wouldn't have any memory to run all
// our programs in at all. Really, I'm just being considerate of
// *your* needs, trying to avoid having to run to you every time we
// instantiate a type, making a few that can be self-sufficient…uh…
//
// **Anyway**, this is movable but not copyable, as you should expect
// from anything with ‘unique’ in the name.
//
class unique_any : public _any::base<unique_any, std::unique_ptr<std::byte[]>> {
  using base = _any::base<unique_any, std::unique_ptr<std::byte[]>>;
  friend base;

  using base::storage;

  // Superclass requirements
  // -----------------------
  //
  // Our storage is a single chunk of RAM owned by a
  // `std::unique_ptr`.
  //
  static constexpr std::size_t capacity = _any::dynamic;
  void* ptr() const noexcept {
    return static_cast<void*>(storage.get());
    return nullptr;
  }

  void* alloc_storage(const std::size_t z) {
    storage.reset(new std::byte[z]);
    return ptr();
  }

  void free_storage() noexcept {
    storage.reset();
  }

public:

  unique_any() noexcept = default;
  ~unique_any() noexcept = default;

  unique_any(const unique_any&) = delete;
  unique_any& operator =(const unique_any&) = delete;

  // We can rely on the behavior of `unique_ptr` and the base class to
  // give us a default move constructor that does the right thing.
  //
  unique_any(unique_any&& rhs) noexcept = default;
  unique_any& operator =(unique_any&& rhs) = default;

  using base::base;
  using base::operator =;
};

inline void swap(unique_any& lhs, unique_any& rhs) noexcept {
  lhs.swap(rhs);
}

template<typename T, typename... Args>
inline unique_any make_unique_any(Args&& ...args) {
  return unique_any(std::in_place_type<T>, std::forward<Args>(args)...);
}

template<typename T, typename U, typename... Args>
inline unique_any make_unique_any(std::initializer_list<U> i, Args&& ...args) {
  return unique_any(std::in_place_type<T>, i, std::forward<Args>(args)...);
}

// `shared_any`
// ============
//
// Once more with feeling!
//
// This is both copyable *and* movable. In case you need that sort of
// thing. It seemed a reasonable completion.
//
class shared_any : public _any::base<shared_any, boost::shared_ptr<std::byte[]>> {
  using base = _any::base<shared_any, boost::shared_ptr<std::byte[]>>;
  friend base;

  using base::storage;

  // Superclass requirements
  // -----------------------
  //
  // Our storage is a single chunk of RAM allocated from the
  // heap. This time it's owned by a `boost::shared_ptr` so we can use
  // `boost::make_shared_noinit`. (This lets us get the optimization
  // that allocates array and control block in one without wasting
  // time on `memset`.)
  //
  static constexpr std::size_t capacity = _any::dynamic;
  void* ptr() const noexcept {
    return static_cast<void*>(storage.get());
  }

  void* alloc_storage(std::size_t n) {
    storage = boost::make_shared_noinit<std::byte[]>(n);
    return ptr();
  }

  void free_storage() noexcept {
    storage.reset();
  }

public:

  shared_any() noexcept = default;
  ~shared_any() noexcept = default;

  shared_any(const shared_any& rhs) noexcept = default;
  shared_any& operator =(const shared_any&) noexcept = default;

  shared_any(shared_any&& rhs) noexcept = default;
  shared_any& operator =(shared_any&& rhs) noexcept = default;

  using base::base;
  using base::operator =;
};

inline void swap(shared_any& lhs, shared_any& rhs) noexcept {
  lhs.swap(rhs);
}

template<typename T, typename... Args>
inline shared_any make_shared_any(Args&& ...args) {
  return shared_any(std::in_place_type<T>, std::forward<Args>(args)...);
}

template<typename T, typename U, typename... Args>
inline shared_any make_shared_any(std::initializer_list<U> i, Args&& ...args) {
  return shared_any(std::in_place_type<T>, i, std::forward<Args>(args)...);
}
}

#endif // INCLUDE_STATIC_ANY
