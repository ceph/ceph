// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <algorithm>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>

#include "include/uses_allocator.h"

/// The problem:
///     - Non-movable types
///     - Non-default-constructible types
///     - Entries whose constructor parameters depend on the index

/// The limitations:
///     - No resize
///     - No insert
///     - No delete


/// The solution, a class with all the methods of vector that are
/// compatible with never moving an element (i.e. no reallocations)

namespace ceph {
template<typename T, typename Allocator = std::allocator<T>>
class dynarray {
  template<typename...>
  struct is_tuple : public std::false_type {};
  template<typename ...Args>
  struct is_tuple<std::tuple<Args...>> : public std::true_type {};

  using elstore = std::aligned_storage_t<sizeof(T), alignof(T)>;
  std::size_t len;
  Allocator alloc;
  elstore* p = nullptr;

  static_assert(std::is_trivially_constructible_v<elstore>);
  static_assert(sizeof(elstore) == sizeof(T));
  static_assert(alignof(elstore) == alignof(T));
  static_assert(sizeof(elstore[5]) == sizeof(T[5]));

  elstore* allocate() {
    if (len == 0)
      return nullptr;

    typename Allocator::template rebind<elstore>::other a(alloc);
    return a.allocate(len);
  }

  void deallocate() {
    if (p) {
      for (std::size_t i = 0; i < len; ++i) {
	reinterpret_cast<T*>(p + i)->~T();
      }
      typename Allocator::template rebind<elstore>::other a(alloc);
      a.deallocate(p, len);
    }
    len = 0;
    p = 0;
  }

public:
  using value_type = T;
  using allocator_type = Allocator;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = typename std::allocator_traits<Allocator>::pointer;
  using const_pointer =
    typename std::allocator_traits<Allocator>::const_pointer;
  using iterator = T*;
  using const_iterator = const T*;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  dynarray() noexcept(noexcept(Allocator()))
    : len(0), alloc({}) {}

  explicit dynarray(const Allocator& alloc) noexcept
    : len(0), alloc({}) {}

  dynarray(std::size_t len, const T& value,
	   const Allocator& alloc = Allocator{})
    : len(len), alloc(alloc), p(allocate()) {
    for (std::size_t i = 0; i < len; ++i)
      uninitialized_construct_using_allocator(reinterpret_cast<T*>(p + i),
					      alloc, value);
  }

  template<typename F>
  dynarray(std::size_t len, F&& f,
	   std::enable_if_t<std::is_invocable_v<F, std::size_t>,
	                    const Allocator&> alloc = Allocator{})
    : len(len), alloc(alloc), p(allocate()) {
    for (std::size_t i = 0; i < len; ++i) {
      if constexpr (std::is_constructible_v<
		    T, std::invoke_result_t<F, std::size_t>>) {
	uninitialized_construct_using_allocator(reinterpret_cast<T*>(p + i),
						alloc,
						std::forward<F>(f)(i));
      } else {
	std::apply([e = reinterpret_cast<T*>(p + i), &alloc](auto&& ...args) {
	  uninitialized_construct_using_allocator(
	    e, alloc, std::forward<decltype(args)>(args)...);
	}, std::forward<F>(f)(i));
      }
    }
  }

  template<typename I>
  dynarray(I start,
	   typename std::enable_if_t<
	     std::is_constructible_v<T,
	       typename std::iterator_traits<I>::reference>, I> end,
	   Allocator alloc = Allocator{})
    : len(std::distance(start, end)), alloc(alloc), p(allocate()) {
    int n = 0;
    for (auto i = start; i != end; ++i, ++n)
      uninitialized_construct_using_allocator(reinterpret_cast<T*>(p + n),
					      alloc, *i);
  }

  dynarray(const dynarray&) = delete;
  dynarray& operator =(const dynarray&) = delete;

  dynarray(dynarray&& rhs) {
    len = rhs.len;
    p = rhs.p;
    alloc = rhs.alloc;
    rhs.len = 0;
    rhs.p = 0;
  }
  dynarray& operator =(dynarray&& rhs) {
    deallocate();
    len = rhs.len;
    p = rhs.p;
    static_assert(
      std::allocator_traits<Allocator>::
      propagate_on_container_move_assignment::value,
      "Since our members may be immovable, we can't reallocate space and "
      "move them.");
    alloc = rhs.alloc;
    rhs.len = 0;
    rhs.p = 0;
    return *this;
  }

  dynarray(std::initializer_list<T> l, Allocator alloc = Allocator{})
    : len(l.size()), alloc(alloc), p(allocate()) {
    int i = 0;
    for (const auto& e : l) {
      uninitialized_construct_using_allocator(reinterpret_cast<T*>(p + i), alloc, e);
      ++i;
    }
  }

  ~dynarray() {
    deallocate();
  }

  allocator_type get_allocator() noexcept {
    return alloc;
  }

  reference& at(std::size_t i) {
    if (i >= len)
      throw std::out_of_range("Index out of range.");

    return *reinterpret_cast<T*>(p + i);
  }

  const_reference& at(std::size_t i) const {
    if (i >= len)
      throw std::out_of_range("Index out of range.");

    return *reinterpret_cast<T*>(p + i);
  }

  reference& operator [](std::size_t i) noexcept {
    return *reinterpret_cast<T*>(p + i);
  }

  const_reference& operator [](std::size_t i) const noexcept {
    return *reinterpret_cast<T*>(p + i);
  }

  reference& front() noexcept {
    return *reinterpret_cast<T*>(p);
  }

  const_reference& front(std::size_t i) const noexcept {
    return *reinterpret_cast<T*>(p);
  }

  reference& back() noexcept {
    return *reinterpret_cast<T*>(p + len - 1);
  }

  const_reference& back(std::size_t i) const noexcept {
    return *reinterpret_cast<T*>(p + len - 1);
  }

  pointer data() noexcept {
    return reinterpret_cast<T*>(p);
  }

  const_pointer data() const noexcept {
    return reinterpret_cast<T*>(p);
  }

  iterator begin() noexcept {
    return data();
  }
  const_iterator begin() const noexcept {
    return data();
  }
  const_iterator cbegin() const noexcept {
    return data();
  }

  reverse_iterator rbegin() noexcept {
    return std::make_reverse_iterator(begin());
  }
  const_reverse_iterator rbegin() const noexcept {
    return std::make_reverse_iterator(begin());
  }
  const_reverse_iterator crbegin() const noexcept {
    return std::make_reverse_iterator(cbegin());
  }

  iterator end() noexcept {
    return data() ? data() + len : nullptr;
  }
  const_iterator end() const noexcept {
    return data() ? data() + len : nullptr;
  }
  const_iterator cend() const noexcept {
    return data() ? data() + len : nullptr;
  }

  reverse_iterator rend() noexcept {
    return std::make_reverse_iterator(end());
  }
  const_reverse_iterator rend() const noexcept {
    return std::make_reverse_iterator(end());
  }
  const_reverse_iterator crend() const noexcept {
    return std::make_reverse_iterator(cend());
  }

  bool empty() const noexcept {
    return !p;
  }

  size_type size() const noexcept {
    return len;
  }

  size_type max_size() const noexcept {
    return std::numeric_limits<difference_type>::max();
  }

  void swap(dynarray& other) {
    using std::swap;
    swap(len, other.len);
    swap(p, other.p);
    swap(alloc, other.alloc);
  }
};

template<typename T, typename Alloc>
bool operator ==(const dynarray<T, Alloc>& l, const dynarray<T, Alloc>& r) {
  if (l.size() != r.size()) return false;
  return std::equal(l.begin(), l.end(), r.begin());
}
template<typename T, typename Alloc>
bool operator !=(const dynarray<T, Alloc>& l, const dynarray<T, Alloc>& r) {
  return !(l == r);
}
template<typename T, typename Alloc>
bool operator <(const dynarray<T, Alloc>& l, const dynarray<T, Alloc>& r) {
  return std::lexicographical_compare(l.begin(), l.end(), r.begin(), r.end());
}
template<typename T, typename Alloc>
bool operator <=(const dynarray<T, Alloc>& l, const dynarray<T, Alloc>& r) {
  return (l < r) || (l == r);
}
template<typename T, typename Alloc>
bool operator >=(const dynarray<T, Alloc>& l, const dynarray<T, Alloc>& r) {
  return !(l < r);
}
template<typename T, typename Alloc>
bool operator >(const dynarray<T, Alloc>& l, const dynarray<T, Alloc>& r) {
  return (l >= r) && (l != r);
}


template<typename T, typename Alloc>
void swap(dynarray<T, Alloc>& a, dynarray<T, Alloc>& b) {
  a.swap(b);
}

template<typename T, typename Alloc>
std::ostream& operator <<(std::ostream& m, const dynarray<T, Alloc>& v) {
  bool first = true;
  m << "[";
  for (const auto& p : v) {
    if (!first) m << ",";
    m << p;
    first = false;
  }
  m << "]";
  return m;
}

/// Deduction guides
template<typename F, typename Alloc =
	 std::allocator<std::invoke_result_t<F, std::size_t>>>
dynarray(std::size_t, F&&, const Alloc& = Alloc{}) ->
  dynarray<std::invoke_result_t<F, std::size_t>, Alloc>;
template<typename I, typename Alloc =
	 std::allocator<typename std::iterator_traits<I>::value_type>>
dynarray(I, I, const Alloc& = Alloc{}) ->
  dynarray<typename std::iterator_traits<I>::value_type, Alloc>;
}
