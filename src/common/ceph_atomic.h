// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <atomic>

// What and why
// ============
//
// ceph::atomic – thin wrapper to differentiate behavior of atomics.
//
// Not all users of the common truly need costly atomic operations to
// synchronize data between CPUs and threads. Some, like crimson-osd,
// stick to shared-nothing approach. Enforcing issue of atomics in
// such cases is wasteful – on x86 any locked instruction works actually
// like a full memory barrier stalling execution till CPU's store and
// load buffers are drained.

#if defined(WITH_CRIMSON) && !defined(WITH_BLUESTORE)

#include <type_traits>

namespace ceph {
  template <class T>
  class dummy_atomic {
    T value;

  public:
    dummy_atomic() = default;
    dummy_atomic(const dummy_atomic&) = delete;
    dummy_atomic(T value) : value(std::move(value)) {
    }
    bool is_lock_free() const noexcept {
      return true;
    }
    void store(T desired, std::memory_order) noexcept {
      value = std::move(desired);
    }
    T load(std::memory_order = std::memory_order_seq_cst) const noexcept {
      return value;
    }
    T operator=(T desired) noexcept {
      value = std::move(desired);
      return *this;
    }
    operator T() const noexcept {
      return value;
    }

    // We need to differentiate with SFINAE as std::atomic offers beefier
    // interface for integral types.

    template<class TT=T>
    std::enable_if_t<!std::is_enum_v<TT> && std::is_integral_v<TT>, TT>  operator++() {
      return ++value;
    }
    template<class TT=T>
    std::enable_if_t<!std::is_enum_v<TT> && std::is_integral_v<TT>, TT> operator++(int) {
      return value++;
    }
    template<class TT=T>
    std::enable_if_t<!std::is_enum_v<TT> && std::is_integral_v<TT>, TT> operator--() {
      return --value;
    }
    template<class TT=T>
    std::enable_if_t<!std::is_enum_v<TT> && std::is_integral_v<TT>, TT> operator--(int) {
      return value--;
    }
    template<class TT=T>
    std::enable_if_t<!std::is_enum_v<TT> && std::is_integral_v<TT>, TT> operator+=(const dummy_atomic& b) {
      value += b;
      return value;
    }
    template<class TT=T>
    std::enable_if_t<!std::is_enum_v<TT> && std::is_integral_v<TT>, TT> operator-=(const dummy_atomic& b) {
      value -= b;
      return value;
    }

    static constexpr bool is_always_lock_free = true;
  };

  template <class T> using atomic = dummy_atomic<T>;
} // namespace ceph

#else  // WITH_CRIMSON

namespace ceph {
  template <class T> using atomic = ::std::atomic<T>;
} // namespace ceph

#endif	// WITH_CRIMSON
