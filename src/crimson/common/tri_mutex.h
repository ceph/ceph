// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/circular_buffer.hh>

/// shared/exclusive mutual exclusion
///
/// similar to seastar::shared_mutex, but instead of two kinds of  waiters,
/// tri_mutex keeps track of three kinds of them:
/// - readers
/// - writers
/// - exclusive users
/// and unlike shared_mutex, tri_mutex have two kinds of shared users of lock:
/// - readers
/// - writers, which are not mutual-exclusive
/// the exclusive users is like the writers in shared_mutex.
class tri_mutex {
public:
  tri_mutex() = default;
  // for shared readers
  seastar::future<> lock_for_read();
  bool try_lock_for_read() noexcept;
  void unlock_for_read();
  // for shared writers
  seastar::future<> lock_for_write(bool greedy);
  bool try_lock_for_write(bool greedy) noexcept;
  void unlock_for_write();
  // for exclusive users
  seastar::future<> lock_for_excl();
  bool try_lock_for_excl() noexcept;
  void unlock_for_excl();

  bool is_acquired() const;

private:
  void wake();
  unsigned readers = 0;
  unsigned writers = 0;
  bool exclusively_used = false;
  enum class type_t : uint8_t {
    read,
    write,
    exclusive,
    none,
  };
  struct waiter_t {
    waiter_t(seastar::promise<>&& pr, type_t type)
      : pr(std::move(pr)), type(type)
    {}
    seastar::promise<> pr;
    type_t type;
  };
  seastar::circular_buffer<waiter_t> waiters;
};
