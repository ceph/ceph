// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/circular_buffer.hh>

class read_lock {
public:
  seastar::future<> lock();
  void unlock();
};

class write_lock {
public:
  seastar::future<> lock();
  void unlock();
};

class excl_lock {
public:
  seastar::future<> lock();
  void unlock();
};

// promote from read to excl
class excl_lock_from_read {
public:
  seastar::future<> lock();
  void unlock();
};

// promote from write to excl
class excl_lock_from_write {
public:
  seastar::future<> lock();
  void unlock();
};

// promote from excl to excl
class excl_lock_from_excl {
public:
  seastar::future<> lock();
  void unlock();
};

/// shared/exclusive mutual exclusion
///
/// this lock design uses reader and writer is entirely and completely
/// independent of the conventional reader/writer lock usage. Here, what we
/// mean is that we can pipeline reads, and we can pipeline writes, but we
/// cannot allow a read while writes are in progress or a write while reads are
/// in progress. Any rmw operation is therefore exclusive.
///
/// tri_mutex is based on seastar::shared_mutex, but instead of two kinds of
/// waiters, tri_mutex keeps track of three kinds of lock users:
/// - readers
/// - writers
/// - exclusive users
class tri_mutex : private read_lock,
                          write_lock,
                          excl_lock,
                          excl_lock_from_read,
                          excl_lock_from_write,
                          excl_lock_from_excl
{
public:
  tri_mutex() = default;
  ~tri_mutex();

  read_lock& for_read() {
    return *this;
  }
  write_lock& for_write() {
    return *this;
  }
  excl_lock& for_excl() {
    return *this;
  }
  excl_lock_from_read& excl_from_read() {
    return *this;
  }
  excl_lock_from_write& excl_from_write() {
    return *this;
  }
  excl_lock_from_excl& excl_from_excl() {
    return *this;
  }

  // for shared readers
  seastar::future<> lock_for_read();
  bool try_lock_for_read() noexcept;
  void unlock_for_read();
  void promote_from_read();
  void demote_to_read();
  unsigned get_readers() const {
    return readers;
  }

  // for shared writers
  seastar::future<> lock_for_write(bool greedy);
  bool try_lock_for_write(bool greedy) noexcept;
  void unlock_for_write();
  void promote_from_write();
  void demote_to_write();
  unsigned get_writers() const {
    return writers;
  }

  // for exclusive users
  seastar::future<> lock_for_excl();
  bool try_lock_for_excl() noexcept;
  void unlock_for_excl();
  bool is_excl_acquired() const {
    return exclusively_used;
  }

  bool is_acquired() const;

  /// pass the provided exception to any waiting waiters
  template<typename Exception>
  void abort(Exception ex) {
    while (!waiters.empty()) {
      auto& waiter = waiters.front();
      waiter.pr.set_exception(std::make_exception_ptr(ex));
      waiters.pop_front();
    }
  }

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
  friend class read_lock;
  friend class write_lock;
  friend class excl_lock;
  friend class excl_lock_from_read;
  friend class excl_lock_from_write;
  friend class excl_lock_from_excl;
};
