// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/circular_buffer.hh>
#include "crimson/common/log.h"

class read_lock {
public:
  std::optional<seastar::future<>> lock();
  void unlock();
};

class write_lock {
public:
  std::optional<seastar::future<>> lock();
  void unlock();
};

class excl_lock {
public:
  std::optional<seastar::future<>> lock();
  void unlock();
};

// promote from read to excl
class excl_lock_from_read {
public:
  void lock();
  void unlock();
};

// promote from write to excl
class excl_lock_from_write {
public:
  void lock();
  void unlock();
};

/// shared/exclusive mutual exclusion
///
/// Unlike reader/write lock, tri_mutex does not enforce the exclusive access
/// of write operations, on the contrary, multiple write operations are allowed
/// to hold the same tri_mutex at the same time. Here, what we mean is that we
/// can pipeline reads, and we can pipeline writes, but we cannot allow a read
/// while writes are in progress or a write while reads are in progress.
///
/// tri_mutex is based on seastar::shared_mutex, but instead of two kinds of
/// waiters, tri_mutex keeps track of three kinds of lock users:
/// - readers
/// - writers
/// - exclusive users
///
/// For lock promotion, a read or a write lock is only allowed to be promoted
/// atomically upon the first locking.
class tri_mutex : private read_lock,
                          write_lock,
                          excl_lock,
                          excl_lock_from_read,
                          excl_lock_from_write
{
public:
  tri_mutex() = default;
#ifdef NDEBUG
  tri_mutex(const std::string obj_name) : name() {}
#else
  tri_mutex(const std::string obj_name) : name(obj_name) {}
#endif
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

  // for shared readers
  std::optional<seastar::future<>> lock_for_read();
  bool try_lock_for_read() noexcept;
  void unlock_for_read();
  void promote_from_read();
  void demote_to_read();
  unsigned get_readers() const {
    return readers;
  }

  // for shared writers
  std::optional<seastar::future<>> lock_for_write();
  bool try_lock_for_write() noexcept;
  void unlock_for_write();
  void promote_from_write();
  void demote_to_write();
  unsigned get_writers() const {
    return writers;
  }

  // for exclusive users
  std::optional<seastar::future<>> lock_for_excl();
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

  std::string_view get_name() const{
    return name;
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
    waiter_t(seastar::promise<>&& pr, type_t type, std::string_view waiter_name)
      : pr(std::move(pr)), type(type)
    {}
    seastar::promise<> pr;
    type_t type;
    std::string_view waiter_name;
  };
  seastar::circular_buffer<waiter_t> waiters;
  const std::string name;
  friend class read_lock;
  friend class write_lock;
  friend class excl_lock;
  friend class excl_lock_from_read;
  friend class excl_lock_from_write;
  friend class excl_lock_from_excl;
  friend std::ostream& operator<<(std::ostream &lhs, const tri_mutex &rhs);
};

inline std::ostream& operator<<(std::ostream& os, const tri_mutex& tm)
{
  os << fmt::format("tri_mutex {} writers {} readers {}"
                    " exclusively_used {} waiters: {}",
                    tm.get_name(), tm.get_writers(), tm.get_readers(),
                    tm.exclusively_used, tm.waiters.size());
  return os;
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<tri_mutex> : fmt::ostream_formatter {};
#endif
