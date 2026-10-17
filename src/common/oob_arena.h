// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>

namespace ceph { class Formatter; }

namespace ceph::oob {

/**
 * The memory that a daemon registers for out-of-band transfers: one
 * mapping of oob_memory bytes, apart from the heap, which each of its
 * transports registers. Transfers take buffers of it in powers of two
 * from MIN to MAX bytes, from a buddy allocator. reserve() takes whole
 * blocks of MAX bytes, in one run, for windows that last as long as the
 * daemon, and always leaves a block for buffers.
 */
class Arena {
public:
  static constexpr uint64_t MIN = 1ull << 20;
  static constexpr uint64_t MAX = 64ull << 20;

  /// a buffer of the arena
  struct slice_t {
    uint64_t offset = 0;
    uint64_t len = 0;
    explicit operator bool() const { return len != 0; }
  };

  /// size, rounded down to whole blocks of MAX bytes, and at least one.
  static std::shared_ptr<Arena> create(uint64_t size, std::string* err);
  ~Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  char* base() const { return base_; }
  uint64_t size() const { return size_; }
  char* ptr(const slice_t& s) const { return base_ + s.offset; }

  /// the size that a buffer of len bytes takes: len rounded up to a
  /// power of two, at least MIN; 0 when len is more than MAX
  static uint64_t round(uint64_t len);

  /// a buffer of round(len) bytes, or an empty one when len is more than
  /// MAX or no such buffer is free
  slice_t alloc(uint64_t len);
  /// give back the buffer at offset that alloc(len) returned
  void free(uint64_t offset, uint64_t len);
  void free(const slice_t& s) { free(s.offset, s.len); }

  /// len bytes, rounded up to whole blocks of MAX, in one run, taken for
  /// the arena's life; null when no such run is free, or when it would
  /// leave no block for buffers
  char* reserve(uint64_t len);
  /// give back the run at p that reserve(len) returned, when the windows
  /// in it did not come up; nothing may read or write it any more
  void unreserve(char* p, uint64_t len);

  struct stats_t {
    uint64_t size = 0;
    uint64_t in_use = 0;    ///< in buffers that alloc() handed out
    uint64_t reserved = 0;  ///< taken by reserve()
    uint64_t allocs = 0;
    uint64_t failed = 0;    ///< alloc() calls that found nothing free
  };
  stats_t stats() const;
  /// the stats, as arena_* fields of the section open in f
  void dump(ceph::Formatter* f) const;

private:
  static constexpr unsigned ORDERS = 7;  // MIN << 6 == MAX
  static_assert((MIN << (ORDERS - 1)) == MAX);

  Arena(char* base, uint64_t size);
  static unsigned order_of(uint64_t len);

  char* base_;
  uint64_t size_;
  mutable std::mutex mtx;
  /// the offsets of the free blocks of each order
  std::array<std::set<uint64_t>, ORDERS> free_;
  void free_locked(uint64_t offset, uint64_t size);
  stats_t st;
};

} // namespace ceph::oob
