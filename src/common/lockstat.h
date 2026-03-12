// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corp
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_LOCKSTAT_H
#define CEPH_COMMON_LOCKSTAT_H


#include <array>
#include <atomic>
#include <thread>

#include "common/Formatter.h"
#include "include/ceph_assert.h"

#include "ceph_time.h"
#include "likely.h"

#ifdef CEPH_LOCKSTAT

namespace ceph {
namespace lockstat_detail {

class LockStatTraits;
class LockStatEntry;
class LockStat;
#if defined __x86_64__ or defined __i386__
using lockstat_clock = ceph::tsc_clock;
#else
using lockstat_clock = ceph::mono_clock;
#endif

/**
 * @brief bin_histogram is a template class which can be used for
 * computing indexes for a histogram with binary bin sizes.
 *
 * It is a template class with parameters for number of bins, starting
 * bin index, and value of lowest and highest bin index.
 */
template <unsigned int Bins, unsigned int StartIndex, uint64_t MinVal, uint64_t MaxVal>
struct bin_histogram {
  static constexpr unsigned int bins = Bins;

  static unsigned int
  get_index(uint64_t val)
  {
    if (val < MinVal) {
      return 0;
    }
    if (val >= MaxVal) {
      return Bins - 1;
    }
    // value is in between [MinVal, MaxVal)
    // val >> __builtin_clzll(MinVal) - __builtin_clzll(val)
    // log2(val) - log2(MinVal) + StartIndex
    // For MinVal = 16, StartIndex = 1:
    // val = 16 (1 << 4), log2 = 4: 4 - 4 + 1 = 1
    // val = 31 (0x1f), log2 = 4: 4 - 4 + 1 = 1
    // val = 32 (1 << 5), log2 = 5: 5 - 4 + 1 = 2
    unsigned int log2_val = 63 - __builtin_clzll(val);
    unsigned int log2_min = 63 - __builtin_clzll(MinVal);
    return log2_val - log2_min + StartIndex;
  }
};

static constexpr uint64_t FNV_64_PRIME = 0x100000001b3ULL;
static constexpr uint64_t FNV1_64_INIT = 0xcbf29ce484222325ULL;

///
/// @brief Calculate the hash value for traits
///
static constexpr uint64_t
hash_str(const char* s, uint64_t h)
{
  for (; *s; ++s) {
    h ^= static_cast<uint64_t>(*s);
    h *= FNV_64_PRIME;
  }
  return h;
}

static constexpr uint64_t
hash_str(const std::string_view s, uint64_t h)
{
  for (char c : s) {
    h ^= static_cast<uint64_t>(c);
    h *= FNV_64_PRIME;
  }
  return h;
}

static constexpr uint64_t
hash_u64(uint64_t val, uint64_t h)
{
  for (int i = 0; i < 8; ++i) {
    h ^= (uint64_t)(val & 0xff);
    h *= FNV_64_PRIME;
    val >>= 8;
  }
  return h;
}

///
/// @brief Calculate the hash value for traits
///
static constexpr uint64_t
hash(
    const char* name,
    const char* file_name,
    uint64_t line_number,
    const char* function_name)
{
  uint64_t r = hash_str(name, FNV1_64_INIT);
  r = hash_str(file_name, r);
  r = hash_u64(line_number, r);
  r = hash_str(function_name, r);
  return r;
}

static constexpr uint64_t
hash(
    const std::string_view& name,
    const char* file_name,
    uint64_t line_number,
    const char* function_name)
{
  uint64_t r = hash_str(name, FNV1_64_INIT);
  r = hash_str(file_name, r);
  r = hash_u64(line_number, r);
  r = hash_str(function_name, r);
  return r;
}

enum class LockMode {
  WRITE = 0,
  READ = 1,
  TRY_READ = 2,
  TRY_WRITE = 3,
  COUNT
};

static constexpr int64_t TIME_sec_to_ns = 1000000000;

/**
 * @brief LockStatTraits manages the metadata for a specific lock location.
 *
 * It stores identifying information such as the lock name, source file, and line number.
 * It is responsible for associating a lock instance with its corresponding LockStatEntry
 * in the global statistics table using an FNV-1a hash of its identity.
 */
class LockStatTraits {
  friend class LockStat;
  friend class LockStatEntry;

public: // Types
  static constexpr size_t kHash_table_max_size = 1021;
  typedef std::array<LockStatTraits, kHash_table_max_size> LockStatTraitsTableT;

  enum class LockStatType : uint8_t {
    UNKNOWN = 'U',
    MUTEX = 'M',
    RW_LOCK = 'R',
    COND_VAR = 'C',
    SPINLOCK = 'S',
    NO_LOCK = 'N'
  };

public: // Constructor
  LockStatTraits();
  ~LockStatTraits();

public: // Methods
  static constexpr uint64_t
  hash(
      const char* name,
      const char* file_name,
      uint64_t line_number,
      const char* function_name)
  {
    return lockstat_detail::hash(name, file_name, line_number, function_name);
  }

  ///
  /// @brief set a lock's type
  ///
  void
  set_lock_type(const LockStatType lockType) const
  {
    if (lockType != get_lock_type()) {
      _set_locktype_impl(lockType);
    }
  }

  ///
  /// @brief generate a name for a lockstat
  ///
  static std::string
  get_lockstat_name(
      const std::string& name,
      const char* file_name,
      const int line_number,
      const char* function_name)
  {
    return name + "[" + function_name + "]" + file_name + "@" +
           std::to_string(line_number);
  }

  ///
  /// @brief Find a traits object from the hash table
  ///
  static LockStatTraits* get_lockstat_traits(
      const std::string& name,
      uint64_t hash_index,
      uint64_t lock_index,
      const char* file_name,
      int line_number,
      const char* function_name);

  ///
  /// @brief record the wait time of a lock
  ///
  static void record_wait_time(
      const LockStatTraits* traits,
      lockstat_clock::duration wait_time,
      LockMode mode);

  ///
  /// @brief Return the entry for given index
  ///
  [[nodiscard]] LockStatEntry*
  get_lockstat_entry() const
  {
    return m_lockstat_entry;
  }

  ///
  /// @brief get a lockstat's id
  ///
  [[nodiscard]] uint64_t get_lock_id() const;

  ///
  /// @brief Return the traits name
  ///
  [[nodiscard]] const std::string& get_name() const;

  ///
  /// @brief Return a unique traits name
  ///
  [[nodiscard]] const std::string get_unique_name(uintptr_t value) const;

  ///
  /// @brief Return the traits lock type
  ///
  [[nodiscard]] LockStatType get_lock_type() const;

  ///
  /// @brief Provide the table of LockStatTraits
  ///
  static LockStatTraitsTableT&
  get_traits_table()
  {
    static LockStatTraitsTableT lockstat_traits_table;
    return lockstat_traits_table;
  }

  [[nodiscard]] static bool
  lockstat_global_enable()
  {
    return g_global_enable;
  }

protected: // members
  ///
  /// @brief hash value of lock derived from lock name, function, source file and line
  ///
  uint64_t m_lockstat_hash;

  ///
  /// @brief Pointer to entry in table
  ///
  LockStatEntry* m_lockstat_entry;

  ///
  /// @brief Lock for managing access to the other fields
  ///
  std::mutex m_lockstat_mutex;

  ///
  /// @brief Change the lock type in the entry
  ///
  void _set_locktype_impl(LockStatType) const;

  ///
  /// @brief get the lockstat traits from the hash table
  ///
  static LockStatTraits* _get_lockstat_traits_impl(
      const std::string& name,
      uint64_t hash_index,
      uint64_t lock_index,
      const char* file_name,
      int line_number,
      const char* function_name);

  ///
  /// @brief Count of collisions looking for unused traits object in table
  ///
  static std::atomic<size_t> g_lock_collisions;

  static const bool g_global_enable;
};

//----------------------------------------------------------------------------
// LockStatEntry declarations
//
// A LockStatEntry is used to maintain statistics for a lock and contains details about
// the lock, i.e. its name, lock type, etc
//
/**
 * @brief LockStatEntry stores the actual accumulated statistics for a lock.
 *
 * It maintains a per-CPU table of LockStats to minimize cache contention during
 * data collection. It also contains metadata like the lock name and type for reporting.
 */
class LockStatEntry {
public: // Constants and types
  /**
   * @brief LockStats holds the raw counters and histograms for a lock.
   *
   * It tracks wait counts, total wait durations, and wait-time distributions
   * (histograms) for different lock modes (READ, WRITE, etc.).
   */
  struct LockStats {
  public: // Constants and types
    static constexpr uint32_t num_bins = 25;
    static constexpr uint32_t bin_low_bits = 4; // Start at 16ns
    static constexpr uint32_t bin_hi_bits = bin_low_bits + num_bins;
    typedef bin_histogram<num_bins + 2, 1, 1ul << bin_low_bits, 1ul << bin_hi_bits>
        LockStatLatBins;

  public: // Constructor/destructor
    ///
    /// @brief Construct a lock_stats object
    ///
    LockStats();

  public: // Methods
    ///
    /// @brief Reset
    ///
    void reset();

    ///
    /// @brief Add other to this stats object
    ///
    LockStats& operator+=(const LockStats& other);

  public: // Member variables
    ///
    /// @brief sum of wait time since start
    ///
    std::array<lockstat_clock::duration, static_cast<size_t>(LockMode::COUNT)>
        m_wait_duration;

    ///
    /// @brief number of times lock has been acquired since start
    ///
    std::array<uint32_t, static_cast<size_t>(LockMode::COUNT)> m_wait_count;

    ///
    /// @brief histogram of wait times
    ///
    std::array<
        std::array<uint32_t, LockStatLatBins::bins>,
        static_cast<size_t>(LockMode::COUNT)>
        m_wait_time_histogram;
  };

  ///
  /// @brief Entry table should encompass the static traits table size and the
  ///        hash table size.
  ///
  typedef std::array<LockStatEntry, LockStatTraits::kHash_table_max_size>
      LockStatEntryTableT;

public: // Constructor/destructors
  LockStatEntry();

  LockStatEntry(const LockStatEntry& other);

  ~LockStatEntry() = default;

public: // Methods
  ///
  /// @brief reset entry
  ///
  void reset();

  ///
  /// @brief get lockstat table
  ///
  static LockStatEntryTableT& get_lockstat_table();

  ///
  /// @brief Return the entry for given index
  ///
  static LockStatEntry&
  get_lockstat_entry(const uint32_t entry_index)
  {
    ceph_assert(entry_index < LockStatEntry::get_lockstat_table().size());
    return LockStatEntry::get_lockstat_table()[entry_index];
  }

  ///
  /// @brief Get sum of per cpu stats
  ///
  [[nodiscard]] LockStats get_stats_sum() const;

  ///
  /// @brief Start capturing lockstat data with a given table size for lock wait time exceeding threshold
  ///
  static int start(lockstat_clock::duration threshold);

  ///
  /// @brief Stop capturing lockstat data
  ///
  static void stop();

  ///
  /// @brief reset entry
  ///
  static void reset_data();

  ///
  /// @brief  Return lockstat data and total usec elapsed
  ///
  static std::pair<LockStatEntryTableT, lockstat_clock::duration> show();

  ///
  /// @brief  Get the offset of this entry in the lockstat entry table
  ///
  [[nodiscard]] uint64_t
  get_lock_id() const
  {
    ceph_assert(m_lock_id < LockStatEntry::get_lockstat_table().size());
    return m_lock_id;
  }

  static void dump_formatted(ceph::Formatter* f);

public: // member variables
  ///
  /// @brief Per-cpu table of stats
  ///
  std::vector<LockStats> m_stats_table;

  ///
  /// @brief Corresponding index in the entry table
  ///
  uint64_t m_lock_id;

  ///
  /// @brief Number of instances of this lock
  ///
  std::atomic<uint64_t> m_num_instances;

  ///
  /// @brief Name of locks this lockstat is for
  ///
  std::string m_lock_name;

  ///
  /// @brief Highest number of cycles spent waiting
  ///
  std::atomic<lockstat_clock::duration> m_max_wait;

  ///
  /// @brief Type of lock for tracking
  ///
  LockStatTraits::LockStatType m_lock_type;

  ///
  /// @brief enable tripwire for this lock
  ///
  bool m_tripwire_enabled;
};

//----------------------------------------------------------------------------
// LockStat declarations
//
// Base class for locks used for tracking statistics. This keeps a traits object
// pointer for each lock and provides functions for recording a lock's wait time.
// This also implements static functions for managing lock stat collection and reporting
//
/**
 * @brief LockStat is the RAII-style interface for recording lock statistics.
 *
 * It is typically embedded within a mutex or lock implementation. On construction,
 * it associates itself with a LockStatTraits object. Its primary purpose is to
 * provide the `record_wait_time` method to update statistics when a lock is acquired.
 */
class LockStat {
  friend class LockStatTraits;
  friend class LockStatEntry;

public: // Methods
  ///
  /// @brief Set the timespec timeout based on whether tripwire is set for thread
  ///
  static void
  get_timeout_tripwire(struct timespec* timeout_tripwire)
  {
    const auto threshold = m_tripwire_threshold.load(std::memory_order_relaxed);
    clock_gettime(CLOCK_REALTIME, timeout_tripwire);
    const auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(threshold);
    const auto nanoseconds =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            threshold % std::chrono::seconds(1));
    timeout_tripwire->tv_sec += seconds.count();
    timeout_tripwire->tv_nsec += nanoseconds.count();
    if (timeout_tripwire->tv_nsec >= TIME_sec_to_ns) {
      timeout_tripwire->tv_nsec -= TIME_sec_to_ns;
      ++timeout_tripwire->tv_sec;
    }
  }

  LockStat() = delete;

  LockStat(LockStatTraits::LockStatType lockType, const LockStatTraits* traits)
  {
    ceph_assert(!traits || traits->get_lockstat_entry() != nullptr);
    m_lockstat_traits = traits;
    if (m_lockstat_traits) {
      m_lockstat_traits->set_lock_type(lockType);
      LockStatEntry* lockstat_entry = m_lockstat_traits->get_lockstat_entry();
      lockstat_entry->m_num_instances.fetch_add(1, std::memory_order_relaxed);
    }
  }

  ~LockStat() noexcept
  {
    if (m_lockstat_traits) {
      LockStatEntry* lockstat_entry = m_lockstat_traits->get_lockstat_entry();
      lockstat_entry->m_num_instances.fetch_sub(1, std::memory_order_relaxed);
    }
  }

  ///
  /// @brief Record the lock's wait time for its trait's
  ///
  void
  record_wait_time(lockstat_clock::duration wait_time, LockMode mode) const
  {
    // record stats if wait threshold exceeded and if record_iopath_locks is set
    // only record stats for threads with thread_iopath_flag set
    if (unlikely(
            wait_time >= g_threshold_cycles.load(std::memory_order_relaxed)) &&
        unlikely(
            !m_record_iopath_locks.load(std::memory_order_relaxed) ||
            m_thread_iopath_flag)) {
      LockStatTraits::record_wait_time(m_lockstat_traits, wait_time, mode);
    }
  }

  ///
  /// @brief True if lockstat is capturing stats
  ///
  static bool
  is_lockstat_enabled()
  {
    return g_start_cycles.load(std::memory_order_relaxed) !=
           lockstat_clock::time_point{};
  }

  ///
  /// @brief Set the per-thread flag to mark iopath threads
  ///
  static void set_thread_iopath(bool flag);

  ///
  /// @brief set the lock tripwire in microseconds
  ///
  static lockstat_clock::duration get_tripwire_threshold();

  ///
  /// @brief set the lock tripwire in microseconds
  ///
  static void set_tripwire_threshold(const lockstat_clock::duration& timeout);

  ///
  /// @brief enable tripwire on a lock
  ///
  static void enable_lock_tripwire(uint32_t lockid, bool enabled);

  ///
  /// @brief Get lock tripwire flag
  ///
  static bool get_lock_tripwire(uint32_t lockid);

  ///
  /// @brief Return tripwire flag for iopath threads
  ///
  static bool get_tripwire();

  ///
  /// @brief enable/disable tripwire checks for iopath threads
  ///
  static void set_tripwire(bool flag);

  ///
  /// @brief Return iopath record flag
  ///
  static bool get_iopath_record();

  ///
  /// @brief enable/disable recording wait time only for threads with
  ///        tripwire flag set
  ///
  static void set_iopath_record(bool flag);

  ///
  /// @brief Return the lock's traits
  ///
  [[nodiscard]] const LockStatTraits*
  get_traits() const
  {
    return m_lockstat_traits;
  }

  ///
  /// @brief Stop collecting lockstats
  ///
  static void lockstat_stop();

protected: // Methods
  ///
  /// @brief Helper function to test if tripwire is enabled for this lock
  ///        running on this thread
  ///
  [[nodiscard]] bool
  is_tripwire_enabled() const
  {
    return m_enable_tripwire.load(std::memory_order_relaxed) &&
           m_thread_iopath_flag &&
           get_traits()->get_lockstat_entry()->m_tripwire_enabled;
  }

protected: // Data
  ///
  /// @brief The traits for this lock
  ///
  const LockStatTraits* m_lockstat_traits;

  ///
  /// @brief non-0 when recording lockstats
  ///
  static std::atomic<lockstat_clock::time_point> g_start_cycles;

  ///
  /// @brief record wait time >= this many cycles
  ///
  static std::atomic<lockstat_clock::duration> g_threshold_cycles;
  static std::atomic<lockstat_clock::duration> g_threshold_cycles_when;

  ///
  /// @brief Check if tripwire exceeded
  ///
  static std::atomic<bool> m_record_iopath_locks;

  ///
  /// @brief Check if tripwire exceeded
  ///
  static std::atomic<bool> m_enable_tripwire;

  ///
  /// @brief tripwire threshold for computing timespec seconds
  ///
  static std::atomic<lockstat_clock::duration> m_tripwire_threshold;

  ///
  /// @brief per-thread flag for checking if tripwire should be enforced
  ///
  static thread_local bool m_thread_iopath_flag;
};

// inline implementations
inline LockStatTraits*
LockStatTraits::get_lockstat_traits(
    const std::string& name,
    const uint64_t hash_index,
    const uint64_t lock_index,
    const char* file_name,
    const int line_number,
    const char* function_name)
{
  if (g_global_enable) {
    LockStatTraitsTableT& traits_table = get_traits_table();
    if (traits_table[lock_index].m_lockstat_hash == hash_index) {
      ceph_assert(traits_table[lock_index].m_lockstat_entry != nullptr);
      ceph_assert(traits_table[lock_index].get_name().find(name) == 0);
      ceph_assert(
          traits_table[lock_index].m_lockstat_entry->get_lock_id() ==
          lock_index);
      return &traits_table[lock_index];
    } else {
      return _get_lockstat_traits_impl(
          name, hash_index, lock_index, file_name, line_number, function_name);
    }
  } else {
    return nullptr;
  }
}

inline uint64_t
LockStatTraits::get_lock_id() const
{
  return get_lockstat_entry()->get_lock_id();
}

inline const std::string&
LockStatTraits::get_name() const
{
  return get_lockstat_entry()->m_lock_name;
}

inline const std::string
LockStatTraits::get_unique_name(uintptr_t value) const
{
  return get_lockstat_entry()->m_lock_name + " (" + std::to_string(value) + ")";
}

inline LockStatTraits::LockStatType
LockStatTraits::get_lock_type() const
{
  return get_lockstat_entry()->m_lock_type;
}

}; // namespace lockstat_detail

// Look up a LockStatTraits object using details hashed from the lock's
// file name, line number, function and name. Compute both the hash value and
// table index using constexpr evaluations
#define LOCKSTAT(name)                                                      \
  (ceph::lockstat_detail::LockStatTraits::get_lockstat_traits(              \
      (name),                                                               \
      ceph::lockstat_detail::hash(                                          \
          name, __builtin_FILE(), __builtin_LINE(), __builtin_FUNCTION()),  \
      ceph::lockstat_detail::hash(                                          \
          name, __builtin_FILE(), __builtin_LINE(), __builtin_FUNCTION()) % \
          ceph::lockstat_detail::LockStatTraits::kHash_table_max_size,      \
      __builtin_FILE(), __builtin_LINE(), __builtin_FUNCTION()))

} // namespace ceph
#else
#define LOCKSTAT(name) (name)
#endif // CEPH_LOCKSTAT
#endif
