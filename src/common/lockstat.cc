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

#include "common/lockstat.h"

#include <algorithm>
#include <ranges>
#include <mutex>
#include <numeric>

#include "include/ceph_assert.h"

#ifdef CEPH_LOCKSTAT

namespace ceph::lockstat_detail {
#ifdef NDEBUG
const bool LockStatTraits::g_global_enable =
    getenv("ENABLE_LOCKSTAT") != nullptr &&
    strcmp(getenv("ENABLE_LOCKSTAT"), "true") == 0;
#else
const bool LockStatTraits::g_global_enable = true;
#endif


// lock_stats definitions
std::atomic<size_t> LockStatTraits::g_lock_collisions;

LockStatTraits::LockStatTraits() :
  m_lockstat_hash(0), m_lockstat_entry(nullptr)
{}

LockStatTraits::~LockStatTraits() = default;

void
LockStatTraits::_set_locktype_impl(const LockStatType lockType) const
{
  if (m_lockstat_entry && m_lockstat_entry->get_lock_id() != 0) {
    m_lockstat_entry->m_lock_type = lockType;
  }
}

LockStatTraits*
LockStatTraits::_get_lockstat_traits_impl(
    const std::string& name,
    const uint64_t hash_index,
    const uint64_t lock_index,
    const char* file_name,
    const int line,
    const char* func)
{
  LockStatTraitsTableT& traits_table = get_traits_table();
  auto iter = std::next(traits_table.begin(), lock_index);
  for (uint64_t i = 0; i < kHash_table_max_size; i++) {
    LockStatTraits* traits = iter;

    if (traits->m_lockstat_hash == hash_index) {
      ceph_assert(traits->m_lockstat_entry != nullptr);
      ceph_assert(traits->get_name().find(name) == 0);
      return traits;
    }

    // If this slot is unused, return it
    if (traits->m_lockstat_hash == 0) {
      // Try to lock the traits object before updating its fields
      std::unique_lock _(traits->m_lockstat_mutex);

      // Perhaps another thread has already acquired this
      // trait for us
      if (traits->m_lockstat_hash == hash_index) {
        ceph_assert(traits->m_lockstat_entry != nullptr);
        ceph_assert(traits->get_name().find(name) == 0);
        return traits;
      }

      // Perhaps another thread acquired the traits object for
      // a different hash, in which case move on to the next
      if (traits->m_lockstat_hash != 0) {
        continue;
      }

      // Now this entry can be updated
      size_t entry_index = (traits - traits_table.begin());
      ceph_assert(entry_index < kHash_table_max_size);
      LockStatEntry* lockstat_entry =
          &LockStatEntry::get_lockstat_table()[entry_index];

      lockstat_entry->m_lock_name =
          get_lockstat_name(name, file_name, line, func);
      ceph_assert(lockstat_entry->m_lock_id == entry_index);
      lockstat_entry->m_lock_type = LockStatType::UNKNOWN;
      lockstat_entry->reset();

      traits->m_lockstat_entry = lockstat_entry;
      traits->m_lockstat_hash = hash_index;
      ceph_assert(
          LockStatEntry::get_lockstat_table()[entry_index].m_lock_name.find(
              name) == 0);

      return traits;
    }

    // Move on to the next entry, loop around to
    // the beginning of the table once hit the end
    iter++;
    if (iter == traits_table.end()) {
      iter = traits_table.begin();
    }

    ++g_lock_collisions;
  }
  ceph_assertf(
      false,
      "No empty lock_stat_traits found. Increase "
      "LockStatTraits::kHash_table_max_size");
  return nullptr;
}

std::atomic<lockstat_clock::time_point> LockStat::g_start_cycles =
    lockstat_clock::zero();
std::atomic<lockstat_clock::duration> LockStat::g_threshold_cycles =
    lockstat_clock::duration{};
std::atomic<lockstat_clock::duration> LockStat::g_threshold_cycles_when =
    lockstat_clock::duration{};
std::atomic<bool> LockStat::m_record_iopath_locks{false};
std::atomic<bool> LockStat::m_enable_tripwire{false};
std::atomic<lockstat_clock::duration> LockStat::m_tripwire_threshold =
    lockstat_clock::duration{};
thread_local bool LockStat::m_thread_iopath_flag;

bool
LockStat::get_tripwire()
{
  return m_enable_tripwire;
}

void
LockStat::set_tripwire(bool flag)
{
  m_enable_tripwire = flag;
}

bool
LockStat::get_iopath_record()
{
  return m_record_iopath_locks;
}

void
LockStat::set_iopath_record(bool flag)
{
  m_record_iopath_locks = flag;
}

void
LockStat::set_thread_iopath(bool flag)
{
  m_thread_iopath_flag = flag;
}

lockstat_clock::duration
LockStat::get_tripwire_threshold()
{
  return m_tripwire_threshold;
}

void
LockStat::set_tripwire_threshold(const lockstat_clock::duration& timeout)
{
  m_tripwire_threshold = timeout;
}

bool
LockStat::get_lock_tripwire(uint32_t lockid)
{
  LockStatEntry& entry = LockStatEntry::get_lockstat_entry(lockid);
  return entry.m_tripwire_enabled;
}

void
LockStat::enable_lock_tripwire(uint32_t lockid, bool enabled)
{
  LockStatEntry& entry = LockStatEntry::get_lockstat_entry(lockid);
  entry.m_tripwire_enabled = enabled;
}

void
LockStat::lockstat_stop()
{
  m_enable_tripwire = false;
  g_threshold_cycles = lockstat_clock::duration{};
  g_start_cycles = lockstat_clock::zero();
  m_tripwire_threshold = lockstat_clock::duration{};
}

template <typename T>
void
atomic_max(std::atomic<T>& maximum_value, T const& value) noexcept
{
  T prev_value = maximum_value;
  while (prev_value < value &&
         !maximum_value.compare_exchange_weak(prev_value, value)) {
  }
}

void
LockStatTraits::record_wait_time(
    const LockStatTraits* traits,
    const lockstat_clock::duration wait_time,
    const LockMode mode)
{
  if (!traits || !traits->m_lockstat_entry) {
    return;
  }
  LockStatEntry* lockstat_entry = traits->m_lockstat_entry;

  LockStatEntry::LockStats& stats(lockstat_entry->m_stats_table[sched_getcpu()]);
  stats.m_wait_count[static_cast<size_t>(mode)]++;
  stats.m_wait_duration[static_cast<size_t>(mode)] += wait_time;
  auto wait_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(wait_time).count();

  stats.m_wait_time_histogram[static_cast<size_t>(
      mode)][LockStatEntry::LockStats::LockStatLatBins::get_index(wait_ns)]++;
  atomic_max(lockstat_entry->m_max_wait, wait_time);
}

LockStatEntry::LockStats::LockStats() { reset(); }

void
LockStatEntry::LockStats::reset()
{
  m_wait_duration.fill(lockstat_clock::duration{});
  m_wait_count.fill(0);
  for (auto& wait_time : m_wait_time_histogram) {
    wait_time.fill(0);
  }
}

LockStatEntry::LockStats&
LockStatEntry::LockStats::operator+=(const LockStats& other)
{
  std::transform(
      m_wait_duration.begin(), m_wait_duration.end(),
      other.m_wait_duration.begin(), m_wait_duration.begin(), std::plus<>());
  std::transform(
      m_wait_count.begin(), m_wait_count.end(), other.m_wait_count.begin(),
      m_wait_count.begin(), std::plus<>());
  for (size_t i = 0; i < m_wait_time_histogram.size(); i++) {
    std::transform(
        m_wait_time_histogram[i].begin(), m_wait_time_histogram[i].end(),
        other.m_wait_time_histogram[i].begin(),
        m_wait_time_histogram[i].begin(), std::plus<>());
  }
  return *this;
}

LockStatEntry::LockStatEntry() :
  m_lock_id(0),
  m_num_instances(0),
  m_max_wait{},
  m_lock_type(LockStatTraits::LockStatType::UNKNOWN),
  m_tripwire_enabled(false)

{
  reset();
}

LockStatEntry::LockStatEntry(const LockStatEntry& other) :
  m_stats_table(other.m_stats_table),
  m_lock_id(other.m_lock_id),
  m_num_instances(other.m_num_instances.load()),
  m_lock_name(other.m_lock_name),
  m_lock_type(other.m_lock_type),
  m_tripwire_enabled(other.m_tripwire_enabled)

{}

///
/// @brief Class to defer static initialization till table needed and
///        initialize entries
///
struct LockStatEntryTableSupport {
  LockStatEntryTableSupport()
  {
    for (LockStatEntry& entry : get_table()) {
      entry.m_lock_id = std::distance(get_table().begin(), &entry);
      ceph_assert(entry.m_lock_id < get_table().size());
    }
  }

  static LockStatEntry::LockStatEntryTableT&
  get_table()
  {
    // Ensure the constructor for lockstat_table runs to init entries
    static LockStatEntry::LockStatEntryTableT lockstat_table;
    return lockstat_table;
  }
};

LockStatEntry::LockStatEntryTableT&
LockStatEntry::get_lockstat_table()
{
  static LockStatEntryTableSupport lockstat_table;
  return lockstat_table.get_table();
}

void
LockStatEntry::reset()
{
  long ncpus = sysconf(_SC_NPROCESSORS_ONLN);
  if (ncpus <= 0)
    ncpus = 1;
  m_stats_table.resize(ncpus);
  for (LockStats& stats : m_stats_table) {
    stats.reset();
  }
}

LockStatEntry::LockStats
LockStatEntry::get_stats_sum() const
{
  LockStats stats_sum;

  for (const LockStats& stats : m_stats_table) {
    stats_sum += stats;
  }
  return stats_sum;
}

int
LockStatEntry::start(const lockstat_clock::duration threshold)
{
  int result = 0;
  if (LockStatTraits::g_global_enable) {
    LockStatEntryTableT& lockstat_table = LockStatEntry::get_lockstat_table();
    for (LockStatEntry& entry : lockstat_table) {
      entry.reset();
    }
    LockStat::g_start_cycles = lockstat_clock::now();
    LockStat::g_threshold_cycles = threshold;
  } else {
    result = EINVAL;
  }

  return result;
}

void
LockStatEntry::stop()
{
  LockStat::lockstat_stop();
}

void
LockStatEntry::reset_data()
{
  if (LockStatTraits::g_global_enable) {
    LockStatEntryTableT& lockstat_table = LockStatEntry::get_lockstat_table();
    for (LockStatEntry& entry : lockstat_table) {
      entry.reset();
    }
    LockStat::g_start_cycles = lockstat_clock::now();
  }
}

std::pair<LockStatEntry::LockStatEntryTableT, lockstat_clock::duration>
LockStatEntry::show()
{
  if (LockStatTraits::g_global_enable) {
    auto total_duration = lockstat_clock::now() - LockStat::g_start_cycles.load();
    return {get_lockstat_table(), total_duration};
  } else {
    return {get_lockstat_table(), lockstat_clock::duration{tsc_rep{0}}};
  }
}

void
LockStatEntry::dump_formatted(Formatter* f)
{
  f->open_object_section("lockstat");
  if (LockStat::is_lockstat_enabled()) {
    f->open_array_section("bin_ranges");
    for (uint32_t b = 0; b < LockStatEntry::LockStats::num_bins + 2; b++) {
      f->open_object_section("bin");
      f->dump_unsigned("bin_id", b);
      if (b == 0) {
        f->dump_unsigned(
            "max_val", (1ul << LockStatEntry::LockStats::bin_low_bits) - 1);
      } else if (b == LockStatEntry::LockStats::num_bins + 1) {
        f->dump_unsigned(
            "min_val", 1ul << LockStatEntry::LockStats::bin_hi_bits);
      } else {
        f->dump_unsigned(
            "min_val", 1ul << (LockStatEntry::LockStats::bin_low_bits + b - 1));
        f->dump_unsigned(
            "max_val", (1ul << (LockStatEntry::LockStats::bin_low_bits + b)) - 1);
      }
      f->close_section(); // bin
    }
    f->close_section(); // bin_ranges
    std::pair<LockStatEntryTableT, lockstat_clock::duration> lockstat_data = show();
    auto total_usec = std::chrono::duration_cast<std::chrono::microseconds>(
        lockstat_data.second);
    f->dump_unsigned("total_usec", total_usec.count());

    LockStatEntryTableT& table(lockstat_data.first);
    std::vector<uint64_t> index_table(table.size());
    std::vector<LockStats> lock_stats_accumulator(table.size());
    for (uint32_t i = 0; i < table.size(); i++) {
      index_table[i] = table[i].get_lock_id();
      lock_stats_accumulator[i] = table[i].get_stats_sum();
    }
    // Save lock id->array index mapping, then sort array by wait time
    std::ranges::sort(index_table, [&](const uint64_t& a, const uint64_t& b) {
      return (std::accumulate(
                 lock_stats_accumulator[a].m_wait_duration.begin(),
                 lock_stats_accumulator[a].m_wait_duration.end(),
                 lockstat_clock::zero())) <
             (std::accumulate(
                 lock_stats_accumulator[b].m_wait_duration.begin(),
                 lock_stats_accumulator[b].m_wait_duration.end(),
                 lockstat_clock::zero()));
    });
    f->open_array_section("entries");
    for (uint64_t li_index : index_table) {
      const LockStatEntry& li = table[li_index];
      const LockStats& li_stats = lock_stats_accumulator[li_index];
      if (li.get_lock_id() &&
          (li.m_num_instances.load(std::memory_order_relaxed) ||
           li_stats.m_wait_count[static_cast<size_t>(LockMode::WRITE)] ||
           li_stats.m_wait_count[static_cast<size_t>(LockMode::READ)] ||
           li_stats.m_wait_count[static_cast<size_t>(LockMode::TRY_WRITE)] ||
           li_stats.m_wait_count[static_cast<size_t>(LockMode::TRY_READ)])) {
        f->open_object_section("entry");
        f->dump_unsigned("id", li.get_lock_id());
        f->dump_string("name", li.m_lock_name);
        f->dump_int("type", static_cast<int>(li.m_lock_type));
        f->dump_unsigned(
            "num_instances", li.m_num_instances.load(std::memory_order_relaxed));
        f->dump_unsigned(
            "max_wait_ns", std::chrono::duration_cast<std::chrono::nanoseconds>(
                               li.m_max_wait.load(std::memory_order_relaxed))
                               .count());

        f->open_array_section("stats");
        auto dump_mode_stats = [&](LockMode mode, const char* mode_name) {
          size_t m = static_cast<size_t>(mode);
          f->open_object_section("mode");
          f->dump_string("name", mode_name);
          f->dump_unsigned("wait_count", li_stats.m_wait_count[m]);
          f->dump_unsigned(
              "wait_duration_ns",
              std::chrono::duration_cast<std::chrono::nanoseconds>(
                  li_stats.m_wait_duration[m])
                  .count());

          f->open_array_section("wait_time_histogram");
          for (uint32_t b = 0; b < LockStats::num_bins + 2; b++) {
            f->dump_unsigned("count", li_stats.m_wait_time_histogram[m][b]);
          }
          f->close_section(); // wait_time_histogram
          f->close_section(); // mode
        };

        dump_mode_stats(LockMode::WRITE, "WRITE");
        dump_mode_stats(LockMode::READ, "READ");
        dump_mode_stats(LockMode::TRY_WRITE, "TRY_WRITE");
        dump_mode_stats(LockMode::TRY_READ, "TRY_READ");
        f->close_section(); // stats
        f->close_section(); // entry
      }
    }
    f->close_section(); // entries
    f->dump_format("status", "Profiling data dumped");
  } else {
    f->dump_format("status", "lockstat is not enabled");
  }
  f->close_section();
}
} // namespace ceph::lockstat_detail
#endif // CEPH_LOCKSTAT
