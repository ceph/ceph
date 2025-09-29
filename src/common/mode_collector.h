// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file  A (small) container for fast mode lookups
 *   ('mode' here is the statistical mode of a set of values, i.e. the
 *   value that appears most frequently in the set).
 */

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory_resource>
#include <ranges>
#include <unordered_map>

/**
 * ModeCollector is designed to collect a set of values (e.g. - the data digest
 * reported by each replica), associating each value with an object ID (in our
 * example - the replica ID), and efficiently finding the mode (the value that
 * appears most frequently) of the collected values.
 *
 * The template parameters are:
 * - OBJ_ID: The type of the object ID (e.g., replica ID).
 * - K: The type of the value being collected.
 * - HSH: The hash function for K, to be used with the unordered_map.
 *   Note: if HSH is std::identity, then K must fit in size_t.
 * - MAX_ELEM is used to calculate the estimated memory footprint of the
 *   unordered_map.
 *
 * ModeCollector uses a monotonic buffer resource to manage memory
 * efficiently, avoiding frequent allocations and deallocations.
 * My tests (see link for details and caveats) show that using the PMR
 * allocator speeds up the mode-finding process by 20% to 40%.
 */

struct ModeFinder {

  /// a 'non-templated' version of mode_status_t, to simplify usage.
  enum class mode_status_t {
    no_mode_value,  ///< No clear victory for any value
    mode_value,	 ///< we have a winner, but it appears in less than half
                 ///< of the samples
    authorative_value  ///< more than half of the samples are of the same value
  };
};

// note the use of std::identity: it's a pretty fast hash function,
// but we are restricted to size_t sized keys (per stdlib implementation
// of the unrdered map).

template <
    typename OBJ_ID, ///< how to identify the object that reported a value
    typename K, ///< the type of the value being collected
    typename HSH = std::identity, ///< the hash function for K
    int MAX_ELEM = 12>
  requires(
      std::invocable<HSH, K> &&
      sizeof(std::invoke_result_t<HSH, K>) <= sizeof(size_t))
class ModeCollector : public ModeFinder {
 private:
  struct node_type_t {
    size_t m_count{0};
    OBJ_ID m_id;  ///< Stores the object ID associated with this value
  };

  // estimated (upper limit) memory footprint of the unordered_map
  // vvvvvvvvvvvvvvvvvvvvvvvvvvvv
  // Bucket array: typically 2x num_elements for good load factor
  static const size_t bucket_array_size = (MAX_ELEM * 2) * sizeof(void*);
  // Node storage: each elem needs hash + next-ptr
  static constexpr size_t node_overhead = sizeof(void*) + sizeof(size_t);
  static constexpr size_t node_storage =
      MAX_ELEM * (sizeof(K) + sizeof(node_type_t) + node_overhead);
  // PMR allocator overhead (alignment, bookkeeping)
  static constexpr size_t pmr_overhead_per_alloc = 16;	// typical
  // bucket array + nodes
  static constexpr size_t total_overhead = pmr_overhead_per_alloc * 2;
  static constexpr size_t m_estimated_memory_footprint =
      bucket_array_size + node_storage + total_overhead;
  // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^

  std::array<std::byte, m_estimated_memory_footprint> m_buffer;
  std::pmr::monotonic_buffer_resource m_mbr{m_buffer.data(), m_buffer.size()};

  /// Map to store the occurrence count of each value
  std::pmr::unordered_map<
      K,
      node_type_t,
      HSH,
      std::equal_to<K> >
      m_frequency_map;

  /// Actual count of elements added
  size_t m_actual_count{0};

 public:
  using mode_status_t = ModeFinder::mode_status_t;

  struct results_t {
    /// do we have a mode value?
    mode_status_t tag;
    /// the mode value (if any)
    K key;
    /// an object ID, "arbitrary" selected from the set of objects that
    /// reported the mode value
    OBJ_ID id;
    /// the number of times the mode value was reported
    size_t count;
    auto operator<=>(const results_t& rhs) const = default;
  };

  explicit ModeCollector() : m_frequency_map(&m_mbr)
  {
    m_frequency_map.reserve(MAX_ELEM);
  }

  /// Add a value to the collector
  void insert(const OBJ_ID& obj, const K& value) noexcept
  {
    auto& node = m_frequency_map[value];
    node.m_count++;
    // Store the object ID associated with this value
    // (note: it's OK to overwrite the ID here)
    node.m_id = obj;
    m_actual_count++;
  }


  /**
   * Find the mode of the collected values
   *
   * Note: we are losing ~4% performance due to find_mode() not being noexcept.
   */
  results_t find_mode()
  {
    assert(!m_frequency_map.empty());

    auto max_elem = std::ranges::max_element(
	m_frequency_map, {},
	[](const auto& pair) { return pair.second.m_count; });

    // Check for clear victory
    if (max_elem->second.m_count > m_actual_count / 2) {
      return {
	  mode_status_t::authorative_value, max_elem->first,
	  max_elem->second.m_id, max_elem->second.m_count};
    }

    // Check for possible ties
    const auto max_elem_cnt = max_elem->second.m_count;

    max_elem->second.m_count = 0;  // Reset the count of the max element
    const auto second_best_elem = std::ranges::max_element(
	m_frequency_map, {},
	[](const auto& pair) { return pair.second.m_count; });
    max_elem->second.m_count = max_elem_cnt;  // Restore the count

    if (second_best_elem->second.m_count == max_elem_cnt) {
      return {
	  mode_status_t::no_mode_value, max_elem->first, max_elem->second.m_id,
	  max_elem_cnt};
    }

    return {
	mode_status_t::mode_value, max_elem->first, max_elem->second.m_id,
	max_elem_cnt};
  }
};

