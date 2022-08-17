#pragma once

#include <algorithm>
#include <iterator>
#include <numeric>
#include <string_view>
#include <utility>

namespace ceph::perf_counters {

/// key/value pair representing a perf counter label
using label_pair = std::pair<std::string_view, std::string_view>;


/// construct a cache key for a perf counter and set of labels. returns a string
/// of the form "counter_name\0key1\0value1\0key2\0value2\0", where label pairs
/// are insertion-sorted by key then value
template <std::size_t Count>
constexpr std::string make_cache_key(std::string_view counter_name,
                                     label_pair (&&sorted)[Count])
{
  std::sort(std::begin(sorted), std::end(sorted));

  // use a null character to delimit strings
  constexpr char delimiter = '\0';

  // calculate the total size and preallocate the buffer
  auto size = std::accumulate(std::begin(sorted), std::end(sorted),
                              counter_name.size() + sizeof(delimiter),
                              [] (std::size_t sum, const label_pair& l) {
                                return sum + l.first.size() + sizeof(delimiter)
                                    + l.second.size() + sizeof(delimiter);
                              });

  std::string result;
  result.resize(size);

  auto pos = result.begin();
  pos = std::copy(counter_name.begin(), counter_name.end(), pos);
  *(pos++) = delimiter;

  for (const auto& label : sorted) {
    pos = std::copy(label.first.begin(), label.first.end(), pos);
    *(pos++) = delimiter;
    pos = std::copy(label.second.begin(), label.second.end(), pos);
    *(pos++) = delimiter;
  }

  return result;
}

constexpr std::string make_cache_key(std::string_view counter_name)
{
  constexpr char delimiter = '\0';
  const auto size = counter_name.size() + sizeof(delimiter);
  std::string result;
  result.resize(size);
  auto pos = result.begin();
  pos = std::copy(counter_name.begin(), counter_name.end(), pos);
  *(pos++) = delimiter;
  return result;
}

// TODO: cache_key_insert() to add more labels to an existing string

} // namespace ceph::perf_counters
