#pragma once

#include <string>
#include <utility>

namespace ceph::perf_counters {

/// key/value pair representing a perf counter label
using label_pair = std::pair<std::string_view, std::string_view>;


/// construct a cache key for a perf counter and set of labels. returns a string
/// of the form "counter_name\0key1\0val1\0key2\0val2\0", where label pairs
/// are sorted by key with duplicates removed
template <std::size_t Count>
std::string cache_key(std::string_view counter_name,
                      label_pair (&&labels)[Count]);

/// \overload
std::string cache_key(std::string_view counter_name);

/// insert additional labels to an existing label set. this returns a new
/// string without modifying the existing one. the returned string has labels
/// in sorted order and no duplicate keys
template <std::size_t Count>
std::string cache_key_insert(std::string_view key,
                             label_pair (&&labels)[Count]);


namespace detail {

std::string create(std::string_view counter_name,
                   label_pair* begin, label_pair* end);

std::string insert(const char* begin1, const char* end1,
                   label_pair* begin2, label_pair* end2);

} // namespace detail

template <std::size_t Count>
std::string cache_key(std::string_view counter_name,
                      label_pair (&&labels)[Count])
{
  return detail::create(counter_name, std::begin(labels), std::end(labels));
}

std::string cache_key(std::string_view counter_name)
{
  label_pair* end = nullptr;
  return detail::create(counter_name, end, end);
}

template <std::size_t Count>
std::string cache_key_insert(std::string_view key,
                             label_pair (&&labels)[Count])
{
  return detail::insert(key.begin(), key.end(),
                        std::begin(labels), std::end(labels));
}

} // namespace ceph::perf_counters
