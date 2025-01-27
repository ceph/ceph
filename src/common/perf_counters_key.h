#pragma once

#include <optional>
#include <string>
#include <utility>

namespace ceph::perf_counters {

/// A key/value pair representing a perf counter label
using label_pair = std::pair<std::string_view, std::string_view>;


/// \brief Construct a key for a perf counter and set of labels.
///
/// Returns a string of the form "counter_name\0key1\0val1\0key2\0val2\0",
/// where label pairs are sorted by key with duplicates removed.
///
/// This string representation avoids extra memory allocations associated
/// with map<string, string>. It also supports the hashing and comparison
/// operators required for use as a key in unordered and ordered containers.
///
/// Example:
/// \code
///   std::string key = key_create("counter_name", {
///         {"key1", "val1"}, {"key2", "val2"}
///       });
/// \endcode
template <std::size_t Count>
std::string key_create(std::string_view counter_name,
                       label_pair (&&labels)[Count]);

/// \brief Construct a key for a perf counter without labels.
/// \overload
std::string key_create(std::string_view counter_name);

/// \brief Insert additional labels into an existing key.
///
/// This returns a new string without modifying the input. The returned
/// string has labels in sorted order and no duplicate keys.
template <std::size_t Count>
std::string key_insert(std::string_view key,
                       label_pair (&&labels)[Count]);

/// \brief Return the counter name for a given key.
std::string_view key_name(std::string_view key);


/// A forward iterator over label_pairs encoded in a key
class label_iterator {
 public:
  using base_iterator = const char*;
  using difference_type = std::ptrdiff_t;
  using value_type = label_pair;
  using pointer = const value_type*;
  using reference = const value_type&;

  label_iterator() = default;
  label_iterator(base_iterator begin, base_iterator end);

  label_iterator& operator++();
  label_iterator operator++(int);

  reference operator*() const { return state->label; }
  pointer operator->() const { return &state->label; }

  auto operator<=>(const label_iterator& rhs) const = default;

 private:
  struct iterator_state {
    base_iterator pos; // end of current label
    base_iterator end; // end of buffer
    label_pair label; // current label

    auto operator<=>(const iterator_state& rhs) const = default;
  };
  // an empty state represents a past-the-end iterator
  std::optional<iterator_state> state;

  // find the next two delimiters and construct the label string views
  static void advance(std::optional<iterator_state>& s);

  // try to parse the first label pair
  static auto make_state(base_iterator begin, base_iterator end)
      -> std::optional<iterator_state>;
};

/// A sorted range of label_pairs
class label_range {
  std::string_view buffer;
 public:
  using iterator = label_iterator;
  using const_iterator = label_iterator;

  label_range(std::string_view buffer) : buffer(buffer) {}

  const_iterator begin() const { return {buffer.begin(), buffer.end()}; }
  const_iterator cbegin() const { return {buffer.begin(), buffer.end()}; }

  const_iterator end() const { return {}; }
  const_iterator cend() const { return {}; }
};

/// \brief Return the sorted range of label_pairs for a given key.
///
/// Example:
/// \code
///   for (label_pair label : key_labels(key)) {
///     std::cout << label.first << ":" << label.second << std::endl;
///   }
/// \endcode
label_range key_labels(std::string_view key);


namespace detail {

std::string create(std::string_view counter_name,
                   label_pair* begin, label_pair* end);

std::string insert(const char* begin1, const char* end1,
                   label_pair* begin2, label_pair* end2);

} // namespace detail

template <std::size_t Count>
std::string key_create(std::string_view counter_name,
                       label_pair (&&labels)[Count])
{
  return detail::create(counter_name, std::begin(labels), std::end(labels));
}

template <std::size_t Count>
std::string key_insert(std::string_view key,
                       label_pair (&&labels)[Count])
{
  return detail::insert(key.begin(), key.end(),
                        std::begin(labels), std::end(labels));
}

} // namespace ceph::perf_counters
