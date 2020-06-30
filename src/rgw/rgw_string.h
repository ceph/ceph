// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_STRING_H
#define CEPH_RGW_STRING_H

#include <errno.h>
#include <stdlib.h>
#include <limits.h>
#include <string_view>

#include <boost/container/small_vector.hpp>

struct ltstr_nocase
{
  bool operator()(const std::string& s1, const std::string& s2) const
  {
    return strcasecmp(s1.c_str(), s2.c_str()) < 0;
  }
};

static inline int stringcasecmp(const std::string& s1, const std::string& s2)
{
  return strcasecmp(s1.c_str(), s2.c_str());
}

static inline int stringcasecmp(const std::string& s1, const char *s2)
{
  return strcasecmp(s1.c_str(), s2);
}

static inline int stringcasecmp(const std::string& s1, int ofs, int size, const std::string& s2)
{
  return strncasecmp(s1.c_str() + ofs, s2.c_str(), size);
}

static inline int stringtoll(const std::string& s, int64_t *val)
{
  char *end;

  long long result = strtoll(s.c_str(), &end, 10);
  if (result == LLONG_MAX)
    return -EINVAL;

  if (*end)
    return -EINVAL;

  *val = (int64_t)result;

  return 0;
}

static inline int stringtoull(const std::string& s, uint64_t *val)
{
  char *end;

  unsigned long long result = strtoull(s.c_str(), &end, 10);
  if (result == ULLONG_MAX)
    return -EINVAL;

  if (*end)
    return -EINVAL;

  *val = (uint64_t)result;

  return 0;
}

static inline int stringtol(const std::string& s, int32_t *val)
{
  char *end;

  long result = strtol(s.c_str(), &end, 10);
  if (result == LONG_MAX)
    return -EINVAL;

  if (*end)
    return -EINVAL;

  *val = (int32_t)result;

  return 0;
}

static inline int stringtoul(const std::string& s, uint32_t *val)
{
  char *end;

  unsigned long result = strtoul(s.c_str(), &end, 10);
  if (result == ULONG_MAX)
    return -EINVAL;

  if (*end)
    return -EINVAL;

  *val = (uint32_t)result;

  return 0;
}

/* A converter between std::string_view and null-terminated C-strings.
 * It copies memory while trying to utilize the local memory instead of
 * issuing dynamic allocations. */
template<std::size_t N = 128>
static inline boost::container::small_vector<char, N>
sview2cstr(const std::string_view& sv)
{
  boost::container::small_vector<char, N> cstr;
  cstr.reserve(sv.size() + sizeof('\0'));

  cstr.assign(std::begin(sv), std::end(sv));
  cstr.push_back('\0');

  return cstr;
}

/* std::strlen() isn't guaranteed to be computable at compile-time. Although
 * newer GCCs actually do that, Clang doesn't. Please be aware this function
 * IS NOT A DROP-IN REPLACEMENT FOR STRLEN -- it returns a different result
 * for strings having \0 in the middle. */
template<size_t N>
static inline constexpr size_t sarrlen(const char (&arr)[N]) {
  return N - 1;
}

namespace detail {

// variadic sum() to add up string lengths for reserve()
static inline constexpr size_t sum() { return 0; }
template <typename... Args>
constexpr size_t sum(size_t v, Args... args) { return v + sum(args...); }

// traits for string_size()
template <typename T>
struct string_traits {
  static constexpr size_t size(const T& s) { return s.size(); }
};
// specializations for char*/const char* use strlen()
template <>
struct string_traits<const char*> {
  static size_t size(const char* s) { return std::strlen(s); }
};
template <>
struct string_traits<char*> : string_traits<const char*> {};
// constexpr specializations for char[]/const char[]
template <std::size_t N>
struct string_traits<const char[N]> {
  static constexpr size_t size_(const char* s, size_t i) {
    return i < N ? (*(s + i) == '\0' ? i : size_(s, i + 1))
        : throw std::invalid_argument("Unterminated string constant.");
  }
  static constexpr size_t size(const char(&s)[N]) { return size_(s, 0); }
};
template <std::size_t N>
struct string_traits<char[N]> : string_traits<const char[N]> {};

// helpers for string_cat_reserve()
static inline void append_to(std::string& s) {}
template <typename... Args>
void append_to(std::string& s, const std::string_view& v, const Args&... args)
{
  s.append(v.begin(), v.end());
  append_to(s, args...);
}

// helpers for string_join_reserve()
static inline void join_next(std::string& s, const std::string_view& d) {}
template <typename... Args>
void join_next(std::string& s, const std::string_view& d,
               const std::string_view& v, const Args&... args)
{
  s.append(d.begin(), d.end());
  s.append(v.begin(), v.end());
  join_next(s, d, args...);
}

static inline void join(std::string& s, const std::string_view& d) {}
template <typename... Args>
void join(std::string& s, const std::string_view& d,
          const std::string_view& v, const Args&... args)
{
  s.append(v.begin(), v.end());
  join_next(s, d, args...);
}

} // namespace detail

/// return the length of a c string, string literal, or string type
template <typename T>
constexpr size_t string_size(const T& s)
{
  return detail::string_traits<T>::size(s);
}

/// concatenates the given string arguments, returning as a std::string that
/// gets preallocated with reserve()
template <typename... Args>
std::string string_cat_reserve(const Args&... args)
{
  size_t total_size = detail::sum(string_size(args)...);
  std::string result;
  result.reserve(total_size);
  detail::append_to(result, args...);
  return result;
}

/// joins the given string arguments with a delimiter, returning as a
/// std::string that gets preallocated with reserve()
template <typename... Args>
std::string string_join_reserve(const std::string_view& delim,
                                const Args&... args)
{
  size_t delim_size = delim.size() * std::max<ssize_t>(0, sizeof...(args) - 1);
  size_t total_size = detail::sum(string_size(args)...) + delim_size;
  std::string result;
  result.reserve(total_size);
  detail::join(result, delim, args...);
  return result;
}
template <typename... Args>
std::string string_join_reserve(char delim, const Args&... args)
{
  return string_join_reserve(std::string_view{&delim, 1}, args...);
}


/// use case-insensitive comparison in match_wildcards()
static constexpr uint32_t MATCH_CASE_INSENSITIVE = 0x01;

/// attempt to match the given input string with the pattern, which may contain
/// the wildcard characters * and ?
extern bool match_wildcards(std::string_view pattern,
                            std::string_view input,
                            uint32_t flags = 0);

#endif
