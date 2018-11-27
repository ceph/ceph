// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_STRING_H
#define CEPH_RGW_STRING_H

#include <string>
#include <cerrno>
#include <cstdlib>
#include <climits>
#include <algorithm>
#include <string_view>

#include <boost/container/small_vector.hpp>
#include <boost/utility/string_view.hpp>

#include "auth/Crypto.h"
#include "common/ceph_context.h"

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

/* A converter between boost::string_view and null-terminated C-strings.
 * It copies memory while trying to utilize the local memory instead of
 * issuing dynamic allocations. */
template<std::size_t N = 128>
static inline boost::container::small_vector<char, N>
sview2cstr(const boost::string_view& sv)
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
void append_to(std::string& s, const boost::string_view& v, const Args&... args)
{
  s.append(v.begin(), v.end());
  append_to(s, args...);
}

// helpers for string_join_reserve()
static inline void join_next(std::string& s, const boost::string_view& d) {}
template <typename... Args>
void join_next(std::string& s, const boost::string_view& d,
               const boost::string_view& v, const Args&... args)
{
  s.append(d.begin(), d.end());
  s.append(v.begin(), v.end());
  join_next(s, d, args...);
}

static inline void join(std::string& s, const boost::string_view& d) {}
template <typename... Args>
void join(std::string& s, const boost::string_view& d,
          const boost::string_view& v, const Args&... args)
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
std::string string_join_reserve(const boost::string_view& delim,
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
  return string_join_reserve(boost::string_view{&delim, 1}, args...);
}


/// use case-insensitive comparison in match_wildcards()
static constexpr uint32_t MATCH_CASE_INSENSITIVE = 0x01;

/// attempt to match the given input string with the pattern, which may contain
/// the wildcard characters * and ?
extern bool match_wildcards(boost::string_view pattern,
                            boost::string_view input,
                            uint32_t flags = 0);

inline void rgw_escape_str(const std::string& s, const char esc_char, const char special_char, std::string& dest)
{
  const char *src = s.c_str();

  dest.resize(s.size() * 2);

  char *destp = dest.data();

  for (size_t i = 0; i < s.size(); i++) {
    char c = src[i];
    if (c == esc_char || c == special_char) {
      *destp++ = esc_char;
    }
    *destp++ = c;
  }
}

inline void rgw_escape_str(const std::string& s, const char esc_char, const char special_char, std::string *dest_ptr)
{
 return rgw_escape_str(s, esc_char, special_char, *dest_ptr);
}

inline std::string rgw_escape_str(const std::string& s, const char esc_char, const char special_char)
{
  std::string result;
  rgw_escape_str(s, esc_char, special_char, result);
  return result;
}

inline ssize_t rgw_unescape_str(const std::string& s, const ssize_t ofs,
                       const char esc_char, const char special_char,
                       std::string& dest)
{
  const char *src = s.c_str();

  dest.resize(s.size());
  char *destp = dest.data();

  bool esc = false;

  for (size_t i = ofs; i < s.size(); i++) {
    char c = src[i];
    if (!esc && c == esc_char) {
      esc = true;
      continue;
    }
    if (!esc && c == special_char) {
      return static_cast<ssize_t>(1 + i);
    }
    *destp++ = c;
    esc = false;
  }

  return string::npos;
}

inline ssize_t rgw_unescape_str(const std::string& s, const ssize_t ofs,
                       const char esc_char, const char special_char,
                       std::string *dest_ptr)
{
  return rgw_unescape_str(s, ofs, esc_char, special_char, *dest_ptr);
}

namespace detail {

// Does NOT null-terminate, assumes output target is of sufficient size:
void gen_rand_buffer_mutate_in_place(CryptoRandom& cr, char *out, const size_t nchars, std::string_view tbl);

inline std::string gen_rand_string(CryptoRandom& cr, const size_t nchars, std::string_view tbl)
{
  std::string result;
 
  result.resize(nchars);
 
  gen_rand_buffer_mutate_in_place(cr, result.data(), result.size(), tbl);
 
  return result;
}

// this is basically a modified base64 charset, url friendly
static const char alphanum_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
static const char alphanum_no_underscore_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.";
static const char alphanum_plain_table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
static const char alphanum_upper_table[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
static const char alphanum_lower_table[] = "0123456789abcdefghijklmnopqrstuvwxyz";

} // namespace detail

// nchars is the length of the required string, sans NULL:
inline std::string gen_rand_alphanumeric_upper(CryptoRandom& cr, const size_t nchars)
{
  return detail::gen_rand_string(cr, nchars, detail::alphanum_upper_table);
}

inline std::string gen_rand_alphanumeric_upper(CephContext *cct, const size_t nchars)
{
  return gen_rand_alphanumeric_upper(*cct->random(), nchars);
}

inline void gen_rand_alphanumeric_upper(CephContext *cct, std::string *dest, const size_t nchars) 
{ 
  *dest = gen_rand_alphanumeric_upper(*cct->random(), nchars);
} 

inline std::string gen_rand_alphanumeric_lower(CryptoRandom& cr, const size_t nchars)
{
  return detail::gen_rand_string(cr, nchars, detail::alphanum_lower_table);
}

inline void gen_rand_alphanumeric_lower(CephContext *cct, std::string *dest, const size_t nchars)
{
  *dest = gen_rand_alphanumeric_lower(*cct->random(), nchars);
}

inline std::string gen_rand_alphanumeric(CryptoRandom& cr, const size_t nchars) 
{
  return detail::gen_rand_string(cr, nchars, detail::alphanum_table);
}

inline std::string gen_rand_alphanumeric(CephContext *cct, const size_t size)
{
  return gen_rand_alphanumeric(*cct->random(), size);
}

inline void gen_rand_alphanumeric(CephContext *cct, std::string *dest, const size_t size) 
{
  *dest = gen_rand_alphanumeric(*cct->random(), size);
}

inline void gen_rand_alphanumeric(CryptoRandom& cr, char *out_buffer, const size_t out_buffer_size)
{
  detail::gen_rand_buffer_mutate_in_place(cr, out_buffer, out_buffer_size - 1, 
                                          detail::alphanum_table);
  out_buffer[out_buffer_size] = 0;
}

inline std::string gen_rand_alphanumeric_no_underscore(CryptoRandom& cr, const size_t nchars) 
{
  return detail::gen_rand_string(cr, nchars, 
                                 detail::alphanum_no_underscore_table);
}

inline std::string gen_rand_alphanumeric_no_underscore(CephContext *cct, const size_t nchars) 
{
  return gen_rand_alphanumeric(*cct->random(), nchars);
}

inline void gen_rand_alphanumeric_no_underscore(CephContext *cct, std::string *dest, const size_t nchars) 
{
  *dest = gen_rand_alphanumeric_no_underscore(*cct->random(), nchars);
}

// nchars is the required string size sans NULL:
inline std::string gen_rand_alphanumeric_plain(CryptoRandom& cr, const size_t nchars)
{
  return detail::gen_rand_string(cr, nchars, 
                                 detail::alphanum_plain_table);
}

// nchars is the required string size sans NULL:
inline std::string gen_rand_alphanumeric_plain(CephContext *cct, const size_t nchars)
{
  return gen_rand_alphanumeric_plain(*cct->random(), nchars);
}

// nchars is the required string size sans NULL:
inline void gen_rand_alphanumeric_plain(CephContext *cct, std::string *dest, const size_t nchars)
{
  *dest = gen_rand_alphanumeric_plain(*cct->random(), nchars);
}

static inline void append_rand_alpha(CephContext *cct, const std::string& src, std::string& dest, const size_t len)
{
  dest.append("_");
  dest.append(gen_rand_alphanumeric(*cct->random(), len));
}

#endif
