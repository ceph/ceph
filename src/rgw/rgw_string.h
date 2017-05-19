// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_STRING_H
#define CEPH_RGW_STRING_H

#include <stdlib.h>
#include <limits.h>

#include <boost/container/small_vector.hpp>
#include <boost/utility/string_view.hpp>
struct ltstr_nocase
{
  bool operator()(const string& s1, const string& s2) const
  {
    return strcasecmp(s1.c_str(), s2.c_str()) < 0;
  }
};

static inline int stringcasecmp(const string& s1, const string& s2)
{
  return strcasecmp(s1.c_str(), s2.c_str());
}

static inline int stringcasecmp(const string& s1, const char *s2)
{
  return strcasecmp(s1.c_str(), s2);
}

static inline int stringcasecmp(const string& s1, int ofs, int size, const string& s2)
{
  return strncasecmp(s1.c_str() + ofs, s2.c_str(), size);
}

static inline int stringtoll(const string& s, int64_t *val)
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

static inline int stringtoull(const string& s, uint64_t *val)
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

static inline int stringtol(const string& s, int32_t *val)
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

static inline int stringtoul(const string& s, uint32_t *val)
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

/* We need this helper function because the interface of std::string::reserve
 * doesn't provide the chaining ability in the type append(). It's required
 * to concatenate string without reallocations in a way const-correct manner. */
template <class StringT>
static inline StringT create_n_reserve(const size_t reserve_len)
{
  StringT ret;
  /* I would love to see reserve() returning "basic_string&" instead of "void"
   * in the standard library! */
  ret.reserve(reserve_len);
  return ret;
}

#endif
