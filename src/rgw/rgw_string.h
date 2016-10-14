// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_STRING_H
#define CEPH_RGW_STRING_H

#include <stdlib.h>
#include <limits.h>

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

#endif
