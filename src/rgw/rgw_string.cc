// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_string.h"

static bool char_eq(char c1, char c2)
{
  return c1 == c2;
}

static bool ci_char_eq(char c1, char c2)
{
  return tolower(c1) == tolower(c2);
}

bool match_wildcards(std::string_view pattern, std::string_view input,
                     uint32_t flags)
{
  const auto eq = (flags & MATCH_CASE_INSENSITIVE) ? &ci_char_eq : &char_eq;

  auto it1 = pattern.begin();
  auto it2 = input.begin();
  while (true) {
    if (it1 == pattern.end())
      return it2 == input.end();
    if (*it1 == '*') {
      if (it1 + 1 == pattern.end())
        return true;
      if (it2 == input.end() || eq(*(it1 + 1), *it2))
        ++it1;
      else
        ++it2;
      continue;
    }
    if (it2 == input.end())
      return false;
    if (*it1 == '?' || eq(*it1, *it2)) {
      ++it1;
      ++it2;
      continue;
    }
    return false;
  }
  return false;
}
