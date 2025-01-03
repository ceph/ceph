// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_string.h"
#include <fnmatch.h>

bool match_wildcards(const std::string& pattern, const std::string& input,
                     uint32_t flags)
{
  bool case_insensive = flags & MATCH_CASE_INSENSITIVE;
  uint32_t  flag = 0;

  if (case_insensive) {
    flag = FNM_CASEFOLD;
  }

  if (fnmatch(pattern.data(), input.data(), flag) == 0) {
    return true;
  } else {
    return false;
  }
}
