// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_string.h"

static bool char_eq(char c1, char c2)
{
  return c1 == c2;
}

static bool ci_char_eq(char c1, char c2)
{
  return tolower(c1) == tolower(c2);
}

static bool match_wildcards(const char *, const char *, uint32_t);
static bool match_asterisk(const char *pattern, const char *input, uint32_t flags)
{
  while ('\0' != *input) {
    if (match_wildcards(pattern, input, flags))
      return true;
    input++;
  }

  return '\0' == *pattern;
}

static bool match_wildcards(const char *pattern, const char *input, uint32_t flags)
{
  const auto eq = (flags & MATCH_CASE_INSENSITIVE) ? &ci_char_eq : &char_eq;

  while (true) {
    if ('\0' == *pattern)
      return '\0' == *input;

    if ('*' == *pattern)
      return match_asterisk(pattern+1, input, flags);

    if ('\0' == *input)
      return false;

    if ('?' == *pattern || eq(*pattern, *input)) {
      pattern++;
      input++;
      continue;
    }

    return false;
  }
}

bool match_wildcards(boost::string_view pattern, boost::string_view input, uint32_t flags)
{
  const char *p_input = input.data();
  const char *p_pattern = pattern.data();
  return match_wildcards(p_pattern, p_input, flags);
}
