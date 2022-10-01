// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pretty_binary.h"
#include <stdexcept>
#include <sstream>

std::string pretty_binary_string_reverse(const std::string& pretty)
{
  size_t i = 0;
  auto raise = [&](size_t failpos) {
    std::ostringstream ss;
    ss << "invalid char at pos " << failpos << " of " << pretty;
    throw std::invalid_argument(ss.str());
  };
  auto hexdigit = [&](unsigned char c) -> int32_t {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
  };
  auto require = [&](unsigned char c) {
    if (i >= pretty.length() || pretty[i] != c) {
      raise(i);
    }
    ++i;
  };
  std::string bin;
  if (pretty.empty())
    return bin;
  bin.reserve(pretty.length());
  bool strmode;
  switch (pretty[0]) {
    case '\'':
      ++i;
      strmode = true;
      break;
    case '0':
      ++i;
      require('x');
      if (i == pretty.length()) {
	raise(i);
      }
      strmode = false;
      break;
    default:
      raise(0);
  }
  for (; i < pretty.length();) {
    if (strmode) {
      if (pretty[i] == '\'') {
	if (i + 1 < pretty.length() && pretty[i + 1] == '\'') {
	  bin.push_back('\'');
	  i += 2;
	} else {
	  ++i;
	  strmode = false;
	  if (i + 1 < pretty.length()) {
	    require('0');
	    require('x');
	    if (i == pretty.length()) {
	      raise(i);
	    }
	  }
	}
      } else {
	bin.push_back(pretty[i]);
	++i;
      }
    } else {
      if (pretty[i] != '\'') {
	int32_t hex0 = hexdigit(pretty[i]);
	if (hex0 < 0) {
	  raise(i);
	}
	++i;
	if (i >= pretty.length()) {
	  raise(i);
	}
	int32_t hex1 = hexdigit(pretty[i]);
	if (hex1 < 0) {
	  raise(i);
	}
	bin.push_back(hex0 * 0x10 + hex1);
	++i;
      } else {
	strmode = true;
	++i;
      }
    }
  }
  if (strmode)
    raise(i);
  return bin;
}
