// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>

template<typename S>
static std::string pretty_binary_string(const S& bin)
{
  std::string pretty;
  if (bin.empty())
    return pretty;
  pretty.reserve(bin.length() * 3);
  auto printable = [](unsigned char c) -> bool {
    return (c >= 32) && (c <= 126);
  };
  auto append_hex = [&](unsigned char c) {
    static const char hex[16] = {'0', '1', '2', '3',
				 '4', '5', '6', '7',
				 '8', '9', 'A', 'B',
				 'C', 'D', 'E', 'F'};
    pretty.push_back(hex[c / 16]);
    pretty.push_back(hex[c % 16]);
  };
  // prologue
  bool strmode = printable(bin[0]);
  if (strmode) {
    pretty.push_back('\'');
  } else {
    pretty.push_back('0');
    pretty.push_back('x');
  }
  for (size_t i = 0; i < bin.length(); ++i) {
    // change mode from hex to str if following 3 characters are printable
    if (strmode) {
      if (!printable(bin[i])) {
	pretty.push_back('\'');
	pretty.push_back('0');
	pretty.push_back('x');
	strmode = false;
      }
    } else {
      if (i + 2 < bin.length() &&
	  printable(bin[i]) &&
	  printable(bin[i + 1]) &&
	  printable(bin[i + 2])) {
	pretty.push_back('\'');
	strmode = true;
      }
    }
    if (strmode) {
      if (bin[i] == '\'')
	pretty.push_back('\'');
      pretty.push_back(bin[i]);
    } else {
      append_hex(bin[i]);
    }
  }
  // epilog
  if (strmode) {
    pretty.push_back('\'');
  }
  return pretty;
}

std::string pretty_binary_string_reverse(const std::string& pretty);
