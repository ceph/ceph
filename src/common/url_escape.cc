// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "url_escape.h"

#include <stdexcept>
#include <sstream>

std::string url_escape(const std::string& s)
{
  std::string out;
  for (auto c : s) {
    if (std::isalnum(c) || c == '-' || c == '.' || c == '_' || c == '~' ||
	c == '/') {
      out.push_back(c);
    } else {
      char t[4];
      snprintf(t, sizeof(t), "%%%02x", (int)(unsigned char)c);
      out.append(t);
    }
  }
  return out;
}

std::string url_unescape(const std::string& s)
{
  std::string out;
  const char *end = s.c_str() + s.size();
  for (const char *c = s.c_str(); c < end; ++c) {
    switch (*c) {
    case '%':
      {
	unsigned char v = 0;
	for (unsigned i=0; i<2; ++i) {
	  ++c;
	  if (c >= end) {
	    std::ostringstream ss;
	    ss << "invalid escaped string at pos " << (c - s.c_str()) << " of '"
	       << s << "'";
	    throw std::runtime_error(ss.str());
	  }
	  v <<= 4;
	  if (*c >= '0' && *c <= '9') {
	    v += *c - '0';
	  } else if (*c >= 'a' && *c <= 'f') {
	    v += *c - 'a' + 10;
	  } else if (*c >= 'A' && *c <= 'F') {
	    v += *c - 'A' + 10;
	  } else {
	    std::ostringstream ss;
	    ss << "invalid escaped string at pos " << (c - s.c_str()) << " of '"
	       << s << "'";
	    throw std::runtime_error(ss.str());
	  }
	}
	out.push_back(v);
      }
      break;
    default:
      out.push_back(*c);
    }
  }
  return out;
}
