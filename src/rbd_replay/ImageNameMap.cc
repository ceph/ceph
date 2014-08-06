// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ImageNameMap.hpp"
#include <boost/foreach.hpp>
#include "include/assert.h"


using namespace std;
using namespace rbd_replay;


static ImageNameMap::Name bad_mapping_abort(ImageNameMap::Name input, string output) {
  assertf(false, "Bad value returned from mapping.  Image: '%s', snap: '%s', output: '%s'", input.first.c_str(), input.second.c_str(), output.c_str());
}

ImageNameMap::ImageNameMap()
  : m_name("((?:[^@\\\\]|\\\\.)*)@((?:[^@\\\\]|\\\\.)*)"),
    m_escaped_at("\\\\@"),
    m_escaped_backslash("\\\\\\\\"),
    m_at("@"),
    m_backslash("\\\\"),
    m_bad_mapping_fallback(bad_mapping_abort) {
}

bool ImageNameMap::parse_mapping(string mapping_string, Mapping *mapping) const {
  return m_map.parse_mapping(mapping_string, mapping);
}

void ImageNameMap::add_mapping(Mapping mapping) {
  m_map.add_mapping(mapping);
}

string ImageNameMap::unescape_at(string s) const {
  s = boost::regex_replace(s, m_escaped_at, "@");
  return  boost::regex_replace(s, m_escaped_backslash, "\\\\");
}

string ImageNameMap::escape_at(string s) const {
  string result = boost::regex_replace(s, m_backslash, "\\\\");
  return boost::regex_replace(result, m_at, "\\\\@");
}

bool ImageNameMap::parse_name(string name_string, Name *name) const {
  boost::smatch what;
  if (boost::regex_match(name_string, what, m_name)) {
    string image(unescape_at(what[1]));
    string snap(unescape_at(what[2]));
    *name = Name(image, snap);
    return true;
  } else {
    return false;
  }
}

string ImageNameMap::format_name(ImageNameMap::Name name) const {
  return escape_at(name.first) + "@" + escape_at(name.second);
}

ImageNameMap::Name ImageNameMap::map(ImageNameMap::Name name) const {
  string in_string(format_name(name));
  string out_string(m_map.map(in_string));
  Name out;
  if (!parse_name(out_string, &out)) {
    return m_bad_mapping_fallback(name, out_string);
  }
  return out;
}

void ImageNameMap::set_bad_mapping_fallback(BadMappingFallback bad_mapping_fallback) {
  m_bad_mapping_fallback = bad_mapping_fallback;
}
