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

#include "NameMap.hpp"
#include <boost/foreach.hpp>
#include "include/assert.h"


using namespace std;
using namespace rbd_replay;


bool NameMap::parse_mapping(string mapping_string, Mapping *mapping) const {
  boost::smatch what;
  if (boost::regex_match(mapping_string, what, m_keyval)) {
      string pattern(what[1]);
      string replacement(what[2]);

      pattern = boost::regex_replace(pattern, m_escaped_equals, "=");
      replacement = boost::regex_replace(replacement, m_escaped_equals, "=");

      *mapping = Mapping(boost::regex(pattern), replacement);
      return true;
  } else {
    return false;
  }
}

void NameMap::add_mapping(Mapping mapping) {
  m_mappings.push_back(mapping);
}

string NameMap::map(string name) const {
  BOOST_FOREACH(const Mapping &m, m_mappings) {
    boost::smatch what;
    if (boost::regex_match(name, what, m.first)) {
      return what.format(m.second);
    }
  }
  return name;
}
