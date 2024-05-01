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


using namespace std;
using namespace rbd_replay;


bool ImageNameMap::parse_mapping(string mapping_string, Mapping *mapping) const {
  string fields[2];
  int field = 0;
  for (size_t i = 0, n = mapping_string.length(); i < n; i++) {
    char c = mapping_string[i];
    switch (c) {
    case '\\':
      if (i != n - 1 && mapping_string[i + 1] == '=') {
	i++;
	fields[field].push_back('=');
      } else {
	fields[field].push_back('\\');
      }
      break;
    case '=':
      if (field == 1) {
	return false;
      }
      field = 1;
      break;
    default:
      fields[field].push_back(c);
    }
  }
  if (field == 0) {
    return false;
  }
  if (!mapping->first.parse(fields[0])) {
    return false;
  }
  if (!mapping->second.parse(fields[1])) {
    return false;
  }
  return true;
}

void ImageNameMap::add_mapping(const Mapping& mapping) {
  m_map.insert(mapping);
}

rbd_loc ImageNameMap::map(const rbd_loc& name) const {
  std::map<rbd_loc, rbd_loc>::const_iterator p(m_map.find(name));
  if (p == m_map.end()) {
    return name;
  } else {
    return p->second;
  }
}
