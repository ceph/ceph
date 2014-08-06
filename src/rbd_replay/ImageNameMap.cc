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


static ImageName bad_mapping_abort(ImageName input, string output) {
  assertf(false, "Bad value returned from mapping.  Image: '%s', snap: '%s', output: '%s'", input.image().c_str(), input.snap().c_str(), output.c_str());
}

ImageNameMap::ImageNameMap()
  : m_bad_mapping_fallback(bad_mapping_abort) {
}

bool ImageNameMap::parse_mapping(string mapping_string, Mapping *mapping) const {
  return m_map.parse_mapping(mapping_string, mapping);
}

void ImageNameMap::add_mapping(Mapping mapping) {
  m_map.add_mapping(mapping);
}

ImageName ImageNameMap::map(const ImageName& name) const {
  string in_string(name.str());
  string out_string(m_map.map(in_string));
  ImageName out;
  if (!out.parse(out_string)) {
    return m_bad_mapping_fallback(name, out_string);
  }
  return out;
}

void ImageNameMap::set_bad_mapping_fallback(BadMappingFallback bad_mapping_fallback) {
  m_bad_mapping_fallback = bad_mapping_fallback;
}
