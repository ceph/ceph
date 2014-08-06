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

#ifndef _INCLUDED_RBD_REPLAY_IMAGENAMEMAP_HPP
#define _INCLUDED_RBD_REPLAY_IMAGENAMEMAP_HPP

#include <map>
#include <string>
#include <vector>
#include <boost/function.hpp>
#include <boost/regex.hpp>
#include "ImageName.hpp"
#include "NameMap.hpp"

namespace rbd_replay {

class ImageNameMap {
public:
  typedef std::pair<boost::regex, std::string> Mapping;

  typedef boost::function<rbd_replay::ImageName (const rbd_replay::ImageName& input, std::string output)> BadMappingFallback;

  ImageNameMap();

  bool parse_mapping(std::string mapping_string, Mapping *mapping) const;

  void add_mapping(Mapping mapping);

  rbd_replay::ImageName map(const rbd_replay::ImageName& name) const;

  void set_bad_mapping_fallback(BadMappingFallback bad_mapping_fallback);

private:
  NameMap m_map;

  BadMappingFallback m_bad_mapping_fallback;
};

}

#endif
