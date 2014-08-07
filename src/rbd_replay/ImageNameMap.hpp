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
#include "rbd_loc.hpp"

namespace rbd_replay {

class ImageNameMap {
public:
  typedef std::pair<rbd_loc, rbd_loc> Mapping;

  bool parse_mapping(std::string mapping_string, Mapping *mapping) const;

  void add_mapping(const Mapping& mapping);

  rbd_loc map(const rbd_loc& name) const;

private:
  std::map<rbd_loc, rbd_loc> m_map;
};

}

#endif
