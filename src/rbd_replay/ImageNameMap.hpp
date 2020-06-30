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

/**
   Maps image names.
 */
class ImageNameMap {
public:
  typedef std::pair<rbd_loc, rbd_loc> Mapping;

  /**
     Parses a mapping.
     If parsing fails, the contents of \c mapping are undefined.
     @param[in] mapping_string string representation of the mapping
     @param[out] mapping stores the parsed mapping
     @retval true parsing was successful
   */
  bool parse_mapping(std::string mapping_string, Mapping *mapping) const;

  void add_mapping(const Mapping& mapping);

  /**
     Maps an image name.
     If no mapping matches the name, it is returned unmodified.
   */
  rbd_loc map(const rbd_loc& name) const;

private:
  std::map<rbd_loc, rbd_loc> m_map;
};

}

#endif
