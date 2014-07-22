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

#ifndef _INCLUDED_RBD_REPLAY_NAMEMAP_HPP
#define _INCLUDED_RBD_REPLAY_NAMEMAP_HPP

#include <map>
#include <string>
#include <vector>
#include <boost/regex.hpp>

namespace rbd_replay {

class NameMap {
public:
  typedef std::pair<boost::regex, std::string> Mapping;

  NameMap()
    : m_keyval("((?:[^=\\\\]|\\\\.)*)=((?:[^=\\\\]|\\\\.)*)"),
      m_escaped_equals("\\\\=") {
  }

  bool parse_mapping(std::string mapping_string, Mapping *mapping) const;

  void add_mapping(Mapping mapping);

  std::string map(std::string name) const;

private:
  std::vector<Mapping> m_mappings;

  // Split "a=b" into "a" and "b", allowing for escaped equal signs
  boost::regex m_keyval;

  // We don't have to worry about an even number of backslahes followed by an equal sign,
  // because that equal sign would have already been matched and removed.
  boost::regex m_escaped_equals;
};

}

#endif
