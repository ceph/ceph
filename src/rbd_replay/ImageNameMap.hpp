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
#include "NameMap.hpp"

namespace rbd_replay {

class ImageNameMap {
public:
  typedef std::pair<boost::regex, std::string> Mapping;

  typedef std::pair<std::string, std::string> Name;

  typedef boost::function<Name (Name input, std::string output)> BadMappingFallback;

  ImageNameMap();

  bool parse_mapping(std::string mapping_string, Mapping *mapping) const;

  void add_mapping(Mapping mapping);

  bool parse_name(std::string name_string, Name *name) const;

  std::string format_name(Name name) const;

  Name map(Name name) const;

  void set_bad_mapping_fallback(BadMappingFallback bad_mapping_fallback);

private:
  std::string escape_at(std::string s) const;

  std::string unescape_at(std::string s) const;

  NameMap m_map;

  // Split "a@b" into "a" and "b", allowing for escaped at sign
  boost::regex m_name;

  // We don't have to worry about an even number of backslahes followed by an at sign,
  // because that at sign would have already been matched and removed.
  boost::regex m_escaped_at;

  boost::regex m_escaped_backslash;

  boost::regex m_at;

  boost::regex m_backslash;

  BadMappingFallback m_bad_mapping_fallback;
};

}

#endif
