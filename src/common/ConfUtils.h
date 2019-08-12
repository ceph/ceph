// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CONFUTILS_H
#define CEPH_CONFUTILS_H

#include <deque>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "include/buffer_fwd.h"

/*
 * Ceph configuration file support.
 *
 * This class loads an INI-style configuration from a file or bufferlist, and
 * holds it in memory. In general, an INI configuration file is composed of
 * sections, which contain key/value pairs. You can put comments on the end of
 * lines by using either a hash mark (#) or the semicolon (;).
 *
 * You can get information out of ConfFile by calling get_key or by examining
 * individual sections.
 *
 * This class could be extended to support modifying configuration files and
 * writing them back out without too much difficulty. Currently, this is not
 * implemented, and the file is read-only.
 */
struct conf_line_t  {
  conf_line_t() = default;
  conf_line_t(const std::string& key, const std::string& val);
  bool operator<(const conf_line_t& rhs) const;
  std::string key;
  std::string val;
};

std::ostream &operator<<(std::ostream& oss, const conf_line_t& line);

class conf_section_t : public std::set<conf_line_t> {
public:
  conf_section_t() = default;
  conf_section_t(const std::string& heading,
		 const std::vector<conf_line_t>& lines);
  std::string heading;
  friend std::ostream& operator<<(std::ostream& os, const conf_section_t&);
};

class ConfFile : public std::map<std::string, conf_section_t> {
  using base_type = std::map<std::string, conf_section_t>;
public:
  ConfFile()
    : ConfFile{std::vector<conf_section_t>{}}
  {}
  ConfFile(const conf_line_t& line)
    : ConfFile{{conf_section_t{"global", {line}}}}
  {}
  ConfFile(const std::vector<conf_section_t>& sections);
  int parse_file(const std::string &fname, std::ostream *warnings);
  int parse_bufferlist(ceph::bufferlist *bl, std::ostream *warnings);
  int read(const std::string& section, std::string_view key,
	   std::string &val) const;
  static std::string normalize_key_name(std::string_view key);
private:
  bool load_from_buffer(std::string_view buf, std::ostream* warning);
};

std::ostream &operator<<(std::ostream& oss, const ConfFile& cf);

#endif
