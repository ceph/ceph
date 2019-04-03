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
class ConfLine {
public:
  ConfLine(const std::string &key_, const std::string &val_,
	   const std::string &newsection_, const std::string &comment_, int line_no_);
  bool operator<(const ConfLine &rhs) const;
  friend std::ostream &operator<<(std::ostream& oss, const ConfLine &l);

  std::string key, val, newsection;
};

class ConfSection {
public:
  typedef std::set <ConfLine>::const_iterator const_line_iter_t;

  std::set <ConfLine> lines;
};

class ConfFile {
public:
  typedef std::map <std::string, ConfSection>::iterator section_iter_t;
  typedef std::map <std::string, ConfSection>::const_iterator const_section_iter_t;

  ConfFile();
  ~ConfFile();
  void clear();
  int parse_file(const std::string &fname, std::deque<std::string> *errors, std::ostream *warnings);
  int parse_bufferlist(ceph::bufferlist *bl, std::deque<std::string> *errors, std::ostream *warnings);
  int read(const std::string &section, const std::string_view key,
	      std::string &val) const;

  const_section_iter_t sections_begin() const;
  const_section_iter_t sections_end() const;

  static void trim_whitespace(std::string &str, bool strip_internal);
  static std::string normalize_key_name(std::string_view key);
  friend std::ostream &operator<<(std::ostream &oss, const ConfFile &cf);

private:
  void load_from_buffer(const char *buf, size_t sz,
			std::deque<std::string> *errors, std::ostream *warnings);
  static ConfLine* process_line(int line_no, const char *line,
			        std::deque<std::string> *errors);

  std::map <std::string, ConfSection> sections;
};

#endif
