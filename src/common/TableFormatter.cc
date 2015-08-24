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

#define LARGE_SIZE 1024

#include "include/int_types.h"

#include "assert.h"
#include "Formatter.h"
#include "TableFormatter.h"
#include "common/escape.h"

#include <iostream>
#include <sstream>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include <set>
#include <boost/format.hpp>

// -----------------------
namespace ceph {
TableFormatter::TableFormatter(bool keyval) : m_keyval(keyval)
{
  reset();
}

void TableFormatter::flush(std::ostream& os)
{
  finish_pending_string();
  std::vector<size_t> column_size = m_column_size;
  std::vector<std::string> column_name = m_column_name;

  std::set<int> need_header_set;

  // auto-sizing columns
  for (size_t i = 0; i < m_vec.size(); i++) {
    for (size_t j = 0; j < m_vec[i].size(); j++) {
      column_size.resize(m_vec[i].size());
      column_name.resize(m_vec[i].size());
      if (i > 0) {
        if (m_vec[i - 1][j] != m_vec[i][j]) {
          // changing row labels require to show the header
          need_header_set.insert(i);
          column_name[i] = m_vec[i][j].first;
        }
      } else {
        column_name[i] = m_vec[i][j].first;
      }

      if (m_vec[i][j].second.length() > column_size[j])
        column_size[j] = m_vec[i][j].second.length();
      if (m_vec[i][j].first.length() > column_size[j])
        column_size[j] = m_vec[i][j].first.length();
    }
  }

  bool need_header = false;
  if ((column_size.size() == m_column_size.size())) {
    for (size_t i = 0; i < column_size.size(); i++) {
      if (column_size[i] != m_column_size[i]) {
        need_header = true;
        break;
      }
    }
  } else {
    need_header = true;
  }

  if (need_header) {
    // first row always needs a header if there wasn't one before
    need_header_set.insert(0);
  }

  m_column_size = column_size;
  for (size_t i = 0; i < m_vec.size(); i++) {
    if (i == 0) {
      if (need_header_set.count(i)) {
        // print the header
        if (!m_keyval) {
          os << "+";
          for (size_t j = 0; j < m_vec[i].size(); j++) {
            for (size_t v = 0; v < m_column_size[j] + 3; v++)
              os << "-";
            os << "+";
          }
          os << "\n";
          os << "|";

          for (size_t j = 0; j < m_vec[i].size(); j++) {
            os << " ";
            std::stringstream fs;
            fs << boost::format("%%-%is") % (m_column_size[j] + 2);
            os << boost::format(fs.str()) % m_vec[i][j].first;
            os << "|";
          }
          os << "\n";
          os << "+";
          for (size_t j = 0; j < m_vec[i].size(); j++) {
            for (size_t v = 0; v < m_column_size[j] + 3; v++)
              os << "-";
            os << "+";
          }
          os << "\n";
        }
      }
    }
    // print body
    if (!m_keyval)
      os << "|";
    for (size_t j = 0; j < m_vec[i].size(); j++) {
      if (!m_keyval)
        os << " ";
      std::stringstream fs;

      if (m_keyval) {
        os << "key::";
        os << m_vec[i][j].first;
        os << "=";
        os << "\"";
        os << m_vec[i][j].second;
        os << "\" ";
      } else {
        fs << boost::format("%%-%is") % (m_column_size[j] + 2);
        os << boost::format(fs.str()) % m_vec[i][j].second;
        os << "|";
      }
    }

    os << "\n";
    if (!m_keyval) {
      if (i == (m_vec.size() - 1)) {
        // print trailer
        os << "+";
        for (size_t j = 0; j < m_vec[i].size(); j++) {
          for (size_t v = 0; v < m_column_size[j] + 3; v++)
            os << "-";
          os << "+";
        }
        os << "\n";
      }
    }
    m_vec[i].clear();
  }
  m_vec.clear();
}

void TableFormatter::reset()
{
  m_ss.clear();
  m_ss.str("");
  m_section_cnt.clear();
  m_column_size.clear();
  m_section_open = 0;
}

void TableFormatter::open_object_section(const char *name)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_object_section_in_ns(const char *name, const char *ns)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_array_section(const char *name)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_array_section_in_ns(const char *name, const char *ns)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_section_in_ns(const char *name, const char *ns, const FormatterAttrs *attrs)
{
  m_section.push_back(name);
  m_section_open++;
}

void TableFormatter::close_section()
{
  //
  m_section_open--;
  if (m_section.size()) {
    m_section_cnt[m_section.back()] = 0;
    m_section.pop_back();
  }
}

size_t TableFormatter::m_vec_index(const char *name)
{
  std::string key(name);

  size_t i = m_vec.size();
  if (i)
    i--;

  // make sure there are vectors to push back key/val pairs
  if (!m_vec.size())
    m_vec.resize(1);

  if (m_vec.size()) {
    if (m_vec[i].size()) {
      if (m_vec[i][0].first == key) {
        // start a new column if a key is repeated
        m_vec.resize(m_vec.size() + 1);
        i++;
      }
    }
  }

  return i;
}

std::string TableFormatter::get_section_name(const char* name)
{
  std::string t_name = name;
  for (size_t i = 0; i < m_section.size(); i++) {
    t_name.insert(0, ":");
    t_name.insert(0, m_section[i]);
  }
  if (m_section_open) {
    std::stringstream lss;
    lss << t_name;
    lss << "[";
    lss << m_section_cnt[t_name]++;
    lss << "]";
    return lss.str();
  } else {
    return t_name;
  }
}

void TableFormatter::dump_unsigned(const char *name, uint64_t u)
{
  finish_pending_string();
  size_t i = m_vec_index(name);
  m_ss << u;
  m_vec[i].push_back(std::make_pair(get_section_name(name), m_ss.str()));
  m_ss.clear();
  m_ss.str("");
}

void TableFormatter::dump_int(const char *name, int64_t u)
{
  finish_pending_string();
  size_t i = m_vec_index(name);
  m_ss << u;
  m_vec[i].push_back(std::make_pair(get_section_name(name), m_ss.str()));
  m_ss.clear();
  m_ss.str("");
}

void TableFormatter::dump_float(const char *name, double d)
{
  finish_pending_string();
  size_t i = m_vec_index(name);
  m_ss << d;

  m_vec[i].push_back(std::make_pair(get_section_name(name), m_ss.str()));
  m_ss.clear();
  m_ss.str("");
}

void TableFormatter::dump_string(const char *name, const std::string& s)
{
  finish_pending_string();
  size_t i = m_vec_index(name);
  m_ss << s;

  m_vec[i].push_back(std::make_pair(get_section_name(name), m_ss.str()));
  m_ss.clear();
  m_ss.str("");
}

void TableFormatter::dump_string_with_attrs(const char *name, const std::string& s, const FormatterAttrs& attrs)
{
  finish_pending_string();
  size_t i = m_vec_index(name);

  std::string attrs_str;
  get_attrs_str(&attrs, attrs_str);
  m_ss << attrs_str << s;

  m_vec[i].push_back(std::make_pair(get_section_name(name), m_ss.str()));
  m_ss.clear();
  m_ss.str("");
}

void TableFormatter::dump_format_va(const char* name, const char *ns, bool quoted, const char *fmt, va_list ap)
{
  finish_pending_string();
  char buf[LARGE_SIZE];
  vsnprintf(buf, LARGE_SIZE, fmt, ap);

  size_t i = m_vec_index(name);
  if (ns) {
    m_ss << ns << "." << buf;
  } else
    m_ss << buf;

  m_vec[i].push_back(std::make_pair(get_section_name(name), m_ss.str()));
  m_ss.clear();
  m_ss.str("");
}

std::ostream& TableFormatter::dump_stream(const char *name)
{
  finish_pending_string();
  // we don't support this
  m_pending_name = name;
  return m_ss;
}

int TableFormatter::get_len() const
{
  // we don't know the size until flush is called
  return 0;
}

void TableFormatter::write_raw_data(const char *data) {
  // not supported
}

void TableFormatter::get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str)
{
  std::stringstream attrs_ss;

  for (std::list<std::pair<std::string, std::string> >::const_iterator iter = attrs->attrs.begin();
       iter != attrs->attrs.end(); ++iter) {
    std::pair<std::string, std::string> p = *iter;
    attrs_ss << " " << p.first << "=" << "\"" << p.second << "\"";
  }

  attrs_str = attrs_ss.str();
}

void TableFormatter::finish_pending_string()
{
  if (m_pending_name.length()) {
    std::string ss = m_ss.str();
    m_ss.clear();
    m_ss.str("");
    std::string pending_name = m_pending_name;
    m_pending_name = "";
    dump_string(pending_name.c_str(), ss);
  }
}

} // namespace ceph
