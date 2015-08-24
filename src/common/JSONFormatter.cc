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
#include "JSONFormatter.h"
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

JSONFormatter::JSONFormatter(bool p)
: m_pretty(p), m_is_pending_string(false)
{
  reset();
}

void JSONFormatter::flush(std::ostream& os)
{
  finish_pending_string();
  os << m_ss.str();
  if (m_pretty)
    os << "\n";
  m_ss.clear();
  m_ss.str("");
}

void JSONFormatter::reset()
{
  m_stack.clear();
  m_ss.clear();
  m_ss.str("");
  m_pending_string.clear();
  m_pending_string.str("");
}

void JSONFormatter::print_comma(json_formatter_stack_entry_d& entry)
{
  if (entry.size) {
    if (m_pretty) {
      m_ss << ",\n";
      for (unsigned i = 1; i < m_stack.size(); i++)
        m_ss << "    ";
    } else {
      m_ss << ",";
    }
  } else if (m_pretty) {
    m_ss << "\n";
    for (unsigned i = 1; i < m_stack.size(); i++)
      m_ss << "    ";
  }
  if (m_pretty && entry.is_array)
    m_ss << "    ";
}

void JSONFormatter::print_quoted_string(const std::string& s)
{
  int len = escape_json_attr_len(s.c_str(), s.size());
  char escaped[len];
  escape_json_attr(s.c_str(), s.size(), escaped);
  m_ss << '\"' << escaped << '\"';
}

void JSONFormatter::print_name(const char *name)
{
  finish_pending_string();
  if (m_stack.empty())
    return;
  struct json_formatter_stack_entry_d& entry = m_stack.back();
  print_comma(entry);
  if (!entry.is_array) {
    if (m_pretty) {
      m_ss << "    ";
    }
    m_ss << "\"" << name << "\"";
    if (m_pretty)
      m_ss << ": ";
    else
      m_ss << ':';
  }
  ++entry.size;
}

void JSONFormatter::open_section(const char *name, bool is_array)
{
  print_name(name);
  if (is_array)
    m_ss << '[';
  else
    m_ss << '{';

  json_formatter_stack_entry_d n;
  n.is_array = is_array;
  m_stack.push_back(n);
}

void JSONFormatter::open_array_section(const char *name)
{
  open_section(name, true);
}

void JSONFormatter::open_array_section_in_ns(const char *name, const char *ns)
{
  std::ostringstream oss;
  oss << name << " " << ns;
  open_section(oss.str().c_str(), true);
}

void JSONFormatter::open_object_section(const char *name)
{
  open_section(name, false);
}

void JSONFormatter::open_object_section_in_ns(const char *name, const char *ns)
{
  std::ostringstream oss;
  oss << name << " " << ns;
  open_section(oss.str().c_str(), false);
}

void JSONFormatter::close_section()
{
  assert(!m_stack.empty());
  finish_pending_string();

  struct json_formatter_stack_entry_d& entry = m_stack.back();
  if (m_pretty && entry.size) {
    m_ss << "\n";
    for (unsigned i = 1; i < m_stack.size(); i++)
      m_ss << "    ";
  }
  m_ss << (entry.is_array ? ']' : '}');
  m_stack.pop_back();
}

void JSONFormatter::finish_pending_string()
{
  if (m_is_pending_string) {
    print_quoted_string(m_pending_string.str());
    m_pending_string.str(std::string());
    m_is_pending_string = false;
  }
}

void JSONFormatter::dump_unsigned(const char *name, uint64_t u)
{
  print_name(name);
  m_ss << u;
}

void JSONFormatter::dump_int(const char *name, int64_t s)
{
  print_name(name);
  m_ss << s;
}

void JSONFormatter::dump_float(const char *name, double d)
{
  print_name(name);
  char foo[30];
  snprintf(foo, sizeof(foo), "%lf", d);
  m_ss << foo;
}

void JSONFormatter::dump_string(const char *name, const std::string& s)
{
  print_name(name);
  print_quoted_string(s);
}

std::ostream& JSONFormatter::dump_stream(const char *name)
{
  print_name(name);
  m_is_pending_string = true;
  return m_pending_string;
}

void JSONFormatter::dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap)
{
  char buf[LARGE_SIZE];
  vsnprintf(buf, LARGE_SIZE, fmt, ap);

  print_name(name);
  if (quoted) {
    print_quoted_string(std::string(buf));
  } else {
    m_ss << std::string(buf);
  }
}

int JSONFormatter::get_len() const
{
  return m_ss.str().size();
}

void JSONFormatter::write_raw_data(const char *data)
{
  m_ss << data;
}

} // namespace ceph
