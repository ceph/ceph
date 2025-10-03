// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include "JSONFormatter.h"
#include "common/escape.h"
#include "common/StackStringStream.h"
#include "include/ceph_assert.h"

#include <boost/container/small_vector.hpp>

#include <cmath> // for std::isfinite(), std::isnan()
#include <limits>
#include <utility>

#define LARGE_SIZE 1024

namespace ceph {

void JSONFormatter::flush(std::ostream& os)
{
  finish_pending_string();
  os << m_ss.str();
  if (m_line_break_enabled)
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
  auto& ss = get_ss();
  if (entry.size) {
    if (m_pretty) {
      ss << ",\n";
      for (unsigned i = 1; i < m_stack.size(); i++)
        ss << "    ";
    } else {
      ss << ",";
    }
  } else if (m_pretty) {
    ss << "\n";
    for (unsigned i = 1; i < m_stack.size(); i++)
      ss << "    ";
  }
  if (m_pretty && entry.is_array)
    ss << "    ";
}

void JSONFormatter::print_quoted_string(std::string_view s)
{
  auto& ss = get_ss();
  ss << '\"' << json_stream_escaper(s) << '\"';
}

void JSONFormatter::print_name(std::string_view name)
{
  auto& ss = get_ss();
  finish_pending_string();
  if (m_stack.empty())
    return;
  struct json_formatter_stack_entry_d& entry = m_stack.back();
  print_comma(entry);
  if (!entry.is_array) {
    if (m_pretty) {
      ss << "    ";
    }
    ss << "\"" << name << "\"";
    if (m_pretty)
      ss << ": ";
    else
      ss << ':';
  }
  ++entry.size;
}

void JSONFormatter::open_section(std::string_view name, const char *ns, bool is_array)
{
  auto& ss = get_ss();
  if (handle_open_section(name, ns, is_array)) {
    return;
  }
  if (ns) {
    std::ostringstream oss;
    oss << name << " " << ns;
    print_name(oss.str().c_str());
  } else {
    print_name(name);
  }
  if (is_array)
    ss << '[';
  else
    ss << '{';

  json_formatter_stack_entry_d n;
  n.is_array = is_array;
  m_stack.push_back(n);
}

void JSONFormatter::open_array_section(std::string_view name)
{
  open_section(name, nullptr, true);
}

void JSONFormatter::open_array_section_in_ns(std::string_view name, const char *ns)
{
  open_section(name, ns, true);
}

void JSONFormatter::open_object_section(std::string_view name)
{
  open_section(name, nullptr, false);
}

void JSONFormatter::open_object_section_in_ns(std::string_view name, const char *ns)
{
  open_section(name, ns, false);
}

void JSONFormatter::close_section()
{
  auto& ss = get_ss();
  if (handle_close_section()) {
    return;
  }
  ceph_assert(!m_stack.empty());
  finish_pending_string();

  struct json_formatter_stack_entry_d& entry = m_stack.back();
  if (m_pretty && entry.size) {
    ss << "\n";
    for (unsigned i = 1; i < m_stack.size(); i++)
      ss << "    ";
  }
  ss << (entry.is_array ? ']' : '}');
  m_stack.pop_back();
  if (m_pretty && m_stack.empty())
    ss << "\n";
}

void JSONFormatter::finish_pending_string()
{
  if (m_is_pending_string) {
    m_is_pending_string = false;
    add_value(m_pending_name.c_str(), m_pending_string.str(), true);
    m_pending_string.str("");
  }
}

void JSONFormatter::add_value(std::string_view name, double val) {
  CachedStackStringStream css;
  if (!std::isfinite(val) || std::isnan(val)) {
    *css << "null";
  } else {
    css->precision(std::numeric_limits<double>::max_digits10);
    *css << val;
  }
  add_value(name, css->strv(), false);
}

template <class T>
void JSONFormatter::add_value(std::string_view name, T val)
{
  CachedStackStringStream css;
  css->precision(std::numeric_limits<T>::max_digits10);
  *css << val;
  add_value(name, css->strv(), false);
}

void JSONFormatter::add_value(std::string_view name, std::string_view val, bool quoted)
{
  auto& ss = get_ss();
  if (handle_value(name, val, quoted)) {
    return;
  }
  print_name(name);
  if (!quoted) {
    ss << val;
  } else {
    print_quoted_string(val);
  }
}

void JSONFormatter::dump_null(std::string_view name)
{
  add_value(name, "null");
}

void JSONFormatter::dump_unsigned(std::string_view name, uint64_t u)
{
  add_value(name, u);
}

void JSONFormatter::dump_int(std::string_view name, int64_t s)
{
  add_value(name, s);
}

void JSONFormatter::dump_float(std::string_view name, double d)
{
  add_value(name, d);
}

void JSONFormatter::dump_string(std::string_view name, std::string_view s)
{
  add_value(name, s, true);
}

std::ostream& JSONFormatter::dump_stream(std::string_view name)
{
  finish_pending_string();
  m_pending_name = name;
  m_is_pending_string = true;
  return m_pending_string;
}

void JSONFormatter::dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap)
{
  auto buf = boost::container::small_vector<char, LARGE_SIZE>{
      LARGE_SIZE, boost::container::default_init};

  va_list ap_copy;
  va_copy(ap_copy, ap);
  int len = vsnprintf(buf.data(), buf.size(), fmt, ap_copy);
  va_end(ap_copy);

  if (std::cmp_greater_equal(len, buf.size())) {
    // output was truncated, allocate a buffer large enough
    buf.resize(len + 1, boost::container::default_init);
    vsnprintf(buf.data(), buf.size(), fmt, ap);
  }

  add_value(name, buf.data(), quoted);
}

int JSONFormatter::get_len() const
{
  return m_ss.tellp();
}

void JSONFormatter::write_raw_data(const char *data)
{
  get_ss() << data;
}

}
