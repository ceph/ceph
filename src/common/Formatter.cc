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

#define LARGE_SIZE 8192

#include "assert.h"
#include "Formatter.h"
#include "common/escape.h"

#include <inttypes.h>
#include <iostream>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

// -----------------------
namespace ceph {

void Formatter::reset()
{
  m_stack.clear();
  m_ss.clear();
  m_pending_string.clear();
}

void Formatter::flush(std::ostream& os)
{
  finish_pending_string();
  assert(m_stack.empty());
  os << m_ss.str();
  m_ss.clear();
}

// -----------------------


void JSONFormatter::print_comma(formatter_stack_entry_d& entry)
{
  if (entry.size) {
    if (m_pretty) {
      m_ss << ",\n";
      for (unsigned i=1; i < m_stack.size(); i++)
	m_ss << "    ";
    } else {
      m_ss << ",";
    }
  } else if (entry.is_array && m_pretty) {
    m_ss << "\n";
    for (unsigned i=1; i < m_stack.size(); i++)
      m_ss << "    ";
  }
  if (m_pretty && entry.is_array)
    m_ss << "    ";
}

void JSONFormatter::print_quoted_string(const char *s)
{
  int len = escape_json_attr_len(s);
  char *escaped = (char*)malloc(len);
  escape_json_attr(s, escaped);
  m_ss << '\"' << escaped << '\"';
  free(escaped);
}

void JSONFormatter::print_name(const char *name)
{
  finish_pending_string();
  if (m_stack.empty())
    return;
  struct formatter_stack_entry_d& entry = m_stack.back();
  print_comma(entry);
  if (!entry.is_array) {
    if (m_pretty) {
      if (entry.size)
	m_ss << "  ";
      else
	m_ss << " ";
    }
    print_quoted_string(name);
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

  formatter_stack_entry_d n;
  n.is_array = is_array;
  m_stack.push_back(n);
}

void JSONFormatter::open_array_section(const char *name)
{
  open_section(name, true);
}

void JSONFormatter::open_object_section(const char *name)
{
  open_section(name, false);
}

void JSONFormatter::close_section()
{
  assert(!m_stack.empty());
  finish_pending_string();

  struct formatter_stack_entry_d& entry = m_stack.back();
  m_ss << (entry.is_array ? ']' : '}');
  m_stack.pop_back();
}

void JSONFormatter::finish_pending_string()
{
  if (m_is_pending_string) {
    print_quoted_string(m_pending_string.str().c_str());
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
  char foo[30];
  snprintf(foo, sizeof(foo), "%lf", d);
  dump_string(name, foo);
}

void JSONFormatter::dump_string(const char *name, std::string s)
{
  print_name(name);
  print_quoted_string(s.c_str());
}

std::ostream& JSONFormatter::dump_stream(const char *name)
{
  print_name(name);
  m_is_pending_string = true;
  return m_pending_string;
}

void JSONFormatter::dump_format(const char *name, const char *fmt, ...)
{
  char buf[LARGE_SIZE];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);

  print_name(name);
  print_quoted_string(buf);
}

}
