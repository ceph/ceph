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

#include "HTMLFormatter.h"
#include "Formatter.h"

#include <sstream>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <string.h>     // for strdup

#include "common/escape.h"

// -----------------------
namespace ceph {

HTMLFormatter::HTMLFormatter(bool pretty)
: XMLFormatter(pretty), m_status(0), m_status_name(NULL)
{
}

HTMLFormatter::~HTMLFormatter()
{
  if (m_status_name) {
    free((void*)m_status_name);
    m_status_name = NULL;
  }
}

void HTMLFormatter::reset()
{
  XMLFormatter::reset();
  m_header_done = false;
  m_status = 0;
  if (m_status_name) {
    free((void*)m_status_name);
    m_status_name = NULL;
  }
}

void HTMLFormatter::set_status(int status, const char* status_name)
{
  m_status = status;
  if (status_name) {
    if (m_status_name) {
      free((void*)m_status_name);
    }
    m_status_name = strdup(status_name);
  }
};

void HTMLFormatter::output_header() {
  if (!m_header_done) {
    m_header_done = true;
    char buf[16];
    snprintf(buf, sizeof(buf), "%d", m_status);
    std::string status_line(buf);
    if (m_status_name) {
      status_line += " ";
      status_line += m_status_name;
    }
    open_object_section("html");
    print_spaces();
    m_ss << "<head><title>" << status_line << "</title></head>";
    if (m_pretty)
      m_ss << "\n";
    open_object_section("body");
    print_spaces();
    m_ss << "<h1>" << status_line << "</h1>";
    if (m_pretty)
      m_ss << "\n";
    open_object_section("ul");
  }
}

template <typename T>
void HTMLFormatter::dump_template(const char *name, T arg)
{
  print_spaces();
  m_ss << "<li>" << name << ": " << arg << "</li>";
  if (m_pretty)
    m_ss << "\n";
}

void HTMLFormatter::dump_unsigned(const char *name, uint64_t u)
{
  dump_template(name, u);
}

void HTMLFormatter::dump_int(const char *name, int64_t u)
{
  dump_template(name, u);
}

void HTMLFormatter::dump_float(const char *name, double d)
{
  dump_template(name, d);
}

void HTMLFormatter::dump_string(const char *name, std::string_view s)
{
  dump_template(name, xml_stream_escaper(s.data()));
}

void HTMLFormatter::dump_string_with_attrs(const char *name, std::string_view s, const FormatterAttrs& attrs)
{
  std::string e(name);
  std::string attrs_str;
  get_attrs_str(&attrs, attrs_str);
  print_spaces();
  m_ss << "<li>" << e << ": " << xml_stream_escaper(s.data()) << attrs_str << "</li>";
  if (m_pretty)
    m_ss << "\n";
}

std::ostream& HTMLFormatter::dump_stream(const char *name)
{
  print_spaces();
  m_pending_string_name = "li";
  m_ss << "<li>" << name << ": ";
  return m_pending_string;
}

void HTMLFormatter::dump_format_va(const char* name, const char *ns, bool quoted, const char *fmt, va_list ap)
{
  char buf[LARGE_SIZE];
  vsnprintf(buf, LARGE_SIZE, fmt, ap);

  std::string e(name);
  print_spaces();
  if (ns) {
    m_ss << "<li xmlns=\"" << ns << "\">" << e << ": " << xml_stream_escaper(buf) << "</li>";
  } else {
    m_ss << "<li>" << e << ": " << xml_stream_escaper(buf) << "</li>";
  }

  if (m_pretty)
    m_ss << "\n";
}

} // namespace ceph
