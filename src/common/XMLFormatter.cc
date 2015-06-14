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
#include "XMLFormatter.h"
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

const char *XMLFormatter::XML_1_DTD =
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

XMLFormatter::XMLFormatter(bool pretty)
: m_pretty(pretty)
{
  reset();
}

void XMLFormatter::flush(std::ostream& os)
{
  finish_pending_string();
  output_footer();
  std::string m_ss_str = m_ss.str();
  os << m_ss_str;
  /* There is a small catch here. If the rest of the formatter had NO output,
   * we should NOT output a newline. This primarily triggers on HTTP redirects */
  if (m_pretty && !m_ss_str.empty())
    os << "\n";
  m_ss.clear();
  m_ss.str("");
}

void XMLFormatter::reset()
{
  m_ss.clear();
  m_ss.str("");
  m_pending_string.clear();
  m_pending_string.str("");
  m_sections.clear();
  m_pending_string_name.clear();
  m_header_done = false;
}

void XMLFormatter::output_header()
{
  if(!m_header_done) {
    m_header_done = true;
    write_raw_data(XMLFormatter::XML_1_DTD);;
    if (m_pretty)
      m_ss << "\n";
  }
}

void XMLFormatter::output_footer()
{
  while(!m_sections.empty()) {
    close_section();
  }
}

void XMLFormatter::open_object_section(const char *name)
{
  output_header();
  open_section_in_ns(name, NULL, NULL);
}

void XMLFormatter::open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs)
{
  output_header();
  open_section_in_ns(name, NULL, &attrs);
}

void XMLFormatter::open_object_section_in_ns(const char *name, const char *ns)
{
  output_header();
  open_section_in_ns(name, ns, NULL);
}

void XMLFormatter::open_array_section(const char *name)
{
  output_header();
  open_section_in_ns(name, NULL, NULL);
}

void XMLFormatter::open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs)
{
  output_header();
  open_section_in_ns(name, NULL, &attrs);
}

void XMLFormatter::open_array_section_in_ns(const char *name, const char *ns)
{
  output_header();
  open_section_in_ns(name, ns, NULL);
}

void XMLFormatter::close_section()
{
  assert(!m_sections.empty());
  finish_pending_string();

  std::string section = m_sections.back();
  m_sections.pop_back();
  print_spaces();
  m_ss << "</" << section << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_unsigned(const char *name, uint64_t u)
{
  std::string e(name);
  output_header();
  print_spaces();
  m_ss << "<" << e << ">" << u << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_int(const char *name, int64_t u)
{
  std::string e(name);
  output_header();
  print_spaces();
  m_ss << "<" << e << ">" << u << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_float(const char *name, double d)
{
  std::string e(name);
  output_header();
  print_spaces();
  m_ss << "<" << e << ">" << d << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_string(const char *name, const std::string& s)
{
  std::string e(name);
  output_header();
  print_spaces();
  m_ss << "<" << e << ">" << escape_xml_str(s.c_str()) << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_string_with_attrs(const char *name, const std::string& s, const FormatterAttrs& attrs)
{
  std::string e(name);
  std::string attrs_str;
  get_attrs_str(&attrs, attrs_str);
  output_header();
  print_spaces();
  m_ss << "<" << e << attrs_str << ">" << escape_xml_str(s.c_str()) << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

std::ostream& XMLFormatter::dump_stream(const char *name)
{
  output_header();
  print_spaces();
  m_pending_string_name = name;
  m_ss << "<" << m_pending_string_name << ">";
  return m_pending_string;
}

void XMLFormatter::dump_format_va(const char* name, const char *ns, bool quoted, const char *fmt, va_list ap)
{
  char buf[LARGE_SIZE];
  vsnprintf(buf, LARGE_SIZE, fmt, ap);

  std::string e(name);
  output_header();
  print_spaces();
  if (ns) {
    m_ss << "<" << e << " xmlns=\"" << ns << "\">" << buf << "</" << e << ">";
  } else {
    m_ss << "<" << e << ">" << escape_xml_str(buf) << "</" << e << ">";
  }

  if (m_pretty)
    m_ss << "\n";
}

int XMLFormatter::get_len() const
{
  return m_ss.str().size();
}

void XMLFormatter::write_raw_data(const char *data)
{
  m_ss << data;
}

void XMLFormatter::get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str)
{
  std::stringstream attrs_ss;

  for (std::list<std::pair<std::string, std::string> >::const_iterator iter = attrs->attrs.begin();
       iter != attrs->attrs.end(); ++iter) {
    std::pair<std::string, std::string> p = *iter;
    attrs_ss << " " << p.first << "=" << "\"" << p.second << "\"";
  }

  attrs_str = attrs_ss.str();
}

void XMLFormatter::open_section_in_ns(const char *name, const char *ns, const FormatterAttrs *attrs)
{
  output_header();
  print_spaces();
  std::string attrs_str;

  if (attrs) {
    get_attrs_str(attrs, attrs_str);
  }

  if (ns) {
    m_ss << "<" << name << attrs_str << " xmlns=\"" << ns << "\">";
  } else {
    m_ss << "<" << name << attrs_str << ">";
  }
  if (m_pretty)
    m_ss << "\n";
  m_sections.push_back(name);
}

void XMLFormatter::finish_pending_string()
{
  if (!m_pending_string_name.empty()) {
    m_ss << escape_xml_str(m_pending_string.str().c_str())
      << "</" << m_pending_string_name << ">";
    m_pending_string_name.clear();
    m_pending_string.str(std::string());
    if (m_pretty) {
      m_ss << "\n";
    }
  }
}

void XMLFormatter::print_spaces()
{
  finish_pending_string();
  if (m_pretty) {
    std::string spaces(m_sections.size(), ' ');
    m_ss << spaces;
  }
}

std::string XMLFormatter::escape_xml_str(const char *str)
{
  int len = escape_xml_attr_len(str);
  std::vector<char> escaped(len, '\0');
  escape_xml_attr(str, &escaped[0]);
  return std::string(&escaped[0]);
}

} // namespace ceph
