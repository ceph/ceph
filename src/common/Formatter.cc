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
#include "common/escape.h"

#include <iostream>
#include <sstream>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>

// -----------------------
namespace ceph {

/*
 * FormatterAttrs(const char *attr, ...)
 *
 * Requires a list of of attrs followed by NULL. The attrs should be char *
 * pairs, first one is the name, second one is the value. E.g.,
 *
 * FormatterAttrs("name1", "value1", "name2", "value2", NULL);
 */
FormatterAttrs::FormatterAttrs(const char *attr, ...)
{
  const char *s = attr;
  va_list ap;
  va_start(ap, attr);
  do {
    const char *val = va_arg(ap, char *);
    if (!val)
      break;

    attrs.push_back(make_pair(std::string(s), std::string(val)));
    s = va_arg(ap, char *);
  } while (s);
  va_end(ap);
}

Formatter::Formatter()
{
}

Formatter::~Formatter()
{
}

Formatter *
new_formatter(const std::string &type)
{
    std::string mytype = type;
    if (mytype == "")
      mytype = "json-pretty";

    if (mytype == "json")
      return new JSONFormatter(false);
    else if (mytype == "json-pretty")
      return new JSONFormatter(true);
    else if (mytype == "xml")
      return new XMLFormatter(false);
    else if (mytype == "xml-pretty")
      return new XMLFormatter(true);
    else
      return (Formatter *)NULL;
}

void Formatter::dump_format(const char *name, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  dump_format_va(name, NULL, true, fmt, ap);
  va_end(ap);
}

void Formatter:: dump_format_ns(const char *name, const char *ns, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  dump_format_va(name, ns, true, fmt, ap);
  va_end(ap);

}


void Formatter::dump_format_unquoted(const char *name, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  dump_format_va(name, NULL, false, fmt, ap);
  va_end(ap);
}

// -----------------------
JSONFormatter::JSONFormatter(bool p)
  : m_pretty(p), m_is_pending_string(false)
{
  reset();
}

void JSONFormatter::flush(std::ostream& os)
{
  finish_pending_string();
  os << m_ss.str();
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
  char *escaped = new char[len];
  escape_json_attr(s, escaped);
  m_ss << '\"' << escaped << '\"';
  delete[] escaped;
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
      if (entry.size)
	m_ss << "  ";
      else
	m_ss << " ";
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
  print_name(name);
  char foo[30];
  snprintf(foo, sizeof(foo), "%lf", d);
  m_ss << foo;
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

void JSONFormatter::dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap)
{
  char buf[LARGE_SIZE];
  vsnprintf(buf, LARGE_SIZE, fmt, ap);

  print_name(name);
  if (quoted) {
    print_quoted_string(buf);
  } else {
    m_ss << buf;
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
  os << m_ss.str();
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
}

void XMLFormatter::open_object_section(const char *name)
{
  open_section_in_ns(name, NULL, NULL);
}

void XMLFormatter::open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs)
{
  open_section_in_ns(name, NULL, &attrs);
}

void XMLFormatter::open_object_section_in_ns(const char *name, const char *ns)
{
  open_section_in_ns(name, ns, NULL);
}

void XMLFormatter::open_array_section(const char *name)
{
  open_section_in_ns(name, NULL, NULL);
}

void XMLFormatter::open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs)
{
  open_section_in_ns(name, NULL, &attrs);
}

void XMLFormatter::open_array_section_in_ns(const char *name, const char *ns)
{
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
  print_spaces();
  m_ss << "<" << e << ">" << u << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_int(const char *name, int64_t u)
{
  std::string e(name);
  print_spaces();
  m_ss << "<" << e << ">" << u << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_float(const char *name, double d)
{
  std::string e(name);
  print_spaces();
  m_ss << "<" << e << ">" << d << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_string(const char *name, std::string s)
{
  std::string e(name);
  print_spaces();
  m_ss << "<" << e << ">" << escape_xml_str(s.c_str()) << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_string_with_attrs(const char *name, std::string s, const FormatterAttrs& attrs)
{
  std::string e(name);
  std::string attrs_str;
  get_attrs_str(&attrs, attrs_str);
  print_spaces();
  m_ss << "<" << e << attrs_str << ">" << escape_xml_str(s.c_str()) << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

std::ostream& XMLFormatter::dump_stream(const char *name)
{
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
  print_spaces();
  if (ns) {
    m_ss << "<" << e  << " xmlns=\"" << ns << "\">" << buf << "</" << e << ">";
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
  print_spaces();
  std::string attrs_str;

  if (attrs) {
    get_attrs_str(attrs, attrs_str);
  }

  if (ns) {
    m_ss << "<" << name << attrs_str << " xmlns=\"" << ns << "\">";
  }
  else {
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

}
