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

#include "XMLFormatter.h"
#include "common/escape.h"
#include "common/StackStringStream.h"
#include "include/ceph_assert.h"

#include <boost/container/small_vector.hpp>

#include <algorithm>
#include <limits>
#include <utility>

#define LARGE_SIZE 1024

namespace ceph {

const char *XMLFormatter::XML_1_DTD =
  "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

XMLFormatter::XMLFormatter(bool pretty, bool lowercased, bool underscored)
: m_pretty(pretty),
  m_lowercased(lowercased),
  m_underscored(underscored)
{
  reset();
}

void XMLFormatter::flush(std::ostream& os)
{
  finish_pending_string();
  std::string m_ss_str = m_ss.str();
  os << m_ss_str;
  /* There is a small catch here. If the rest of the formatter had NO output,
   * we should NOT output a newline. This primarily triggers on HTTP redirects */
  if (m_pretty && !m_ss_str.empty())
    os << "\n";
  else if (m_line_break_enabled)
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
    write_raw_data(XMLFormatter::XML_1_DTD);
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

void XMLFormatter::open_object_section(std::string_view name)
{
  open_section_in_ns(name, NULL, NULL);
}

void XMLFormatter::open_object_section_with_attrs(std::string_view name, const FormatterAttrs& attrs)
{
  open_section_in_ns(name, NULL, &attrs);
}

void XMLFormatter::open_object_section_in_ns(std::string_view name, const char *ns)
{
  open_section_in_ns(name, ns, NULL);
}

void XMLFormatter::open_array_section(std::string_view name)
{
  open_section_in_ns(name, NULL, NULL);
}

void XMLFormatter::open_array_section_with_attrs(std::string_view name, const FormatterAttrs& attrs)
{
  open_section_in_ns(name, NULL, &attrs);
}

void XMLFormatter::open_array_section_in_ns(std::string_view name, const char *ns)
{
  open_section_in_ns(name, ns, NULL);
}

std::string XMLFormatter::get_xml_name(std::string_view name) const
{
  std::string e(name);
  std::transform(e.begin(), e.end(), e.begin(),
      [this](char c) { return this->to_lower_underscore(c); });
  return e;
}

void XMLFormatter::close_section()
{
  ceph_assert(!m_sections.empty());
  finish_pending_string();

  auto section = get_xml_name(m_sections.back());
  m_sections.pop_back();
  print_spaces();
  m_ss << "</" << section << ">";
  if (m_pretty)
    m_ss << "\n";
}

template <class T>
void XMLFormatter::add_value(std::string_view name, T val)
{
  auto e = get_xml_name(name);
  print_spaces();
  m_ss.precision(std::numeric_limits<T>::max_digits10);
  m_ss << "<" << e << ">" << val << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_null(std::string_view name)
{
  print_spaces();
  m_ss << "<" << get_xml_name(name) << " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:nil=\"true\" />";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_unsigned(std::string_view name, uint64_t u)
{
  add_value(name, u);
}

void XMLFormatter::dump_int(std::string_view name, int64_t s)
{
  add_value(name, s);
}

void XMLFormatter::dump_float(std::string_view name, double d)
{
  add_value(name, d);
}

void XMLFormatter::dump_string(std::string_view name, std::string_view s)
{
  auto e = get_xml_name(name);
  print_spaces();
  m_ss << "<" << e << ">" << xml_stream_escaper(s) << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

void XMLFormatter::dump_string_with_attrs(std::string_view name, std::string_view s, const FormatterAttrs& attrs)
{
  auto e = get_xml_name(name);
  std::string attrs_str;
  get_attrs_str(&attrs, attrs_str);
  print_spaces();
  m_ss << "<" << e << attrs_str << ">" << xml_stream_escaper(s) << "</" << e << ">";
  if (m_pretty)
    m_ss << "\n";
}

std::ostream& XMLFormatter::dump_stream(std::string_view name)
{
  print_spaces();
  m_pending_string_name = name;
  m_ss << "<" << m_pending_string_name << ">";
  return m_pending_string;
}

void XMLFormatter::dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap)
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

  auto e = get_xml_name(name);

  print_spaces();
  if (ns) {
    m_ss << "<" << e << " xmlns=\"" << ns << "\">" << xml_stream_escaper(std::string_view(buf.data(), len)) << "</" << e << ">";
  } else {
    m_ss << "<" << e << ">" << xml_stream_escaper(std::string_view(buf.data(), len)) << "</" << e << ">";
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

void XMLFormatter::write_bin_data(const char* buff, int buf_len)
{
  std::stringbuf *pbuf = m_ss.rdbuf();
  pbuf->sputn(buff, buf_len);
  m_ss.seekg(buf_len);
}

void XMLFormatter::get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str) const
{
  CachedStackStringStream css;

  for (const auto &p : attrs->attrs) {
    *css << " " << p.first << "=" << "\"" << p.second << "\"";
  }

  attrs_str = css->strv();
}

void XMLFormatter::open_section_in_ns(std::string_view name, const char *ns, const FormatterAttrs *attrs)
{
  print_spaces();
  std::string attrs_str;

  if (attrs) {
    get_attrs_str(attrs, attrs_str);
  }

  auto e = get_xml_name(name);

  if (ns) {
    m_ss << "<" << e << attrs_str << " xmlns=\"" << ns << "\">";
  } else {
    m_ss << "<" << e << attrs_str << ">";
  }
  if (m_pretty)
    m_ss << "\n";
  m_sections.push_back(std::string(name));
}

void XMLFormatter::finish_pending_string()
{
  if (!m_pending_string_name.empty()) {
    m_ss << xml_stream_escaper(m_pending_string.str())
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

char XMLFormatter::to_lower_underscore(char c) const
{
  if (m_underscored && c == ' ') {
      return '_';
  } else if (m_lowercased) {
    return std::tolower(c);
  }
  return c;
}

}
