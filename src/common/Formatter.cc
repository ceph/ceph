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
#include "common/escape.h"
#include "include/buffer.h"

#include <fmt/format.h>
#include <algorithm>
#include <set>
#include <limits>

// -----------------------
namespace ceph {

std::string
fixed_u_to_string(uint64_t num, int scale)
{
	std::ostringstream t;

	t.fill('0');
	t.width(scale + 1);
	t << num;
	int len = t.str().size();
	return t.str().substr(0,len - scale) + "." + t.str().substr(len - scale);
}

std::string
fixed_to_string(int64_t num, int scale)
{
	std::ostringstream t;
	bool neg = num < 0;
	if (neg) num = -num;

	t.fill('0');
	t.width(scale + 1);
	t << num;
	int len = t.str().size();
	return (neg ? "-" : "") + t.str().substr(0,len - scale) + "." + t.str().substr(len - scale);
}

/*
 * FormatterAttrs(const char *attr, ...)
 *
 * Requires a list of attrs followed by NULL. The attrs should be char *
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

void Formatter::write_bin_data(const char*, int){}

Formatter::Formatter() { }

Formatter::~Formatter() { }

Formatter *Formatter::create(std::string_view type,
			     std::string_view default_type,
			     std::string_view fallback)
{
  std::string_view mytype(type);
  if (mytype.empty()) {
    mytype = default_type;
  }

  if (mytype == "json")
    return new JSONFormatter(false);
  else if (mytype == "json-pretty")
    return new JSONFormatter(true);
  else if (mytype == "xml")
    return new XMLFormatter(false);
  else if (mytype == "xml-pretty")
    return new XMLFormatter(true);
  else if (mytype == "table")
    return new TableFormatter();
  else if (mytype == "table-kv")
    return new TableFormatter(true);
  else if (mytype == "html")
    return new HTMLFormatter(false);
  else if (mytype == "html-pretty")
    return new HTMLFormatter(true);
  else if (fallback != "")
    return create(fallback, "", "");
  else
    return (Formatter *) NULL;
}


void Formatter::flush(bufferlist &bl)
{
  std::stringstream os;
  flush(os);
  bl.append(os.str());
}

void Formatter::dump_format(std::string_view name, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  dump_format_va(name, NULL, true, fmt, ap);
  va_end(ap);
}

void Formatter::dump_format_ns(std::string_view name, const char *ns, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  dump_format_va(name, ns, true, fmt, ap);
  va_end(ap);

}

void Formatter::dump_format_unquoted(std::string_view name, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  dump_format_va(name, NULL, false, fmt, ap);
  va_end(ap);
}

// -----------------------

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

template <class T>
void JSONFormatter::add_value(std::string_view name, T val)
{
  std::stringstream ss;
  ss.precision(std::numeric_limits<T>::max_digits10);
  ss << val;
  add_value(name, ss.str(), false);
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
  char buf[LARGE_SIZE];
  vsnprintf(buf, LARGE_SIZE, fmt, ap);

  add_value(name, buf, quoted);
}

int JSONFormatter::get_len() const
{
  return m_ss.tellp();
}

void JSONFormatter::write_raw_data(const char *data)
{
  get_ss() << data;
}

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
  char buf[LARGE_SIZE];
  size_t len = vsnprintf(buf, LARGE_SIZE, fmt, ap);
  auto e = get_xml_name(name);

  print_spaces();
  if (ns) {
    m_ss << "<" << e << " xmlns=\"" << ns << "\">" << xml_stream_escaper(std::string_view(buf, len)) << "</" << e << ">";
  } else {
    m_ss << "<" << e << ">" << xml_stream_escaper(std::string_view(buf, len)) << "</" << e << ">";
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
            os << fmt::format(" {:<{}}|",
                              m_vec[i][j].first, m_column_size[j] + 2);
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
      if (m_keyval) {
        os << "key::";
        os << m_vec[i][j].first;
        os << "=";
        os << "\"";
        os << m_vec[i][j].second;
        os << "\" ";
      } else {
        os << fmt::format("{:<{}}|", m_vec[i][j].second, m_column_size[j] + 2);
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

void TableFormatter::open_object_section(std::string_view name)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_object_section_with_attrs(std::string_view name, const FormatterAttrs& attrs)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_object_section_in_ns(std::string_view name, const char *ns)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_array_section(std::string_view name)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_array_section_with_attrs(std::string_view name, const FormatterAttrs& attrs)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_array_section_in_ns(std::string_view name, const char *ns)
{
  open_section_in_ns(name, NULL, NULL);
}

void TableFormatter::open_section_in_ns(std::string_view name, const char *ns, const FormatterAttrs *attrs)
{
  m_section.push_back(std::string(name));
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

size_t TableFormatter::m_vec_index(std::string_view name)
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

std::string TableFormatter::get_section_name(std::string_view name)
{
  std::string t_name{name};
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

template <class T>
void TableFormatter::add_value(std::string_view name, T val) {
  finish_pending_string();
  size_t i = m_vec_index(name);
  m_ss.precision(std::numeric_limits<double>::max_digits10);
  m_ss << val;

  m_vec[i].push_back(std::make_pair(get_section_name(name), m_ss.str()));
  m_ss.clear();
  m_ss.str("");
}

void TableFormatter::dump_null(std::string_view name)
{
  add_value(name, "null");
}

void TableFormatter::dump_unsigned(std::string_view name, uint64_t u)
{
  add_value(name, u);
}

void TableFormatter::dump_int(std::string_view name, int64_t s)
{
  add_value(name, s);
}

void TableFormatter::dump_float(std::string_view name, double d)
{
  add_value(name, d);
}

void TableFormatter::dump_string(std::string_view name, std::string_view s)
{
  finish_pending_string();
  size_t i = m_vec_index(name);
  m_ss << s;

  m_vec[i].push_back(std::make_pair(get_section_name(name), m_ss.str()));
  m_ss.clear();
  m_ss.str("");
}

void TableFormatter::dump_string_with_attrs(std::string_view name, std::string_view s, const FormatterAttrs& attrs)
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

void TableFormatter::dump_format_va(std::string_view name,
				    const char *ns, bool quoted,
				    const char *fmt, va_list ap)
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

std::ostream& TableFormatter::dump_stream(std::string_view name)
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
}

