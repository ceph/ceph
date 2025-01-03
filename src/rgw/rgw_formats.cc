// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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

#include <boost/format.hpp>

#include "common/escape.h"
#include "common/Formatter.h"
#include "rgw/rgw_common.h"
#include "rgw/rgw_formats.h"
#include "rgw/rgw_rest.h"

#define LARGE_SIZE 8192

#define dout_subsys ceph_subsys_rgw

using namespace std;

RGWFormatter_Plain::RGWFormatter_Plain(const bool ukv)
  : use_kv(ukv)
{
}

RGWFormatter_Plain::~RGWFormatter_Plain()
{
  free(buf);
}

void RGWFormatter_Plain::flush(ostream& os)
{
  if (!buf)
    return;

  if (len) {
    os << buf;
    os.flush();
  }

  reset_buf();
}

void RGWFormatter_Plain::reset_buf()
{
  free(buf);
  buf = NULL;
  len = 0;
  max_len = 0;
}

void RGWFormatter_Plain::reset()
{
  reset_buf();
  stack.clear();
  min_stack_level = 0;
}

void RGWFormatter_Plain::open_array_section(std::string_view name)
{
  struct plain_stack_entry new_entry;
  new_entry.is_array = true;
  new_entry.size = 0;

  if (use_kv && min_stack_level > 0 && !stack.empty()) {
    struct plain_stack_entry& entry = stack.back();

    if (!entry.is_array)
      dump_format(name, "");
  }

  stack.push_back(new_entry);
}

void RGWFormatter_Plain::open_array_section_in_ns(std::string_view name, const char *ns)
{
  ostringstream oss;
  oss << name << " " << ns;
  open_array_section(oss.str().c_str());
}

void RGWFormatter_Plain::open_object_section(std::string_view name)
{
  struct plain_stack_entry new_entry;
  new_entry.is_array = false;
  new_entry.size = 0;

  if (use_kv && min_stack_level > 0)
    dump_format(name, "");

  stack.push_back(new_entry);
}

void RGWFormatter_Plain::open_object_section_in_ns(std::string_view name,
						   const char *ns)
{
  ostringstream oss;
  oss << name << " " << ns;
  open_object_section(oss.str().c_str());
}

void RGWFormatter_Plain::close_section()
{
  stack.pop_back();
}

void RGWFormatter_Plain::dump_null(std::string_view name)
{
  dump_value_int(name, "null"); /* I feel a little bad about this. */
}

void RGWFormatter_Plain::dump_unsigned(std::string_view name, uint64_t u)
{
  dump_value_int(name, "%" PRIu64, u);
}

void RGWFormatter_Plain::dump_int(std::string_view name, int64_t u)
{
  dump_value_int(name, "%" PRId64, u);
}

void RGWFormatter_Plain::dump_float(std::string_view name, double d)
{
  dump_value_int(name, "%f", d);
}

void RGWFormatter_Plain::dump_string(std::string_view name, std::string_view s)
{
  dump_format(name, "%.*s", s.size(), s.data());
}

std::ostream& RGWFormatter_Plain::dump_stream(std::string_view name)
{
  // TODO: implement this!
  ceph_abort();
}

void RGWFormatter_Plain::dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap)
{
  char buf[LARGE_SIZE];

  struct plain_stack_entry& entry = stack.back();

  if (!min_stack_level)
    min_stack_level = stack.size();

  bool should_print = ((stack.size() == min_stack_level && !entry.size) || use_kv);

  entry.size++;

  if (!should_print)
    return;

  vsnprintf(buf, LARGE_SIZE, fmt, ap);

  const char *eol;
  if (wrote_something) {
    if (use_kv && entry.is_array && entry.size > 1)
      eol = ", ";
    else
      eol = "\n";
  } else
    eol = "";
  wrote_something = true;

  if (use_kv && !entry.is_array) {
    write_data("%s%.*s: %s", eol, static_cast<int>(name.size()), name.data(), buf);
  } else {
    write_data("%s%s", eol, buf);
  }
}

int RGWFormatter_Plain::get_len() const
{
  // don't include null termination in length
  return (len ? len - 1 : 0);
}

void RGWFormatter_Plain::write_raw_data(const char *data)
{
  write_data("%s", data);
}

void RGWFormatter_Plain::write_data(const char *fmt, ...)
{
#define LARGE_ENOUGH_LEN 128
  int n, size = LARGE_ENOUGH_LEN;
  char s[size + 8];
  char *p, *np;
  bool p_on_stack;
  va_list ap;
  int pos;

  p = s;
  p_on_stack = true;

  while (1) {
    va_start(ap, fmt);
    n = vsnprintf(p, size, fmt, ap);
    va_end(ap);

    if (n > -1 && n < size)
      goto done;
    /* Else try again with more space. */
    if (n > -1)    /* glibc 2.1 */
      size = n+1; /* precisely what is needed */
    else           /* glibc 2.0 */
      size *= 2;  /* twice the old size */
    if (p_on_stack)
      np = (char *)malloc(size + 8);
    else
      np = (char *)realloc(p, size + 8);
    if (!np)
      goto done_free;
    p = np;
    p_on_stack = false;
  }
done:
#define LARGE_ENOUGH_BUF 4096
  if (!buf) {
    max_len = std::max(LARGE_ENOUGH_BUF, size);
    buf = (char *)malloc(max_len);
    if (!buf) {
      cerr << "ERROR: RGWFormatter_Plain::write_data: failed allocating " << max_len << " bytes" << std::endl;
      goto done_free;
    }
  }

  if (len + size > max_len) {
    max_len = len + size + LARGE_ENOUGH_BUF;
    void *_realloc = NULL;
    if ((_realloc = realloc(buf, max_len)) == NULL) {
      cerr << "ERROR: RGWFormatter_Plain::write_data: failed allocating " << max_len << " bytes" << std::endl;
      goto done_free;
    } else {
      buf = (char *)_realloc;
    }
  }

  pos = len;
  if (len)
    pos--; // squash null termination
  strcpy(buf + pos, p);
  len = pos + strlen(p) + 1;
done_free:
  if (!p_on_stack)
    free(p);
}

void RGWFormatter_Plain::dump_value_int(std::string_view name, const char *fmt, ...)
{
  char buf[LARGE_SIZE];
  va_list ap;

  if (!min_stack_level)
    min_stack_level = stack.size();

  struct plain_stack_entry& entry = stack.back();
  bool should_print = ((stack.size() == min_stack_level && !entry.size) || use_kv);

  entry.size++;

  if (!should_print)
    return;

  va_start(ap, fmt);
  vsnprintf(buf, LARGE_SIZE, fmt, ap);
  va_end(ap);

  const char *eol;
  if (wrote_something) {
    eol = "\n";
  } else
    eol = "";
  wrote_something = true;

  if (use_kv && !entry.is_array) {
    write_data("%s%.*s: %s", eol, static_cast<int>(name.size()), name.data(), buf);
  } else {
    write_data("%s%s", eol, buf);
  }

}


/* An utility class that serves as a mean to access the protected static
 * methods of XMLFormatter. */
class HTMLHelper : public XMLFormatter {
public:
  static std::string escape(const std::string& unescaped_str) {
    int len = escape_xml_attr_len(unescaped_str.c_str());
    std::string escaped(len, 0);
    escape_xml_attr(unescaped_str.c_str(), escaped.data());
    return escaped;
  }
};

void RGWSwiftWebsiteListingFormatter::generate_header(
  const std::string& dir_path,
  const std::string& css_path)
{
  ss << R"(<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 )"
     << R"(Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">)";

  ss << "<html><head><title>Listing of " << xml_stream_escaper(dir_path)
     << "</title>";

  if (! css_path.empty()) {
    ss << boost::format(R"(<link rel="stylesheet" type="text/css" href="%s" />)")
                                % url_encode(css_path);
  } else {
    ss << R"(<style type="text/css">)"
       << R"(h1 {font-size: 1em; font-weight: bold;})"
       << R"(th {text-align: left; padding: 0px 1em 0px 1em;})"
       << R"(td {padding: 0px 1em 0px 1em;})"
       << R"(a {text-decoration: none;})"
       << R"(</style>)";
  }

  ss << "</head><body>";

  ss << R"(<h1 id="title">Listing of )" << xml_stream_escaper(dir_path) << "</h1>"
     << R"(<table id="listing">)"
     << R"(<tr id="heading">)"
     << R"(<th class="colname">Name</th>)"
     << R"(<th class="colsize">Size</th>)"
     << R"(<th class="coldate">Date</th>)"
     << R"(</tr>)";

  if (! prefix.empty()) {
    ss << R"(<tr id="parent" class="item">)"
       << R"(<td class="colname"><a href="../">../</a></td>)"
       << R"(<td class="colsize">&nbsp;</td>)"
       << R"(<td class="coldate">&nbsp;</td>)"
       << R"(</tr>)";
  }
}

void RGWSwiftWebsiteListingFormatter::generate_footer()
{
  ss << R"(</table></body></html>)";
}

std::string RGWSwiftWebsiteListingFormatter::format_name(
  const std::string& item_name) const
{
  return item_name.substr(prefix.length());
}

void RGWSwiftWebsiteListingFormatter::dump_object(const rgw_bucket_dir_entry& objent)
{
  const auto name = format_name(objent.key.name);
  ss << boost::format(R"(<tr class="item %s">)")
                                % "default"
     << boost::format(R"(<td class="colname"><a href="%s">%s</a></td>)")
                                % url_encode(name)
                                % HTMLHelper::escape(name)
     << boost::format(R"(<td class="colsize">%lld</td>)") % objent.meta.size
     << boost::format(R"(<td class="coldate">%s</td>)")
                                % dump_time_to_str(objent.meta.mtime)
     << R"(</tr>)";
}

void RGWSwiftWebsiteListingFormatter::dump_subdir(const std::string& name)
{
  const auto fname = format_name(name);
  ss << R"(<tr class="item subdir">)"
     << boost::format(R"(<td class="colname"><a href="%s">%s</a></td>)")
                                % url_encode(fname)
                                % HTMLHelper::escape(fname)
     << R"(<td class="colsize">&nbsp;</td>)"
     << R"(<td class="coldate">&nbsp;</td>)"
     << R"(</tr>)";
}
