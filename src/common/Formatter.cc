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

#include "Formatter.h"
#include "JSONFormatter.h"
#include "HTMLFormatter.h"
#include "TableFormatter.h"
#include "XMLFormatter.h"
#include "common/escape.h"
#include "common/StackStringStream.h"
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
  CachedStackStringStream css;

  css->fill('0');
  css->width(scale + 1);
  *css << num;
  auto len = css->strv().size();

  CachedStackStringStream css2;
  *css2 << css->strv().substr(0, len - scale)
        << "."
        << css->strv().substr(len - scale);
  return css2->str();
}

std::string
fixed_to_string(int64_t num, int scale)
{
  CachedStackStringStream css;

  bool neg = num < 0;
  if (neg) num = -num;

  css->fill('0');
  css->width(scale + 1);
  *css << num;
  auto len = css->strv().size();

  CachedStackStringStream css2;
  *css2 << (neg ? "-" : "")
        << css->strv().substr(0, len - scale)
        << "."
        << css->strv().substr(len - scale);
  return css2->str();
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

    attrs.emplace_back(s, val);
    s = va_arg(ap, char *);
  } while (s);
  va_end(ap);
}

void Formatter::write_bin_data(const char*, int){}

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
  CachedStackStringStream css;
  flush(*css);
  bl.append(css->strv());
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

}
