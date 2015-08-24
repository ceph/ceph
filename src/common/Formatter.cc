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
#include "TableFormatter.h"
#include "XMLFormatter.h"
#include "HTMLFormatter.h"
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

Formatter::Formatter() { }

Formatter::~Formatter() { }

Formatter *Formatter::create(const std::string &type,
			     const std::string& default_type,
			     const std::string& fallback)
{
  std::string mytype = type;
  if (mytype == "")
    mytype = default_type;

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

void Formatter::dump_format(const char *name, const char *fmt, ...)
{
  va_list ap;
  va_start(ap, fmt);
  dump_format_va(name, NULL, true, fmt, ap);
  va_end(ap);
}

void Formatter::dump_format_ns(const char *name, const char *ns, const char *fmt, ...)
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

} // namespace ceph
