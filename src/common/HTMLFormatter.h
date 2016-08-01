// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_HTML_FORMATTER_H
#define CEPH_HTML_FORMATTER_H

#include "include/int_types.h"

#include <deque>
#include <iosfwd>
#include <list>
#include <vector>
#include <sstream>
#include <stdarg.h>
#include <string>
#include <map>

#include "include/buffer.h"
#include "Formatter.h"

namespace ceph {
  class HTMLFormatter : public XMLFormatter {
  public:
    explicit HTMLFormatter(bool pretty = false);
    ~HTMLFormatter();
    void reset();

    virtual void set_status(int status, const char* status_name);
    virtual void output_header();

    void dump_unsigned(const char *name, uint64_t u);
    void dump_int(const char *name, int64_t u);
    void dump_float(const char *name, double d);
    void dump_string(const char *name, const std::string& s);
    std::ostream& dump_stream(const char *name);
    void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap);

    /* with attrs */
    void dump_string_with_attrs(const char *name, const std::string& s, const FormatterAttrs& attrs);
  private:
    template <typename T> void dump_template(const char *name, T arg);

    int m_status;
    const char* m_status_name;
  };

}

#endif
