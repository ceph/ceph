// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_FORMATTER_H
#define CEPH_FORMATTER_H

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

namespace ceph {

  struct FormatterAttrs {
    std::list< std::pair<std::string, std::string> > attrs;

    FormatterAttrs(const char *attr, ...);
  };

  class Formatter {
  public:
    static Formatter *create(const std::string& type,
			     const std::string& default_type,
			     const std::string& fallback);
    static Formatter *create(const std::string& type,
			     const std::string& default_type) {
      return create(type, default_type, "");
    }
    static Formatter *create(const std::string& type) {
      return create(type, "json-pretty", "");
    }

    Formatter();
    virtual ~Formatter();

    virtual void flush(std::ostream& os) = 0;
    void flush(bufferlist &bl)
    {
      std::stringstream os;
      flush(os);
      bl.append(os.str());
    }
    virtual void reset() = 0;

    virtual void open_array_section(const char *name) = 0;
    virtual void open_array_section_in_ns(const char *name, const char *ns) = 0;
    virtual void open_object_section(const char *name) = 0;
    virtual void open_object_section_in_ns(const char *name, const char *ns) = 0;
    virtual void close_section() = 0;
    virtual void dump_unsigned(const char *name, uint64_t u) = 0;
    virtual void dump_int(const char *name, int64_t s) = 0;
    virtual void dump_float(const char *name, double d) = 0;
    virtual void dump_string(const char *name, const std::string& s) = 0;
    virtual void dump_bool(const char *name, bool b)
    {
      dump_format_unquoted(name, "%s", (b ? "true" : "false"));
    }
    template<typename T>
    void dump_object(const char *name, const T& foo) {
      open_object_section(name);
      foo.dump(this);
      close_section();
    }
    virtual std::ostream& dump_stream(const char *name) = 0;
    virtual void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap) = 0;
    virtual void dump_format(const char *name, const char *fmt, ...);
    virtual void dump_format_ns(const char *name, const char *ns, const char *fmt, ...);
    virtual void dump_format_unquoted(const char *name, const char *fmt, ...);
    virtual int get_len() const = 0;
    virtual void write_raw_data(const char *data) = 0;
    /* with attrs */
    virtual void open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs)
    {
      open_array_section(name);
    }
    virtual void open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs)
    {
      open_object_section(name);
    }
    virtual void dump_string_with_attrs(const char *name, const std::string& s, const FormatterAttrs& attrs)
    {
      dump_string(name, s);
    }
  };

}
#endif
