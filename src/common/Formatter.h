// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_FORMATTER_H
#define CEPH_FORMATTER_H

#include "include/buffer_fwd.h"

#include <list>
#include <memory>
#include <string>

#include <stdarg.h>

namespace ceph {

  struct FormatterAttrs {
    std::list< std::pair<std::string, std::string> > attrs;

    FormatterAttrs(const char *attr, ...);
  };

  class Formatter {
  public:
    class ObjectSection {
      Formatter& formatter;

    public:
      ObjectSection(Formatter& f, std::string_view name) : formatter(f) {
        formatter.open_object_section(name);
      }
      ObjectSection(Formatter& f, std::string_view name, const char *ns) : formatter(f) {
        formatter.open_object_section_in_ns(name, ns);
      }
      ~ObjectSection() {
        formatter.close_section();
      }
    };
    class ArraySection {
      Formatter& formatter;

    public:
      ArraySection(Formatter& f, std::string_view name) : formatter(f) {
        formatter.open_array_section(name);
      }
      ArraySection(Formatter& f, std::string_view name, const char *ns) : formatter(f) {
        formatter.open_array_section_in_ns(name, ns);
      }
      ~ArraySection() {
        formatter.close_section();
      }
    };

    static Formatter *create(std::string_view type,
			     std::string_view default_type,
			     std::string_view fallback);
    static Formatter *create(std::string_view type,
			     std::string_view default_type) {
      return create(type, default_type, "");
    }
    static Formatter *create(std::string_view type) {
      return create(type, "json-pretty", "");
    }
    template <typename... Params>
    static std::unique_ptr<Formatter> create_unique(Params &&...params)
    {
      return std::unique_ptr<Formatter>(
	  Formatter::create(std::forward<Params>(params)...));
    }

    Formatter() = default;
    virtual ~Formatter() = default;

    virtual void enable_line_break() = 0;
    virtual void flush(std::ostream& os) = 0;
    void flush(bufferlist &bl);
    virtual void reset() = 0;

    virtual void set_status(int status, const char* status_name) = 0;
    virtual void output_header() = 0;
    virtual void output_footer() = 0;

    virtual void open_array_section(std::string_view name) = 0;
    virtual void open_array_section_in_ns(std::string_view name, const char *ns) = 0;
    virtual void open_object_section(std::string_view name) = 0;
    virtual void open_object_section_in_ns(std::string_view name, const char *ns) = 0;
    virtual void close_section() = 0;
    virtual void dump_null(std::string_view name) = 0;
    virtual void dump_unsigned(std::string_view name, uint64_t u) = 0;
    virtual void dump_int(std::string_view name, int64_t s) = 0;
    virtual void dump_float(std::string_view name, double d) = 0;
    virtual void dump_string(std::string_view name, std::string_view s) = 0;
    virtual void dump_bool(std::string_view name, bool b)
    {
      dump_format_unquoted(name, "%s", (b ? "true" : "false"));
    }
    template<typename T>
    void dump_object(std::string_view name, const T& foo) {
      open_object_section(name);
      foo.dump(this);
      close_section();
    }
    virtual std::ostream& dump_stream(std::string_view name) = 0;
    virtual void dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap) = 0;
    virtual void dump_format(std::string_view name, const char *fmt, ...);
    virtual void dump_format_ns(std::string_view name, const char *ns, const char *fmt, ...);
    virtual void dump_format_unquoted(std::string_view name, const char *fmt, ...);
    virtual int get_len() const = 0;
    virtual void write_raw_data(const char *data) = 0;
    /* with attrs */
    virtual void open_array_section_with_attrs(std::string_view name, const FormatterAttrs& attrs)
    {
      open_array_section(name);
    }
    virtual void open_object_section_with_attrs(std::string_view name, const FormatterAttrs& attrs)
    {
      open_object_section(name);
    }
    virtual void dump_string_with_attrs(std::string_view name, std::string_view s, const FormatterAttrs& attrs)
    {
      dump_string(name, s);
    }

    virtual void *get_external_feature_handler(const std::string& feature) {
      return nullptr;
    }
    virtual void write_bin_data(const char* buff, int buf_len);
  };

  std::string fixed_to_string(int64_t num, int scale);
  std::string fixed_u_to_string(uint64_t num, int scale);
}
#endif

