// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_JSON_FORMATTER_H
#define CEPH_JSON_FORMATTER_H

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
  class JSONFormatter : public Formatter {
  public:
    JSONFormatter(bool p = false);

    void flush(std::ostream& os);
    void reset();
    virtual void open_array_section(const char *name);
    void open_array_section_in_ns(const char *name, const char *ns);
    void open_object_section(const char *name);
    void open_object_section_in_ns(const char *name, const char *ns);
    void close_section();
    void dump_unsigned(const char *name, uint64_t u);
    void dump_int(const char *name, int64_t u);
    void dump_float(const char *name, double d);
    void dump_string(const char *name, const std::string& s);
    std::ostream& dump_stream(const char *name);
    void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap);
    int get_len() const;
    void write_raw_data(const char *data);

  private:

    struct json_formatter_stack_entry_d {
      int size;
      bool is_array;
      json_formatter_stack_entry_d() : size(0), is_array(false) { }
    };

    bool m_pretty;
    void open_section(const char *name, bool is_array);
    void print_quoted_string(const std::string& s);
    void print_name(const char *name);
    void print_comma(json_formatter_stack_entry_d& entry);
    void finish_pending_string();

    std::stringstream m_ss, m_pending_string;
    std::list<json_formatter_stack_entry_d> m_stack;
    bool m_is_pending_string;
  };

}

#endif
