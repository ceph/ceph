// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_TABLE_FORMATTER_H
#define CEPH_TABLE_FORMATTER_H

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
  class TableFormatter : public Formatter {
  public:
    TableFormatter(bool keyval = false);

    virtual void set_status(const char* status, const char* status_name) {};
    virtual void output_header() {};
    virtual void output_footer() {};
    void flush(std::ostream& os);
    void reset();
    virtual void open_array_section(const char *name);
    void open_array_section_in_ns(const char *name, const char *ns);
    void open_object_section(const char *name);
    void open_object_section_in_ns(const char *name, const char *ns);

    void open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs);
    void open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs);

    void close_section();
    void dump_unsigned(const char *name, uint64_t u);
    void dump_int(const char *name, int64_t u);
    void dump_float(const char *name, double d);
    void dump_string(const char *name, const std::string& s);
    void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap);
    void dump_string_with_attrs(const char *name, const std::string& s, const FormatterAttrs& attrs);
    std::ostream& dump_stream(const char *name);

    int get_len() const;
    void write_raw_data(const char *data);
    void get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str);

  private:
    void open_section_in_ns(const char *name, const char *ns, const FormatterAttrs *attrs);
    std::vector< std::vector<std::pair<std::string, std::string> > > m_vec;
    std::stringstream m_ss;
    size_t m_vec_index(const char* name);
    std::string get_section_name(const char* name);
    void finish_pending_string();
    std::string m_pending_name;
    bool m_keyval;

    int m_section_open;
    std::vector< std::string > m_section;
    std::map<std::string, int> m_section_cnt;
    std::vector<size_t> m_column_size;
    std::vector< std::string > m_column_name;
  };


}

#endif
