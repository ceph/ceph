// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_XML_FORMATTER_H
#define CEPH_XML_FORMATTER_H

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
  class XMLFormatter : public Formatter {
  public:
    static const char *XML_1_DTD;
    XMLFormatter(bool pretty = false);

    virtual void set_status(const char* status, const char* status_name) {};
    virtual void output_header();
    virtual void output_footer();

    void flush(std::ostream& os);
    void reset();
    void open_array_section(const char *name);
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

    /* with attrs */
    void open_array_section_with_attrs(const char *name, const FormatterAttrs& attrs);
    void open_object_section_with_attrs(const char *name, const FormatterAttrs& attrs);
    void dump_string_with_attrs(const char *name, const std::string& s, const FormatterAttrs& attrs);
  protected:
    void open_section_in_ns(const char *name, const char *ns, const FormatterAttrs *attrs);
    void finish_pending_string();
    void print_spaces();
    static std::string escape_xml_str(const char *str);
    void get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str);

    std::stringstream m_ss, m_pending_string;
    std::deque<std::string> m_sections;
    bool m_pretty;
    std::string m_pending_string_name;
    bool m_header_done;
  };

}

#endif
