// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "common/Formatter.h"

#include <deque>
#include <sstream>

namespace ceph {

  class XMLFormatter : public Formatter {
  public:
    static const char *XML_1_DTD;
    XMLFormatter(bool pretty = false, bool lowercased = false, bool underscored = true);

    void set_status(int status, const char* status_name) override {}
    void output_header() override;
    void output_footer() override;

    void enable_line_break() override { m_line_break_enabled = true; }
    void flush(std::ostream& os) override;
    using Formatter::flush; // don't hide Formatter::flush(bufferlist &bl)
    void reset() override;
    void open_array_section(std::string_view name) override;
    void open_array_section_in_ns(std::string_view name, const char *ns) override;
    void open_object_section(std::string_view name) override;
    void open_object_section_in_ns(std::string_view name, const char *ns) override;
    void close_section() override;
    void dump_null(std::string_view name) override;
    void dump_unsigned(std::string_view name, uint64_t u) override;
    void dump_int(std::string_view name, int64_t s) override;
    void dump_float(std::string_view name, double d) override;
    void dump_string(std::string_view name, std::string_view s) override;
    std::ostream& dump_stream(std::string_view name) override;
    void dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap) override;
    int get_len() const override;
    void write_raw_data(const char *data) override;
    void write_bin_data(const char* buff, int len) override;

    /* with attrs */
    void open_array_section_with_attrs(std::string_view name, const FormatterAttrs& attrs) override;
    void open_object_section_with_attrs(std::string_view name, const FormatterAttrs& attrs) override;
    void dump_string_with_attrs(std::string_view name, std::string_view s, const FormatterAttrs& attrs) override;

  protected:
    void open_section_in_ns(std::string_view name, const char *ns, const FormatterAttrs *attrs);
    void finish_pending_string();
    void print_spaces();
    void get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str) const;
    char to_lower_underscore(char c) const;
    std::string get_xml_name(std::string_view name) const;

    std::stringstream m_ss, m_pending_string;
    std::deque<std::string> m_sections;
    const bool m_pretty;
    const bool m_lowercased;
    const bool m_underscored;
    std::string m_pending_string_name;
    bool m_header_done;
    bool m_line_break_enabled = false;
  private:
    template <class T>
    void add_value(std::string_view name, T val);
  };

}
