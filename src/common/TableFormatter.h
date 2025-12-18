// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "common/Formatter.h"

#include <memory>
#include <vector>
#include <sstream>
#include <map>

namespace ceph {

  class TableFormatter : public Formatter {
  public:
    explicit TableFormatter(bool keyval = false);

    void set_status(int status, const char* status_name) override {};
    void output_header() override {};
    void output_footer() override {};
    void enable_line_break() override {};
    void flush(std::ostream& os) override;
    using Formatter::flush; // don't hide Formatter::flush(bufferlist &bl)
    void reset() override;
    void open_array_section(std::string_view name) override;
    void open_array_section_in_ns(std::string_view name, const char *ns) override;
    void open_object_section(std::string_view name) override;
    void open_object_section_in_ns(std::string_view name, const char *ns) override;

    void open_array_section_with_attrs(std::string_view name, const FormatterAttrs& attrs) override;
    void open_object_section_with_attrs(std::string_view name, const FormatterAttrs& attrs) override;

    void close_section() override;
    void dump_null(std::string_view name) override;
    void dump_unsigned(std::string_view name, uint64_t u) override;
    void dump_int(std::string_view name, int64_t s) override;
    void dump_float(std::string_view name, double d) override;
    void dump_string(std::string_view name, std::string_view s) override;
    void dump_format_va(std::string_view name, const char *ns, bool quoted, const char *fmt, va_list ap) override;
    void dump_string_with_attrs(std::string_view name, std::string_view s, const FormatterAttrs& attrs) override;
    std::ostream& dump_stream(std::string_view name) override;

    int get_len() const override;
    void write_raw_data(const char *data) override;
    void get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str) const;

  private:
    template <class T>
    void add_value(std::string_view name, T val);
    void open_section_in_ns(std::string_view name, const char *ns, const FormatterAttrs *attrs);
    std::vector< std::vector<std::pair<std::string, std::string> > > m_vec;
    std::stringstream m_ss;
    size_t m_vec_index(std::string_view name);
    std::string get_section_name(std::string_view name);
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
