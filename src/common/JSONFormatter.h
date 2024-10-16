// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "common/Formatter.h"

#include <sstream>
#include <vector>

namespace ceph {

  class JSONFormatter : public Formatter {
  public:
    explicit JSONFormatter(bool p = false) : m_pretty(p) {}
    JSONFormatter(const JSONFormatter& f) :
      m_pretty(f.m_pretty),
      m_pending_name(f.m_pending_name),
      m_stack(f.m_stack),
      m_is_pending_string(f.m_is_pending_string),
      m_line_break_enabled(f.m_line_break_enabled)
    {
      m_ss.str(f.m_ss.str());
      m_pending_string.str(f.m_pending_string.str());
    }
    JSONFormatter(JSONFormatter&& f) :
      m_pretty(f.m_pretty),
      m_ss(std::move(f.m_ss)),
      m_pending_string(std::move(f.m_pending_string)),
      m_pending_name(f.m_pending_name),
      m_stack(std::move(f.m_stack)),
      m_is_pending_string(f.m_is_pending_string),
      m_line_break_enabled(f.m_line_break_enabled)
    {
    }
    JSONFormatter& operator=(const JSONFormatter& f)
    {
      m_pretty = f.m_pretty;
      m_ss.str(f.m_ss.str());
      m_pending_string.str(f.m_pending_string.str());
      m_pending_name = f.m_pending_name;
      m_stack = f.m_stack;
      m_is_pending_string = f.m_is_pending_string;
      m_line_break_enabled = f.m_line_break_enabled;
      return *this;
    }

    JSONFormatter& operator=(JSONFormatter&& f)
    {
      m_pretty = f.m_pretty;
      m_ss = std::move(f.m_ss);
      m_pending_string = std::move(f.m_pending_string);
      m_pending_name = f.m_pending_name;
      m_stack = std::move(f.m_stack);
      m_is_pending_string = f.m_is_pending_string;
      m_line_break_enabled = f.m_line_break_enabled;
      return *this;
    }

    void set_status(int status, const char* status_name) override {};
    void output_header() override {};
    void output_footer() override {};
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

protected:
    virtual bool handle_value(std::string_view name, std::string_view s, bool quoted) {
      return false; /* is handling done? */
    }

    virtual bool handle_open_section(std::string_view name, const char *ns, bool is_array) {
      return false; /* is handling done? */
    }

    virtual bool handle_close_section() {
      return false; /* is handling done? */
    }

    int stack_size() { return m_stack.size(); }

    virtual std::ostream& get_ss() {
      return m_ss;
    }

    void finish_pending_string();

private:
    struct json_formatter_stack_entry_d {
      int size = 0;
      bool is_array = false;
    };

    bool m_pretty = false;
    void open_section(std::string_view name, const char *ns, bool is_array);
    void print_quoted_string(std::string_view s);
    void print_name(std::string_view name);
    void print_comma(json_formatter_stack_entry_d& entry);
    void add_value(std::string_view name, double val);

    template <class T>
    void add_value(std::string_view name, T val);
    void add_value(std::string_view name, std::string_view val, bool quoted);

    mutable std::stringstream m_ss; // mutable for get_len
    std::stringstream m_pending_string;
    std::string m_pending_name;
    std::vector<json_formatter_stack_entry_d> m_stack;
    bool m_is_pending_string = false;
    bool m_line_break_enabled = false;
  };

}
