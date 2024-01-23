// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_FORMATTER_H
#define CEPH_FORMATTER_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"

#include <deque>
#include <list>
#include <vector>
#include <stdarg.h>
#include <sstream>
#include <map>

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

    Formatter();
    virtual ~Formatter();

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

  class copyable_sstream : public std::stringstream {
  public:
    copyable_sstream() {}
    copyable_sstream(const copyable_sstream& rhs) {
      str(rhs.str());
    }
    copyable_sstream& operator=(const copyable_sstream& rhs) {
      str(rhs.str());
      return *this;
    }
  };

  class JSONFormatter : public Formatter {
  public:
    explicit JSONFormatter(bool p = false);

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

  private:

    struct json_formatter_stack_entry_d {
      int size;
      bool is_array;
      json_formatter_stack_entry_d() : size(0), is_array(false) { }
    };

    bool m_pretty;
    void open_section(std::string_view name, const char *ns, bool is_array);
    void print_quoted_string(std::string_view s);
    void print_name(std::string_view name);
    void print_comma(json_formatter_stack_entry_d& entry);
    void finish_pending_string();

    template <class T>
    void add_value(std::string_view name, T val);
    void add_value(std::string_view name, std::string_view val, bool quoted);

    copyable_sstream m_ss;
    copyable_sstream m_pending_string;
    std::string m_pending_name;
    std::list<json_formatter_stack_entry_d> m_stack;
    bool m_is_pending_string;
    bool m_line_break_enabled = false;
  };

  template <class T>
  void add_value(std::string_view name, T val);

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
    void get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str);
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
    void get_attrs_str(const FormatterAttrs *attrs, std::string& attrs_str);

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

  std::string fixed_to_string(int64_t num, int scale);
  std::string fixed_u_to_string(uint64_t num, int scale);
}
#endif

