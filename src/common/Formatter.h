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
#include <functional>

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


  /**
   * @class Trimmer
   *
   * Trimmer is a transform for @ref Formatter that eliminates unwanted sections of data structure.
   *
   * Trimmer treats incoming sequence of @ref Formatter calls as tree.
   * Pairs of @ref Formatter::open_array_section / @ref Formatter::open_object_section and
   * @ref Formatter::close_section form branches of tree, and functions @ref Formatter.dump* form leaves.
   * Both branches and leaves are named, and identified by path leading to them.
   * Trimmer treats branches and leaves equally.
   * Rules that exclude branches are the same as ones that exclude leaves.
   *
   * Trimmer preserves tree paths. When some branch is excluded
   * but a far subbranch is included then Trimmer reconstructs missing path.
   *
   * Example (XML):
   * Rule "-a.b, +a.b.c2.d" transforms
   * <a><b><c0>X</c0><c1>Y</c1><c2><d>Z</d></c2></b></a>
   * into
   * <a><b><c2><d>Z</d></c2></b></a> .
   *
   * Rule is defined as sequence of paths. Each path can either be inclusion or exclustion,
   * denoted by prefix "+" or prefix "-", respectively. Lack of prefix means inclusion.
   * Rules are applied in sequence. Next rule always overrides previous rule.
   * So, exclusion of specialized path is countered by inclusion of more general path specified later.
   */
  class Trimmer {
  public:
    virtual ~Trimmer() {}
    /**
     * @brief
     * Creates Trimmer instance
     *
     * @retval newly created Trimmer
     */
    static Trimmer* create();
    /**
     * @brief
     * Sets up rule for trimming data tree
     *
     * Sets up tree filtering behaviour for all @ref Formatters obtained by @ref attach.
     * @c stencil is a comma "," separated list of multipart branches.
     * Each multipart branch is prefixed with "+" or "-" that denote inclusion and exclustion respectively.
     * Lack of prefix is considered to be inclusion "+".
     * Branches are separated by dot ".".
     * Example: "+objects,-objects.id".
     *
     * If error in parsing @c stencil occurs, @c error contains human-readable,
     * possibly multiline text containing problem description.
     *
     * A Trimmer configured with @ref setup can be used multiple times
     * as long as previous @ref attach are paired with @ref detach.
     *
     * @param stencil[in] Tree-trimming rule.
     * @param error[out] Error description
     *
     * @retval true - @c stencil successfully parsed, @c error is empty.
     * @retval false - @c stencil invalid, @c contains error.
     */
    virtual bool setup(std::string_view stencil, std::string& error) = 0;
    /**
     * @brief
     * Connects formatter for processing
     *
     * Prepares Trimmer for transforming @ref ceph::Formatter operations.
     * Returned @ref Formatter is input that should recieve structuralized data to be formatted.
     * Only one @ref Formatter can be connected at any time.
     * One has to @ref detach before calling @ref attach again.
     *
     * @param sink[in] Formatter that is used by Trimmer as output
     *
     * @retval Formatter input, stream data into this.
     *
     * @note Returned formatter is an internal object, do not delete it.
     */
    virtual Formatter* attach(Formatter* sink) = 0;
    /**
     * @brief
     * Disconnects formatter
     *
     * Complementary operation to @ref attach.
     * After it is called, it is illegal to use @ref Formatter obtained by previous call to @ref attach.
     */
    virtual void detach() = 0;
  };


  /**
   * @class Buffer
   *
   * Buffer is a @ref Formatter transform that either passes input unchanged or eliminates it completely
   * Formatting data streamed to input is stored (copied) into internal buffers.
   * When @ref unbuffer is called, stored formatter input is sent to output.
   * After that @ref Buffer ceases its buffering behaviour until @ref attach is called again.
   */
  class Buffer {
  public:
    virtual ~Buffer() {};
    /**
     * @brief
     * Creates Buffer instance
     *
     * @retval newly created Buffer
     */
    static Buffer* create();
    /**
     * @brief
     * Connects formatter for processing
     *
     * Prepares Buffer for incoming @ref ceph::Formatter operations.
     * Returned @ref Formatter is input that should recieve structuralized data to be formatted.
     * Only one @ref Formatter can be connected at any time.
     * One has to @ref detach before calling @ref attach again.
     *
     * @param sink[in] Formatter that will recieve data on @ref unbuffer
     *
     * @retval Formatter input, stream data into this.
     *
     * @note Returned formatter is an internal object, do not delete it.
     */
    virtual Formatter* attach(Formatter* sink) = 0;
    /**
     * @brief
     * Disconnects formatter
     *
     * Complementary operation to @ref attach. Deletes any stored formatter data.
     * After it is called, it is illegal to use @ref Formatter obtained by previous call to @ref attach.
     */
    virtual void detach() = 0;
    /**
     * @brief
     * Disconnects formatter
     *
     * Complementary operation to @ref attach. Deletes any stored formatter data.
     * After it is called, it is illegal to use @ref Formatter obtained by previous call to @ref attach.
     */
    virtual void unbuffer() = 0;
  };

  /**
   * @class Filter
   *
   * Filter is a @ref Formatter transform that checks if data/structure matches selected condition.
   * Input data passes unchanged to output immediatelly.
   *
   * Examples:
   * 1) .object.size > 1000
   * 2) .object.name has "osdmap" && exists .object.shards
   * 3) (.object.shards.shard.offset + .object.shards.shard.len) % 0x1000 != 0
   *
   * Filter treats incoming sequence of @ref Formatter calls as tree.
   * Pairs of @ref Formatter::open_array_section / @ref Formatter::open_object_section and
   * @ref Formatter::close_section form branches of tree, and functions @ref Formatter.dump* form leaves.
   * Both branches and leaves are named, and identified by path leading to them.
   *
   * ::Condition::
   * Condition is an expression that is formed by combining values from tree,
   * operators and constants to give boolean result.
   * When condition is met @ref on_match function is immediatelly called.
   *
   * ::Expressions::
   * expression := literal |
   *               number |
   *               variable |
   *               '(' expression ')' |
   *               left_operator expression |
   *               expression binary_operator expression
   *
   * literal := '"' in-literal '"'
   * in-literal := |
   *              {single_character} in-literal |
   *              '\"' in-literal
   *
   * number := [0-9]* |
   *          '0x' [0-9a-f]*
   *
   * variable := branch |
   *            variable branch
   *
   * branch := '.' [_a-zA-Z0-9]*
   *
   * left_operator := 'exists' | '-' | '!'
   *
   * binary_operator := '==' | '!=' | '>=' | '<=' | '<' | '>' |
   *                    '&&' | '||' | 'has' | '+' | '-' |
   *                    '*' | '/' | '%'
   *
   * ::Variables::
   * Variables used in expressions are obtained from processed tree.
   * Names of variables are equal to branch paths. If path is leading to a leaf
   * (data put by @ref Formatter::dump* function) dumped value is stored as variable value.
   * If path is leading to a branch, variable does not have any value, but is marked as present.
   * Operator 'exists' is used to test if branch exists in tree.
   *
   * ::Condition matching::
   * A condition can be constructed using multiple variables.
   * When tree is processed current branch path is tracked. When branch path equals to any variable
   * that is part of condition, value is snapshot into variable.
   * Each time value of any variable changes, condition is evaluated.
   * If result of condition is true (after possible convertion to bool) @ref on_match function is called.
   * Each time setting variables makes condition true, @ref on_match will be invoked.
   *
   * ::Operators::
   * Boolean operators: == != && || !
   * Aritmetic operators: + - * / %
   * Relational operators: >= <= < > == !=
   * Substring operator: has
   *   A 'has' B is true iff B is a substring of A.
   * Structure inspection: exists
   *   'exists' A is true iff branch/leaf A exists in tree.
   *
   * ::Operator priorities::
   * (highest) 0: ! % / * exists has
   *           1: + -
   *           2: < > <= >= != ==
   * (lowest)  3: && ||
   * When operands between arguments are of same priority, left is evaluated first.
   *
   * ::Data types::
   * There are 3 data types used in expression evaluation: boolean, string, integer(signed 64bit).
   * Initial types are from variables are string and integer.
   * String type is used when @ref dump_string, @ref dump_float, @ref dump_stream or @ref dump_format_va are used.
   * Integer type is used when @ref dump_unsigned or @ref dump_int are used.
   * Boolean type is used when @ref dump_bool is used.
   *
   * ::Conversions::
   * When type of argument is not consistent with used unary operator,
   * or types of arguments are different for binary operators,
   * then conversion is performed. For binary operators right argument is converted into type of left argument.
   *
   * Integer to string conversion does signed 64 bit print into string.
   * String to integer conversion allows for optional '0x' hex interpretation.
   * Integer to boolean conversion maps 0->0, *->1.
   * Boolean to integer conversion maps 0->0, 1->1.
   */
  class Filter {
  public:
    virtual ~Filter() {};
    /**
     * @brief
     * Creates Filter instance
     *
     * @retval newly created Filter
     */
    static Filter* create();
    /**
     * @brief
     * Sets up matching condition
     *
     * Sets up condition that is used to validate if passed @ref Formatter structured data
     * adheres to required specification. Grammar for it is described in @ref class Filter.
     *
     * A Filter configured with @ref setup can be used multiple times
     * as long as previous @ref attach are paired with @ref detach.
     *
     * @param condition[in] Formatter-matching rule.
     * @param error[out] Error description
     *
     * @retval true - @c condition successfully parsed, @c error is empty.
     * @retval false - @c condition invalid, @c contains error.
     */
    virtual bool setup(const std::string& condition,
		       std::string& error) = 0;
    /**
     * @brief
     * Connects formatter for pattern matching
     *
     * Prepares Filter to sniff passing @ref ceph::Formatter operations.
     *
     * Returned @ref Formatter is input. Any action on input is passed to sink (output) immediately.
     * Filter inspects passing data, and if it matches condition @c on_match is called.
     * Function @c on_match can be called multiple times, if values passing through Filter keep
     * matching condition.
     * Only one @ref Formatter can be connected at any time.
     * Call @ref detach before calling @ref attach again.
     *
     * @param sink[in] Formatter that is used by Filter as output
     * @param on_match[in] function to call when condition matches
     *
     * @retval Formatter input, stream structuralized data into this.
     *
     * @note Returned formatter is an internal object, do not delete it.
     */
    virtual Formatter* attach(Formatter* sink, std::function<void()> on_match) = 0;
    /**
     * @brief
     * Disconnects formatter
     *
     * Complementary operation to @ref attach.
     * After it is called, it is illegal to use @ref Formatter obtained by previous call to @ref attach.
     */
    virtual void detach() = 0;
  };
}
#endif
