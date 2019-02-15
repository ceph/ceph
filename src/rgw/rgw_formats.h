// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_FORMATS_H
#define CEPH_RGW_FORMATS_H

#include "common/Formatter.h"

#include <list>
#include <stdint.h>
#include <string>
#include <ostream>

struct plain_stack_entry {
  int size;
  bool is_array;
};

/* FIXME: this class is mis-named.
 * FIXME: This was a hack to send certain swift messages.
 * There is a much better way to do this.
 */
class RGWFormatter_Plain : public Formatter {
  void reset_buf();
public:
  explicit RGWFormatter_Plain(bool use_kv = false);
  ~RGWFormatter_Plain() override;

  void set_status(int status, const char* status_name) override {};
  void output_header() override {};
  void output_footer() override {};
  void enable_line_break() override {};
  void flush(ostream& os) override;
  void reset() override;

  void open_array_section(const char *name) override;
  void open_array_section_in_ns(const char *name, const char *ns) override;
  void open_object_section(const char *name) override;
  void open_object_section_in_ns(const char *name, const char *ns) override;
  void close_section() override;
  void dump_unsigned(const char *name, uint64_t u) override;
  void dump_int(const char *name, int64_t u) override;
  void dump_float(const char *name, double d) override;
  void dump_string(const char *name, std::string_view s) override;
  std::ostream& dump_stream(const char *name) override;
  void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap) override;
  int get_len() const override;
  void write_raw_data(const char *data) override;

private:
  void write_data(const char *fmt, ...);
  void dump_value_int(const char *name, const char *fmt, ...);

  char *buf = nullptr;
  int len = 0;
  int max_len = 0;

  std::list<struct plain_stack_entry> stack;
  size_t min_stack_level = 0;
  bool use_kv;
  bool wrote_something = 0;
};


/* This is a presentation layer. No logic inside, please. */
class RGWSwiftWebsiteListingFormatter {
  std::ostream& ss;
  const std::string prefix;
protected:
  std::string format_name(const std::string& item_name) const;
public:
  RGWSwiftWebsiteListingFormatter(std::ostream& ss,
                                  std::string prefix)
    : ss(ss),
      prefix(std::move(prefix)) {
  }

  /* The supplied css_path can be empty. In such situation a default,
   * embedded style sheet will be generated. */
  void generate_header(const std::string& dir_path,
                       const std::string& css_path);
  void generate_footer();
  void dump_object(const rgw_bucket_dir_entry& objent);
  void dump_subdir(const std::string& name);
};


class RGWFormatterFlusher {
protected:
  Formatter *formatter;
  bool flushed;
  bool started;
  virtual void do_flush() = 0;
  virtual void do_start(int ret) {}
  void set_formatter(Formatter *f) {
    formatter = f;
  }
public:
  explicit RGWFormatterFlusher(Formatter *f) : formatter(f), flushed(false), started(false) {}
  virtual ~RGWFormatterFlusher() {}

  void flush() {
    do_flush();
    flushed = true;
  }

  virtual void start(int client_ret) {
    if (!started)
      do_start(client_ret);
    started = true;
  }

  Formatter *get_formatter() { return formatter; }
  bool did_flush() { return flushed; }
  bool did_start() { return started; }
};

class RGWStreamFlusher : public RGWFormatterFlusher {
  ostream& os;
protected:
  void do_flush() override {
    formatter->flush(os);
  }
public:
  RGWStreamFlusher(Formatter *f, ostream& _os) : RGWFormatterFlusher(f), os(_os) {}
};

class RGWNullFlusher : public RGWFormatterFlusher {
protected:
  void do_flush() override {
  }
public:
  RGWNullFlusher() : RGWFormatterFlusher(nullptr) {}
};

#endif
