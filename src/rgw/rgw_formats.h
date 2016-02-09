// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RGW_FORMATS_H
#define CEPH_RGW_FORMATS_H

#include "common/Formatter.h"

#include <list>
#include <stdint.h>
#include <string>

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
  virtual ~RGWFormatter_Plain();

  virtual void set_status(int status, const char* status_name) {};
  virtual void output_header() {};
  virtual void output_footer() {};
  virtual void flush(ostream& os);
  virtual void reset();

  virtual void open_array_section(const char *name);
  virtual void open_array_section_in_ns(const char *name, const char *ns);
  virtual void open_object_section(const char *name);
  virtual void open_object_section_in_ns(const char *name, const char *ns);
  virtual void close_section();
  virtual void dump_unsigned(const char *name, uint64_t u);
  virtual void dump_int(const char *name, int64_t u);
  virtual void dump_float(const char *name, double d);
  virtual void dump_string(const char *name, const std::string& s);
  virtual std::ostream& dump_stream(const char *name);
  virtual void dump_format_va(const char *name, const char *ns, bool quoted, const char *fmt, va_list ap);
  virtual int get_len() const;
  virtual void write_raw_data(const char *data);

private:
  void write_data(const char *fmt, ...);
  void dump_value_int(const char *name, const char *fmt, ...);

  char *buf;
  int len;
  int max_len;

  std::list<struct plain_stack_entry> stack;
  size_t min_stack_level;
  bool use_kv;
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
  virtual void do_flush() {
    formatter->flush(os);
  }
public:
  RGWStreamFlusher(Formatter *f, ostream& _os) : RGWFormatterFlusher(f), os(_os) {}
};

#endif
