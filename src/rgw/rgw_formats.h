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
 * FIXME: This was a hack to send certain openstack messages.
 * There is a much better way to do this.
 */
class RGWFormatter_Plain : public Formatter {
public:
  RGWFormatter_Plain();
  virtual ~RGWFormatter_Plain();

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
  virtual void dump_string(const char *name, std::string s);
  virtual std::ostream& dump_stream(const char *name);
  virtual void dump_format(const char *name, const char *fmt, ...);
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
};

#endif
