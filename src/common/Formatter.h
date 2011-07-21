#ifndef CEPH_FORMATTER_H
#define CEPH_FORMATTER_H

#include <ostream>
#include <sstream>
#include <string>
#include <list>

namespace ceph {

struct formatter_stack_entry_d {
  int size;
  bool is_array;
  formatter_stack_entry_d() : size(0), is_array(false) {}
};
  
class Formatter {
 public:
  virtual void reset();

  virtual void flush(std::ostream& os);

  virtual void open_array_section(const char *name) = 0;
  virtual void open_object_section(const char *name) = 0;
  virtual void close_section() = 0;
  virtual void dump_unsigned(const char *name, uint64_t u) = 0;
  virtual void dump_int(const char *name, int64_t s) = 0;
  virtual void dump_float(const char *name, double d) = 0;
  virtual void dump_string(const char *name, std::string s) = 0;
  virtual std::ostream& dump_stream(const char *name) = 0;
  virtual void dump_format(const char *name, const char *fmt, ...) = 0;

  Formatter() : m_is_pending_string(false) {}
  virtual ~Formatter() {}

 protected:
  std::stringstream m_ss, m_pending_string;
  std::list<formatter_stack_entry_d> m_stack;
  bool m_is_pending_string;
  virtual void finish_pending_string() = 0;
};


class JSONFormatter : public Formatter {
 public:
  void open_array_section(const char *name);
  void open_object_section(const char *name);
  void close_section();
  void dump_unsigned(const char *name, uint64_t u);
  void dump_int(const char *name, int64_t u);
  void dump_float(const char *name, double d);
  void dump_string(const char *name, std::string s);
  std::ostream& dump_stream(const char *name);
  void dump_format(const char *name, const char *fmt, ...);


  JSONFormatter(bool p=false) : m_pretty(p) {}
 private:
  bool m_pretty;
  void open_section(const char *name, bool is_array);
  void print_quoted_string(const char *s);
  void print_name(const char *name);
  void print_comma(formatter_stack_entry_d& entry);
  void finish_pending_string();
};

}
#endif
