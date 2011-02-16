#ifndef CEPH_RGW_FORMATS_H
#define CEPH_RGW_FORMATS_H

class RGWFormatter_Plain : public RGWFormatter {
protected:
  void formatter_init() {}

public:
  RGWFormatter_Plain() : RGWFormatter() {}
  ~RGWFormatter_Plain() {}

  void open_array_section(const char *name);
  void open_obj_section(const char *name);
  void close_section(const char *name);
  void dump_value_int(const char *name, const char *fmt, ...);
  void dump_value_str(const char *name, const char *fmt, ...);
};

class RGWFormatter_XML : public RGWFormatter {
  int indent;

  void open_section(const char *name);

protected:
  void formatter_init();

public:
  RGWFormatter_XML() : RGWFormatter() {}
  ~RGWFormatter_XML() {}

  void open_array_section(const char *name) {
    open_section(name);
  }
  void open_obj_section(const char *name) {
    open_section(name);
  }
  void close_section(const char *name);
  void dump_value_int(const char *name, const char *fmt, ...);
  void dump_value_str(const char *name, const char *fmt, ...);
};

struct json_stack_entry {
  int size;
  bool is_array;
};

class RGWFormatter_JSON : public RGWFormatter {
  std::list<struct json_stack_entry> stack;

  void open_section(bool is_array);
protected:
  void formatter_init();

public:
  RGWFormatter_JSON() : RGWFormatter() {}
  ~RGWFormatter_JSON() {}

  void open_array_section(const char *name);
  void open_obj_section(const char *name);
  void close_section(const char *name);
  void dump_value_int(const char *name, const char *fmt, ...);
  void dump_value_str(const char *name, const char *fmt, ...);
};

#endif
