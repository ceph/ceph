// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef COMMON_ZIPKIN_TRACE_H
#define COMMON_ZIPKIN_TRACE_H

#include "acconfig.h"
#include "include/encoding.h"

#ifdef WITH_BLKIN

#include <ztracer.hpp>

#else // !WITH_BLKIN

// add stubs for noop Trace and Endpoint

// match the "real" struct
struct blkin_trace_info {
    int64_t trace_id;
    int64_t span_id;
    int64_t parent_span_id;
};

namespace ZTracer
{
static inline int ztrace_init() { return 0; }

class Endpoint {
 public:
  Endpoint(const char *name) {}
  Endpoint(const char *ip, int port, const char *name) {}

  void copy_ip(const std::string &newip) {}
  void copy_name(const std::string &newname) {}
  void copy_address_from(const Endpoint *endpoint) {}
  void share_address_from(const Endpoint *endpoint) {}
  void set_port(int p) {}
};

class Trace {
 public:
  Trace() {}
  Trace(const char *name, const Endpoint *ep, const Trace *parent = NULL) {}
  Trace(const char *name, const Endpoint *ep,
        const blkin_trace_info *i, bool child=false) {}

  bool valid() const { return false; }
  operator bool() const { return false; }

  int init(const char *name, const Endpoint *ep, const Trace *parent = NULL) {
    return 0;
  }
  int init(const char *name, const Endpoint *ep,
           const blkin_trace_info *i, bool child=false) {
    return 0;
  }

  void copy_name(const std::string &newname) {}

  const blkin_trace_info* get_info() const { return NULL; }
  void set_info(const blkin_trace_info *i) {}

  void keyval(const char *key, const char *val) const {}
  void keyval(const char *key, int64_t val) const {}
  void keyval(const char *key, const char *val, const Endpoint *ep) const {}
  void keyval(const char *key, int64_t val, const Endpoint *ep) const {}

  void event(const char *event) const {}
  void event(const char *event, const Endpoint *ep) const {}
};
} // namespace ZTrace

#endif // !WITH_BLKIN

static inline void encode(const blkin_trace_info& b, ceph::buffer::list& bl)
{
  using ceph::encode;
  encode(b.trace_id, bl);
  encode(b.span_id, bl);
  encode(b.parent_span_id, bl);
}

static inline void decode(blkin_trace_info& b, ceph::buffer::list::const_iterator& p)
{
  using ceph::decode;
  decode(b.trace_id, p);
  decode(b.span_id, p);
  decode(b.parent_span_id, p);
}



#endif // COMMON_ZIPKIN_TRACE_H
