// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef COMMON_ZIPKIN_TRACE_H
#define COMMON_ZIPKIN_TRACE_H

#include "acconfig.h"
#include "include/encoding.h"

// match the "real" struct
struct blkin_trace_info {
    int64_t trace_id;
    int64_t span_id;
    int64_t parent_span_id;
};

static inline void decode(blkin_trace_info& b, ceph::buffer::list::const_iterator& p)
{
  using ceph::decode;
  decode(b.trace_id, p);
  decode(b.span_id, p);
  decode(b.parent_span_id, p);
}

#endif // COMMON_ZIPKIN_TRACE_H
