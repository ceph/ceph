// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_BLKIN_WRAPPER_H
#define CEPH_BLKIN_WRAPPER_H
#ifdef WITH_BLKIN

// string helper
#define BLKIN_OSS(OSS, A) \
  ostringstream OSS; \
  OSS << A;

// src/msg/Message.h
#define BLKIN_GET_MASTER() \
  ZTracer::ZTraceRef blkin_master_trace = get_master_trace();

#define BLKIN_MSG_GET_MASTER(MT, MSG) \
  ZTracer::ZTraceRef MT = (MSG)->get_master_trace();

#define BLKIN_ZTRACE_INIT() do { ZTracer::ztrace_init(); } while(0)

#define BLKIN_INIT_TRACE(T) do { init_trace_info(T); } while(0)

#define BLKIN_MSG_INIT_TRACE(M, T) do { (M)->init_trace_info(T); } while(0)

#define BLKIN_MSG_INIT_TRACE_IF(C, M, T) \
  do { if (C) (M)->init_trace_info(T); } while(0)

#define BLKIN_MSG_DO_INIT_TRACE() \
  struct blkin_trace_info tinfo; \
  tinfo.trace_id = 0; \
  tinfo.span_id = 0; \
  tinfo.parent_span_id = 0;

#define BLKIN_MSG_ENCODE_TRACE() \
  do { \
    ceph::bufferlist trace_bl; \
    if (blkin_master_trace) { \
      struct blkin_trace_info tinfo; \
      blkin_master_trace->get_trace_info(&tinfo); \
      ::encode(tinfo.trace_id, trace_bl); \
      ::encode(tinfo.span_id, trace_bl); \
      ::encode(tinfo.parent_span_id, trace_bl); \
    }; \
    ::encode(trace_bl, payload); \
  } while(0)

#define BLKIN_MSG_DECODE_TRACE(V) \
  do { \
    if (header.version >= V) { \
      ceph::bufferlist trace_bl; \
      ::decode(trace_bl, p); \
      if (trace_bl.length()) { \
        ceph::bufferlist::iterator it = trace_bl.begin(); \
        ::decode(tinfo.trace_id, it); \
        ::decode(tinfo.span_id, it); \
        ::decode(tinfo.parent_span_id, it); \
      } \
    }; \
    init_trace_info(&tinfo); \
  } while(0)

#define BLKIN_MSG_CHECK_SPAN() \
  do { \
    struct blkin_trace_info tinfo; \
    ZTracer::ZTraceRef mt = req->get_master_trace(); \
    assert(mt != NULL); \
    mt->get_trace_info(&tinfo); \
  } while(0)

#define BLKIN_MSG_INFO_DECL(TYPE) \
  void trace_msg_info() \
  { \
    assert(master_trace != NULL); \
    ostringstream oss; \
    oss << get_reqid(); \
    master_trace->keyval("Type", TYPE); \
    master_trace->keyval("Reqid", oss.str()); \
  }

#define BLKIN_MSG_END_DECL(TYPE) \
  bool create_message_endpoint() \
  { \
    message_endpoint = ZTracer::create_ZTraceEndpoint("0.0.0.0", 0, TYPE); \
    assert(message_endpoint != NULL); \
    return true; \
  }

#define BLKIN_MSG_END(EP, IP, PORT, NAME) \
  do { \
    EP = ZTracer::create_ZTraceEndpoint(IP, PORT, NAME); \
  } while(0)

#define BLKIN_MSG_TRACE_EVENT(M, E) do { (M)->trace(E); } while(0)

#define BLKIN_MSG_TRACE_EVENT_IF(C, M, E) do { if (C) (M)->trace(E); } while(0)

#define BLKIN_MSG_TRACE_KEYVAL(M, K, V) do { (M)->trace(K, V); } while(0)

// src/msg/Pipe.h
#define BLKIN_PIPE_ENDPOINT() do { set_endpoint(); } while(0)

// src/common/TrackedOp.h
typedef ZTracer::ZTraceEndpointRef TrackedOpEndpointRef;
typedef ZTracer::ZTraceRef TrackedOpTraceRef;

#define BLKIN_END_REF(X) TrackedOpEndpointRef X;
#define BLKIN_TRACE_REF(X) TrackedOpTraceRef X;

#define BLKIN_CREATE_TRACE(REF, ...) \
  do { REF = ZTracer::create_ZTrace(__VA_ARGS__); } while(0)

#define BLKIN_OP_CREATE_TRACE(OP, TYPE, EP) \
  do { (OP)->create_ ## TYPE ## _trace(EP); } while(0)

#define BLKIN_OP_TRACE_EVENT(OP, TYPE, E) \
  do { (OP)->trace_ ## TYPE (E); } while(0)

#define BLKIN_TYPE_TRACE_EVENT(TYPE, E) \
  do { trace_ ## TYPE (E); } while(0)

#define BLKIN_OP_TRACE_KEYVAL(OP, TYPE, K, V) \
  do { (OP)->trace_ ## TYPE (K, V); } while(0)

#define BLKIN_OP_EVENT(O, E) do { (O)->event(E); } while(0)

#define BLKIN_OP_EVENT_IF(C, O, E) do { if (C) (O)->event(E); } while(0)

#define BLKIN_OP_SET_TRACE_DECL() \
  void set_trace(TrackedOpTraceRef t) \
  { \
    trace = t; \
  }

#define BLKIN_OP_SET_TRACE(O, T) do { (O)->set_trace(T); } while(0)

#else // Not WITH_BLKIN

// string helper
#define BLKIN_OSS(OSS, A) do { } while(0)

// src/msg/Message.h
#define BLKIN_GET_MASTER(MT) do { } while(0)
#define BLKIN_MSG_GET_MASTER(MT, MSG) do { } while(0)
#define BLKIN_ZTRACE_INIT() do { } while(0)
#define BLKIN_INIT_TRACE(T) do { } while(0)
#define BLKIN_MSG_INIT_TRACE(M, T) do { } while(0)
#define BLKIN_MSG_INIT_TRACE_IF(C, M, T) do { } while(0)
#define BLKIN_MSG_DO_INIT_TRACE() do { } while(0)

#define BLKIN_MSG_ENCODE_TRACE() \
  do { \
    ceph::bufferlist trace_bl; \
    ::encode(trace_bl, payload); \
  } while(0)

#define BLKIN_MSG_DECODE_TRACE(V) \
  do { \
    if (header.version >= V) { \
      ceph::bufferlist trace_bl; \
      ::decode(trace_bl, p); \
    }; \
  } while(0)

#define BLKIN_MSG_CHECK_SPAN() do { } while(0)
#define BLKIN_MSG_INFO_DECL(TYPE)
#define BLKIN_MSG_END_DECL(TYPE)
#define BLKIN_MSG_END(EP, IP, PORT, NAME) do { } while(0)
#define BLKIN_MSG_TRACE_EVENT(M, E) do { } while(0)
#define BLKIN_MSG_TRACE_EVENT_IF(C, M, E) do { } while(0)
#define BLKIN_MSG_TRACE_KEYVAL(M, K, V) do { } while(0)

// src/msg/Pipe.h
#define BLKIN_PIPE_ENDPOINT() do { } while(0)

// src/common/TrackedOp.h
#define BLKIN_END_REF(X)
#define BLKIN_TRACE_REF(X)
#define BLKIN_CREATE_TRACE(REF, ...) do { } while(0)
#define BLKIN_OP_CREATE_TRACE(OP, TYPE, EP) do { } while(0)
#define BLKIN_OP_TRACE_EVENT(OP, TYPE, E) do { } while(0)
#define BLKIN_TYPE_TRACE_EVENT(TYPE, E) do { } while(0)
#define BLKIN_OP_TRACE_KEYVAL(OP, TYPE, K, V) do { } while(0)
#define BLKIN_OP_EVENT(O, E) do { } while(0)
#define BLKIN_OP_EVENT_IF(C, O, E) do { } while(0)
#define BLKIN_OP_SET_TRACE_DECL()
#define BLKIN_OP_SET_TRACE(O, T) do { } while(0)

#endif // WITH_BLKIN
#endif // CEPH_BLKIN_WRAPPER_H
