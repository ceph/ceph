// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_MDS_TRACER_H
#define CEPH_MDS_TRACER_H

#include "common/tracer.h"
#include "common/Clock.h"
#include "include/common_fwd.h"
#include "include/utime.h"
#include "mgr/MDSPerfMetricTypes.h"

#include <atomic>
#include <map>
#include <string>
#include <vector>

namespace ceph { class Formatter; }

/**
 * TraceData - Completed trace information for sliding window storage
 * 
 * Uses span_id/parent_span_id to track call hierarchy.
 * span_id=0 is the root span.
 */
struct TraceData {
  std::string trace_id;
  std::string name;
  utime_t start_time;
  utime_t end_time;
  double duration_ms = 0;
  int result = 0;
  std::map<std::string, std::string> attributes;
  
  // Spans with hierarchy (parent_span_id=0 means direct child of root)
  struct Span {
    uint32_t span_id;         // Unique ID within this trace
    uint32_t parent_span_id;  // 0 = direct child of root
    std::string name;
    utime_t start_time;
    utime_t end_time;
    double duration_ms;
    bool async = false;       // true for async spans (may outlive parent)
  };
  std::vector<Span> child_spans;
  
  // Counter for generating span IDs (root span is 0)
  // Note: Not thread-safe, but each MDRequest has its own TraceData
  uint32_t next_span_id = 1;
  
  // Current span context for hierarchy tracking (0 = root)
  uint32_t current_span_id = 0;
  
  // Pending spans for async operations (started but not yet completed)
  struct PendingSpan {
    uint32_t span_id;
    uint32_t parent_span_id;
    std::string name;
    utime_t start_time;
  };
  std::map<uint32_t, PendingSpan> pending_spans;
  
  // Get next span ID
  uint32_t alloc_span_id() { return next_span_id++; }
  
  // Start an async/pending span - returns span_id to pass to end_pending_span
  // Note: Async spans may outlive their parent span - this is valid and shows
  // the causal relationship (who initiated the async work).
  uint32_t start_pending_span(const char* name) {
    PendingSpan ps;
    ps.span_id = alloc_span_id();
    ps.parent_span_id = current_span_id;  // Actual parent that started this
    ps.name = name;
    ps.start_time = ceph_clock_now();
    pending_spans[ps.span_id] = ps;
    return ps.span_id;
  }
  
  // End a pending span and add it to child_spans
  void end_pending_span(uint32_t span_id) {
    auto it = pending_spans.find(span_id);
    if (it != pending_spans.end()) {
      Span s;
      s.span_id = it->second.span_id;
      s.parent_span_id = it->second.parent_span_id;
      s.name = it->second.name;
      s.start_time = it->second.start_time;
      s.end_time = ceph_clock_now();
      s.duration_ms = (s.end_time - s.start_time) * 1000.0;
      s.async = true;  // Mark as async span
      child_spans.push_back(s);
      pending_spans.erase(it);
    }
  }
  
  void dump(ceph::Formatter* f) const;
  std::string to_json() const;
};

/**
 * MDSTracer - Distributed tracing for MDS operations
 *
 * Follows the same ownership pattern as MetricsHandler:
 * - Owned by MDSRank as a member
 * - init()/shutdown() lifecycle
 *
 * Features:
 * - Creates OTEL spans for request tracing
 * - Stores completed traces in sliding window buffer
 * - Supports admin socket dump via "trace dump" command
 * - Trace ID correlation with log records
 */
class MDSTracer {
public:
  // MDS-specific span attribute names
  static constexpr const char* ATTR_OP_TYPE = "mds.op_type";
  static constexpr const char* ATTR_OP_NAME = "mds.op_name";
  static constexpr const char* ATTR_CLIENT_ID = "mds.client_id";
  static constexpr const char* ATTR_INODE = "mds.inode";
  static constexpr const char* ATTR_PATH = "mds.path";
  static constexpr const char* ATTR_PATH2 = "mds.path2";
  static constexpr const char* ATTR_REQID = "mds.reqid";
  static constexpr const char* ATTR_MDS_RANK = "mds.rank";
  static constexpr const char* ATTR_SESSION_ID = "mds.session_id";
  static constexpr const char* ATTR_RESULT = "mds.result";
  static constexpr const char* ATTR_INTERNAL_OP = "mds.internal_op";

  MDSTracer(CephContext* cct);
  ~MDSTracer() = default;

  // Lifecycle (called by MDSRank)
  void init();
  void shutdown();

  // Check if tracing is enabled
  bool is_enabled() const;

  // Span creation
  jspan_ptr start_trace(const char* name);
  jspan_ptr add_span(const char* name, const jspan_ptr& parent);
  jspan_ptr add_span(const char* name, const jspan_context& parent_ctx);

  // Store completed trace in sliding window
  void store_trace(const TraceData& trace);
  
  // Dump all traces in sliding window
  void dump_traces(ceph::Formatter* f);
  
  // Get trace count in window
  size_t get_trace_count() const;
  
  // Prune old traces
  void prune_old_traces();

  // Get trace ID string from span (for log correlation)
  static std::string get_trace_id(const jspan_ptr& span);

  // Create and store a simple trace for housekeeping operations
  // (operations not tied to an MDRequest)
  void trace_event(const char* name, double duration_ms, int result = 0);

private:
  CephContext* cct;
  tracing::Tracer tracer;
  
  // Sliding window for completed traces (reuses generic SlidingWindowTracker)
  SlidingWindowTracker<TraceData> completed_traces;
};

/**
 * ScopedSpan - RAII helper for child spans with automatic duration tracking
 *
 * Creates a child span on construction, ends it on destruction.
 * Duration is automatically captured by OTEL and recorded to TraceData.
 * Supports hierarchical spans via span_id/parent_span_id tracking.
 *
 * Usage:
 *   void some_function(MDRequestRef& mdr) {
 *     ScopedSpan span(mds->tracer, "function_name", mdr->trace_span, &mdr->trace_data);
 *     // ... function body ...
 *     // Nested spans automatically get this span as parent:
 *     {
 *       ScopedSpan inner(mds->tracer, "inner_op", mdr->trace_span, &mdr->trace_data);
 *     }
 *   }  // span ends automatically, duration recorded to both OTEL and trace_data
 */
class ScopedSpan {
public:
  ScopedSpan(MDSTracer& tracer, const char* name, const jspan_ptr& parent,
             TraceData* trace_data = nullptr)
    : span_name(name), start_time(ceph_clock_now()), trace_data_ptr(trace_data) {
    if (parent && parent->IsRecording()) {
      span = tracer.add_span(name, parent);
    }
    // Track hierarchy in trace_data
    if (trace_data_ptr) {
      my_span_id = trace_data_ptr->alloc_span_id();
      parent_span_id = trace_data_ptr->current_span_id;
      trace_data_ptr->current_span_id = my_span_id;
    }
  }
  
  ~ScopedSpan() {
    if (span) {
      span->End();
    }
    // Auto-record to trace_data if provided
    if (trace_data_ptr && !span_name.empty()) {
      TraceData::Span s;
      s.span_id = my_span_id;
      s.parent_span_id = parent_span_id;
      s.name = span_name;
      s.start_time = start_time;
      s.end_time = ceph_clock_now();
      s.duration_ms = (s.end_time - s.start_time) * 1000.0;
      trace_data_ptr->child_spans.push_back(s);
      // Restore parent context
      trace_data_ptr->current_span_id = parent_span_id;
    }
  }

  // Add an event to this span
  void AddEvent(const char* name) {
    if (span && span->IsRecording()) {
      span->AddEvent(name);
    }
  }

  // Set an attribute on this span
  template<typename T>
  void SetAttribute(const char* key, const T& value) {
    if (span && span->IsRecording()) {
      span->SetAttribute(key, value);
    }
  }

  // Check if span is valid/recording
  bool IsRecording() const {
    return span && span->IsRecording();
  }
  
  // Get this span's ID (for manual hierarchy tracking)
  uint32_t get_span_id() const { return my_span_id; }

  // Non-copyable
  ScopedSpan(const ScopedSpan&) = delete;
  ScopedSpan& operator=(const ScopedSpan&) = delete;

private:
  jspan_ptr span;
  std::string span_name;
  utime_t start_time;
  TraceData* trace_data_ptr;
  uint32_t my_span_id = 0;
  uint32_t parent_span_id = 0;
};

/**
 * ScopedTrace - RAII helper for standalone traces (housekeeping operations)
 *
 * For operations not tied to an MDRequest. Creates a complete trace
 * and stores it in the sliding window on destruction.
 *
 * Usage:
 *   void housekeeping_function() {
 *     ScopedTrace trace(mds->tracer, "mds:housekeeping:trim_cache");
 *     // ... do work ...
 *   }  // trace stored automatically with duration
 */
class ScopedTrace {
public:
  ScopedTrace(MDSTracer& tracer, const char* name)
    : tracer_ref(tracer), trace_name(name), start_time(ceph_clock_now()) {
    if (tracer.is_enabled()) {
      span = tracer.start_trace(name);
    }
  }
  
  ~ScopedTrace() {
    if (span) {
      span->End();
    }
    if (tracer_ref.is_enabled()) {
      utime_t end_time = ceph_clock_now();
      double duration_ms = (end_time - start_time) * 1000.0;
      
      TraceData trace;
      trace.trace_id = MDSTracer::get_trace_id(span);
      trace.name = trace_name;
      trace.start_time = start_time;
      trace.end_time = end_time;
      trace.duration_ms = duration_ms;
      trace.result = result_code;
      trace.attributes = attributes;
      // No child spans for standalone traces (they are the root)
      
      tracer_ref.store_trace(trace);
    }
  }

  void set_result(int r) { result_code = r; }
  
  void set_attribute(const std::string& key, const std::string& value) {
    attributes[key] = value;
    if (span && span->IsRecording()) {
      span->SetAttribute(key.c_str(), value);
    }
  }

  void add_event(const char* name) {
    if (span && span->IsRecording()) {
      span->AddEvent(name);
    }
  }

  // Non-copyable
  ScopedTrace(const ScopedTrace&) = delete;
  ScopedTrace& operator=(const ScopedTrace&) = delete;

private:
  MDSTracer& tracer_ref;
  jspan_ptr span;
  std::string trace_name;
  utime_t start_time;
  int result_code = 0;
  std::map<std::string, std::string> attributes;
};

#endif // CEPH_MDS_TRACER_H
