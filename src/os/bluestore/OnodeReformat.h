// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_OSD_ONODE_REFORMAT_H
#define CEPH_OSD_ONODE_REFORMAT_H

#include <string>
#include <memory>
#include "BlueStore.h"

class OnodeReformatAction {
protected:
public:
  virtual ~OnodeReformatAction() {}
  virtual bool call(OnodeReformatContext* ctx, std::string_view) = 0;
};

class OnodeReformatBasicValidateAction : public OnodeReformatAction {
  uint64_t offset;
  size_t length;
  uint32_t op_flags;
  uint64_t min_alloc_size;
public:
  OnodeReformatBasicValidateAction(
    uint64_t offset,
    size_t length,
    uint32_t op_flags,
    uint64_t mas) :
    offset(offset),
    length(length),
    op_flags(op_flags),
    min_alloc_size(mas) {
  }
  bool call(OnodeReformatContext* ctx, std::string_view args);
};

class OnodeReformatRecompressAction : public OnodeReformatAction {
  BlueStore::Collection* c;
  uint64_t offset;
  size_t length;
  bufferlist& bl;
  uint64_t min_alloc_size;
public:
  OnodeReformatRecompressAction(
    BlueStore::Collection* c,
    uint64_t offset,
    size_t length,
    bufferlist& bl,
    uint64_t mas) :
    c(c),
    offset(offset),
    length(length),
    bl(bl),
    min_alloc_size(mas) {
    ceph_assert(c);
  }
  bool call(OnodeReformatContext* ctx, std::string_view args);
};

class OnodeReformatDefragmentAction : public OnodeReformatAction {
  uint64_t offset;
  size_t length;
  uint64_t min_alloc_size;
public:
  OnodeReformatDefragmentAction(
    uint64_t offset,
    size_t length,
    uint64_t mas) :
    offset(offset),
    length(length),
    min_alloc_size(mas) {
  }
  bool call(OnodeReformatContext* ctx, std::string_view args);
};

struct OnodeReformatEngine {
  bool enabled = false;
  std::string args;
  std::unique_ptr<OnodeReformatAction> validate;
  std::unique_ptr<OnodeReformatAction> execute;
  OnodeReformatEngine() {}
  OnodeReformatEngine(OnodeReformatAction* _validate,
    OnodeReformatAction* _execute) :
    validate(_validate), execute(_execute) {
  }
};

class OnodeReformatContext {
public:
  enum {
    // This is effectively an engine priority, i.e. the order engines are called in.
    RECOMPRESS_ENGINE = 0,
    DEFRAGMENT_ENGINE = 1,
    MAX_ENGINES
  };
private:

  size_t enabled_engines_count = 0;
  std::array<OnodeReformatEngine, OnodeReformatContext::MAX_ENGINES> engines;

  bool to_be_applied = false;
  BlueStore::WriteContext wctx;
  span_stat_t span_stat;
  Allocator* alloc = nullptr;
  PExtentVectorSlicer* prealloc_slicer = nullptr; // pointer to the one from wctx if configured
public:
  BlueStore* store = nullptr;
  PerfCounters* logger = nullptr;

  OnodeReformatContext(BlueStore* store, PerfCounters* _logger);
  ~OnodeReformatContext();

  void add_engine(int pri,
    OnodeReformatAction* _validate,
    OnodeReformatAction* _execute);
  bool maybe_allocate(size_t need, size_t min_alloc_size,
    std::function<bool(int64_t, size_t)> acceptor);

  bool is_enabled() const { return enabled_engines_count > 0; }
  bool is_applied() const { return to_be_applied; }
  BlueStore::WriteContext& get_write_context() { return wctx; }
  span_stat_t& access_span_stats() {
    return span_stat;
  }
  const span_stat_t& get_span_stats() const {
    return span_stat;
  }

  void maybe_enable_engine(int pri, std::string_view args);
  void exec_engines();
  void clear();
};

#endif
