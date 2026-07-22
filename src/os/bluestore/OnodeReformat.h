// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_OSD_ONODE_REFORMAT_H
#define CEPH_OSD_ONODE_REFORMAT_H

#include <string>
#include <memory>
#include "BlueStore.h"

class OnodeReformatEngine {
protected:
  std::string args;
public:
  OnodeReformatEngine(std::string_view args) {
  }
  virtual ~OnodeReformatEngine() {}
  virtual bool validate(OnodeReformatContext& ctx);
  virtual bool execute(OnodeReformatContext& ctx,
    PerfCounters& _logger, BlueStore::Collection*, BlueStore::OnodeRef&) = 0;
};

class OnodeReformatRecompressEngine : public OnodeReformatEngine {
  BlueStore::Collection* c;
public:
  OnodeReformatRecompressEngine(std::string_view args)
    : OnodeReformatEngine(args)
  {
  }
  bool execute(OnodeReformatContext& ctx,
    PerfCounters& _logger, BlueStore::Collection*, BlueStore::OnodeRef&) override;
};

class OnodeReformatDefragmentEngine : public OnodeReformatEngine {
  BlueStore::Collection* c;
public:
  OnodeReformatDefragmentEngine(std::string_view args)
    : OnodeReformatEngine(args)
  {
  }
  bool execute(OnodeReformatContext& ctx,
    PerfCounters& _logger, BlueStore::Collection*, BlueStore::OnodeRef&) override;
};

class OnodeReformatContext {

private:

  size_t enabled_engines_count = 0;
  reformat_engines_t engines;

  bool to_be_applied = false;
  BlueStore::WriteContext wctx;
  span_stat_t span_stat;
  Allocator* alloc = nullptr;
  PExtentVectorSlicer* prealloc_slicer = nullptr; // pointer to the one from wctx if configured
public:
  BlueStore::read_context_t rctx;

  OnodeReformatContext(BlueStore::read_context_t& _rctx,
    const reformat_engines_t& _engines);
  ~OnodeReformatContext();

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

  void exec_engines(PerfCounters& _logger,
    BlueStore::Collection* c, BlueStore::OnodeRef& o);
  void clear();
};

#endif
