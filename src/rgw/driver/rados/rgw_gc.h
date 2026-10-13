// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "rgw_common.h"
#include "rgw_sal.h"
#include "rgw_rados.h"
#include "cls/rgw/cls_rgw_types.h"

#include <future>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/strand.hpp>

#include "common/ceph_time.h"

namespace rados::cls::lock { class Lock; }
namespace ceph::async { class spawn_throttle; }

class RGWGC : public DoutPrefixProvider {
  CephContext *cct;
  RGWRados *store;
  int max_objs;
  std::string *obj_names;

  static constexpr uint64_t seed = 8675309;

  using executor_t = boost::asio::io_context::executor_type;
  std::optional<boost::asio::strand<executor_t>> proc_strand;
  boost::asio::cancellation_signal proc_signal;
  std::future<void> proc_future;

  int tag_index(const std::string& tag);
  int send_chain(const cls_rgw_obj_chain& chain, const std::string& tag, optional_yield y);

  void process_cycles(boost::asio::yield_context yield);
  int process(bool expired_only, std::optional<int> shard_id,
              boost::asio::yield_context yield);
  int process_shard(int index, bool expired_only, boost::asio::yield_context yield);
  void process_locked(int index, int max_secs, bool expired_only,
                      rados::cls::lock::Lock& l, boost::asio::yield_context yield);

  using pool_ctx_map = std::map<std::string, std::optional<librados::IoCtx>>;
  bool process_omap(int index, ceph::coarse_mono_clock::time_point deadline,
                    bool expired_only, rados::cls::lock::Lock& l,
                    pool_ctx_map& pool_ctxs, boost::asio::yield_context yield);
  void process_queue(int index, ceph::coarse_mono_clock::time_point deadline,
                     bool expired_only, rados::cls::lock::Lock& l,
                     pool_ctx_map& pool_ctxs, boost::asio::yield_context yield);
  void admit_entry(ceph::async::spawn_throttle& workpool, std::vector<bool>& failed,
                   pool_ctx_map& pool_ctxs, const cls_rgw_gc_obj_info& info);

  int list_shard(int index, std::string& marker, uint32_t max,
                 std::list<cls_rgw_gc_obj_info>& entries, bool& truncated,
                 boost::asio::yield_context y);
  int trim_shard(int index, librados::ObjectWriteOperation&& op, size_t count,
                 boost::asio::yield_context y);
  int list_shard_omap(int index, std::string& marker, uint32_t max, bool expired_only,
                      std::list<cls_rgw_gc_obj_info>& entries, bool& truncated,
                      boost::asio::yield_context y);
  int delete_object(pool_ctx_map& pool_ctxs, const cls_rgw_obj& obj, const std::string& tag,
                    boost::asio::yield_context y);

public:
  RGWGC() : cct(NULL), store(NULL), max_objs(0), obj_names(NULL) {}
  ~RGWGC();
  std::vector<bool> transitioned_objects_cache;
  std::tuple<int, std::optional<cls_rgw_obj_chain>> send_split_chain(const cls_rgw_obj_chain& chain, const std::string& tag, optional_yield y);

  void initialize(CephContext *_cct, RGWRados *_store, optional_yield y);
  void finalize();

  int list(int& index, std::string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool& truncated, bool& processing_queue, std::optional<int> shard_id = std::nullopt);
  int process(bool expired_only, std::optional<int> shard_id);

  void start_processor(boost::asio::io_context& context);
  void stop_processor();

  CephContext *get_cct() const override { return store->ctx(); }
  unsigned get_subsys() const;

  std::ostream& gen_prefix(std::ostream& out) const;

};
