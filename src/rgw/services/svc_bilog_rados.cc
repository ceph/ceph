// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_bilog_rados.h"
#include "svc_bi_rados.h"
#include "rgw_bucket_layout.h"

#include "rgw_asio_thread.h"
#include <boost/asio/co_spawn.hpp>
#include "driver/rados/shard_io.h"
#include "include/neorados/RADOS.hpp"
#include "common/errno.h"
#include "cls/rgw/cls_rgw_client.h"

#include "common/async/blocked_completion.h"
#include "common/async/blocked_completion.h"
#include "common/async/co_throttle.h"
#include "common/async/librados_completion.h"
#include "common/async/yield_context.h"

#include "rgw_log_backing.h"
#include "neorados/cls/fifo.h"
#include "rgw_bilog.h"
#include "rgw_tools.h"


#define dout_subsys ceph_subsys_rgw

using namespace std;
using rgwrados::shard_io::Result;
namespace asio = boost::asio;
namespace fifo = neorados::cls::fifo;

RGWSI_BILog_RADOS::RGWSI_BILog_RADOS(CephContext *cct) : RGWServiceInstance(cct)
{
}

void RGWSI_BILog_RADOS_InIndex::init(RGWSI_BucketIndex_RADOS *bi_rados_svc)
{
  svc.bi = bi_rados_svc;
}

struct TrimWriter : rgwrados::shard_io::RadosWriter {
  const BucketIndexShardsManager& start;
  const BucketIndexShardsManager& end;

  TrimWriter(const DoutPrefixProvider& dpp,
             boost::asio::any_io_executor ex,
             librados::IoCtx& ioctx,
             const BucketIndexShardsManager& start,
             const BucketIndexShardsManager& end)
    : RadosWriter(dpp, std::move(ex), ioctx), start(start), end(end)
  {}
  void prepare_write(int shard, librados::ObjectWriteOperation& op) override {
    cls_rgw_bilog_trim(op,  start.get(shard, ""), end.get(shard, ""));
  }
  Result on_complete(int, boost::system::error_code ec) override {
    // continue trimming until -ENODATA or other error
    if (ec == boost::system::errc::no_message_available) {
      return Result::Success;
    } else if (ec) {
      return Result::Error;
    } else {
      return Result::Retry;
    }
  }
  void add_prefix(std::ostream& out) const override {
    out << "trim bilog shards: ";
  }
};

int RGWSI_BILog_RADOS_InIndex::log_trim(const DoutPrefixProvider *dpp, optional_yield y,
				const RGWBucketInfo& bucket_info,
				const rgw::bucket_log_layout_generation& log_layout,
				int shard_id,
				std::string_view end_marker)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;

  BucketIndexShardsManager start_marker_mgr;
  BucketIndexShardsManager end_marker_mgr;

  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, current_index, &index_pool, &bucket_objs, nullptr);
  if (r < 0) {
    return r;
  }

  // empty string since the introduction of cls fifo
  r = start_marker_mgr.from_string(std::string_view{}, shard_id);
  if (r < 0) {
    return r;
  }

  r = end_marker_mgr.from_string(end_marker, shard_id);
  if (r < 0) {
    return r;
  }

  const size_t max_aio = cct->_conf->rgw_bucket_index_max_aio;
  boost::system::error_code ec;
  if (y) {
    // run on the coroutine's executor and suspend until completion
    auto yield = y.get_yield_context();
    auto ex = yield.get_executor();
    auto writer = TrimWriter{*dpp, ex, index_pool, start_marker_mgr, end_marker_mgr};

    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio, yield[ec]);
  } else {
    // run a strand on the system executor and block on a condition variable
    auto ex = boost::asio::make_strand(boost::asio::system_executor{});
    auto writer = TrimWriter{*dpp, ex, index_pool, start_marker_mgr, end_marker_mgr};

    maybe_warn_about_blocking(dpp);
    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio,
                                     ceph::async::use_blocked[ec]);
  }
  return ceph::from_error_code(ec);
}

struct StartWriter : rgwrados::shard_io::RadosWriter {
  using RadosWriter::RadosWriter;
  void prepare_write(int, librados::ObjectWriteOperation& op) override {
    cls_rgw_bilog_start(op);
  }
  void add_prefix(std::ostream& out) const override {
    out << "restart bilog shards: ";
  }
};

int RGWSI_BILog_RADOS_InIndex::log_start(const DoutPrefixProvider *dpp, optional_yield y,
                                 const RGWBucketInfo& bucket_info,
                                 const rgw::bucket_log_layout_generation& log_layout,
                                 int shard_id)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, current_index, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;

  const size_t max_aio = cct->_conf->rgw_bucket_index_max_aio;
  boost::system::error_code ec;
  if (y) {
    // run on the coroutine's executor and suspend until completion
    auto yield = y.get_yield_context();
    auto ex = yield.get_executor();
    auto writer = StartWriter{*dpp, ex, index_pool};

    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio, yield[ec]);
  } else {
    // run a strand on the system executor and block on a condition variable
    auto ex = boost::asio::make_strand(boost::asio::system_executor{});
    auto writer = StartWriter{*dpp, ex, index_pool};

    maybe_warn_about_blocking(dpp);
    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio,
                                     ceph::async::use_blocked[ec]);
  }
  return ceph::from_error_code(ec);
}

struct StopWriter : rgwrados::shard_io::RadosWriter {
  using RadosWriter::RadosWriter;
  void prepare_write(int, librados::ObjectWriteOperation& op) override {
    cls_rgw_bilog_stop(op);
  }
  void add_prefix(std::ostream& out) const override {
    out << "stop bilog shards: ";
  }
};

int RGWSI_BILog_RADOS_InIndex::log_stop(const DoutPrefixProvider *dpp, optional_yield y,
                                const RGWBucketInfo& bucket_info,
                                const rgw::bucket_log_layout_generation& log_layout,
                                int shard_id)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, current_index, &index_pool, &bucket_objs, nullptr);
  if (r < 0)
    return r;

  const size_t max_aio = cct->_conf->rgw_bucket_index_max_aio;
  boost::system::error_code ec;
  if (y) {
    // run on the coroutine's executor and suspend until completion
    auto yield = y.get_yield_context();
    auto ex = yield.get_executor();
    auto writer = StopWriter{*dpp, ex, index_pool};

    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio, yield[ec]);
  } else {
    // run a strand on the system executor and block on a condition variable
    auto ex = boost::asio::make_strand(boost::asio::system_executor{});
    auto writer = StopWriter{*dpp, ex, index_pool};

    maybe_warn_about_blocking(dpp);
    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio,
                                     ceph::async::use_blocked[ec]);
  }
  return ceph::from_error_code(ec);
}

static void build_bucket_index_marker(const string& shard_id_str,
                                      const string& shard_marker,
                                      string *marker) {
  if (marker) {
    *marker = shard_id_str;
    marker->append(BucketIndexShardsManager::KEY_VALUE_SEPARATOR);
    marker->append(shard_marker);
  }
}

struct LogReader : rgwrados::shard_io::RadosReader {
  const BucketIndexShardsManager& start;
  uint32_t max;
  std::map<int, cls_rgw_bi_log_list_ret>& logs;

  LogReader(const DoutPrefixProvider& dpp, boost::asio::any_io_executor ex,
            librados::IoCtx& ioctx, const BucketIndexShardsManager& start,
            uint32_t max, std::map<int, cls_rgw_bi_log_list_ret>& logs)
    : RadosReader(dpp, std::move(ex), ioctx),
      start(start), max(max), logs(logs)
  {}
  void prepare_read(int shard, librados::ObjectReadOperation& op) override {
    auto& result = logs[shard];
    cls_rgw_bilog_list(op, start.get(shard, ""), max, &result, nullptr);
  }
  void add_prefix(std::ostream& out) const override {
    out << "list bilog shards: ";
  }
};

static int bilog_list(const DoutPrefixProvider* dpp, optional_yield y,
                      RGWSI_BucketIndex_RADOS* svc_bi,
                      const RGWBucketInfo& bucket_info,
                      const rgw::bucket_log_layout_generation& log_layout,
                      const BucketIndexShardsManager& start,
                      int shard_id, uint32_t max,
                      std::map<int, cls_rgw_bi_log_list_ret>& logs)
{
  librados::IoCtx index_pool;
  map<int, string> oids;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = svc_bi->open_bucket_index(dpp, bucket_info, shard_id, current_index, &index_pool, &oids, nullptr);
  if (r < 0)
    return r;

  const size_t max_aio = dpp->get_cct()->_conf->rgw_bucket_index_max_aio;
  boost::system::error_code ec;
  if (y) {
    // run on the coroutine's executor and suspend until completion
    auto yield = y.get_yield_context();
    auto ex = yield.get_executor();
    auto reader = LogReader{*dpp, ex, index_pool, start, max, logs};

    rgwrados::shard_io::async_reads(reader, oids, max_aio, yield[ec]);
  } else {
    // run a strand on the system executor and block on a condition variable
    auto ex = boost::asio::make_strand(boost::asio::system_executor{});
    auto reader = LogReader{*dpp, ex, index_pool, start, max, logs};

    maybe_warn_about_blocking(dpp);
    rgwrados::shard_io::async_reads(reader, oids, max_aio,
                                    ceph::async::use_blocked[ec]);
  }
  return ceph::from_error_code(ec);
}

int RGWSI_BILog_RADOS_InIndex::log_list(const DoutPrefixProvider *dpp, optional_yield y,
				const RGWBucketInfo& bucket_info,
				const rgw::bucket_log_layout_generation& log_layout,
				int shard_id, string& marker, uint32_t max,
                                std::list<rgw_bi_log_entry>& result, bool *truncated)
{
  ldpp_dout(dpp, 20) << __func__ << ": " << bucket_info.bucket << " marker " << marker << " shard_id=" << shard_id << " max " << max << dendl;
  result.clear();

  BucketIndexShardsManager marker_mgr;
  const bool has_shards = (shard_id >= 0);
  // If there are multiple shards for the bucket index object, the marker
  // should have the pattern '{shard_id_1}#{shard_marker_1},{shard_id_2}#
  // {shard_marker_2}...', if there is no sharding, the bi_log_list should
  // only contain one record, and the key is the bucket instance id.
  int r = marker_mgr.from_string(marker, shard_id);
  if (r < 0)
    return r;

  std::map<int, cls_rgw_bi_log_list_ret> bi_log_lists;
  r = bilog_list(dpp, y, svc.bi, bucket_info, log_layout, marker_mgr,
                 shard_id, max, bi_log_lists);
  if (r < 0)
    return r;

  map<int, list<rgw_bi_log_entry>::iterator> vcurrents;
  map<int, list<rgw_bi_log_entry>::iterator> vends;
  if (truncated) {
    *truncated = false;
  }
  map<int, cls_rgw_bi_log_list_ret>::iterator miter = bi_log_lists.begin();
  for (; miter != bi_log_lists.end(); ++miter) {
    int shard_id = miter->first;
    vcurrents[shard_id] = miter->second.entries.begin();
    vends[shard_id] = miter->second.entries.end();
    if (truncated) {
      *truncated = (*truncated || miter->second.truncated);
    }
  }

  size_t total = 0;
  bool has_more = true;
  map<int, list<rgw_bi_log_entry>::iterator>::iterator viter;
  map<int, list<rgw_bi_log_entry>::iterator>::iterator eiter;
  while (total < max && has_more) {
    has_more = false;

    viter = vcurrents.begin();
    eiter = vends.begin();

    for (; total < max && viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());

      int shard_id = viter->first;
      list<rgw_bi_log_entry>::iterator& liter = viter->second;

      if (liter == eiter->second){
        continue;
      }
      rgw_bi_log_entry& entry = *(liter);
      if (has_shards) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%d", shard_id);
        string tmp_id;
        build_bucket_index_marker(buf, entry.id, &tmp_id);
        entry.id.swap(tmp_id);
      }
      marker_mgr.add(shard_id, entry.id);
      result.push_back(entry);
      total++;
      has_more = true;
      ++liter;
    }
  }

  if (truncated) {
    for (viter = vcurrents.begin(), eiter = vends.begin(); viter != vcurrents.end(); ++viter, ++eiter) {
      assert (eiter != vends.end());
      *truncated = (*truncated || (viter->second != eiter->second));
    }
  }

  // Refresh marker, if there are multiple shards, the output will look like
  // '{shard_oid_1}#{shard_marker_1},{shard_oid_2}#{shard_marker_2}...',
  // if there is no sharding, the simply marker (without oid) is returned
  if (has_shards) {
    marker_mgr.to_string(&marker);
  } else {
    if (!result.empty()) {
      marker = result.rbegin()->id;
    }
  }

  return 0;
}

int RGWSI_BILog_RADOS_InIndex::log_get_max_marker(const DoutPrefixProvider *dpp,
                                      const RGWBucketInfo& bucket_info,
                                      const std::map<int, rgw_bucket_dir_header>& headers,
                                      const int shard_id,
                                      std::map<int, std::string> *max_markers,
				      optional_yield y)
{
 for (const auto& [ header_shard_id, header ] : headers) {
    if (shard_id >= 0) {
      (*max_markers)[shard_id] = header.max_marker;
    } else {
      (*max_markers)[header_shard_id] = header.max_marker;
    }
  }

  return 0;
}

int RGWSI_BILog_RADOS_InIndex::log_get_max_marker(const DoutPrefixProvider *dpp,
                                      const RGWBucketInfo& bucket_info,
                                      const std::map<int, rgw_bucket_dir_header>& headers,
                                      const int shard_id,
                                      std::string* max_marker,
				      optional_yield y)
{
  if (shard_id < 0) {
  BucketIndexShardsManager marker_mgr;
  for (const auto& [ header_shard_id, header ] : headers) {
    marker_mgr.add(header_shard_id, header.max_marker);
  }
  marker_mgr.to_string(max_marker);
  } else if (!headers.empty()) {
  *max_marker = std::end(headers)->second.max_marker;
  }
  return 0;
}

// FIFO interface

void RGWSI_BILog_RADOS_FIFO::init(RGWSI_BucketIndex_RADOS *bi_rados_svc)
{
  svc.bi = bi_rados_svc;
}

std::unique_ptr<RGWBILogFIFO>
RGWSI_BILog_RADOS_FIFO::create_bilog_fifo(const DoutPrefixProvider *dpp,
                                          const RGWBucketInfo& bucket_info) const {
  librados::IoCtx index_pool;
  std::string bucket_oid;
  int ret = svc.bi->open_bucket_index(dpp, bucket_info, &index_pool, &bucket_oid);
  if (ret < 0) {
    throw std::system_error(-ret, std::system_category(),
                           "Failed to open bucket index");
  }

  try {
    
    auto loc = rgw::init_iocontext(dpp, rados, index_pool,
                                   rgw::create, ceph::async::use_blocked);
    
    return std::make_unique<RGWBILogFIFO>(rados, std::move(loc), bucket_info);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: failed to initialize ioctx: "
                      << e.what() << dendl;
    throw;
  }
}

asio::awaitable<void>
RGWSI_BILog_RADOS_FIFO::log_start(const DoutPrefixProvider *dpp,
                                         const RGWBucketInfo& bucket_info,
                                         const rgw::bucket_log_layout_generation& log_layout,
                                         int shard_id)
{
  if (shard_id > 0) {
    co_return;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  
  // Lazy initialized upon first use
  co_return;
}

asio::awaitable<void>
RGWSI_BILog_RADOS_FIFO::log_stop(const DoutPrefixProvider *dpp,
                                        const RGWBucketInfo& bucket_info,
                                        const rgw::bucket_log_layout_generation& log_layout,
                                        int shard_id)
{
  if (shard_id > 0) {
    co_return;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  
  // No explicit cleanup required for LazyFIFO
  co_return;
}

asio::awaitable<void> 
RGWSI_BILog_RADOS_FIFO::log_trim(const DoutPrefixProvider *dpp,
                                 const RGWBucketInfo& bucket_info,
                                 const rgw::bucket_log_layout_generation& log_layout,
                                 int shard_id, std::string_view marker) {
  if (shard_id > 0) {
    co_return;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);

  auto bilog_fifo = create_bilog_fifo(dpp, bucket_info);
  co_return co_await bilog_fifo->trim(dpp, marker);
}

asio::awaitable<std::tuple<std::vector<rgw_bi_log_entry>, std::string, bool>>
RGWSI_BILog_RADOS_FIFO::log_list(const DoutPrefixProvider *dpp,
                                 const RGWBucketInfo& bucket_info,
                                 const rgw::bucket_log_layout_generation& log_layout,
                                 int shard_id, std::string marker, uint32_t max_entries) {
  ldpp_dout(dpp, 20) << __func__ << ": " << bucket_info.bucket
                     << " marker=" << marker << " shard_id=" << shard_id
                     << " max_entries=" << max_entries << dendl;

  if (shard_id > 0) {
    co_return std::make_tuple(std::vector<rgw_bi_log_entry>{}, std::string{}, false);
  }
  ceph_assert(shard_id == 0 || shard_id == -1);

  auto bilog_fifo = create_bilog_fifo(dpp, bucket_info);
  co_return co_await bilog_fifo->list(dpp, marker, max_entries);
}

asio::awaitable<std::string>
RGWSI_BILog_RADOS_FIFO::log_get_max_marker(const DoutPrefixProvider *dpp,
                                           const RGWBucketInfo& bucket_info,
                                           const std::map<int, rgw_bucket_dir_header>& headers,
                                           int shard_id) {
  if (shard_id > 0) {
    co_return std::string{};
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  
  auto bilog_fifo = create_bilog_fifo(dpp, bucket_info);
  co_return co_await bilog_fifo->get_max_marker(dpp);
}

int RGWSI_BILog_RADOS_FIFO::log_start(const DoutPrefixProvider *dpp, optional_yield y,
                                      const RGWBucketInfo& bucket_info,
                                      const rgw::bucket_log_layout_generation& log_layout,
                                      int shard_id) {
  if (shard_id > 0) {
    return 0; // Only shard 0 for FIFO
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  
  // No-op for FIFO - lazy initialization handles creation
  return 0;
}

int RGWSI_BILog_RADOS_FIFO::log_stop(const DoutPrefixProvider *dpp, optional_yield y,
                                     const RGWBucketInfo& bucket_info,
                                     const rgw::bucket_log_layout_generation& log_layout,
                                     int shard_id) {
  if (shard_id > 0) {
    return 0; // Only shard 0 for FIFO
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  
  // No-op for FIFO - no explicit cleanup needed
  return 0;
}

int RGWSI_BILog_RADOS_FIFO::log_trim(const DoutPrefixProvider *dpp, optional_yield y,
                                     const RGWBucketInfo& bucket_info,
                                     const rgw::bucket_log_layout_generation& log_layout,
                                     int shard_id, std::string_view marker) {
  if (shard_id > 0) {
    return 0;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);

  try {
    auto bilog_fifo = create_bilog_fifo(dpp, bucket_info);
    
    if (y) {
      bilog_fifo->trim(dpp, marker, y.get_yield_context());
    } else {
      maybe_warn_about_blocking(dpp);
      asio::spawn(rados.get_executor(),
                  [&](asio::yield_context yield) {
                      bilog_fifo->trim(dpp, marker, yield);
                    }, ceph::async::use_blocked);
    }

    return 0;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: trim failed: " << e.what() << dendl;
    return -EIO;
  }
}

int RGWSI_BILog_RADOS_FIFO::log_list(const DoutPrefixProvider *dpp, optional_yield y,
                                     const RGWBucketInfo& bucket_info,
                                     const rgw::bucket_log_layout_generation& log_layout,
                                     int shard_id, std::string& marker,
                                     uint32_t max_entries,
                                     std::list<rgw_bi_log_entry>& result,
                                     bool *truncated) {
  ldpp_dout(dpp, 20) << __func__ << ": " << bucket_info.bucket
                     << " marker=" << marker << " shard_id=" << shard_id
                     << " max_entries=" << max_entries << dendl;

  if (shard_id > 0) {
    return 0;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);

  try {
    auto bilog_fifo = create_bilog_fifo(dpp, bucket_info);
    
    std::vector<rgw_bi_log_entry> entries;
    std::string out_marker;
    bool is_truncated;

    if (y) {
      std::tie(entries, out_marker, is_truncated) = 
        bilog_fifo->list(dpp, marker, max_entries, y.get_yield_context());
    } else {
      maybe_warn_about_blocking(dpp);
      boost::system::error_code ec;
      asio::spawn(rados.get_executor(),
                  [&](asio::yield_context yield) {
                      std::tie(entries, out_marker, is_truncated) = 
                      bilog_fifo->list(dpp, marker, max_entries, yield);
                    }, ceph::async::use_blocked);
    }
    
    result.clear();
    for (auto& entry : entries) {
      result.push_back(std::move(entry));
    }
    
    if (truncated) {
      *truncated = is_truncated;
    }
    
    marker = out_marker;
    return 0;
    
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: list failed: " << e.what() << dendl;
    return -EIO;
  }
}

int RGWSI_BILog_RADOS_FIFO::log_get_max_marker(const DoutPrefixProvider *dpp,
                                               const RGWBucketInfo& bucket_info,
                                               const std::map<int, rgw_bucket_dir_header>& headers,
                                               const int shard_id,
                                               std::string *max_marker,
                                               optional_yield y) {
  if (shard_id > 0) {
    *max_marker = std::string{};
    return 0;
  }
  ceph_assert(shard_id == 0 || shard_id == -1);
  
  try {
    auto bilog_fifo = create_bilog_fifo(dpp, bucket_info);
    
    std::string marker;
    if (y) {
      marker = bilog_fifo->get_max_marker(dpp, y.get_yield_context());
    } else {
      maybe_warn_about_blocking(dpp);
      asio::spawn(rados.get_executor(),
                  [&](asio::yield_context yield) {
                      marker = bilog_fifo->get_max_marker(dpp, yield);
                  }, ceph::async::use_blocked);
    }

    *max_marker = std::move(marker);
    return 0;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: get_max_marker failed: " << e.what() << dendl;
    return -EIO;
  }
}

int RGWSI_BILog_RADOS_FIFO::log_get_max_marker(const DoutPrefixProvider *dpp,
                                               const RGWBucketInfo& bucket_info,
                                               const std::map<int, rgw_bucket_dir_header>& headers,
                                               const int shard_id,
                                               std::map<int, std::string> *max_markers,
                                               optional_yield y) {
  auto& max_marker = (*max_markers)[0];
  return log_get_max_marker(dpp, bucket_info, headers, shard_id, &max_marker, y);
}


RGWSI_BILog_RADOS_BackendDispatcher::RGWSI_BILog_RADOS_BackendDispatcher(
  CephContext* cct, neorados::RADOS rados)
  : RGWSI_BILog_RADOS(cct),
    backend_inindex(cct),
    backend_fifo(cct, std::move(rados)) {
}

void RGWSI_BILog_RADOS_BackendDispatcher::init(RGWSI_BucketIndex_RADOS *bi_rados_svc) {
  // initialize both backends
  backend_inindex.init(bi_rados_svc);
  backend_fifo.init(bi_rados_svc);
}

RGWSI_BILog_RADOS& RGWSI_BILog_RADOS_BackendDispatcher::get_backend(
  const RGWBucketInfo& bucket_info)
{
  // check if bucket has new FIFO-based log layout
  if (!bucket_info.layout.logs.empty() && 
      bucket_info.layout.logs.back().layout.type == rgw::BucketLogType::FIFO) {
    ldpp_dout(this, 20) << "RGWSI_BILog_RADOS_BackendDispatcher: Using FIFO backend for bucket " 
                        << bucket_info.bucket << dendl;
    return backend_fifo;
  } 
  
  // default to in-index backend for:
  // - buckets with no layout (old buckets)
  // - buckets with InIndex layout
  // - any other cases
  ldpp_dout(this, 20) << "RGWSI_BILog_RADOS_BackendDispatcher: Using InIndex backend for bucket " 
                      << bucket_info.bucket << dendl;
  return backend_inindex;
}

// delegate operations to the appropriate backend
int RGWSI_BILog_RADOS_BackendDispatcher::log_start(const DoutPrefixProvider *dpp, optional_yield y,
                                                   const RGWBucketInfo& bucket_info,
                                                   const rgw::bucket_log_layout_generation& log_layout,
                                                   int shard_id) {
  auto& backend = get_backend(bucket_info);
  return backend.log_start(dpp, y, bucket_info, log_layout, shard_id);
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_stop(const DoutPrefixProvider *dpp, optional_yield y,
                                                  const RGWBucketInfo& bucket_info,
                                                  const rgw::bucket_log_layout_generation& log_layout,
                                                  int shard_id) {
  auto& backend = get_backend(bucket_info);
  return backend.log_stop(dpp, y, bucket_info, log_layout, shard_id);
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_trim(const DoutPrefixProvider *dpp, optional_yield y,
                                                  const RGWBucketInfo& bucket_info,
                                                  const rgw::bucket_log_layout_generation& log_layout,
                                                  int shard_id,
                                                  std::string_view marker) {
  auto& backend = get_backend(bucket_info);
  return backend.log_trim(dpp, y, bucket_info, log_layout, shard_id, marker);
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_list(const DoutPrefixProvider *dpp, optional_yield y,
                                                  const RGWBucketInfo& bucket_info,
                                                  const rgw::bucket_log_layout_generation& log_layout,
                                                  int shard_id,
                                                  std::string& marker,
                                                  uint32_t max,
                                                  std::list<rgw_bi_log_entry>& result,
                                                  bool *truncated) {
  auto& backend = get_backend(bucket_info);
  return backend.log_list(dpp, y, bucket_info, log_layout, shard_id, marker, max, result, truncated);
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_get_max_marker(const DoutPrefixProvider *dpp,
                                                           const RGWBucketInfo& bucket_info,
                                                           const std::map<int, rgw_bucket_dir_header>& headers,
                                                           const int shard_id,
                                                           std::string *max_marker,
                                                           optional_yield y) {
  auto& backend = get_backend(bucket_info);
  return backend.log_get_max_marker(dpp, bucket_info, headers, shard_id, max_marker, y);
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_get_max_marker(const DoutPrefixProvider *dpp,
                                                           const RGWBucketInfo& bucket_info,
                                                           const std::map<int, rgw_bucket_dir_header>& headers,
                                                           const int shard_id,
                                                           std::map<int, std::string> *max_markers,
                                                           optional_yield y) {
  auto& backend = get_backend(bucket_info);
  return backend.log_get_max_marker(dpp, bucket_info, headers, shard_id, max_markers, y);
}

