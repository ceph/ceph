// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "svc_bilog_rados.h"
#include "svc_bi_rados.h"

#include "rgw_asio_thread.h"
#include "driver/rados/shard_io.h"
#include "driver/rados/rgw_bilog.h"
#include "driver/rados/rgw_log_backing.h"
#include "cls/rgw/cls_rgw_client.h"
#include "common/async/blocked_completion.h"

#include <boost/asio/spawn.hpp>

#define dout_subsys ceph_subsys_rgw

using namespace std;
using rgwrados::shard_io::Result;
namespace asio = boost::asio;
namespace fifo = neorados::cls::fifo;

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
    cls_rgw_bilog_trim(op, start.get(shard, ""), end.get(shard, ""));
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

struct StartWriter : rgwrados::shard_io::RadosWriter {
  StartWriter(const DoutPrefixProvider& dpp,
              boost::asio::any_io_executor ex,
              librados::IoCtx& ioctx)
    : RadosWriter(dpp, std::move(ex), ioctx)
  {}
  void prepare_write(int /*shard*/,
                     librados::ObjectWriteOperation& op) override {
    cls_rgw_bilog_start(op);
  }
  void add_prefix(std::ostream& out) const override {
    out << "start bilog shards: ";
  }
};

struct StopWriter : rgwrados::shard_io::RadosWriter {
  StopWriter(const DoutPrefixProvider& dpp,
             boost::asio::any_io_executor ex,
             librados::IoCtx& ioctx)
    : RadosWriter(dpp, std::move(ex), ioctx)
  {}
  void prepare_write(int /*shard*/,
                     librados::ObjectWriteOperation& op) override {
    cls_rgw_bilog_stop(op);
  }
  void add_prefix(std::ostream& out) const override {
    out << "stop bilog shards: ";
  }
};


void RGWSI_BILog_RADOS_InIndex::init(RGWSI_BucketIndex_RADOS *bi_rados_svc,
                                     neorados::RADOS rados_neo_)
{
  svc.bi = bi_rados_svc;
  // rados_neo stored on base for rgw_rados.cc access
  rados_neo.emplace(std::move(rados_neo_));
}

int RGWSI_BILog_RADOS_InIndex::log_trim(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id,
    std::string_view start_marker,
    std::string_view end_marker)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  BucketIndexShardsManager start_marker_mgr;
  BucketIndexShardsManager end_marker_mgr;

  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, current_index,
                                    &index_pool, &bucket_objs, nullptr);
  if (r < 0) {
    return r;
  }

  r = start_marker_mgr.from_string(start_marker, shard_id);
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
    auto yield = y.get_yield_context();
    auto ex = yield.get_executor();
    auto writer = TrimWriter{*dpp, ex, index_pool, start_marker_mgr, end_marker_mgr};
    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio, yield[ec]);
  } else {
    auto ex = boost::asio::make_strand(boost::asio::system_executor{});
    auto writer = TrimWriter{*dpp, ex, index_pool, start_marker_mgr, end_marker_mgr};
    maybe_warn_about_blocking(dpp);
    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio,
                                     ceph::async::use_blocked[ec]);
  }
  return ceph::from_error_code(ec);
}

int RGWSI_BILog_RADOS_InIndex::log_start(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, current_index,
                                    &index_pool, &bucket_objs, nullptr);
  if (r < 0) {
    return r;
  }

  const size_t max_aio = cct->_conf->rgw_bucket_index_max_aio;
  boost::system::error_code ec;
  if (y) {
    auto yield = y.get_yield_context();
    auto ex = yield.get_executor();
    auto writer = StartWriter{*dpp, ex, index_pool};
    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio, yield[ec]);
  } else {
    auto ex = boost::asio::make_strand(boost::asio::system_executor{});
    auto writer = StartWriter{*dpp, ex, index_pool};
    maybe_warn_about_blocking(dpp);
    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio,
                                     ceph::async::use_blocked[ec]);
  }
  return ceph::from_error_code(ec);
}

int RGWSI_BILog_RADOS_InIndex::log_stop(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id)
{
  librados::IoCtx index_pool;
  map<int, string> bucket_objs;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = svc.bi->open_bucket_index(dpp, bucket_info, shard_id, current_index,
                                    &index_pool, &bucket_objs, nullptr);
  if (r < 0) {
    return r;
  }

  const size_t max_aio = cct->_conf->rgw_bucket_index_max_aio;
  boost::system::error_code ec;
  if (y) {
    auto yield = y.get_yield_context();
    auto ex = yield.get_executor();
    auto writer = StopWriter{*dpp, ex, index_pool};
    rgwrados::shard_io::async_writes(writer, bucket_objs, max_aio, yield[ec]);
  } else {
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
  if (r < 0) {
    return r;
  }

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

int RGWSI_BILog_RADOS_InIndex::log_list(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id, string& marker, uint32_t max,
    std::list<rgw_bi_log_entry>& result, bool *truncated)
{
  ldpp_dout(dpp, 20) << __func__ << ": " << bucket_info.bucket
                     << " marker " << marker << " shard_id=" << shard_id
                     << " max " << max << dendl;
  result.clear();

  BucketIndexShardsManager marker_mgr;
  const bool has_shards = (shard_id >= 0);
  int r = marker_mgr.from_string(marker, shard_id);
  if (r < 0) {
    return r;
  }

  std::map<int, cls_rgw_bi_log_list_ret> bi_log_lists;
  r = bilog_list(dpp, y, svc.bi, bucket_info, log_layout, marker_mgr,
                 shard_id, max, bi_log_lists);
  if (r < 0) {
    return r;
  }

  map<int, list<rgw_bi_log_entry>::iterator> vcurrents;
  map<int, list<rgw_bi_log_entry>::iterator> vends;
  if (truncated) {
    *truncated = false;
  }
  for (auto& [sid, ret] : bi_log_lists) {
    vcurrents[sid] = ret.entries.begin();
    vends[sid] = ret.entries.end();
    if (truncated) {
      *truncated = (*truncated || ret.truncated);
    }
  }

  size_t total = 0;
  bool has_more = true;
  while (total < max && has_more) {
    has_more = false;
    auto viter = vcurrents.begin();
    auto eiter = vends.begin();
    for (; total < max && viter != vcurrents.end(); ++viter, ++eiter) {
      assert(eiter != vends.end());
      int sid = viter->first;
      auto& liter = viter->second;
      if (liter == eiter->second) {
        continue;
      }
      rgw_bi_log_entry& entry = *liter;
      if (has_shards) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%d", sid);
        string tmp_id;
        build_bucket_index_marker(buf, entry.id, &tmp_id);
        entry.id.swap(tmp_id);
      }
      marker_mgr.add(sid, entry.id);
      result.push_back(entry);
      total++;
      has_more = true;
      ++liter;
    }
  }

  if (truncated) {
    for (auto viter = vcurrents.begin(), eiter = vends.begin();
         viter != vcurrents.end(); ++viter, ++eiter) {
      assert(eiter != vends.end());
      *truncated = (*truncated || (viter->second != eiter->second));
    }
  }

  if (has_shards) {
    marker_mgr.to_string(&marker);
  } else if (!result.empty()) {
    marker = result.rbegin()->id;
  }

  return 0;
}

int RGWSI_BILog_RADOS_InIndex::get_log_status(
    const DoutPrefixProvider *dpp,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id,
    map<int, string> *markers,
    optional_yield y)
{
  vector<rgw_bucket_dir_header> headers;
  map<int, string> bucket_instance_ids;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = svc.bi->cls_bucket_head(dpp, bucket_info, current_index, shard_id,
                                   &headers, &bucket_instance_ids, y);
  if (r < 0) {
    return r;
  }

  ceph_assert(headers.size() == bucket_instance_ids.size());

  auto iter = headers.begin();
  auto viter = bucket_instance_ids.begin();
  for (; iter != headers.end(); ++iter, ++viter) {
    if (shard_id >= 0) {
      (*markers)[shard_id] = iter->max_marker;
    } else {
      (*markers)[viter->first] = iter->max_marker;
    }
  }

  return 0;
}

// FIFO backend
// ------------
// each bucket shard keeps its bilog in a dedicated FIFO object whose name is
// derived from the shard index OID via bilog_fifo_oid().  FIFO objects are
// created lazily on the first write. there is no explicit creation step at
// bucket creation time.
// callers just construct a LazyFIFO and call list/trim/etc.
//
// Marker format
// -------------
// raw FIFO markers are strings like "00000001".
// when a bucket has multiple shards, callers of log_list expect
// markers of the form "<shard_id>#<raw_marker>" (e.g. "3#00000001") — the
// same format used by the InIndex backend. we build those in log_entry.id
// via build_bucket_index_marker()

void RGWSI_BILog_RADOS_FIFO::init(RGWSI_BucketIndex_RADOS *bi_rados_svc,
                                   neorados::RADOS rados_neo_)
{
  bi_ = bi_rados_svc;
  rados_neo.emplace(std::move(rados_neo_));
}

int RGWSI_BILog_RADOS_FIFO::log_start(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id)
{
  return 0; // FIFO has no pause/resume state
}

int RGWSI_BILog_RADOS_FIFO::log_stop(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id)
{
  return 0; // FIFO has no explicit cleanup
}

int RGWSI_BILog_RADOS_FIFO::log_trim(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id,
    std::string_view start_marker,
    std::string_view end_marker)
{
  // discard all FIFO entries up to (but not including) end_marker from every
  // relevant shard.  start_marker is intentionally unused: FIFO trim always
  // progresses from the tail and cls_fifo tracks what has already been
  // trimmed internally, so there is no need to pass a lower bound.
  //
  // end_marker is a BucketIndexShardsManager composite string of the form
  // "<sid>#<raw>,...".  We look up each shard's raw marker
  // with end_marker_mgr.get(sid, "").  Shards absent from the composite
  // string are skipped (their per-shard marker is empty).
  //
  // LazyFIFO::trim(inclusive=false) trims everything *before* the given
  // marker, consistent with InIndex bilog semantics where the end marker
  // itself is not removed.
  if (end_marker.empty()) {
    return 0;
  }

  librados::IoCtx index_pool;
  std::map<int, std::string> shard_oids;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = bi_->open_bucket_index(dpp, bucket_info, shard_id, current_index,
                                 &index_pool, &shard_oids, nullptr);
  if (r < 0) {
    return r;
  }

  neorados::IOContext neo_loc(index_pool.get_id());
  BucketIndexShardsManager end_marker_mgr;
  r = end_marker_mgr.from_string(end_marker, shard_id);
  if (r < 0) {
    return r;
  }

  int ret = 0;
  for (auto& [sid, oid] : shard_oids) {
    const std::string shard_end = end_marker_mgr.get(sid, "");
    if (shard_end.empty()) {
      continue;
    }
    // each bucket index shard OID maps to a FIFO object named
    // <shard_oid>.bilog.fifo.  LazyFIFO opens (or creates) it on demand.
    const std::string fifo_oid = bilog_fifo_oid(oid);
    LazyFIFO lf(*rados_neo, fifo_oid, neo_loc);
    try {
      if (y) {
        lf.trim(dpp, shard_end, false /* inclusive */, y.get_yield_context());
      } else {
        maybe_warn_about_blocking(dpp);
        asio::spawn(
            rados_neo->get_executor(),
            [&](asio::yield_context inner_y) {
              lf.trim(dpp, shard_end, false /* inclusive */, inner_y);
            },
            ceph::async::use_blocked);
      }
    } catch (const boost::system::system_error& e) {
      // log and continue trimming remaining shards; trim is best-effort and
      // the trim cycle will retry any skipped shards on its next iteration.
      ldpp_dout(dpp, 5) << __func__ << ": FIFO trim on " << fifo_oid
                        << " shard=" << sid << " failed: " << e.what()
                        << " (continuing)" << dendl;
      ret = ceph::from_error_code(e.code());
    }
  }
  return ret;
}

int RGWSI_BILog_RADOS_FIFO::log_list(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id, string& marker, uint32_t max,
    std::list<rgw_bi_log_entry>& result, bool *truncated)
{
  // read up to `max` bilog entries across all relevant shards starting after
  // `marker`. shards are visited in ascending shard-id order (shard_oids is a
  // std::map). entries from each shard are appended consecutively
  ldpp_dout(dpp, 20) << __func__ << ": " << bucket_info.bucket
                     << " marker " << marker << " shard_id=" << shard_id
                     << " max " << max << dendl;
  result.clear();
  if (truncated) {
    *truncated = false;
  }
  if (max == 0) {
    return 0;
  }

  librados::IoCtx index_pool;
  std::map<int, std::string> shard_oids;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = bi_->open_bucket_index(dpp, bucket_info, shard_id, current_index,
                                 &index_pool, &shard_oids, nullptr);
  if (r < 0) {
    return r;
  }

  neorados::IOContext neo_loc(index_pool.get_id());
  BucketIndexShardsManager marker_mgr;
  r = marker_mgr.from_string(marker, shard_id);
  if (r < 0) {
    return r;
  }

  uint32_t total = 0;
  for (auto& [sid, oid] : shard_oids) {
    if (total >= max) {
      if (truncated) {
        *truncated = true;
      }
      break;
    }

    static constexpr uint32_t FIFO_BATCH_SIZE = 128;
    const uint32_t fetch = std::min<uint32_t>(max - total, FIFO_BATCH_SIZE);
    std::vector<fifo::entry> entries_buf(fetch);
    const std::string fifo_oid = bilog_fifo_oid(oid);
    LazyFIFO lf(*rados_neo, fifo_oid, neo_loc);
    // raw per-shard resume marker. empty string means from the beginning.
    std::string shard_marker = marker_mgr.get(sid, "");
    // next_marker is returned by LazyFIFO::list when more entries exist
    // beyond the batch
    std::optional<std::string> next_marker;
    std::span<fifo::entry> got;

    try {
      if (y) {
        std::tie(got, next_marker) =
            lf.list(dpp, shard_marker, std::span{entries_buf},
                    y.get_yield_context());
      } else {
        maybe_warn_about_blocking(dpp);
        asio::spawn(
            rados_neo->get_executor(),
            [&](asio::yield_context inner_y) {
              std::tie(got, next_marker) =
                  lf.list(dpp, shard_marker, std::span{entries_buf}, inner_y);
            },
            ceph::async::use_blocked);
      }
    } catch (const boost::system::system_error& e) {
      ldpp_dout(dpp, 5) << __func__ << ": FIFO list on " << fifo_oid
                        << " failed: " << e.what() << dendl;
      return ceph::from_error_code(e.code());
    }

    for (auto& entry : got) {
      // each FIFO entry is an encoded rgw_bi_log_entry.
      rgw_bi_log_entry log_entry;
      try {
        auto p = entry.data.cbegin();
        decode(log_entry, p);
      } catch (const ceph::buffer::error& e) {
        ldpp_dout(dpp, 0) << __func__ << ": failed to decode bilog entry from "
                          << fifo_oid << ": " << e.what() << dendl;
        return -EIO;
      }
      // build the composite id returned to the caller. multi-shard queries
      // require "<sid>#<raw>". single-shard queries use the raw marker directly.
      if (shard_id >= 0) {
        char buf[16];
        snprintf(buf, sizeof(buf), "%d", sid);
        std::string prefixed;
        build_bucket_index_marker(buf, entry.marker, &prefixed);
        log_entry.id = std::move(prefixed);
      } else {
        log_entry.id = entry.marker;
      }
      // track the raw FIFO marker.
      // marker_mgr.to_string() will prepend the shard prefix.
      marker_mgr.add(sid, entry.marker);
      result.push_back(std::move(log_entry));
      ++total;
    }
    if (next_marker && truncated) {
      *truncated = true;
    }
  }

  if (shard_id >= 0) {
    marker_mgr.to_string(&marker);
  } else if (!result.empty()) {
    marker = result.rbegin()->id;
  }

  return 0;
}

int RGWSI_BILog_RADOS_FIFO::get_log_status(
    const DoutPrefixProvider *dpp,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id,
    map<int, string> *markers,
    optional_yield y)
{
  // returns the marker of the most recently pushed entry in each shard FIFO.
  // callers (e.g. radosgw-admin bilog status, bilog trim logic) use this as
  // the upper bound for a subsequent log_trim() call.
  //
  // LazyFIFO::last_entry_info() reads the FIFO tail pointer and returns a
  // tuple<marker, timestamp>.

  // if a FIFO object does not yet exist for a shard (new bucket with no
  // operations logged), last_entry_info() returns an empty marker string.
  // the caller should treat an empty marker as nothing to trim.
  librados::IoCtx index_pool;
  std::map<int, std::string> shard_oids;
  const auto& current_index = rgw::log_to_index_layout(log_layout);
  int r = bi_->open_bucket_index(dpp, bucket_info, shard_id, current_index,
                                 &index_pool, &shard_oids, nullptr);
  if (r < 0) {
    return r;
  }

  neorados::IOContext neo_loc(index_pool.get_id());
  for (auto& [sid, oid] : shard_oids) {
    const std::string fifo_oid = bilog_fifo_oid(oid);
    LazyFIFO lf(*rados_neo, fifo_oid, neo_loc);
    try {
      auto get_info = [&]() -> std::tuple<std::string, ceph::real_time> {
        if (y) {
          return lf.last_entry_info(dpp, y.get_yield_context());
        } else {
          maybe_warn_about_blocking(dpp);
          std::tuple<std::string, ceph::real_time> result;
          asio::spawn(
              rados_neo->get_executor(),
              [&](asio::yield_context inner_y) {
                result = lf.last_entry_info(dpp, inner_y);
              },
              ceph::async::use_blocked);
          return result;
        }
      };
      (*markers)[sid] = std::get<0>(get_info());
    } catch (const boost::system::system_error& e) {
      ldpp_dout(dpp, 5) << __func__ << ": FIFO last_entry_info on "
                        << fifo_oid << " failed: " << e.what() << dendl;
      return ceph::from_error_code(e.code());
    }
  }

  return 0;
}

// ── BackendDispatcher ─────────────────────────────────────────────────────────

RGWSI_BILog_RADOS_BackendDispatcher::RGWSI_BILog_RADOS_BackendDispatcher(
    CephContext *cct)
  : RGWSI_BILog_RADOS(cct),
    backend_inindex(cct),
    backend_fifo(cct)
{
}

void RGWSI_BILog_RADOS_BackendDispatcher::init(
    RGWSI_BucketIndex_RADOS *bi_rados_svc, neorados::RADOS rados_neo_)
{
  rados_neo.emplace(rados_neo_);
  backend_inindex.init(bi_rados_svc, rados_neo_);
  backend_fifo.init(bi_rados_svc, std::move(rados_neo_));
}

RGWSI_BILog_RADOS& RGWSI_BILog_RADOS_BackendDispatcher::get_backend(
    const rgw::bucket_log_layout_generation& log_layout)
{
  if (log_layout.layout.type == rgw::BucketLogType::FIFO) {
    return backend_fifo;
  }
  return backend_inindex;
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_start(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout, int shard_id)
{
  return get_backend(log_layout).log_start(dpp, y, bucket_info, log_layout,
                                           shard_id);
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_stop(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout, int shard_id)
{
  return get_backend(log_layout).log_stop(dpp, y, bucket_info, log_layout,
                                          shard_id);
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_trim(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id, std::string_view start_marker, std::string_view end_marker)
{
  return get_backend(log_layout).log_trim(dpp, y, bucket_info, log_layout,
                                          shard_id, start_marker, end_marker);
}

int RGWSI_BILog_RADOS_BackendDispatcher::log_list(
    const DoutPrefixProvider *dpp, optional_yield y,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id, string& marker, uint32_t max,
    std::list<rgw_bi_log_entry>& result, bool *truncated)
{
  return get_backend(log_layout).log_list(dpp, y, bucket_info, log_layout,
                                          shard_id, marker, max, result,
                                          truncated);
}

int RGWSI_BILog_RADOS_BackendDispatcher::get_log_status(
    const DoutPrefixProvider *dpp,
    const RGWBucketInfo& bucket_info,
    const rgw::bucket_log_layout_generation& log_layout,
    int shard_id, map<int, string> *markers, optional_yield y)
{
  return get_backend(log_layout).get_log_status(dpp, bucket_info, log_layout,
                                                 shard_id, markers, y);
}

