// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_gc.h"

#include "rgw_tools.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/errno.h"
#include "common/error_code.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "rgw_perf_counters.h"
#include "cls/lock/cls_lock_client.h"
#include "include/random.h"
#include "rgw_gc_log.h"
#include "rgw_sal_rados.h"
#include "yield_completion.h"

#include <algorithm>
#include <list> // XXX
#include <sstream>
#include <vector>
#include <boost/system/system_error.hpp>
#include "xxhash.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

static string gc_oid_prefix = "gc";
static string gc_index_lock_name = "gc_process";

int RGWGC::initialize(CephContext *_cct, RGWRados *_store, optional_yield y) {
  cct = _cct;
  store = _store;

  max_objs = min(static_cast<int>(cct->_conf->rgw_gc_max_objs), rgw_shards_max());

  obj_names = new string[max_objs];
  fifos.clear();
  fifos.reserve(max_objs);

  // max_entry: FIFO max record size, matching send_split_chain.
  const uint64_t max_entry = cct->_conf->rgw_max_chunk_size ?
    static_cast<uint64_t>(cct->_conf->rgw_max_chunk_size) :
    neorados::cls::fifo::FIFO::default_max_entry_size;
  // max_part: max size of one FIFO part object. must exceed max_entry plus per-entry
  // header.
  const uint64_t max_part = std::max(neorados::cls::fifo::FIFO::default_max_part_size,
                                     max_entry * 2);

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = gc_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);

    librados::ObjectWriteOperation op;
    op.create(false);
    const uint64_t queue_size = cct->_conf->rgw_gc_max_queue_size;
    gc_log_init2(op, queue_size, 0);
    store->gc_operate(this, obj_names[i], std::move(op), y);

    try {
      auto fifo_tmp = neorados::cls::fifo::FIFO::create(
        this, store->driver->get_neorados(), fifo_oid(i),
        *store->get_gc_pool_neo_ctx(), rgw::maybe_yield(this, y),
        std::nullopt, std::nullopt, false, max_part, max_entry);
      if (!fifo_tmp) {
        ldpp_dout(this, -1) << "creating gc fifo " << fifo_oid(i)
                            << " returned empty handle" << dendl;
        finalize();
        return -ENOMEM;
      }
      fifos.push_back(std::move(fifo_tmp));
    } catch (const boost::system::system_error& e) {
      ldpp_dout(this, -1) << "creating gc fifo " << fifo_oid(i)
                          << " failed: " << e.what() << dendl;
      finalize();
      return ceph::from_error_code(e.code());
    }
  }
  return 0;
}

void RGWGC::finalize()
{
  delete[] obj_names;
  obj_names = nullptr;
  fifos.clear();
}

int RGWGC::tag_index(const string& tag)
{
  return rgw_shards_mod(XXH64(tag.c_str(), tag.size(), seed), max_objs);
}

std::string RGWGC::fifo_oid(int index) const
{
  return std::string(fifo_oid_prefix) + "." + std::to_string(index);
}

int RGWGC::fifo_push(int index, const cls_rgw_gc_obj_info& info, optional_yield y)
{
  if (index < 0 || static_cast<size_t>(index) >= fifos.size() || !fifos[index]) {
    return -ENOENT;
  }

  bufferlist bl;
  encode(info, bl);

  try {
    fifos[index]->push(this, std::move(bl), rgw::maybe_yield(this, y));
  } catch (const boost::system::system_error& e) {
    ldpp_dout(this, 0) << "ERROR: fifo_push failed oid=" << fifo_oid(index)
                       << ": " << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }
  return 0;
}

int RGWGC::fifo_list(int index, const std::string& marker, uint32_t max,
                     bool expired_only, std::list<cls_rgw_gc_obj_info>& entries,
                     bool* truncated, std::string* next_marker, optional_yield y)
{
  entries.clear();
  if (truncated) {
    *truncated = false;
  }
  if (next_marker) {
    next_marker->clear();
  }
  if (max == 0) {
    return 0;
  }
  if (index < 0 || static_cast<size_t>(index) >= fifos.size() || !fifos[index]) {
    return -ENOENT;
  }

  std::vector<neorados::cls::fifo::entry> fifo_entries(max);
  try {
    auto [lentries, lmarker] = fifos[index]->list(this, marker, fifo_entries,
                                                rgw::maybe_yield(this, y));
    const auto now = ceph::real_clock::now();
    std::string last;
    for (const auto& e : lentries) {
      cls_rgw_gc_obj_info info;
      auto iter = e.data.cbegin();
      try {
        decode(info, iter);
      } catch (const buffer::error&) {
        ldpp_dout(this, 0) << "ERROR: fifo_list failed to decode entry oid="
                           << fifo_oid(index) << " marker=" << e.marker
                           << " len=" << e.data.length() << dendl;
        return -EIO;
      }
      // grace period (rgw_gc_obj_min_wait) not over. stop listing
      if (expired_only && info.time > now) {
        if (truncated) {
          *truncated = false;
        }
        if (next_marker) {
          *next_marker = last; // last expired record or empty
        }
        return 0;
      }
      entries.push_back(std::move(info));
      last = e.marker;
    }
    if (truncated) {
      *truncated = !lmarker.empty();
    }
    if (next_marker) {
      *next_marker = last;
    }
  } catch (const boost::system::system_error& e) {
    if (e.code() == boost::system::errc::no_such_file_or_directory) {
      return 0;
    }
    ldpp_dout(this, 0) << "ERROR: fifo_list failed oid=" << fifo_oid(index)
                       << ": " << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }
  return 0;
}

int RGWGC::fifo_trim(int index, const std::string& marker, optional_yield y)
{
  if (marker.empty()) {
    return 0;
  }
  if (index < 0 || static_cast<size_t>(index) >= fifos.size() || !fifos[index]) {
    return -ENOENT;
  }

  try {
    fifos[index]->trim(this, marker, false, rgw::maybe_yield(this, y));
  } catch (const boost::system::system_error& e) {
    ldpp_dout(this, 0) << "ERROR: fifo_trim failed oid=" << fifo_oid(index)
                       << " marker=" << marker << ": " << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }
  return 0;
}

std::tuple<int, std::optional<cls_rgw_obj_chain>> RGWGC::send_split_chain(const cls_rgw_obj_chain& chain, const std::string& tag, optional_yield y)
{
  ldpp_dout(this, 20) << "RGWGC::send_split_chain - tag is: " << tag << dendl;

  if (cct->_conf->rgw_max_chunk_size) {
    cls_rgw_obj_chain broken_chain;
    cls_rgw_gc_set_entry_op op;
    op.info.tag = tag;
    size_t base_encoded_size = op.estimate_encoded_size();
    size_t total_encoded_size = base_encoded_size;

    ldpp_dout(this, 20) << "RGWGC::send_split_chain - rgw_max_chunk_size is: " << cct->_conf->rgw_max_chunk_size << dendl;

    for (auto it = chain.objs.begin(); it != chain.objs.end(); it++) {
      ldpp_dout(this, 20) << "RGWGC::send_split_chain - adding obj with name: " << it->key << dendl;
      broken_chain.objs.emplace_back(*it);
      total_encoded_size += it->estimate_encoded_size();

      ldpp_dout(this, 20) << "RGWGC::send_split_chain - total_encoded_size is: " << total_encoded_size << dendl;

      if (total_encoded_size > cct->_conf->rgw_max_chunk_size) { //dont add to chain, and send to gc
        broken_chain.objs.pop_back();
        --it;
        ldpp_dout(this, 20) << "RGWGC::send_split_chain - more than, dont add to broken chain and send chain" << dendl;
        auto ret = send_chain(broken_chain, tag, y);
        if (ret < 0) {
          broken_chain.objs.insert(broken_chain.objs.end(), it, chain.objs.end()); // add all the remainder objs to the list to be deleted inline
          ldpp_dout(this, 0) << "RGWGC::send_split_chain - send chain returned error: " << ret << dendl;
          return {ret, {broken_chain}};
        }
        broken_chain.objs.clear();
        total_encoded_size = base_encoded_size;
      }
    }
    if (!broken_chain.objs.empty()) { //when the chain is smaller than or equal to rgw_max_chunk_size
      ldpp_dout(this, 20) << "RGWGC::send_split_chain - sending leftover objects" << dendl;
      auto ret = send_chain(broken_chain, tag, y);
      if (ret < 0) {
        ldpp_dout(this, 0) << "RGWGC::send_split_chain - send chain returned error: " << ret << dendl;
        return {ret, {broken_chain}};
      }
    }
  } else {
    auto ret = send_chain(chain, tag, y);
    if (ret < 0) {
      ldpp_dout(this, 0) << "RGWGC::send_split_chain - send chain returned error: " << ret << dendl;
      return {ret, {std::move(chain)}};
    }
  }
  return {0, {}};
}

int RGWGC::send_chain(const cls_rgw_obj_chain& chain, const string& tag, optional_yield y)
{
  cls_rgw_gc_obj_info info;
  info.chain = chain;
  info.tag = tag;
  info.time = ceph::real_clock::now() +
    ceph::make_timespan(cct->_conf->rgw_gc_obj_min_wait);

  int i = tag_index(tag);

  ldpp_dout(this, 20) << "RGWGC::send_chain on fifo: " << fifo_oid(i)
                      << " tag is: " << tag << dendl;

  return fifo_push(i, info, y);
}

int RGWGC::remove(int index, int num_entries, optional_yield y)
{
  ObjectWriteOperation op;
  cls_rgw_gc_queue_remove_entries(op, num_entries);

  return store->gc_operate(this, obj_names[index], std::move(op), y);
}

int RGWGC::list(int& index, string& marker, uint32_t max, bool expired_only,
                std::list<cls_rgw_gc_obj_info>& result, bool& truncated,
                bool& processing_fifo,
                std::optional<int> shard_id)
{
  result.clear();

  int max_index = shard_id.has_value() ? (shard_id.value() + 1) : max_objs;
  if (shard_id.has_value()) {
    index = shard_id.value();
  }

  for (; index < max_index && result.size() < max; ) {
    string next_marker;
    bool more = false;
    const uint32_t remain = max - result.size();

    if (!processing_fifo) {
      std::list<cls_rgw_gc_obj_info> queue_entries;
      int ret = cls_rgw_gc_queue_list_entries(store->gc_pool_ctx, obj_names[index],
                                              marker, remain, expired_only,
                                              queue_entries, more, next_marker);
      if (ret < 0) {
        return ret;
      }
      for (auto& e : queue_entries) {
        result.push_back(std::move(e));
      }
      if (more && !queue_entries.empty()) {
        marker = next_marker;
        processing_fifo = false;
        truncated = true;
        return 0;
      }
      marker.clear();
      processing_fifo = true;
      if (result.size() == max) {
        truncated = true;
        return 0;
      }
    }

    std::list<cls_rgw_gc_obj_info> fifo_entries;
    int ret = fifo_list(index, marker, max - result.size(), expired_only,
                        fifo_entries, &more, &next_marker, null_yield);
    if (ret < 0) {
      return ret;
    }
    for (auto& e : fifo_entries) {
      result.push_back(std::move(e));
    }
    if (more) {
      marker = next_marker;
      processing_fifo = true;
      truncated = true;
      return 0;
    }
    processing_fifo = false;
    marker.clear();
    ++index;
  }

  truncated = (index < max_index);
  if (!truncated) {
    processing_fifo = false;
  }
  return 0;
}

class RGWGCIOManager {
  const DoutPrefixProvider* dpp;
  CephContext *cct;
  RGWGC *gc;

  struct IO {
    enum Type {
      UnknownIO = 0,
      TailIO = 1,
    } type{UnknownIO};
    librados::AioCompletion *c{nullptr};
    string oid;
    int index{-1};
    string tag;
  };

  deque<IO> ios;

#define MAX_AIO_DEFAULT 10
  size_t max_aio{MAX_AIO_DEFAULT};

public:
  RGWGCIOManager(const DoutPrefixProvider* _dpp, CephContext *_cct, RGWGC *_gc) : dpp(_dpp),
                                                                                  cct(_cct),
                                                                                  gc(_gc) {
    max_aio = cct->_conf->rgw_gc_max_concurrent_io;
  }

  ~RGWGCIOManager() {
    for (auto io : ios) {
      io.c->release();
    }
  }

  int schedule_io(IoCtx *ioctx, const string& oid, ObjectWriteOperation *op,
		  int index, const string& tag) {
    while (ios.size() > max_aio) {
      if (gc->going_down()) {
        return 0;
      }
      auto ret = handle_next_completion();
      if (ret < 0) {
        return ret;
      }
    }

    aio_completion_ptr c{librados::Rados::aio_create_completion(nullptr, nullptr)};
    int ret = ioctx->aio_operate(oid, c.get(), op);
    if (ret < 0) {
      return ret;
    }
    ios.push_back(IO{IO::TailIO, c.get(), oid, index, tag});
    c.release();

    return 0;
  }

  int handle_next_completion() {
    ceph_assert(!ios.empty());
    IO& io = ios.front();
    io.c->wait_for_complete();
    int ret = io.c->get_return_value();
    io.c->release();

    if (ret == -ENOENT) {
      ret = 0;
    }

    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: gc could not remove oid=" << io.oid <<
	", ret=" << ret << dendl;
    }

    ios.pop_front();
    return ret;
  }

  int drain_ios() {
    int ret_val = 0;
    while (!ios.empty()) {
      if (gc->going_down()) {
        return -EAGAIN;
      }
      auto ret = handle_next_completion();
      if (ret < 0) {
        ret_val = ret;
      }
    }
    return ret_val;
  }

  void drain() {
    drain_ios();
  }

  int remove_queue_entries(int index, int num_entries, optional_yield y) {
    int ret = gc->remove(index, num_entries, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to remove queue entries on index=" <<
	    index << " ret=" << ret << dendl;
      return ret;
    }
    if (perfcounter) {
      /* log the count of tags retired for rate estimation */
      perfcounter->inc(l_rgw_gc_retire, num_entries);
    }
    return 0;
  }
}; // class RGWGCIOManger

int RGWGC::process_chains(RGWGCIOManager& io_manager, IoCtx*& ctx,
                          string& last_pool, int index,
                          std::list<cls_rgw_gc_obj_info>& entries, utime_t end)
{
  for (auto& info : entries) {
    ldpp_dout(this, 20) << "RGWGC::process iterating over entry tag='" <<
      info.tag << "', time=" << info.time << ", chain.objs.size()=" <<
      info.chain.objs.size() << dendl;

    if (ceph_clock_now() >= end) {
      return -EAGAIN;
    }
    if (info.chain.objs.empty()) {
      continue;
    }
    for (const auto& obj : info.chain.objs) {
      if (obj.pool != last_pool) {
        IoCtx *new_ctx = new IoCtx;
        int ret = rgw_init_ioctx(this, store->get_rados_handle(), obj.pool, *new_ctx);
        if (ret < 0) {
          delete new_ctx;
          if (ret != -ENOENT) {
            return ret;
          }
          ldpp_dout(this, 0) << "ERROR: failed to create ioctx pool=" <<
            obj.pool << dendl;
          continue;
        }
        delete ctx;
        ctx = new_ctx;
        last_pool = obj.pool;
      }

      ctx->locator_set_key(obj.loc);
      ctx->set_pool_full_try();

      const string& oid = obj.key.name;

      ldpp_dout(this, 5) << "RGWGC::process removing " << obj.pool <<
        ":" << obj.key.name << dendl;
      ObjectWriteOperation op;
      cls_refcount_put(op, info.tag, true);

      int ret = io_manager.schedule_io(ctx, oid, &op, index, info.tag);
      if (ret < 0) {
        ldpp_dout(this, 0) <<
          "WARNING: failed to schedule deletion for oid=" << oid << dendl;
        return ret;
      }
      if (going_down()) {
        return -EAGAIN;
      }
    }
  }
  return 0;
}

int RGWGC::process(int index, int max_secs, bool expired_only,
                   RGWGCIOManager& io_manager, optional_yield y)
{
  ldpp_dout(this, 20) << "RGWGC::process entered with GC index_shard=" <<
    index << ", max_secs=" << max_secs << ", expired_only=" <<
    expired_only << dendl;

  rados::cls::lock::Lock l(gc_index_lock_name);
  utime_t end = ceph_clock_now();

  /* max_secs should be greater than zero. We don't want a zero max_secs
   * to be translated as no timeout, since we'd then need to break the
   * lock and that would require a manual intervention. In this case
   * we can just wait it out. */
  if (max_secs <= 0)
    return -EAGAIN;

  end += max_secs;
  utime_t time(max_secs, 0);
  l.set_duration(time);

  int ret = l.lock_exclusive(&store->gc_pool_ctx, obj_names[index]);
  if (ret == -EBUSY) { /* already locked by another gc processor */
    ldpp_dout(this, 10) << "RGWGC::process failed to acquire lock on " <<
      obj_names[index] << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  IoCtx *ctx = new IoCtx;
  string last_pool;

  for (const bool use_fifo : {false, true}) {
    if (use_fifo && going_down()) {
      break;
    }

    string marker;
    string next_marker;
    bool truncated = false;
    do {
      int max = 100;
      std::list<cls_rgw_gc_obj_info> entries;

      if (!use_fifo) {
        ret = cls_rgw_gc_queue_list_entries(store->gc_pool_ctx, obj_names[index],
                                            marker, max, expired_only, entries,
                                            truncated, next_marker);
      } else {
        ret = fifo_list(index, marker, max, expired_only, entries, &truncated,
                        &next_marker, y);
      }
      ldpp_dout(this, 20) <<
        "RGWGC::process " << (use_fifo ? "fifo_list" : "cls_rgw_gc_queue_list_entries") <<
        " returned with return value:" << ret <<
        ", entries.size=" << entries.size() << ", truncated=" << truncated <<
        ", next_marker='" << next_marker << "'" << dendl;
      if (ret < 0) {
        goto done;
      }
      if (entries.empty()) {
        break;
      }

      marker = next_marker;

      ret = process_chains(io_manager, ctx, last_pool, index, entries, end);
      if (ret < 0) {
        goto done;
      }
      ret = io_manager.drain_ios();
      if (ret < 0) {
        goto done;
      }
      if (!use_fifo) {
        ldpp_dout(this, 5) << "RGWGC::process removing queue entries, marker: " << marker << dendl;
        ret = io_manager.remove_queue_entries(index, entries.size(), null_yield);
        if (ret < 0) {
          ldpp_dout(this, 0) <<
            "WARNING: failed to remove queue entries" << dendl;
          goto done;
        }
      } else {
        ldpp_dout(this, 5) << "RGWGC::process trimming fifo entries, marker: " << marker << dendl;
        ret = fifo_trim(index, marker, y);
        if (ret < 0) {
          ldpp_dout(this, 0) <<
            "WARNING: failed to trim fifo entries" << dendl;
          goto done;
        }
        if (perfcounter) {
          perfcounter->inc(l_rgw_gc_retire, entries.size());
        }
      }
    } while (truncated && !going_down());
  }

done:
  /* we don't drain here, because if we're going down we don't want to
   * hold the system if backend is unresponsive
   */
  l.unlock(&store->gc_pool_ctx, obj_names[index]);
  delete ctx;

  return 0;
}

int RGWGC::process(bool expired_only, optional_yield y, std::optional<int> shard_id)
{
  int max_secs = cct->_conf->rgw_gc_processor_max_time;

  RGWGCIOManager io_manager(this, store->ctx(), this);

  if (shard_id.has_value()) {
    // Process only the specified shard
    int ret = process(shard_id.value(), max_secs, expired_only, io_manager, y);
    if (ret < 0)
      return ret;
  } else {
    // Process all shards with random start
    const int start = ceph::util::generate_random_number(0, max_objs - 1);
    for (int i = 0; i < max_objs; i++) {
      int index = (i + start) % max_objs;
      int ret = process(index, max_secs, expired_only, io_manager, y);
      if (ret < 0)
        return ret;
    }
  }
  if (!going_down()) {
    io_manager.drain();
  }

  return 0;
}

bool RGWGC::going_down()
{
  return down_flag;
}

void RGWGC::start_processor()
{
  worker = new GCWorker(this, cct, this);
  worker->create("rgw_gc");
}

void RGWGC::stop_processor()
{
  down_flag = true;
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}

unsigned RGWGC::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWGC::gen_prefix(std::ostream& out) const
{
  return out << "garbage collection: ";
}

void *RGWGC::GCWorker::entry() {
  do {
    utime_t start = ceph_clock_now();
    ldpp_dout(dpp, 2) << "garbage collection: start" << dendl;
    int r = gc->process(true, null_yield);
    if (r < 0) {
      ldpp_dout(dpp, 0) << "ERROR: garbage collection process() returned error r=" << r << dendl;
    }
    ldpp_dout(dpp, 2) << "garbage collection: stop" << dendl;

    if (gc->going_down())
      break;

    utime_t end = ceph_clock_now();
    end -= start;
    int secs = cct->_conf->rgw_gc_processor_period;

    if (secs <= end.sec())
      continue; // next round

    secs -= end.sec();

    std::unique_lock locker{lock};
    cond.wait_for(locker, std::chrono::seconds(secs));
  } while (!gc->going_down());

  return NULL;
}

void RGWGC::GCWorker::stop()
{
  std::lock_guard l{lock};
  cond.notify_all();
}
