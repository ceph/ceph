// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_gc.h"

#include "rgw_tools.h"
#include "include/scope_guard.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/version/cls_version_client.h"
#include "rgw_perf_counters.h"
#include "cls/lock/cls_lock_client.h"
#include "common/async/lease.h"
#include "common/async/lock_rados.h"
#include "common/async/spawn_throttle.h"
#include "common/error_code.h"
#include "include/random.h"
#include "rgw_gc_log.h"

#include <algorithm>
#include <list> // XXX
#include <sstream>
#include <boost/asio/basic_waitable_timer.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_future.hpp>
#include "xxhash.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

static string gc_oid_prefix = "gc";
static string gc_index_lock_name = "gc_process";

void RGWGC::initialize(CephContext *_cct, RGWRados *_store, optional_yield y) {
  cct = _cct;
  store = _store;

  max_objs = min(static_cast<int>(cct->_conf->rgw_gc_max_objs), rgw_shards_max());

  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = gc_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);

    auto it = transitioned_objects_cache.begin() + i;
    transitioned_objects_cache.insert(it, false);

    //version = 0 -> not ready for transition
    //version = 1 -> marked ready for transition
    librados::ObjectWriteOperation op;
    op.create(false);
    const uint64_t queue_size = cct->_conf->rgw_gc_max_queue_size;
    gc_log_init2(op, queue_size, 0);
    store->gc_operate(this, obj_names[i], std::move(op), y);
  }
}

void RGWGC::finalize()
{
  delete[] obj_names;
}

int RGWGC::tag_index(const string& tag)
{
  return rgw_shards_mod(XXH64(tag.c_str(), tag.size(), seed), max_objs);
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
  ObjectWriteOperation op;
  cls_rgw_gc_obj_info info;
  info.chain = chain;
  info.tag = tag;
  gc_log_enqueue2(op, cct->_conf->rgw_gc_obj_min_wait, info);

  int i = tag_index(tag);

  ldpp_dout(this, 20) << "RGWGC::send_chain - on object name: " << obj_names[i] << "tag is: " << tag << dendl;

  auto ret = store->gc_operate(this, obj_names[i], std::move(op), y);
  if (ret != -ECANCELED && ret != -EPERM) {
    return ret;
  }
  ObjectWriteOperation set_entry_op;
  cls_rgw_gc_set_entry(set_entry_op, cct->_conf->rgw_gc_obj_min_wait, info);
  return store->gc_operate(this, obj_names[i], std::move(set_entry_op), y);
}

static int gc_list(const DoutPrefixProvider* dpp, optional_yield y, librados::IoCtx& io_ctx,
                   std::string& oid, std::string& marker, uint32_t max, bool expired_only,
                   std::list<cls_rgw_gc_obj_info>& entries, bool& truncated, std::string& next_marker)
{
  librados::ObjectReadOperation op;
  bufferlist bl;
  cls_rgw_gc_list(op, marker, max, expired_only, bl);
  int ret = rgw_rados_operate(dpp, io_ctx, oid, std::move(op), nullptr, y);
  if (ret < 0) {
    return ret;
  }
  return cls_rgw_gc_list_decode(bl, entries, truncated, next_marker);
}

int RGWGC::list(int& index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool& truncated, bool& processing_queue, std::optional<int> shard_id)
{
  result.clear();
  string next_marker;
  bool check_queue = false;

  int max_index = shard_id.has_value() ? (shard_id.value() + 1) : max_objs;
  if (shard_id.has_value()) {
    index = shard_id.value();
  }

  for (; index < max_index && result.size() < max; index++, marker.clear(), check_queue = false) {
    std::list<cls_rgw_gc_obj_info> entries, queue_entries;
    int ret = 0;

    //processing_queue is set to true from previous iteration if the queue was under process and probably has more elements in it.
    if (! transitioned_objects_cache[index] && ! check_queue && ! processing_queue) {
      ret = gc_list(this, null_yield, store->gc_pool_ctx, obj_names[index], marker, max - result.size(), expired_only, entries, truncated, next_marker);
      if (ret != -ENOENT && ret < 0) {
        return ret;
      }
      obj_version objv;
      cls_version_read(store->gc_pool_ctx, obj_names[index], &objv);
      if (ret == -ENOENT || entries.size() == 0) {
        if (objv.ver == 0) {
          continue;
        } else {
          if (! expired_only) {
            transitioned_objects_cache[index] = true;
            marker.clear();
          } else {
            std::list<cls_rgw_gc_obj_info> non_expired_entries;
            ret = gc_list(this, null_yield, store->gc_pool_ctx, obj_names[index], marker, 1, false, non_expired_entries, truncated, next_marker);
            if (non_expired_entries.size() == 0) {
              transitioned_objects_cache[index] = true;
              marker.clear();
            }
          }
        }
      }
      if ((objv.ver == 1) && (entries.size() < max - result.size())) {
        check_queue = true;
        marker.clear();
      }
    }
    if (transitioned_objects_cache[index] || check_queue || processing_queue) {
      processing_queue = false;
      ret = cls_rgw_gc_queue_list_entries(store->gc_pool_ctx, obj_names[index], marker, (max - result.size()) - entries.size(), expired_only, queue_entries, truncated, next_marker);
      if (ret < 0) {
        return ret;
      }
    }
    if (entries.size() == 0 && queue_entries.size() == 0)
      continue;

    std::list<cls_rgw_gc_obj_info>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      result.push_back(*iter);
    }

    for (iter = queue_entries.begin(); iter != queue_entries.end(); ++iter) {
      result.push_back(*iter);
    }

    marker = next_marker;

    if (index == max_index - 1) {
      if (queue_entries.size() > 0 && truncated) {
        processing_queue = true;
      } else {
        processing_queue = false;
      }
      /* we cut short here, truncated will hold the correct value */
      return 0;
    }

    if (result.size() == max) {
      if (queue_entries.size() > 0 && truncated) {
        processing_queue = true;
      } else {
        processing_queue = false;
        index += 1; //move to next gc object
      }

      /* close approximation, it might be that the next of the objects don't hold
       * anything, in this case truncated should have been false, but we can find
       * that out on the next iteration
       */
      truncated = true;
      return 0;
    }
  }
  truncated = false;
  processing_queue = false;

  return 0;
}

// an op cut short by cancellation is shutdown, not an osd error
static void throw_if_op_cancelled(int r, const boost::asio::yield_context& y)
{
  if (r == -ECANCELED && y.cancelled() != boost::asio::cancellation_type::none) {
    throw boost::system::system_error(boost::asio::error::operation_aborted);
  }
}

int RGWGC::list_shard(int index, std::string& marker, uint32_t max,
                      std::list<cls_rgw_gc_obj_info>& entries, bool& truncated,
                      boost::asio::yield_context y)
{
  librados::ObjectReadOperation op;
  bufferlist bl;
  // removal counts unexpired entries too, so list them all
  cls_rgw_gc_queue_list_entries(op, marker, max, false, bl);
  int ret = store->gc_operate(this, obj_names[index], std::move(op), nullptr, y);
  throw_if_op_cancelled(ret, y);
  std::string next_marker;
  if (ret >= 0) {
    ret = cls_rgw_gc_queue_list_entries_decode(bl, entries, truncated, next_marker);
  }
  ldpp_dout(this, 20) <<
    "RGWGC::process cls_rgw_gc_queue_list_entries returned with return value:" << ret <<
    ", entries.size=" << entries.size() << ", truncated=" << truncated <<
    ", next_marker='" << next_marker << "'" << dendl;
  if (ret < 0) {
    return ret;
  }
  marker = std::move(next_marker);
  return 0;
}

// apply a removal of count entries that asserts the shard lock
int RGWGC::trim_shard(int index, librados::ObjectWriteOperation&& op, size_t count,
                      boost::asio::yield_context y)
{
  int ret = store->gc_operate(this, obj_names[index], std::move(op), y);
  throw_if_op_cancelled(ret, y);
  if (ret < 0) {
    ldpp_dout(this, 0) << "ERROR: failed to trim gc shard index=" << index <<
      " ret=" << ret << dendl;
    return ret;
  }
  if (perfcounter) {
    /* log the count of tags retired for rate estimation */
    perfcounter->inc(l_rgw_gc_retire, count);
  }
  return 0;
}

int RGWGC::list_shard_omap(int index, std::string& marker, uint32_t max, bool expired_only,
                           std::list<cls_rgw_gc_obj_info>& entries, bool& truncated,
                           boost::asio::yield_context y)
{
  std::string next_marker;
  int ret = gc_list(this, y, store->gc_pool_ctx, obj_names[index], marker, max,
                    expired_only, entries, truncated, next_marker);
  throw_if_op_cancelled(ret, y);
  ldpp_dout(this, 20) <<
    "RGWGC::process cls_rgw_gc_list returned with returned:" << ret <<
    ", entries.size=" << entries.size() << ", truncated=" << truncated <<
    ", next_marker='" << next_marker << "'" << dendl;
  if (ret == -ENOENT) {
    truncated = false;
  } else if (ret < 0) {
    return ret;
  }
  marker = std::move(next_marker);
  return 0;
}

int RGWGC::delete_object(pool_ctx_map& pool_ctxs, const cls_rgw_obj& obj,
                         const std::string& tag, boost::asio::yield_context y)
{
  auto p = pool_ctxs.find(obj.pool);
  if (p == pool_ctxs.end()) {
    // a missing pool waits for the latest osdmap, so look up each pool once
    librados::IoCtx ioctx;
    int ret = rgw_init_ioctx(this, store->get_rados_handle(), obj.pool, ioctx);
    if (ret < 0) {
      ldpp_dout(this, 0) << "ERROR: failed to create ioctx pool=" <<
        obj.pool << dendl;
      if (ret != -ENOENT) {
        return ret;
      }
      p = pool_ctxs.emplace(obj.pool, std::nullopt).first;
    } else {
      ioctx.set_pool_full_try(); // allow deletion at pool quota limit
      p = pool_ctxs.emplace(obj.pool, std::move(ioctx)).first;
    }
  }
  if (!p->second) {
    return 0; // the pool is gone
  }

  // dup() so concurrent children don't share the locator key
  librados::IoCtx ioctx;
  ioctx.dup(*p->second);
  ioctx.locator_set_key(obj.loc);

  const string& oid = obj.key.name; /* just stored raw oid there */

  ldpp_dout(this, 5) << "RGWGC::process removing " << obj.pool <<
    ":" << obj.key.name << dendl;
  librados::ObjectWriteOperation op;
  cls_refcount_put(op, tag, true);
  int ret = rgw_rados_operate(this, ioctx, oid, std::move(op), y);
  throw_if_op_cancelled(ret, y);
  if (ret == -ENOENT) {
    return 0;
  }
  if (ret < 0) {
    ldpp_dout(this, 0) << "WARNING: gc could not remove oid=" << oid <<
      ", ret=" << ret << dendl;
  }
  return ret;
}

using Clock = ceph::coarse_mono_clock;

static constexpr uint32_t list_max = 100;

// run f with a workpool. if f throws, cancel the children and wait for all
// of them outside the handler before rethrowing. a second cancellation can
// cut a wait short, so keep waiting until none remain
template <typename F>
static auto with_workpool(CephContext* cct, boost::asio::yield_context yield, F&& f)
{
  // clamp signed first: a negative value would wrap to a huge size_t
  const auto limit = std::max<int64_t>(1, cct->_conf->rgw_gc_max_concurrent_io);
  ceph::async::spawn_throttle workpool{yield, static_cast<size_t>(limit)};
  std::exception_ptr eptr;
  try {
    return f(workpool);
  } catch (const std::exception&) {
    eptr = std::current_exception();
  }
  const bool armed = yield.throw_if_cancelled();
  yield.throw_if_cancelled(false);
  workpool.cancel();
  for (bool joined = false; !joined; ) {
    try {
      workpool.wait();
      joined = true;
    } catch (const std::exception&) {
      // eptr already holds the first error
    }
  }
  yield.throw_if_cancelled(armed);
  std::rethrow_exception(eptr);
}

// position of the first failed entry, or failed.size() if none failed
static size_t first_failed(const std::vector<bool>& failed)
{
  return std::find(failed.begin(), failed.end(), true) - failed.begin();
}

// spawn a child per tail object; may wait for a slot. failed[i] is set
// once any object of the i-th admitted entry fails
void RGWGC::admit_entry(ceph::async::spawn_throttle& workpool,
                        std::vector<bool>& failed, pool_ctx_map& pool_ctxs,
                        const cls_rgw_gc_obj_info& info)
{
  ldpp_dout(this, 20) << "RGWGC::process iterating over entry tag='" <<
    info.tag << "', time=" << info.time << ", chain.objs.size()=" <<
    info.chain.objs.size() << dendl;
  const size_t pos = failed.size();
  failed.push_back(false);
  for (const auto& obj : info.chain.objs) {
    workpool.spawn([this, &pool_ctxs, &failed, pos, obj, tag = info.tag]
                   (boost::asio::yield_context cy) {
        cy.throw_if_cancelled(true);
        auto disarm = make_scope_guard([&] { cy.throw_if_cancelled(false); });
        if (delete_object(pool_ctxs, obj, tag, cy) < 0) {
          failed[pos] = true;
        }
      });
  }
}

// returns true once the omap log is empty and the shard may use the queue
bool RGWGC::process_omap(int index, Clock::time_point deadline, bool expired_only,
                         rados::cls::lock::Lock& l, pool_ctx_map& pool_ctxs,
                         boost::asio::yield_context yield)
{
  const size_t trim_chunk = static_cast<size_t>(
      std::max<int64_t>(1, cct->_conf->rgw_gc_max_trim_chunk));
  // the children use these, so they outlive the workpool
  std::vector<bool> failed; // per admitted entry of the batch
  std::vector<std::string> tags;
  bool trim_failed = false;

  // remove the collected tags once at least min are ready
  auto flush = [&] (size_t min) {
    if (trim_failed || tags.empty() || tags.size() < min) {
      return;
    }
    ldpp_dout(this, 20) << "RGWGC::process removing entries from gc log shard index=" <<
      index << ", size=" << tags.size() << ", entries=" << tags << dendl;
    librados::ObjectWriteOperation op;
    l.assert_locked_exclusive(&op);
    cls_rgw_gc_remove(op, tags);
    // a failed removal may have applied, so stop trimming in this lease
    trim_failed = trim_shard(index, std::move(op), tags.size(), yield) < 0;
    tags.clear();
  };

  return with_workpool(cct, yield, [&] (ceph::async::spawn_throttle& workpool) {
      std::string marker;
      bool truncated = true;
      bool stop = false;
      while (!stop && !trim_failed && truncated) {
        std::list<cls_rgw_gc_obj_info> entries;
        if (list_shard_omap(index, marker, list_max, expired_only, entries, truncated,
                            yield) < 0) {
          stop = true;
          break;
        }
        failed.clear();
        for (const auto& info : entries) {
          if (Clock::now() >= deadline) {
            stop = true;
            break;
          }
          admit_entry(workpool, failed, pool_ctxs, info);
        }
        workpool.wait();
        // a failed entry keeps its tag for the next cycle
        auto entry = entries.begin();
        for (const bool f : failed) {
          if (!f) {
            tags.push_back(entry->tag);
            flush(trim_chunk);
          }
          ++entry;
        }
      }
      flush(1);

      bool drained = false;
      if (!stop && !trim_failed) {
        // listed out; the shard must be ready for the queue with nothing left.
        // an unreadable version stays 0
        obj_version objv;
        librados::ObjectReadOperation op;
        cls_version_read(op, &objv);
        throw_if_op_cancelled(store->gc_operate(this, obj_names[index], std::move(op),
                                                nullptr, yield), yield);
        std::string probe_marker;
        std::list<cls_rgw_gc_obj_info> left;
        drained = objv.ver == 1 &&
            list_shard_omap(index, probe_marker, 1, false, left, truncated, yield) == 0 &&
            left.empty();
      }
      return drained;
    });
}

void RGWGC::process_queue(int index, Clock::time_point deadline, bool expired_only,
                          rados::cls::lock::Lock& l, pool_ctx_map& pool_ctxs,
                          boost::asio::yield_context yield)
{
  // the children use this, so it outlives the workpool
  std::vector<bool> failed; // per admitted entry of the batch

  with_workpool(cct, yield, [&] (ceph::async::spawn_throttle& workpool) {
      std::string marker;
      bool truncated = true;
      bool stop = false;
      while (!stop && truncated) {
        const auto now_real = ceph::real_clock::now();
        std::list<cls_rgw_gc_obj_info> entries;
        if (list_shard(index, marker, list_max, entries, truncated, yield) < 0 ||
            entries.empty()) {
          break;
        }
        failed.clear();
        for (const auto& info : entries) {
          // stop admitting at the deadline, a failed delete or the first unexpired entry
          if (Clock::now() >= deadline || first_failed(failed) < failed.size() ||
              (expired_only && info.time > now_real)) {
            stop = true;
            break;
          }
          admit_entry(workpool, failed, pool_ctxs, info);
        }
        workpool.wait();
        // trim the finished prefix; stop after a failed delete or removal
        const size_t count = first_failed(failed);
        if (count > 0) {
          librados::ObjectWriteOperation op;
          l.assert_locked_exclusive(&op);
          cls_rgw_gc_queue_remove_entries(op, count);
          if (trim_shard(index, std::move(op), count, yield) < 0) {
            break;
          }
        }
        if (count < failed.size()) {
          break;
        }
      }
    });
}

void RGWGC::process_locked(int index, int max_secs, bool expired_only,
                           rados::cls::lock::Lock& l, boost::asio::yield_context yield)
{
  yield.throw_if_cancelled(true);
  auto disarm = make_scope_guard([&] { yield.throw_if_cancelled(false); });

  const auto deadline = Clock::now() + std::chrono::seconds(max_secs);
  // each phase joins its children before returning, so this outlives them
  pool_ctx_map pool_ctxs;
  if (!transitioned_objects_cache[index]) {
    if (!process_omap(index, deadline, expired_only, l, pool_ctxs, yield)) {
      return;
    }
    transitioned_objects_cache[index] = true;
    ldpp_dout(this, 20) << "RGWGC::process cls_rgw_gc_list returned NO non expired "
      "entries, so setting cache entry to TRUE" << dendl;
  }
  process_queue(index, deadline, expired_only, l, pool_ctxs, yield);
}

int RGWGC::process_shard(int index, bool expired_only, boost::asio::yield_context yield)
{
  ldpp_dout(this, 20) << "RGWGC::process entered with GC index_shard=" <<
    index << ", expired_only=" << expired_only << dendl;

  /* max_secs should be greater than zero. We don't want a zero max_secs
   * to be translated as no timeout, since we'd then need to break the
   * lock and that would require a manual intervention. In this case
   * we can just wait it out. */
  const int max_secs = cct->_conf->rgw_gc_processor_max_time;
  if (max_secs <= 0) {
    return -EAGAIN;
  }

  rados::cls::lock::Lock l(gc_index_lock_name);
  l.set_cookie(gen_rand_alphanumeric(cct, 16));
  ceph::async::RadosLockClient client{yield.get_executor(), store->gc_pool_ctx,
                                      obj_names[index], l, false};
  std::exception_ptr eptr;
  try {
    ceph::async::with_lease(client, ceph::make_timespan(max_secs), yield,
        [&] (boost::asio::yield_context ly) {
          process_locked(index, max_secs, expired_only, l, ly);
        });
    return 0;
  } catch (const std::exception&) {
    eptr = std::current_exception();
  }

  if (yield.cancelled() != boost::asio::cancellation_type::none) {
    // with_lease() keeps the lock on cancellation; release it so the next
    // gc processor doesn't wait out the lease
    const bool armed = yield.throw_if_cancelled();
    yield.throw_if_cancelled(false);
    boost::system::error_code ec;
    client.async_release(yield[ec]);
    yield.throw_if_cancelled(armed);
    throw boost::system::system_error(boost::asio::error::operation_aborted);
  }

  try {
    std::rethrow_exception(eptr);
  } catch (const ceph::async::lease_aborted& e) {
    ldpp_dout(this, 0) << "RGWGC::process lost the lock on " << obj_names[index] <<
      ": " << e.code().message() << dendl;
  } catch (const boost::system::system_error& e) {
    const int ret = ceph::from_error_code(e.code());
    if (ret == -EBUSY || ret == -EEXIST) { /* already locked by another gc processor */
      ldpp_dout(this, 10) << "RGWGC::process failed to acquire lock on " <<
        obj_names[index] << dendl;
    } else {
      ldpp_dout(this, 0) << "ERROR: RGWGC::process failed on " << obj_names[index] <<
        " ret=" << ret << dendl;
    }
  } catch (const std::exception& e) {
    ldpp_dout(this, 0) << "ERROR: RGWGC::process unexpected exception on " <<
      obj_names[index] << ": " << e.what() << dendl;
    return -EIO;
  }
  return 0;
}

int RGWGC::process(bool expired_only, std::optional<int> shard_id,
                   boost::asio::yield_context yield)
{
  if (shard_id.has_value()) {
    // Process only the specified shard
    return process_shard(shard_id.value(), expired_only, yield);
  }
  // Process all shards with random start; a busy shard returns 0
  const int start = ceph::util::generate_random_number(0, max_objs - 1);
  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process_shard(index, expired_only, yield);
    if (ret < 0) {
      return ret;
    }
  }
  return 0;
}

int RGWGC::process(bool expired_only, std::optional<int> shard_id)
{
  boost::asio::io_context context;
  auto result = boost::asio::spawn(context,
      [this, expired_only, shard_id] (boost::asio::yield_context yield) {
        return process(expired_only, shard_id, yield);
      }, boost::asio::use_future);
  context.run();
  return result.get();
}

void RGWGC::process_cycles(boost::asio::yield_context yield)
{
  // spawn() reports completion by suspending one last time, so a pending
  // cancellation would make throw_if_cancelled() throw past the coroutine
  // frame and terminate the process. disarm it before the coroutine exits
  yield.throw_if_cancelled(true);
  auto disarm = make_scope_guard([&] { yield.throw_if_cancelled(false); });

  boost::asio::basic_waitable_timer<Clock> timer{yield.get_executor()};
  while (yield.cancelled() == boost::asio::cancellation_type::none) {
    const auto start = Clock::now();
    ldpp_dout(this, 2) << "garbage collection: start" << dendl;
    // shard errors come back as return codes; cancellation propagates
    int r = process(true, std::nullopt, yield);
    if (r < 0) {
      ldpp_dout(this, 0) << "ERROR: garbage collection process() returned error r="
                         << r << dendl;
    }
    ldpp_dout(this, 2) << "garbage collection: stop" << dendl;

    // start-to-start period: a deadline already in the past fires immediately
    timer.expires_at(start + std::chrono::seconds(cct->_conf->rgw_gc_processor_period));
    boost::system::error_code ec;
    timer.async_wait(yield[ec]); // cancellation ends the loop
  }
}

RGWGC::~RGWGC()
{
  if (proc_future.valid()) {
    ldpp_dout(this, -1) << "ERROR: RGWGC destructed without stop_processor" << dendl;
  }
  finalize();
}

void RGWGC::start_processor(boost::asio::io_context& context)
{
  // strand: process_locked's children share its state without a lock
  proc_strand.emplace(context.get_executor());
  proc_future = boost::asio::spawn(*proc_strand,
      [this] (boost::asio::yield_context yield) { process_cycles(yield); },
      boost::asio::bind_cancellation_slot(proc_signal.slot(),
                                          boost::asio::use_future));
}

void RGWGC::stop_processor()
{
  if (!proc_future.valid()) {
    return;
  }
  // cancellation_signal isn't thread-safe; emit it on the strand
  boost::asio::post(*proc_strand,
      [this] { proc_signal.emit(boost::asio::cancellation_type::terminal); });
  try {
    proc_future.get();
  } catch (const boost::system::system_error& e) {
    if (e.code() != boost::asio::error::operation_aborted) {
      ldpp_dout(this, -1) << "ERROR: gc processor coroutine exited: "
                          << e.what() << dendl;
    }
  } catch (const std::exception& e) {
    ldpp_dout(this, -1) << "ERROR: gc processor coroutine exited: "
                        << e.what() << dendl;
  }
}

unsigned RGWGC::get_subsys() const
{
  return dout_subsys;
}

std::ostream& RGWGC::gen_prefix(std::ostream& out) const
{
  return out << "garbage collection: ";
}
