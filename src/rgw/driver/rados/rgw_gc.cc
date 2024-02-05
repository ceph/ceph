// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include "include/random.h"
#include "rgw_gc_log.h"

#include <list> // XXX
#include <sstream>
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
    const uint64_t queue_size = cct->_conf->rgw_gc_max_queue_size, num_deferred_entries = cct->_conf->rgw_gc_max_deferred;
    gc_log_init2(op, queue_size, num_deferred_entries);
    store->gc_operate(this, obj_names[i], &op, y);
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

  auto ret = store->gc_operate(this, obj_names[i], &op, y);
  if (ret != -ECANCELED && ret != -EPERM) {
    return ret;
  }
  ObjectWriteOperation set_entry_op;
  cls_rgw_gc_set_entry(set_entry_op, cct->_conf->rgw_gc_obj_min_wait, info);
  return store->gc_operate(this, obj_names[i], &set_entry_op, y);
}

struct defer_chain_state {
  librados::AioCompletion* completion = nullptr;
  // TODO: hold a reference on the state in RGWGC to avoid use-after-free if
  // RGWGC destructs before this completion fires
  RGWGC* gc = nullptr;
  cls_rgw_gc_obj_info info;

  ~defer_chain_state() {
    if (completion) {
      completion->release();
    }
  }
};

static void async_defer_callback(librados::completion_t, void* arg)
{
  std::unique_ptr<defer_chain_state> state{static_cast<defer_chain_state*>(arg)};
  if (state->completion->get_return_value() == -ECANCELED) {
    state->gc->on_defer_canceled(state->info);
  }
}

void RGWGC::on_defer_canceled(const cls_rgw_gc_obj_info& info)
{
  const std::string& tag = info.tag;
  const int i = tag_index(tag);

  // ECANCELED from cls_version_check() tells us that we've transitioned
  transitioned_objects_cache[i] = true;

  ObjectWriteOperation op;
  cls_rgw_gc_queue_defer_entry(op, cct->_conf->rgw_gc_obj_min_wait, info);
  cls_rgw_gc_remove(op, {tag});

  aio_completion_ptr c{librados::Rados::aio_create_completion(nullptr, nullptr)};

  store->gc_aio_operate(obj_names[i], c.get(), &op);
}

int RGWGC::async_defer_chain(const string& tag, const cls_rgw_obj_chain& chain)
{
  const int i = tag_index(tag);
  cls_rgw_gc_obj_info info;
  info.chain = chain;
  info.tag = tag;

  // if we've transitioned this shard object, we can rely on the cls_rgw_gc queue
  if (transitioned_objects_cache[i]) {
    ObjectWriteOperation op;
    cls_rgw_gc_queue_defer_entry(op, cct->_conf->rgw_gc_obj_min_wait, info);

    // this tag may still be present in omap, so remove it once the cls_rgw_gc
    // enqueue succeeds
    cls_rgw_gc_remove(op, {tag});

    aio_completion_ptr c{librados::Rados::aio_create_completion(nullptr, nullptr)};

    int ret = store->gc_aio_operate(obj_names[i], c.get(), &op);
    return ret;
  }

  // if we haven't seen the transition yet, write the defer to omap with cls_rgw
  ObjectWriteOperation op;

  // assert that we haven't initialized cls_rgw_gc queue. this prevents us
  // from writing new entries to omap after the transition
  gc_log_defer1(op, cct->_conf->rgw_gc_obj_min_wait, info);

  // prepare a callback to detect the transition via ECANCELED from cls_version_check()
  auto state = std::make_unique<defer_chain_state>();
  state->gc = this;
  state->info.chain = chain;
  state->info.tag = tag;
  state->completion = librados::Rados::aio_create_completion(
      state.get(), async_defer_callback);

  int ret = store->gc_aio_operate(obj_names[i], state->completion, &op);
  if (ret == 0) {
    // coverity[leaked_storage:SUPPRESS]
    state.release(); // release ownership until async_defer_callback()
  }
  return ret;
}

int RGWGC::remove(int index, const std::vector<string>& tags, AioCompletion **pc, optional_yield y)
{
  ObjectWriteOperation op;
  cls_rgw_gc_remove(op, tags);

  aio_completion_ptr c{librados::Rados::aio_create_completion(nullptr, nullptr)};
  int ret = store->gc_aio_operate(obj_names[index], c.get(), &op);
  if (ret >= 0) {
    *pc = c.get();
    c.release();
  }
  return ret;
}

int RGWGC::remove(int index, int num_entries, optional_yield y)
{
  ObjectWriteOperation op;
  cls_rgw_gc_queue_remove_entries(op, num_entries);

  return store->gc_operate(this, obj_names[index], &op, y);
}

int RGWGC::list(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated, bool& processing_queue)
{
  result.clear();
  string next_marker;
  bool check_queue = false;

  for (; *index < max_objs && result.size() < max; (*index)++, marker.clear(), check_queue = false) {
    std::list<cls_rgw_gc_obj_info> entries, queue_entries;
    int ret = 0;

    //processing_queue is set to true from previous iteration if the queue was under process and probably has more elements in it.
    if (! transitioned_objects_cache[*index] && ! check_queue && ! processing_queue) {
      ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[*index], marker, max - result.size(), expired_only, entries, truncated, next_marker);
      if (ret != -ENOENT && ret < 0) {
        return ret;
      }
      obj_version objv;
      cls_version_read(store->gc_pool_ctx, obj_names[*index], &objv);
      if (ret == -ENOENT || entries.size() == 0) {
        if (objv.ver == 0) {
          continue;
        } else {
          if (! expired_only) {
            transitioned_objects_cache[*index] = true;
            marker.clear();
          } else {
            std::list<cls_rgw_gc_obj_info> non_expired_entries;
            ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[*index], marker, 1, false, non_expired_entries, truncated, next_marker);
            if (non_expired_entries.size() == 0) {
              transitioned_objects_cache[*index] = true;
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
    if (transitioned_objects_cache[*index] || check_queue || processing_queue) {
      processing_queue = false;
      ret = cls_rgw_gc_queue_list_entries(store->gc_pool_ctx, obj_names[*index], marker, (max - result.size()) - entries.size(), expired_only, queue_entries, truncated, next_marker);
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

    if (*index == max_objs - 1) {
      if (queue_entries.size() > 0 && *truncated) {
        processing_queue = true;
      } else {
        processing_queue = false;
      }
      /* we cut short here, truncated will hold the correct value */
      return 0;
    }

    if (result.size() == max) {
      if (queue_entries.size() > 0 && *truncated) {
        processing_queue = true;
      } else {
        processing_queue = false;
        *index += 1; //move to next gc object
      }

      /* close approximation, it might be that the next of the objects don't hold
       * anything, in this case truncated should have been false, but we can find
       * that out on the next iteration
       */
      *truncated = true;
      return 0;
    }
  }
  *truncated = false;
  processing_queue = false;

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
      IndexIO = 2,
    } type{UnknownIO};
    librados::AioCompletion *c{nullptr};
    string oid;
    int index{-1};
    string tag;
  };

  deque<IO> ios;
  vector<std::vector<string> > remove_tags;
  /* tracks the number of remaining shadow objects for a given tag in order to
   * only remove the tag once all shadow objects have themselves been removed
   */
  vector<map<string, size_t> > tag_io_size;

#define MAX_AIO_DEFAULT 10
  size_t max_aio{MAX_AIO_DEFAULT};

public:
  RGWGCIOManager(const DoutPrefixProvider* _dpp, CephContext *_cct, RGWGC *_gc) : dpp(_dpp),
                                                                                  cct(_cct),
                                                                                  gc(_gc) {
    max_aio = cct->_conf->rgw_gc_max_concurrent_io;
    remove_tags.resize(min(static_cast<int>(cct->_conf->rgw_gc_max_objs), rgw_shards_max()));
    tag_io_size.resize(min(static_cast<int>(cct->_conf->rgw_gc_max_objs), rgw_shards_max()));
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
      //Return error if we are using queue, else ignore it
      if (gc->transitioned_objects_cache[index] && ret < 0) {
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

    if (io.type == IO::IndexIO && ! gc->transitioned_objects_cache[io.index]) {
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "WARNING: gc cleanup of tags on gc shard index=" <<
	  io.index << " returned error, ret=" << ret << dendl;
      }
      goto done;
    }

    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: gc could not remove oid=" << io.oid <<
	", ret=" << ret << dendl;
      goto done;
    }

    if (! gc->transitioned_objects_cache[io.index]) {
      schedule_tag_removal(io.index, io.tag);
    }

  done:
    ios.pop_front();
    return ret;
  }

  /* This is a request to schedule a tag removal. It will be called once when
   * there are no shadow objects. But it will also be called for every shadow
   * object when there are any. Since we do not want the tag to be removed
   * until all shadow objects have been successfully removed, the scheduling
   * will not happen until the shadow object count goes down to zero
   */
  void schedule_tag_removal(int index, string tag) {
    auto& ts = tag_io_size[index];
    auto ts_it = ts.find(tag);
    if (ts_it != ts.end()) {
      auto& size = ts_it->second;
      --size;
      // wait all shadow obj delete return
      if (size != 0)
        return;

      ts.erase(ts_it);
    }

    auto& rt = remove_tags[index];

    rt.push_back(tag);
    if (rt.size() >= (size_t)cct->_conf->rgw_gc_max_trim_chunk) {
      flush_remove_tags(index, rt);
    }
  }

  void add_tag_io_size(int index, string tag, size_t size) {
    auto& ts = tag_io_size[index];
    ts.emplace(tag, size);
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
    flush_remove_tags();
    /* the tags draining might have generated more ios, drain those too */
    drain_ios();
  }

  void flush_remove_tags(int index, vector<string>& rt) {
    IO index_io;
    index_io.type = IO::IndexIO;
    index_io.index = index;

    ldpp_dout(dpp, 20) << __func__ <<
      " removing entries from gc log shard index=" << index << ", size=" <<
      rt.size() << ", entries=" << rt << dendl;

    auto rt_guard = make_scope_guard(
      [&]
	{
	  rt.clear();
	}
      );

    int ret = gc->remove(index, rt, &index_io.c, null_yield);
    if (ret < 0) {
      /* we already cleared list of tags, this prevents us from
       * ballooning in case of a persistent problem
       */
      ldpp_dout(dpp, 0) << "WARNING: failed to remove tags on gc shard index=" <<
	index << " ret=" << ret << dendl;
      return;
    }
    if (perfcounter) {
      /* log the count of tags retired for rate estimation */
      perfcounter->inc(l_rgw_gc_retire, rt.size());
    }
    ios.push_back(index_io);
  }

  void flush_remove_tags() {
    int index = 0;
    for (auto& rt : remove_tags) {
      if (! gc->transitioned_objects_cache[index]) {
        flush_remove_tags(index, rt);
      }
      ++index;
    }
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

  string marker;
  string next_marker;
  bool truncated = false;
  IoCtx *ctx = new IoCtx;
  do {
    int max = 100;
    std::list<cls_rgw_gc_obj_info> entries;

    int ret = 0;

    if (! transitioned_objects_cache[index]) {
      ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[index], marker, max, expired_only, entries, &truncated, next_marker);
      ldpp_dout(this, 20) <<
      "RGWGC::process cls_rgw_gc_list returned with returned:" << ret <<
      ", entries.size=" << entries.size() << ", truncated=" << truncated <<
      ", next_marker='" << next_marker << "'" << dendl;
      obj_version objv;
      cls_version_read(store->gc_pool_ctx, obj_names[index], &objv);
      if ((objv.ver == 1) && entries.size() == 0) {
        std::list<cls_rgw_gc_obj_info> non_expired_entries;
        ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[index], marker, 1, false, non_expired_entries, &truncated, next_marker);
        if (non_expired_entries.size() == 0) {
          transitioned_objects_cache[index] = true;
          marker.clear();
          ldpp_dout(this, 20) << "RGWGC::process cls_rgw_gc_list returned NO non expired entries, so setting cache entry to TRUE" << dendl;
        } else {
          ret = 0;
          goto done;
        }
      }
      if ((objv.ver == 0) && (ret == -ENOENT || entries.size() == 0)) {
        ret = 0;
        goto done;
      }
    }

    if (transitioned_objects_cache[index]) {
      ret = cls_rgw_gc_queue_list_entries(store->gc_pool_ctx, obj_names[index], marker, max, expired_only, entries, &truncated, next_marker);
      ldpp_dout(this, 20) <<
      "RGWGC::process cls_rgw_gc_queue_list_entries returned with return value:" << ret <<
      ", entries.size=" << entries.size() << ", truncated=" << truncated <<
      ", next_marker='" << next_marker << "'" << dendl;
      if (entries.size() == 0) {
        ret = 0;
        goto done;
      }
    }

    if (ret < 0)
      goto done;

    marker = next_marker;

    string last_pool;
    std::list<cls_rgw_gc_obj_info>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      cls_rgw_gc_obj_info& info = *iter;

      ldpp_dout(this, 20) << "RGWGC::process iterating over entry tag='" <<
	info.tag << "', time=" << info.time << ", chain.objs.size()=" <<
	info.chain.objs.size() << dendl;

      std::list<cls_rgw_obj>::iterator liter;
      cls_rgw_obj_chain& chain = info.chain;

      utime_t now = ceph_clock_now();
      if (now >= end) {
        goto done;
      }
      if (! transitioned_objects_cache[index]) {
        if (chain.objs.empty()) {
          io_manager.schedule_tag_removal(index, info.tag);
        } else {
          io_manager.add_tag_io_size(index, info.tag, chain.objs.size());
        }
      }
      if (! chain.objs.empty()) {
	for (liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
	  cls_rgw_obj& obj = *liter;

	  if (obj.pool != last_pool) {
	    delete ctx;
	    ctx = new IoCtx;
	    ret = rgw_init_ioctx(this, store->get_rados_handle(), obj.pool, *ctx);
	    if (ret < 0) {
        if (transitioned_objects_cache[index]) {
          goto done;
        }
	      last_pool = "";
	      ldpp_dout(this, 0) << "ERROR: failed to create ioctx pool=" <<
		obj.pool << dendl;
	      continue;
	    }
	    last_pool = obj.pool;
	  }

	  ctx->locator_set_key(obj.loc);

	  const string& oid = obj.key.name; /* just stored raw oid there */

	  ldpp_dout(this, 5) << "RGWGC::process removing " << obj.pool <<
	    ":" << obj.key.name << dendl;
	  ObjectWriteOperation op;
	  cls_refcount_put(op, info.tag, true);

	  ret = io_manager.schedule_io(ctx, oid, &op, index, info.tag);
	  if (ret < 0) {
	    ldpp_dout(this, 0) <<
	      "WARNING: failed to schedule deletion for oid=" << oid << dendl;
      if (transitioned_objects_cache[index]) {
        //If deleting oid failed for any of them, we will not delete queue entries
        goto done;
      }
	  }
	  if (going_down()) {
	    // leave early, even if tag isn't removed, it's ok since it
	    // will be picked up next time around
	    goto done;
	  }
	} // chains loop
      } // else -- chains not empty
    } // entries loop
    if (transitioned_objects_cache[index] && entries.size() > 0) {
      ret = io_manager.drain_ios();
      if (ret < 0) {
        goto done;
      }
      //Remove the entries from the queue
      ldpp_dout(this, 5) << "RGWGC::process removing entries, marker: " << marker << dendl;
      ret = io_manager.remove_queue_entries(index, entries.size(), null_yield);
      if (ret < 0) {
        ldpp_dout(this, 0) <<
          "WARNING: failed to remove queue entries" << dendl;
        goto done;
      }
    }
  } while (truncated);

done:
  /* we don't drain here, because if we're going down we don't want to
   * hold the system if backend is unresponsive
   */
  l.unlock(&store->gc_pool_ctx, obj_names[index]);
  delete ctx;

  return 0;
}

int RGWGC::process(bool expired_only, optional_yield y)
{
  int max_secs = cct->_conf->rgw_gc_processor_max_time;

  const int start = ceph::util::generate_random_number(0, max_objs - 1);

  RGWGCIOManager io_manager(this, store->ctx(), this);

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process(index, max_secs, expired_only, io_manager, y);
    if (ret < 0)
      return ret;
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
