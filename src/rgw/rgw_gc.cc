// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_gc.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/lock/cls_lock_client.h"
#include "include/random.h"

#include <list>
#include <sstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace librados;

static string gc_oid_prefix = "gc";
static string gc_index_lock_name = "gc_process";


void RGWGC::initialize(CephContext *_cct, RGWRados *_store) {
  cct = _cct;
  store = _store;

  max_objs = min(static_cast<int>(cct->_conf->rgw_gc_max_objs), rgw_shards_max());

  obj_names = new string[max_objs];

  for (int i = 0; i < max_objs; i++) {
    obj_names[i] = gc_oid_prefix;
    char buf[32];
    snprintf(buf, 32, ".%d", i);
    obj_names[i].append(buf);
  }
}

void RGWGC::finalize()
{
  delete[] obj_names;
}

int RGWGC::tag_index(const string& tag)
{
  return rgw_shard_id(tag, max_objs);
}

void RGWGC::add_chain(ObjectWriteOperation& op, cls_rgw_obj_chain& chain, const string& tag)
{
  cls_rgw_gc_obj_info info;
  info.chain = chain;
  info.tag = tag;

  cls_rgw_gc_set_entry(op, cct->_conf->rgw_gc_obj_min_wait, info);
}

int RGWGC::send_chain(cls_rgw_obj_chain& chain, const string& tag, bool sync)
{
  ObjectWriteOperation op;
  add_chain(op, chain, tag);

  int i = tag_index(tag);

  if (sync)
    return store->gc_operate(obj_names[i], &op);

  return store->gc_aio_operate(obj_names[i], &op);
}

int RGWGC::defer_chain(const string& tag, bool sync)
{
  ObjectWriteOperation op;
  cls_rgw_gc_defer_entry(op, cct->_conf->rgw_gc_obj_min_wait, tag);

  int i = tag_index(tag);

  if (sync)
    return store->gc_operate(obj_names[i], &op);

  return store->gc_aio_operate(obj_names[i], &op);
}

int RGWGC::remove(int index, const std::vector<string>& tags, AioCompletion **pc)
{
  ObjectWriteOperation op;
  cls_rgw_gc_remove(op, tags);
  return store->gc_aio_operate(obj_names[index], &op, pc);
}

int RGWGC::list(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated)
{
  result.clear();
  string next_marker;

  for (; *index < max_objs && result.size() < max; (*index)++, marker.clear()) {
    std::list<cls_rgw_gc_obj_info> entries;
    int ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[*index], marker, max - result.size(), expired_only, entries, truncated, next_marker);
    if (ret == -ENOENT)
      continue;
    if (ret < 0)
      return ret;

    std::list<cls_rgw_gc_obj_info>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      result.push_back(*iter);
    }

    marker = next_marker;

    if (*index == max_objs - 1) {
      /* we cut short here, truncated will hold the correct value */
      return 0;
    }

    if (result.size() == max) {
      /* close approximation, it might be that the next of the objects don't hold
       * anything, in this case truncated should have been false, but we can find
       * that out on the next iteration
       */
      *truncated = true;
      return 0;
    }

  }
  *truncated = false;

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

#define MAX_AIO_DEFAULT 10
  size_t max_aio{MAX_AIO_DEFAULT};

public:
  RGWGCIOManager(const DoutPrefixProvider* _dpp, CephContext *_cct, RGWGC *_gc) : dpp(_dpp),
                                                  cct(_cct),
                                                  gc(_gc),
                                                  remove_tags(cct->_conf->rgw_gc_max_objs) {
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
      handle_next_completion();
    }

    AioCompletion *c = librados::Rados::aio_create_completion(NULL, NULL, NULL);
    int ret = ioctx->aio_operate(oid, c, op);
    if (ret < 0) {
      return ret;
    }
    ios.push_back(IO{IO::TailIO, c, oid, index, tag});

    return 0;
  }

  void handle_next_completion() {
    ceph_assert(!ios.empty());
    IO& io = ios.front();
    io.c->wait_for_safe();
    int ret = io.c->get_return_value();
    io.c->release();

    if (ret == -ENOENT) {
      ret = 0;
    }

    if (io.type == IO::IndexIO) {
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

    schedule_tag_removal(io.index, io.tag);

  done:
    ios.pop_front();
  }

  void schedule_tag_removal(int index, string tag) {
    auto& rt = remove_tags[index];

    // since every element of a chain tries to add the same tag, and
    // since chains are handled sequentially, check to make sure it's
    // not already on the list
    if (rt.empty() || rt.back() != tag) {
      rt.push_back(tag);
      if (rt.size() >= (size_t)cct->_conf->rgw_gc_max_trim_chunk) {
	flush_remove_tags(index, rt);
      }
    }
  }

  void drain_ios() {
    while (!ios.empty()) {
      if (gc->going_down()) {
        return;
      }
      handle_next_completion();
    }
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

    // use lambda to assemble list, so it will only get executed if
    // we're at the appropirate logging level
    auto lister = [&rt]() -> std::string {
      std::stringstream out;
      bool first = true;

      for (const auto& s : rt) {
	if (first) {
	  first = false;
	} else {
	  out << ", ";
	}
	out << s;
      }

      return out.str();
    };

    ldpp_dout(dpp, 20) << __func__ <<
      " removing entries from gc log shard index=" << index << ", size=" <<
      rt.size() << ", entries=[" << lister() << "]" << dendl;

    int ret = gc->remove(index, rt, &index_io.c);
    rt.clear();
    if (ret < 0) {
      /* we already cleared list of tags, this prevents us from
       * ballooning in case of a persistent problem
       */
      ldpp_dout(dpp, 0) << "WARNING: failed to remove tags on gc shard index=" <<
	index << " ret=" << ret << dendl;
      return;
    }

    ios.push_back(index_io);
  }

  void flush_remove_tags() {
    int index = 0;
    for (auto& rt : remove_tags) {
      flush_remove_tags(index, rt);
      ++index;
    }
  }
}; // class RGWGCIOManger

int RGWGC::process(int index, int max_secs, bool expired_only,
                   RGWGCIOManager& io_manager)
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
  bool truncated;
  IoCtx *ctx = new IoCtx;
  do {
    int max = 100;
    std::list<cls_rgw_gc_obj_info> entries;

    ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[index], marker, max,
			  expired_only, entries, &truncated, next_marker);
    ldpp_dout(this, 20) <<
      "RGWGC::process cls_rgw_gc_list returned with returned:" << ret <<
      ", entries.size=" << entries.size() << ", truncated=" << truncated <<
      ", next_marker='" << next_marker << "'" << dendl;

    if (ret == -ENOENT) {
      ret = 0;
      goto done;
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

      if (chain.objs.empty()) {
        io_manager.schedule_tag_removal(index, info.tag);
      } else {
	for (liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
	  cls_rgw_obj& obj = *liter;

	  if (obj.pool != last_pool) {
	    delete ctx;
	    ctx = new IoCtx;
	    ret = rgw_init_ioctx(store->get_rados_handle(), obj.pool, *ctx);
	    if (ret < 0) {
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
	  }
	  if (going_down()) {
	    // leave early, even if tag isn't removed, it's ok since it
	    // will be picked up next time around
	    goto done;
	  }
	} // chains loop
      } // else -- chains not empty
    } // entries loop
  } while (truncated);

done:
  /* we don't drain here, because if we're going down we don't want to
   * hold the system if backend is unresponsive
   */
  l.unlock(&store->gc_pool_ctx, obj_names[index]);
  delete ctx;

  return 0;
}

int RGWGC::process(bool expired_only)
{
  int max_secs = cct->_conf->rgw_gc_processor_max_time;

  const int start = ceph::util::generate_random_number(0, max_objs - 1);

  RGWGCIOManager io_manager(this, store->ctx(), this);

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    int ret = process(index, max_secs, expired_only, io_manager);
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
    int r = gc->process(true);
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

    lock.Lock();
    cond.WaitInterval(lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!gc->going_down());

  return NULL;
}

void RGWGC::GCWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}
