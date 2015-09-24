// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_gc.h"
#include "include/rados/librados.hpp"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/refcount/cls_refcount_client.h"
#include "cls/lock/cls_lock_client.h"
#include "auth/Crypto.h"

#include <list>

#define dout_subsys ceph_subsys_rgw

using namespace std;
using namespace librados;

static string gc_oid_prefix = "gc";
static string gc_index_lock_name = "gc_process";


#define HASH_PRIME 7877

void RGWGC::initialize(CephContext *_cct, RGWRados *_store) {
  cct = _cct;
  store = _store;

  max_objs = cct->_conf->rgw_gc_max_objs;
  if (max_objs > HASH_PRIME)
    max_objs = HASH_PRIME;

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
  return ceph_str_hash_linux(tag.c_str(), tag.size()) % HASH_PRIME % max_objs;
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

int RGWGC::remove(int index, const std::list<string>& tags)
{
  ObjectWriteOperation op;
  cls_rgw_gc_remove(op, tags);
  return store->gc_operate(obj_names[index], &op);
}

int RGWGC::list(int *index, string& marker, uint32_t max, bool expired_only, std::list<cls_rgw_gc_obj_info>& result, bool *truncated)
{
  result.clear();

  for (; *index < max_objs && result.size() < max; (*index)++, marker.clear()) {
    std::list<cls_rgw_gc_obj_info> entries;
    int ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[*index], marker, max - result.size(), expired_only, entries, truncated);
    if (ret == -ENOENT)
      continue;
    if (ret < 0)
      return ret;

    std::list<cls_rgw_gc_obj_info>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      result.push_back(*iter);
    }

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

int RGWGC::process(int index, int max_secs)
{
  rados::cls::lock::Lock l(gc_index_lock_name);
  utime_t end = ceph_clock_now(g_ceph_context);
  std::list<string> remove_tags;

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
    dout(0) << "RGWGC::process() failed to acquire lock on " << obj_names[index] << dendl;
    return 0;
  }
  if (ret < 0)
    return ret;

  string marker;
  bool truncated;
  IoCtx *ctx = new IoCtx;
  do {
    int max = 100;
    std::list<cls_rgw_gc_obj_info> entries;
    ret = cls_rgw_gc_list(store->gc_pool_ctx, obj_names[index], marker, max, true, entries, &truncated);
    if (ret == -ENOENT) {
      ret = 0;
      goto done;
    }
    if (ret < 0)
      goto done;

    string last_pool;
    std::list<cls_rgw_gc_obj_info>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      bool remove_tag;
      cls_rgw_gc_obj_info& info = *iter;
      std::list<cls_rgw_obj>::iterator liter;
      cls_rgw_obj_chain& chain = info.chain;

      utime_t now = ceph_clock_now(g_ceph_context);
      if (now >= end)
        goto done;

      remove_tag = true;
      for (liter = chain.objs.begin(); liter != chain.objs.end(); ++liter) {
        cls_rgw_obj& obj = *liter;

        if (obj.pool != last_pool) {
          delete ctx;
          ctx = new IoCtx;
	  ret = store->get_rados_handle()->ioctx_create(obj.pool.c_str(), *ctx);
	  if (ret < 0) {
	    dout(0) << "ERROR: failed to create ioctx pool=" << obj.pool << dendl;
	    continue;
	  }
          last_pool = obj.pool;
        }

        ctx->locator_set_key(obj.loc);
        rgw_obj key_obj;
        key_obj.set_obj(obj.key.name);
        key_obj.set_instance(obj.key.instance);

	dout(0) << "gc::process: removing " << obj.pool << ":" << key_obj.get_object() << dendl;
	ObjectWriteOperation op;
	cls_refcount_put(op, info.tag, true);
        ret = ctx->operate(key_obj.get_object(), &op);
	if (ret == -ENOENT)
	  ret = 0;
        if (ret < 0) {
          remove_tag = false;
          dout(0) << "failed to remove " << obj.pool << ":" << key_obj.get_object() << "@" << obj.loc << dendl;
        }

        if (going_down()) // leave early, even if tag isn't removed, it's ok
          goto done;
      }
      if (remove_tag) {
        remove_tags.push_back(info.tag);
#define MAX_REMOVE_CHUNK 16
        if (remove_tags.size() > MAX_REMOVE_CHUNK) {
          RGWGC::remove(index, remove_tags);
          remove_tags.clear();
        }
      }
    }
  } while (truncated);

done:
  if (!remove_tags.empty())
    RGWGC::remove(index, remove_tags);
  l.unlock(&store->gc_pool_ctx, obj_names[index]);
  delete ctx;
  return 0;
}

int RGWGC::process()
{
  int max_secs = cct->_conf->rgw_gc_processor_max_time;

  unsigned start;
  int ret = get_random_bytes((char *)&start, sizeof(start));
  if (ret < 0)
    return ret;

  for (int i = 0; i < max_objs; i++) {
    int index = (i + start) % max_objs;
    ret = process(index, max_secs);
    if (ret < 0)
      return ret;
  }

  return 0;
}

bool RGWGC::going_down()
{
  return (down_flag.read() != 0);
}

void RGWGC::start_processor()
{
  worker = new GCWorker(cct, this);
  worker->create();
}

void RGWGC::stop_processor()
{
  down_flag.set(1);
  if (worker) {
    worker->stop();
    worker->join();
  }
  delete worker;
  worker = NULL;
}

void *RGWGC::GCWorker::entry() {
  do {
    utime_t start = ceph_clock_now(cct);
    dout(2) << "garbage collection: start" << dendl;
    int r = gc->process();
    if (r < 0) {
      dout(0) << "ERROR: garbage collection process() returned error r=" << r << dendl;
    }
    dout(2) << "garbage collection: stop" << dendl;

    if (gc->going_down())
      break;

    utime_t end = ceph_clock_now(cct);
    end -= start;
    int secs = cct->_conf->rgw_gc_processor_period;

    if (secs <= end.sec())
      continue; // next round

    secs -= end.sec();

    lock.Lock();
    cond.WaitInterval(cct, lock, utime_t(secs, 0));
    lock.Unlock();
  } while (!gc->going_down());

  return NULL;
}

void RGWGC::GCWorker::stop()
{
  Mutex::Locker l(lock);
  cond.Signal();
}

