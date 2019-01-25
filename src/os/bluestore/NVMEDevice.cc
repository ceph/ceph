// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
//
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
  *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <unistd.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <functional>
#include <map>
#include <thread>

#include <spdk/nvme.h>

#include "include/stringify.h"
#include "include/types.h"
#include "include/compat.h"
#include "common/align.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/perf_counters.h"

#include "NVMEDevice.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << sn << ") "

thread_local SharedDriverQueueData *queue_t;

static constexpr uint16_t data_buffer_default_num = 1024;

static constexpr uint32_t data_buffer_size = 8192;

static constexpr uint16_t inline_segment_num = 32;

enum {
  l_bluestore_nvmedevice_first = 632430,
  l_bluestore_nvmedevice_write_lat,
  l_bluestore_nvmedevice_read_lat,
  l_bluestore_nvmedevice_flush_lat,
  l_bluestore_nvmedevice_write_queue_lat,
  l_bluestore_nvmedevice_read_queue_lat,
  l_bluestore_nvmedevice_flush_queue_lat,
  l_bluestore_nvmedevice_queue_ops,
  l_bluestore_nvmedevice_polling_lat,
  l_bluestore_nvmedevice_buffer_alloc_failed,
  l_bluestore_nvmedevice_last
};

static void io_complete(void *t, const struct spdk_nvme_cpl *completion);

struct IORequest {
  uint16_t cur_seg_idx = 0;
  uint16_t nseg;
  uint32_t cur_seg_left = 0;
  void *inline_segs[inline_segment_num];
  void **extra_segs = nullptr;
};

struct Task;

class SharedDriverData {
  unsigned id;
  spdk_nvme_transport_id trid;
  spdk_nvme_ctrlr *ctrlr;
  spdk_nvme_ns *ns;
  uint32_t block_size = 0;
  uint64_t size = 0;

  public:
  std::vector<NVMEDevice*> registered_devices;
  friend class SharedDriverQueueData;
  SharedDriverData(unsigned id_, const spdk_nvme_transport_id& trid_,
                   spdk_nvme_ctrlr *c, spdk_nvme_ns *ns_)
      : id(id_),
        trid(trid_),
        ctrlr(c),
        ns(ns_) {
    block_size = spdk_nvme_ns_get_extended_sector_size(ns);
    size = spdk_nvme_ns_get_size(ns);
  }

  bool is_equal(const spdk_nvme_transport_id& trid2) const {
    return spdk_nvme_transport_id_compare(&trid, &trid2) == 0;
  }
  ~SharedDriverData() {
  }

  void register_device(NVMEDevice *device) {
    registered_devices.push_back(device);
  }

  void remove_device(NVMEDevice *device) {
    std::vector<NVMEDevice*> new_devices;
    for (auto &&it : registered_devices) {
      if (it != device)
        new_devices.push_back(it);
    }
    registered_devices.swap(new_devices);
  }

  uint32_t get_block_size() {
    return block_size;
  }
  uint64_t get_size() {
    return size;
  }
};

class SharedDriverQueueData {
  NVMEDevice *bdev;
  SharedDriverData *driver;
  spdk_nvme_ctrlr *ctrlr;
  spdk_nvme_ns *ns;
  std::string sn;
  uint32_t block_size;
  uint32_t max_queue_depth;
  struct spdk_nvme_qpair *qpair;
  bool reap_io = false;
  int alloc_buf_from_pool(Task *t, bool write);

  public:
    uint32_t current_queue_depth = 0;
    std::atomic_ulong completed_op_seq, queue_op_seq;
    std::vector<void*> data_buf_mempool;
    PerfCounters *logger = nullptr;
    void _aio_handle(Task *t, IOContext *ioc);

    SharedDriverQueueData(NVMEDevice *bdev, SharedDriverData *driver)
      : bdev(bdev),
        driver(driver) {
    ctrlr = driver->ctrlr;
    ns = driver->ns;
    block_size = driver->block_size;

    struct spdk_nvme_io_qpair_opts opts = {};
    spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
    opts.qprio = SPDK_NVME_QPRIO_URGENT;
    // usable queue depth should minus 1 to aovid overflow.
    max_queue_depth = opts.io_queue_size - 1;
    qpair = spdk_nvme_ctrlr_alloc_io_qpair(ctrlr, &opts, sizeof(opts));
    ceph_assert(qpair != NULL);

    // allocate spdk dma memory
    for (uint16_t i = 0; i < data_buffer_default_num; i++) {
      void *b = spdk_dma_zmalloc(data_buffer_size, CEPH_PAGE_SIZE, NULL);
      if (!b) {
        derr << __func__ << " failed to create memory pool for nvme data buffer" << dendl;
        ceph_assert(b);
      }
      data_buf_mempool.push_back(b);
    }

    PerfCountersBuilder b(g_ceph_context, string("NVMEDevice-AIOThread-"+stringify(this)),
                          l_bluestore_nvmedevice_first, l_bluestore_nvmedevice_last);
    b.add_time_avg(l_bluestore_nvmedevice_write_lat, "write_lat", "Average write completing latency");
    b.add_time_avg(l_bluestore_nvmedevice_read_lat, "read_lat", "Average read completing latency");
    b.add_time_avg(l_bluestore_nvmedevice_flush_lat, "flush_lat", "Average flush completing latency");
    b.add_u64(l_bluestore_nvmedevice_queue_ops, "queue_ops", "Operations in nvme queue");
    b.add_time_avg(l_bluestore_nvmedevice_polling_lat, "polling_lat", "Average polling latency");
    b.add_time_avg(l_bluestore_nvmedevice_write_queue_lat, "write_queue_lat", "Average queue write request latency");
    b.add_time_avg(l_bluestore_nvmedevice_read_queue_lat, "read_queue_lat", "Average queue read request latency");
    b.add_time_avg(l_bluestore_nvmedevice_flush_queue_lat, "flush_queue_lat", "Average queue flush request latency");
    b.add_u64_counter(l_bluestore_nvmedevice_buffer_alloc_failed, "buffer_alloc_failed", "Alloc data buffer failed count");
    logger = b.create_perf_counters();
    g_ceph_context->get_perfcounters_collection()->add(logger);
    bdev->queue_number++;
    if (bdev->queue_number.load() == 1)
      reap_io = true;
  }

  ~SharedDriverQueueData() {
    g_ceph_context->get_perfcounters_collection()->remove(logger);
    if (qpair) {
      spdk_nvme_ctrlr_free_io_qpair(qpair);
      bdev->queue_number--;
    }

    // free all spdk dma memory;
    if (!data_buf_mempool.empty()) {
      for (uint16_t i = 0; i < data_buffer_default_num; i++) {
        void *b = data_buf_mempool[i];
        ceph_assert(b);
        spdk_dma_free(b);
      }
      data_buf_mempool.clear();
    }

    delete logger;
  }
};

struct Task {
  NVMEDevice *device;
  IOContext *ctx = nullptr;
  IOCommand command;
  uint64_t offset;
  uint64_t len;
  bufferlist bl;
  std::function<void()> fill_cb;
  Task *next = nullptr;
  int64_t return_code;
  ceph::coarse_real_clock::time_point start;
  IORequest io_request;
  ceph::mutex lock = ceph::make_mutex("Task::lock");
  ceph::condition_variable cond;
  SharedDriverQueueData *queue = nullptr;
  Task(NVMEDevice *dev, IOCommand c, uint64_t off, uint64_t l, int64_t rc = 0)
    : device(dev), command(c), offset(off), len(l),
      return_code(rc),
      start(ceph::coarse_real_clock::now()) {}
  ~Task() {
    ceph_assert(!io_request.nseg);
  }
  void release_segs(SharedDriverQueueData *queue_data) {
    if (io_request.extra_segs) {
      for (uint16_t i = 0; i < io_request.nseg; i++)
        queue_data->data_buf_mempool.push_back(io_request.extra_segs[i]);
      delete io_request.extra_segs;
    } else if (io_request.nseg) {
      for (uint16_t i = 0; i < io_request.nseg; i++)
        queue_data->data_buf_mempool.push_back(io_request.inline_segs[i]);
    }
    ctx->total_nseg -= io_request.nseg;
    io_request.nseg = 0;
  }

  void copy_to_buf(char *buf, uint64_t off, uint64_t len) {
    uint64_t copied = 0;
    uint64_t left = len;
    void **segs = io_request.extra_segs ? io_request.extra_segs : io_request.inline_segs;
    uint16_t i = 0;
    while (left > 0) {
      char *src = static_cast<char*>(segs[i++]);
      uint64_t need_copy = std::min(left, data_buffer_size-off);
      memcpy(buf+copied, src+off, need_copy);
      off = 0;
      left -= need_copy;
      copied += need_copy;
    }
  }
};

static void data_buf_reset_sgl(void *cb_arg, uint32_t sgl_offset)
{
  Task *t = static_cast<Task*>(cb_arg);
  uint32_t i = sgl_offset / data_buffer_size;
  uint32_t offset = i * data_buffer_size;
  ceph_assert(i <= t->io_request.nseg);

  for (; i < t->io_request.nseg; i++) {
    offset += data_buffer_size;
    if (offset > sgl_offset) {
      if (offset > t->len)
        offset = t->len;
      break;
    }
  }

  t->io_request.cur_seg_idx = i;
  t->io_request.cur_seg_left = offset - sgl_offset;
  return ;
}

static int data_buf_next_sge(void *cb_arg, void **address, uint32_t *length)
{
  uint32_t size;
  void *addr;
  Task *t = static_cast<Task*>(cb_arg);
  if (t->io_request.cur_seg_idx >= t->io_request.nseg) {
    *length = 0;
    *address = 0;
    return 0;
  }

  addr = t->io_request.extra_segs ? t->io_request.extra_segs[t->io_request.cur_seg_idx] : t->io_request.inline_segs[t->io_request.cur_seg_idx];

  size = data_buffer_size;
  if (t->io_request.cur_seg_idx == t->io_request.nseg - 1) {
      uint64_t tail = t->len % data_buffer_size;
      if (tail) {
        size = (uint32_t) tail;
      }
  }
 
  if (t->io_request.cur_seg_left) {
    *address = (void *)((uint64_t)addr + size - t->io_request.cur_seg_left);
    *length = t->io_request.cur_seg_left;
    t->io_request.cur_seg_left = 0;
  } else {
    *address = addr;
    *length = size;
  }
  
  t->io_request.cur_seg_idx++;
  return 0;
}

int SharedDriverQueueData::alloc_buf_from_pool(Task *t, bool write)
{
  uint64_t count = t->len / data_buffer_size;
  if (t->len % data_buffer_size)
    ++count;
  void **segs;
  if (count > data_buf_mempool.size())
    return -ENOMEM;
  if (count <= inline_segment_num) {
    segs = t->io_request.inline_segs;
  } else {
    t->io_request.extra_segs = new void*[count];
    segs = t->io_request.extra_segs;
  }
  for (uint16_t i = 0; i < count; i++) {
    segs[i] = data_buf_mempool.back();
    data_buf_mempool.pop_back();
  }
  t->io_request.nseg = count;
  t->ctx->total_nseg += count;
  if (write) {
    auto blp = t->bl.begin();
    uint32_t len = 0;
    uint16_t i = 0;
    for (; i < count - 1; ++i) {
      blp.copy(data_buffer_size, static_cast<char*>(segs[i]));
      len += data_buffer_size;
    }
    blp.copy(t->bl.length() - len, static_cast<char*>(segs[i]));
  }

  return 0;
}

void SharedDriverQueueData::_aio_handle(Task *t, IOContext *ioc)
{
  dout(20) << __func__ << " start" << dendl;

  int r = 0;
  uint64_t lba_off, lba_count;
  uint32_t max_io_completion = (uint32_t)g_conf().get_val<uint64_t>("bluestore_spdk_max_io_completion");
  uint64_t io_sleep_in_us = g_conf().get_val<uint64_t>("bluestore_spdk_io_sleep");

  ceph::coarse_real_clock::time_point cur, start
    = ceph::coarse_real_clock::now();
  while (ioc->num_running) {
 again:
    dout(40) << __func__ << " polling" << dendl;
    if (current_queue_depth) {
      r = spdk_nvme_qpair_process_completions(qpair, max_io_completion);
      if (r < 0) {
        ceph_abort();
      } else if (r == 0) {
        usleep(io_sleep_in_us);
      }
    }

    for (; t; t = t->next) {
      if (current_queue_depth == max_queue_depth) {
        // no slots
        goto again;
      }

      t->queue = this;
      lba_off = t->offset / block_size;
      lba_count = t->len / block_size;
      switch (t->command) {
        case IOCommand::WRITE_COMMAND:
        {
          dout(20) << __func__ << " write command issued " << lba_off << "~" << lba_count << dendl;
          r = alloc_buf_from_pool(t, true);
          if (r < 0) {
            logger->inc(l_bluestore_nvmedevice_buffer_alloc_failed);
            goto again;
          }

          r = spdk_nvme_ns_cmd_writev(
              ns, qpair, lba_off, lba_count, io_complete, t, 0,
              data_buf_reset_sgl, data_buf_next_sge);
          if (r < 0) {
            derr << __func__ << " failed to do write command" << dendl;
            t->ctx->nvme_task_first = t->ctx->nvme_task_last = nullptr;
            t->release_segs(this);
            delete t;
            ceph_abort();
          }
          cur = ceph::coarse_real_clock::now();
          auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - t->start);
          logger->tinc(l_bluestore_nvmedevice_write_queue_lat, dur);
          break;
        }
        case IOCommand::READ_COMMAND:
        {
          dout(20) << __func__ << " read command issued " << lba_off << "~" << lba_count << dendl;
          r = alloc_buf_from_pool(t, false);
          if (r < 0) {
            logger->inc(l_bluestore_nvmedevice_buffer_alloc_failed);
            goto again;
          }

          r = spdk_nvme_ns_cmd_readv(
              ns, qpair, lba_off, lba_count, io_complete, t, 0,
              data_buf_reset_sgl, data_buf_next_sge);
          if (r < 0) {
            derr << __func__ << " failed to read" << dendl;
            t->release_segs(this);
            delete t;
            ceph_abort();
          } else {
            cur = ceph::coarse_real_clock::now();
            auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - t->start);
            logger->tinc(l_bluestore_nvmedevice_read_queue_lat, dur);
          }
          break;
        }
        case IOCommand::FLUSH_COMMAND:
        {
          dout(20) << __func__ << " flush command issueed " << dendl;
          r = spdk_nvme_ns_cmd_flush(ns, qpair, io_complete, t);
          if (r < 0) {
            derr << __func__ << " failed to flush" << dendl;
            t->release_segs(this);
            delete t;
            ceph_abort();
          } else {
            cur = ceph::coarse_real_clock::now();
            auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - t->start);
            logger->tinc(l_bluestore_nvmedevice_flush_queue_lat, dur);
          }
          break;
        }
      }
      current_queue_depth++;
    }
    cur = ceph::coarse_real_clock::now();
    auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - start);
    logger->tinc(l_bluestore_nvmedevice_polling_lat, dur);
    start = ceph::coarse_real_clock::now();
  }

  if (reap_io)
    bdev->reap_ioc();
  dout(20) << __func__ << " end" << dendl;
}

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev "

class NVMEManager {
 public:
  struct ProbeContext {
    spdk_nvme_transport_id trid;
    NVMEManager *manager;
    SharedDriverData *driver;
    bool done;
  };

 private:
  ceph::mutex lock = ceph::make_mutex("NVMEManager::lock");
  bool stopping = false;
  std::vector<SharedDriverData*> shared_driver_datas;
  std::thread dpdk_thread;
  ceph::mutex probe_queue_lock = ceph::make_mutex("NVMEManager::probe_queue_lock");
  ceph::condition_variable probe_queue_cond;
  std::list<ProbeContext*> probe_queue;

 public:
  NVMEManager() {}
  ~NVMEManager() {
    if (!dpdk_thread.joinable())
      return;
    {
      std::lock_guard guard(probe_queue_lock);
      stopping = true;
      probe_queue_cond.notify_all();
    }
    dpdk_thread.join();
  }

  int try_get(const spdk_nvme_transport_id& trid, SharedDriverData **driver);
  void register_ctrlr(const spdk_nvme_transport_id& trid, spdk_nvme_ctrlr *c, SharedDriverData **driver) {
    ceph_assert(ceph_mutex_is_locked(lock));
    spdk_nvme_ns *ns;
    int num_ns = spdk_nvme_ctrlr_get_num_ns(c);
    ceph_assert(num_ns >= 1);
    if (num_ns > 1) {
      dout(0) << __func__ << " namespace count larger than 1, currently only use the first namespace" << dendl;
    }
    ns = spdk_nvme_ctrlr_get_ns(c, 1);
    if (!ns) {
      derr << __func__ << " failed to get namespace at 1" << dendl;
      ceph_abort();
    }
    dout(1) << __func__ << " successfully attach nvme device at" << trid.traddr << dendl;

    // only support one device per osd now!
    ceph_assert(shared_driver_datas.empty());
    // index 0 is occurred by master thread
    shared_driver_datas.push_back(new SharedDriverData(shared_driver_datas.size()+1, trid, c, ns));
    *driver = shared_driver_datas.back();
  }
};

static NVMEManager manager;

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid, struct spdk_nvme_ctrlr_opts *opts)
{
  NVMEManager::ProbeContext *ctx = static_cast<NVMEManager::ProbeContext*>(cb_ctx);

  if (trid->trtype != SPDK_NVME_TRANSPORT_PCIE) {
    dout(0) << __func__ << " only probe local nvme device" << dendl;
    return false;
  }

  dout(0) << __func__ << " found device at: "
	  << "trtype=" << spdk_nvme_transport_id_trtype_str(trid->trtype) << ", "
          << "traddr=" << trid->traddr << dendl;
  if (spdk_nvme_transport_id_compare(&ctx->trid, trid)) {
    dout(0) << __func__ << " device traddr (" << ctx->trid.traddr << ") not match " << trid->traddr << dendl;
    return false;
  }

  return true;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                      struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
  auto ctx = static_cast<NVMEManager::ProbeContext*>(cb_ctx);
  ctx->manager->register_ctrlr(ctx->trid, ctrlr, &ctx->driver);
}

int NVMEManager::try_get(const spdk_nvme_transport_id& trid, SharedDriverData **driver)
{
  std::lock_guard l(lock);
  for (auto &&it : shared_driver_datas) {
    if (it->is_equal(trid)) {
      *driver = it;
      return 0;
    }
  }

  auto coremask_arg = g_conf().get_val<std::string>("bluestore_spdk_coremask");
  int m_core_arg = -1;
  try {
    auto core_value = stoull(coremask_arg, nullptr, 16);
    m_core_arg = ffsll(core_value);
  } catch (const std::logic_error& e) {
    derr << __func__ << " invalid bluestore_spdk_coremask: "
	 << coremask_arg << dendl;
    return -EINVAL;
  }
  // at least one core is needed for using spdk
  if (m_core_arg == 0) {
    derr << __func__ << " invalid bluestore_spdk_coremask, "
	 << "at least one core is needed" << dendl;
    return -ENOENT;
  }
  m_core_arg -= 1;

  uint32_t mem_size_arg = (uint32_t)g_conf().get_val<Option::size_t>("bluestore_spdk_mem");

  if (!dpdk_thread.joinable()) {
    dpdk_thread = std::thread(
      [this, coremask_arg, m_core_arg, mem_size_arg]() {
        static struct spdk_env_opts opts;
        int r;

        spdk_env_opts_init(&opts);
        opts.name = "nvme-device-manager";
        opts.core_mask = coremask_arg.c_str();
        opts.master_core = m_core_arg;
        opts.mem_size = mem_size_arg;
        spdk_env_init(&opts);
        spdk_unaffinitize_thread();

        spdk_nvme_retry_count = g_ceph_context->_conf->bdev_nvme_retry_count;
        if (spdk_nvme_retry_count < 0)
          spdk_nvme_retry_count = SPDK_NVME_DEFAULT_RETRY_COUNT;

        std::unique_lock l(probe_queue_lock);
        while (!stopping) {
          if (!probe_queue.empty()) {
            ProbeContext* ctxt = probe_queue.front();
            probe_queue.pop_front();
            r = spdk_nvme_probe(NULL, ctxt, probe_cb, attach_cb, NULL);
            if (r < 0) {
              ceph_assert(!ctxt->driver);
              derr << __func__ << " device probe nvme failed" << dendl;
            }
            ctxt->done = true;
            probe_queue_cond.notify_all();
          } else {
            probe_queue_cond.wait(l);
          }
        }
        for (auto p : probe_queue)
          p->done = true;
        probe_queue_cond.notify_all();
      }
    );
  }

  ProbeContext ctx{trid, this, nullptr, false};
  {
    std::unique_lock l(probe_queue_lock);
    probe_queue.push_back(&ctx);
    while (!ctx.done)
      probe_queue_cond.wait(l);
  }
  if (!ctx.driver)
    return -1;
  *driver = ctx.driver;

  return 0;
}

void io_complete(void *t, const struct spdk_nvme_cpl *completion)
{
  Task *task = static_cast<Task*>(t);
  IOContext *ctx = task->ctx;
  SharedDriverQueueData *queue = task->queue;

  ceph_assert(queue != NULL);
  ceph_assert(ctx != NULL);
  --queue->current_queue_depth;
  auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(
      ceph::coarse_real_clock::now() - task->start);
  if (task->command == IOCommand::WRITE_COMMAND) {
    queue->logger->tinc(l_bluestore_nvmedevice_write_lat, dur);
    ceph_assert(!spdk_nvme_cpl_is_error(completion));
    dout(20) << __func__ << " write/zero op successfully, left "
             << queue->queue_op_seq - queue->completed_op_seq << dendl;
    // check waiting count before doing callback (which may
    // destroy this ioc).
    if (ctx->priv) {
      if (!--ctx->num_running) {
        task->device->aio_callback(task->device->aio_callback_priv, ctx->priv);
      }
    } else {
      ctx->try_aio_wake();
    }
    task->release_segs(queue);
    delete task;
  } else if (task->command == IOCommand::READ_COMMAND) {
    queue->logger->tinc(l_bluestore_nvmedevice_read_lat, dur);
    ceph_assert(!spdk_nvme_cpl_is_error(completion));
    dout(20) << __func__ << " read op successfully" << dendl;
    task->fill_cb();
    task->release_segs(queue);
    // read submitted by AIO
    if (!task->return_code) {
      if (ctx->priv) {
	if (!--ctx->num_running) {
          task->device->aio_callback(task->device->aio_callback_priv, ctx->priv);
	}
      } else {
        ctx->try_aio_wake();
      }
      delete task;
    } else {
      task->return_code = 0;
      ctx->try_aio_wake();
    }
  } else {
    ceph_assert(task->command == IOCommand::FLUSH_COMMAND);
    ceph_assert(!spdk_nvme_cpl_is_error(completion));
    queue->logger->tinc(l_bluestore_nvmedevice_flush_lat, dur);
    dout(20) << __func__ << " flush op successfully" << dendl;
    task->return_code = 0;
  }
}

// ----------------
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << name << ") "

NVMEDevice::NVMEDevice(CephContext* cct, aio_callback_t cb, void *cbpriv)
  :   BlockDevice(cct, cb, cbpriv),
      driver(nullptr)
{
}

int NVMEDevice::open(const string& p)
{
  dout(1) << __func__ << " path " << p << dendl;

  std::ifstream ifs(p);
  if (!ifs) {
    derr << __func__ << " unable to open " << p << dendl;
    return -1;
  }
  string val;
  std::getline(ifs, val);
  spdk_nvme_transport_id trid;
  if (int r = spdk_nvme_transport_id_parse(&trid, val.c_str()); r) {
    derr << __func__ << " unable to read " << p << ": " << cpp_strerror(r)
	 << dendl;
    return r;
  }
  if (int r = manager.try_get(trid, &driver); r < 0) {
    derr << __func__ << " failed to get nvme device with transport address " << trid.traddr << dendl;
    return r;
  }

  driver->register_device(this);
  block_size = driver->get_block_size();
  size = driver->get_size();
  name = trid.traddr;

  //nvme is non-rotational device.
  rotational = false;

  // round size down to an even block
  size &= ~(block_size - 1);

  dout(1) << __func__ << " size " << size << " (" << byte_u_t(size) << ")"
          << " block_size " << block_size << " (" << byte_u_t(block_size)
          << ")" << dendl;


  return 0;
}

void NVMEDevice::close()
{
  dout(1) << __func__ << dendl;

  delete queue_t;
  queue_t = nullptr;
  name.clear();
  driver->remove_device(this);

  dout(1) << __func__ << " end" << dendl;
}

int NVMEDevice::collect_metadata(const string& prefix, map<string,string> *pm) const
{
  (*pm)[prefix + "rotational"] = "0";
  (*pm)[prefix + "size"] = stringify(get_size());
  (*pm)[prefix + "block_size"] = stringify(get_block_size());
  (*pm)[prefix + "driver"] = "NVMEDevice";
  (*pm)[prefix + "type"] = "nvme";
  (*pm)[prefix + "access_mode"] = "spdk";
  (*pm)[prefix + "nvme_serial_number"] = name;

  return 0;
}

int NVMEDevice::flush()
{
  return 0;
}

void NVMEDevice::aio_submit(IOContext *ioc)
{
  dout(20) << __func__ << " ioc " << ioc << " pending "
           << ioc->num_pending.load() << " running "
           << ioc->num_running.load() << dendl;
  int pending = ioc->num_pending.load();
  Task *t = static_cast<Task*>(ioc->nvme_task_first);
  if (pending && t) {
    ioc->num_running += pending;
    ioc->num_pending -= pending;
    ceph_assert(ioc->num_pending.load() == 0);  // we should be only thread doing this
    // Only need to push the first entry
    ioc->nvme_task_first = ioc->nvme_task_last = nullptr;
    if (!queue_t)
	queue_t = new SharedDriverQueueData(this, driver);
    queue_t->_aio_handle(t, ioc);
  }
}

static void write_split(
    NVMEDevice *dev,
    uint64_t off,
    bufferlist &bl,
    IOContext *ioc)
{
  uint64_t remain_len = bl.length(), begin = 0, write_size;
  Task *t, *first, *last;
  // This value may need to be got from configuration later.
  uint64_t split_size = 131072; // 128KB.

  while (remain_len > 0) {
    write_size = std::min(remain_len, split_size);
    t = new Task(dev, IOCommand::WRITE_COMMAND, off + begin, write_size);
    // TODO: if upper layer alloc memory with known physical address,
    // we can reduce this copy
    bl.splice(0, write_size, &t->bl);
    remain_len -= write_size;
    t->ctx = ioc;
    first = static_cast<Task*>(ioc->nvme_task_first);
    last = static_cast<Task*>(ioc->nvme_task_last);
    if (last)
      last->next = t;
    if (!first)
      ioc->nvme_task_first = t;
    ioc->nvme_task_last = t;
    ++ioc->num_pending;
    begin += write_size;
  }
}

int NVMEDevice::aio_write(
    uint64_t off,
    bufferlist &bl,
    IOContext *ioc,
    bool buffered,
    int write_hint)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " " << off << "~" << len << " ioc " << ioc
           << " buffered " << buffered << dendl;
  ceph_assert(is_valid_io(off, len));

  write_split(this, off, bl, ioc);
  dout(5) << __func__ << " " << off << "~" << len << dendl;

  return 0;
}

int NVMEDevice::write(uint64_t off, bufferlist &bl, bool buffered, int write_hint)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " " << off << "~" << len << " buffered "
           << buffered << dendl;
  ceph_assert(off % block_size == 0);
  ceph_assert(len % block_size == 0);
  ceph_assert(len > 0);
  ceph_assert(off < size);
  ceph_assert(off + len <= size);

  IOContext ioc(cct, NULL);
  write_split(this, off, bl, &ioc);
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  aio_submit(&ioc);
  ioc.aio_wait();
  return 0;
}

int NVMEDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
                     IOContext *ioc,
                     bool buffered)
{
  dout(5) << __func__ << " " << off << "~" << len << " ioc " << ioc << dendl;
  ceph_assert(is_valid_io(off, len));

  Task *t = new Task(this, IOCommand::READ_COMMAND, off, len, 1);
  bufferptr p = buffer::create_small_page_aligned(len);
  int r = 0;
  t->ctx = ioc;
  char *buf = p.c_str();
  t->fill_cb = [buf, t]() {
    t->copy_to_buf(buf, 0, t->len);
  };

  ++ioc->num_pending;
  ioc->nvme_task_first = t;
  aio_submit(ioc);
  ioc->aio_wait();

  pbl->push_back(std::move(p));
  r = t->return_code;
  delete t;
  return r;
}

int NVMEDevice::aio_read(
    uint64_t off,
    uint64_t len,
    bufferlist *pbl,
    IOContext *ioc)
{
  dout(20) << __func__ << " " << off << "~" << len << " ioc " << ioc << dendl;
  ceph_assert(is_valid_io(off, len));

  Task *t = new Task(this, IOCommand::READ_COMMAND, off, len);

  bufferptr p = buffer::create_small_page_aligned(len);
  pbl->append(p);
  t->ctx = ioc;
  char* buf = p.c_str();
  t->fill_cb = [buf, t]() {
    t->copy_to_buf(buf, 0, t->len);
  };

  Task *first = static_cast<Task*>(ioc->nvme_task_first);
  Task *last = static_cast<Task*>(ioc->nvme_task_last);
  if (last)
    last->next = t;
  if (!first)
    ioc->nvme_task_first = t;
  ioc->nvme_task_last = t;
  ++ioc->num_pending;

  return 0;
}

int NVMEDevice::read_random(uint64_t off, uint64_t len, char *buf, bool buffered)
{
  ceph_assert(len > 0);
  ceph_assert(off < size);
  ceph_assert(off + len <= size);

  uint64_t aligned_off = align_down(off, block_size);
  uint64_t aligned_len = align_up(off+len, block_size) - aligned_off;
  dout(5) << __func__ << " " << off << "~" << len
          << " aligned " << aligned_off << "~" << aligned_len << dendl;
  IOContext ioc(g_ceph_context, nullptr);
  Task *t = new Task(this, IOCommand::READ_COMMAND, aligned_off, aligned_len, 1);
  int r = 0;
  t->ctx = &ioc;
  t->fill_cb = [buf, t, off, len]() {
    t->copy_to_buf(buf, off-t->offset, len);
  };

  ++ioc.num_pending;
  ioc.nvme_task_first = t;
  aio_submit(&ioc);
  ioc.aio_wait();

  r = t->return_code;
  delete t;
  return r;
}

int NVMEDevice::invalidate_cache(uint64_t off, uint64_t len)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  return 0;
}
