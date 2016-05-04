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
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <functional>
#include <map>
#include <thread>
#include <xmmintrin.h>

#include <spdk/pci.h>
#include <spdk/nvme.h>

#include <rte_config.h>
#include <rte_cycles.h>
#include <rte_mempool.h>
#include <rte_malloc.h>
#include <rte_lcore.h>

#include "include/stringify.h"
#include "include/types.h"
#include "include/compat.h"
#include "common/align.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/perf_counters.h"

#include "NVMEDevice.h"

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << sn << ") "

rte_mempool *request_mempool = nullptr;
std::vector<void*> data_buf_mempool;

static constexpr uint16_t data_buffer_default_num = 2048;

static constexpr uint32_t data_buffer_size = 8192;

static constexpr uint16_t inline_segment_num = 32;

enum {
  l_bluestore_nvmedevice_first = 632430,
  l_bluestore_nvmedevice_aio_write_lat,
  l_bluestore_nvmedevice_aio_zero_lat,
  l_bluestore_nvmedevice_read_lat,
  l_bluestore_nvmedevice_flush_lat,
  l_bluestore_nvmedevice_aio_write_queue_lat,
  l_bluestore_nvmedevice_aio_zero_queue_lat,
  l_bluestore_nvmedevice_read_queue_lat,
  l_bluestore_nvmedevice_flush_queue_lat,
  l_bluestore_nvmedevice_queue_ops,
  l_bluestore_nvmedevice_polling_lat,
  l_bluestore_nvmedevice_buffer_alloc_failed,
  l_bluestore_nvmedevice_last
};

static void io_complete(void *t, const struct spdk_nvme_cpl *completion);

int dpdk_thread_adaptor(void *f)
{
  (*static_cast<std::function<void ()>*>(f))();
  return 0;
}

struct IOSegment {
  uint32_t len;
  void *addr;
};

struct IORequest {
  uint16_t cur_seg_idx = 0;
  uint16_t nseg;
  uint32_t cur_seg_left = 0;
  void *inline_segs[inline_segment_num];
  void **extra_segs = nullptr;
};

struct Task {
  NVMEDevice *device;
  IOContext *ctx = nullptr;
  IOCommand command;
  uint64_t offset;
  uint64_t len;
  bufferlist write_bl;
  std::function<void()> fill_cb;
  Task *next = nullptr;
  int64_t return_code;
  ceph::coarse_real_clock::time_point start;
  IORequest io_request;
  Task(NVMEDevice *dev, IOCommand c, uint64_t off, uint64_t l, int64_t rc = 0)
    : device(dev), command(c), offset(off), len(l),
      return_code(rc),
      start(ceph::coarse_real_clock::now(g_ceph_context)) {}
  ~Task() {
    assert(!io_request.nseg);
  }
  void release_segs() {
    if (io_request.extra_segs) {
      for (uint16_t i = 0; i < io_request.nseg; i++)
        data_buf_mempool.push_back(io_request.extra_segs[i]);
      delete io_request.extra_segs;
    } else if (io_request.nseg) {
      for (uint16_t i = 0; i < io_request.nseg; i++)
        data_buf_mempool.push_back(io_request.inline_segs[i]);
    }
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

class SharedDriverData {
  unsigned id;
  std::string sn;
  std::string name;
  spdk_nvme_ctrlr *ctrlr;
  spdk_nvme_ns *ns;
  std::function<void ()> run_func;

  uint64_t block_size = 0;
  uint64_t sector_size = 0;
  uint64_t size = 0;
  std::vector<NVMEDevice*> registered_devices;
  friend class AioCompletionThread;

  bool aio_stop = false;
  void _aio_thread();
  void _aio_start() {
    int r = rte_eal_remote_launch(dpdk_thread_adaptor, static_cast<void*>(&run_func), id);
    assert(r == 0);
  }
  void _aio_stop() {
    {
      Mutex::Locker l(queue_lock);
      aio_stop = true;
      queue_cond.Signal();
    }
    int r = rte_eal_wait_lcore(id);
    assert(r == 0);
    aio_stop = false;
  }
  std::atomic_bool queue_empty;
  Mutex queue_lock;
  Cond queue_cond;
  std::queue<Task*> task_queue;

  Mutex flush_lock;
  Cond flush_cond;
  std::atomic_int flush_waiters;
  std::set<uint64_t> flush_waiter_seqs;

 public:
  bool zero_command_support;
  std::atomic_ulong completed_op_seq, queue_op_seq;
  PerfCounters *logger = nullptr;

  SharedDriverData(unsigned i, const std::string &sn_tag, const std::string &n,
                   spdk_nvme_ctrlr *c, spdk_nvme_ns *ns)
      : id(i),
        sn(sn_tag),
        name(n),
        ctrlr(c),
        ns(ns),
        run_func([this]() { _aio_thread(); }),
        queue_empty(false),
        queue_lock("NVMEDevice::queue_lock"),
        flush_lock("NVMEDevice::flush_lock"),
        flush_waiters(0),
        completed_op_seq(0), queue_op_seq(0) {
    sector_size = spdk_nvme_ns_get_sector_size(ns);
    block_size = std::max(CEPH_PAGE_SIZE, spdk_nvme_ns_get_sector_size(ns));
    size = spdk_nvme_ns_get_sector_size(ns) * spdk_nvme_ns_get_num_sectors(ns);
    zero_command_support = spdk_nvme_ns_get_flags(ns) & SPDK_NVME_NS_WRITE_ZEROES_SUPPORTED;

    PerfCountersBuilder b(g_ceph_context, string("NVMEDevice-AIOThread-"+stringify(this)),
                          l_bluestore_nvmedevice_first, l_bluestore_nvmedevice_last);
    b.add_time_avg(l_bluestore_nvmedevice_aio_write_lat, "aio_write_lat", "Average write completing latency");
    b.add_time_avg(l_bluestore_nvmedevice_aio_zero_lat, "aio_zero_lat", "Average zero completing latency");
    b.add_time_avg(l_bluestore_nvmedevice_read_lat, "read_lat", "Average read completing latency");
    b.add_time_avg(l_bluestore_nvmedevice_flush_lat, "flush_lat", "Average flush completing latency");
    b.add_u64(l_bluestore_nvmedevice_queue_ops, "queue_ops", "Operations in nvme queue");
    b.add_time_avg(l_bluestore_nvmedevice_polling_lat, "polling_lat", "Average polling latency");
    b.add_time_avg(l_bluestore_nvmedevice_aio_write_queue_lat, "aio_write_queue_lat", "Average queue write request latency");
    b.add_time_avg(l_bluestore_nvmedevice_aio_zero_queue_lat, "aio_zero_queue_lat", "Average queue zero request latency");
    b.add_time_avg(l_bluestore_nvmedevice_read_queue_lat, "read_queue_lat", "Average queue read request latency");
    b.add_time_avg(l_bluestore_nvmedevice_flush_queue_lat, "flush_queue_lat", "Average queue flush request latency");
    b.add_u64_counter(l_bluestore_nvmedevice_buffer_alloc_failed, "buffer_alloc_failed", "Alloc data buffer failed count");
    logger = b.create_perf_counters();
    g_ceph_context->get_perfcounters_collection()->add(logger);
    _aio_start();
  }
  ~SharedDriverData() {
    g_ceph_context->get_perfcounters_collection()->remove(logger);
    delete logger;
  }

  bool is_equal(const string &tag) const { return sn == tag; }
  void register_device(NVMEDevice *device) {
    // in case of registered_devices, we stop thread now.
    // Because release is really a rare case, we could bear this
    _aio_stop();
    registered_devices.push_back(device);
    _aio_start();
  }
  void remove_device(NVMEDevice *device) {
    _aio_stop();
    std::vector<NVMEDevice*> new_devices;
    for (auto &&it : registered_devices) {
      if (it != device)
        new_devices.push_back(it);
    }
    registered_devices.swap(new_devices);
    _aio_start();
  }

  uint64_t get_block_size() {
    return block_size;
  }
  uint64_t get_size() {
    return size;
  }
  void queue_task(Task *t, uint64_t ops = 1) {
    queue_op_seq += ops;
    Mutex::Locker l(queue_lock);
    task_queue.push(t);
    if (queue_empty.load()) {
      queue_empty = false;
      queue_cond.Signal();
    }
  }

  void flush_wait() {
    uint64_t cur_seq = queue_op_seq.load();
    uint64_t left = cur_seq - completed_op_seq.load();
    if (cur_seq > completed_op_seq) {
      // TODO: this may contains read op
      dout(10) << __func__ << " existed inflight ops " << left << dendl;
      Mutex::Locker l(flush_lock);
      ++flush_waiters;
      flush_waiter_seqs.insert(cur_seq);
      while (cur_seq > completed_op_seq.load()) {
        flush_cond.Wait(flush_lock);
      }
      flush_waiter_seqs.erase(cur_seq);
      --flush_waiters;
    }
  }
};

static void data_buf_reset_sgl(void *cb_arg, uint32_t sgl_offset)
{
  Task *t = static_cast<Task*>(cb_arg);
  uint32_t i = sgl_offset / data_buffer_size;
  uint32_t offset = i * data_buffer_size;
  assert(i <= t->io_request.nseg);

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

static int data_buf_next_sge(void *cb_arg, uint64_t *address, uint32_t *length)
{
  Task *t = static_cast<Task*>(cb_arg);
  if (t->io_request.cur_seg_idx >= t->io_request.nseg) {
    *length = 0;
    *address = 0;
    return 0;
  }

  void *addr = t->io_request.extra_segs ? t->io_request.extra_segs[t->io_request.cur_seg_idx] : t->io_request.inline_segs[t->io_request.cur_seg_idx];

  if (t->io_request.cur_seg_left) {
    *length = t->io_request.cur_seg_left;
    *address = rte_malloc_virt2phy(addr) + data_buffer_size - t->io_request.cur_seg_left;
    if (t->io_request.cur_seg_idx == t->io_request.nseg - 1) {
      uint64_t tail = t->len % data_buffer_size;
      if (tail) {
        *address = rte_malloc_virt2phy(addr) + tail - t->io_request.cur_seg_left;
      }
    }
    t->io_request.cur_seg_left = 0;
  } else {
    *address = rte_malloc_virt2phy(addr);
    *length = data_buffer_size;
    if (t->io_request.cur_seg_idx == t->io_request.nseg - 1) {
      uint64_t tail = t->len % data_buffer_size;
      if (tail)
        *length = tail;
    }
  }
  t->io_request.cur_seg_idx++;
  return 0;
}

static int alloc_buf_from_pool(Task *t, bool write)
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
  if (write) {
    auto blp = t->write_bl.begin();
    uint32_t len = 0;
    uint16_t i = 0;
    for (; i < count - 1; ++i) {
      blp.copy(data_buffer_size, static_cast<char*>(segs[i]));
      len += data_buffer_size;
    }
    blp.copy(t->write_bl.length() - len, static_cast<char*>(segs[i]));
  }

  return 0;
}

void SharedDriverData::_aio_thread()
{
  dout(1) << __func__ << " start" << dendl;
  if (spdk_nvme_register_io_thread() != 0) {
    assert(0);
  }

  if (data_buf_mempool.empty()) {
    for (uint16_t i = 0; i < data_buffer_default_num; i++) {
      void *b = rte_malloc_socket(
          "nvme_data_buffer", data_buffer_size, CEPH_PAGE_SIZE, rte_socket_id());
      if (!b) {
        derr << __func__ << " failed to create memory pool for nvme data buffer" << dendl;
        assert(b);
      }
      data_buf_mempool.push_back(b);
    }
  }

  Task *t = nullptr;
  int r = 0;
  const int max = 4;
  uint64_t lba_off, lba_count;
  ceph::coarse_real_clock::time_point cur, start = ceph::coarse_real_clock::now(g_ceph_context);
  while (true) {
    bool inflight = queue_op_seq.load() - completed_op_seq.load();
 again:
    dout(40) << __func__ << " polling" << dendl;
    if (inflight) {
      if (!spdk_nvme_ctrlr_process_io_completions(ctrlr, max)) {
        dout(30) << __func__ << " idle, have a pause" << dendl;
        _mm_pause();
      }
    }

    for (; t; t = t->next) {
      lba_off = t->offset / sector_size;
      lba_count = t->len / sector_size;
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
              ns, lba_off, lba_count, io_complete, t, 0,
              data_buf_reset_sgl, data_buf_next_sge);
          if (r < 0) {
            t->ctx->nvme_task_first = t->ctx->nvme_task_last = nullptr;
            delete t;
            derr << __func__ << " failed to do write command" << dendl;
            assert(0);
          }
          cur = ceph::coarse_real_clock::now(g_ceph_context);
          auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - start);
          logger->tinc(l_bluestore_nvmedevice_aio_write_queue_lat, dur);
          break;
        }
        case IOCommand::ZERO_COMMAND:
        {
          dout(20) << __func__ << " zero command issued " << lba_off << "~" << lba_count << dendl;
          assert(zero_command_support);
          r = spdk_nvme_ns_cmd_write_zeroes(ns, lba_off, lba_count, io_complete, t, 0);
          if (r < 0) {
            t->ctx->nvme_task_first = t->ctx->nvme_task_last = nullptr;
            delete t;
            derr << __func__ << " failed to do zero command" << dendl;
            assert(0);
          }
          cur = ceph::coarse_real_clock::now(g_ceph_context);
          auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - start);
          logger->tinc(l_bluestore_nvmedevice_aio_zero_queue_lat, dur);
          break;
        }
        case IOCommand::READ_COMMAND:
        {
          dout(20) << __func__ << " read command issueed " << lba_off << "~" << lba_count << dendl;
          r = alloc_buf_from_pool(t, false);
          if (r < 0) {
            logger->inc(l_bluestore_nvmedevice_buffer_alloc_failed);
            goto again;
          }

          r = spdk_nvme_ns_cmd_readv(
              ns, lba_off, lba_count, io_complete, t, 0,
              data_buf_reset_sgl, data_buf_next_sge);
          if (r < 0) {
            derr << __func__ << " failed to read" << dendl;
            --t->ctx->num_reading;
            t->return_code = r;
            std::unique_lock<std::mutex> l(t->ctx->lock);
            t->ctx->cond.notify_all();
          } else {
            cur = ceph::coarse_real_clock::now(g_ceph_context);
            auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - start);
            logger->tinc(l_bluestore_nvmedevice_read_queue_lat, dur);
          }
          break;
        }
        case IOCommand::FLUSH_COMMAND:
        {
          dout(20) << __func__ << " flush command issueed " << dendl;
          r = spdk_nvme_ns_cmd_flush(ns, io_complete, t);
          if (r < 0) {
            derr << __func__ << " failed to flush" << dendl;
            t->return_code = r;
            std::unique_lock<std::mutex> l(t->ctx->lock);
            t->ctx->cond.notify_all();
          } else {
            cur = ceph::coarse_real_clock::now(g_ceph_context);
            auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - start);
            logger->tinc(l_bluestore_nvmedevice_flush_queue_lat, dur);
          }
          break;
        }
      }
    }

    if (!queue_empty.load()) {
      Mutex::Locker l(queue_lock);
      if (!task_queue.empty()) {
        t = task_queue.front();
        task_queue.pop();
        logger->set(l_bluestore_nvmedevice_queue_ops, task_queue.size());
      }
      if (!t)
        queue_empty = true;
    } else {
      if (flush_waiters.load()) {
        Mutex::Locker l(flush_lock);
        if (*flush_waiter_seqs.begin() <= completed_op_seq.load())
          flush_cond.Signal();
      }


      if (!inflight) {
        for (auto &&it : registered_devices)
          it->reap_ioc();

        Mutex::Locker l(queue_lock);
        if (queue_empty.load()) {
          cur = ceph::coarse_real_clock::now(g_ceph_context);
          auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(cur - start);
          logger->tinc(l_bluestore_nvmedevice_polling_lat, dur);
          if (aio_stop)
            break;
          queue_cond.Wait(queue_lock);
          start = ceph::coarse_real_clock::now(g_ceph_context);
        }
      }
    }
  }
  assert(data_buf_mempool.size() == data_buffer_default_num);
  spdk_nvme_unregister_io_thread();
  dout(1) << __func__ << " end" << dendl;
}

#define dout_subsys ceph_subsys_bdev
#undef dout_prefix
#define dout_prefix *_dout << "bdev "

class NVMEManager {
 public:
  struct ProbeContext {
    string sn_tag;
    NVMEManager *manager;
    SharedDriverData *driver;
    bool done;
  };

 private:
  Mutex lock;
  bool init = false;
  std::vector<SharedDriverData*> shared_driver_datas;
  std::thread dpdk_thread;
  std::mutex probe_queue_lock;
  std::condition_variable probe_queue_cond;
  std::list<ProbeContext*> probe_queue;

 public:
  NVMEManager()
      : lock("NVMEDevice::NVMEManager::lock") {}
  int try_get(const string &sn_tag, SharedDriverData **driver);
  void register_ctrlr(const string &sn_tag, spdk_nvme_ctrlr *c, struct spdk_pci_device *pci_dev,
                      SharedDriverData **driver) {
    assert(lock.is_locked());
    spdk_nvme_ns *ns;
    int num_ns = spdk_nvme_ctrlr_get_num_ns(c);
    string name = spdk_pci_device_get_device_name(pci_dev) ? spdk_pci_device_get_device_name(pci_dev) : "Unknown";
    assert(num_ns >= 1);
    if (num_ns > 1) {
      dout(0) << __func__ << " namespace count larger than 1, currently only use the first namespace" << dendl;
    }
    ns = spdk_nvme_ctrlr_get_ns(c, 1);
    if (!ns) {
      derr << __func__ << " failed to get namespace at 1" << dendl;
      assert(0);
    }
    dout(1) << __func__ << " successfully attach nvme device at" << name
            << " " << spdk_pci_device_get_bus(pci_dev) << ":" << spdk_pci_device_get_dev(pci_dev) << ":" << spdk_pci_device_get_func(pci_dev) << dendl;

    // only support one device per osd now!
    assert(shared_driver_datas.empty());
    // index 0 is occured by master thread
    shared_driver_datas.push_back(new SharedDriverData(shared_driver_datas.size()+1, sn_tag, name, c, ns));
    *driver = shared_driver_datas.back();
  }
};

static NVMEManager manager;

static bool probe_cb(void *cb_ctx, struct spdk_pci_device *pci_dev)
{
  NVMEManager::ProbeContext *ctx = static_cast<NVMEManager::ProbeContext*>(cb_ctx);
  char serial_number[128];
  string name = spdk_pci_device_get_device_name(pci_dev) ? spdk_pci_device_get_device_name(pci_dev) : "Unknown";
  dout(0) << __func__ << " found device at name: " << name
          << " bus: " << spdk_pci_device_get_bus(pci_dev) << ":" << spdk_pci_device_get_dev(pci_dev) << ":"
          << spdk_pci_device_get_func(pci_dev) << " vendor:0x" << spdk_pci_device_get_vendor_id(pci_dev) << " device:0x" << spdk_pci_device_get_device_id(pci_dev)
          << dendl;
  int r = spdk_pci_device_get_serial_number(pci_dev, serial_number, 128);
  if (r < 0) {
    dout(10) << __func__ << " failed to get serial number from " << name << dendl;
    return false;
  }

  if (ctx->sn_tag.compare(string(serial_number, 16))) {
    dout(0) << __func__ << " device serial number not match " << serial_number << dendl;
    return false;
  }

  if (spdk_pci_device_has_non_uio_driver(pci_dev)) {
    /*NVMe kernel driver case*/
    if (g_conf->bdev_nvme_unbind_from_kernel) {
      r =  spdk_pci_device_switch_to_uio_driver(pci_dev);
      if (r < 0) {
        derr << __func__ << " device " << name
             << " " << spdk_pci_device_get_bus(pci_dev)
             << ":" << spdk_pci_device_get_dev(pci_dev)
             << ":" << spdk_pci_device_get_func(pci_dev)
             << " switch to uio driver failed" << dendl;
        return false;
      }
    } else {
      derr << __func__ << " device has kernel nvme driver attached" << dendl;
      return false;
    }
  } else {
    r =  spdk_pci_device_bind_uio_driver(pci_dev);
    if (r < 0) {
      derr << __func__ << " device " << name
           << " " << spdk_pci_device_get_bus(pci_dev)
           << ":" << spdk_pci_device_get_dev(pci_dev) << ":"
           << spdk_pci_device_get_func(pci_dev)
           << " bind to uio driver failed, may lack of uio_pci_generic kernel module" << dendl;
      return false;
    }
  }

  return true;
}

static void attach_cb(void *cb_ctx, struct spdk_pci_device *dev, struct spdk_nvme_ctrlr *ctrlr)
{
  NVMEManager::ProbeContext *ctx = static_cast<NVMEManager::ProbeContext*>(cb_ctx);
  ctx->manager->register_ctrlr(ctx->sn_tag, ctrlr, dev, &ctx->driver);
}

int NVMEManager::try_get(const string &sn_tag, SharedDriverData **driver)
{
  Mutex::Locker l(lock);
  int r = 0;
  if (sn_tag.empty()) {
    r = -ENOENT;
    derr << __func__ << " empty serial number: " << cpp_strerror(r) << dendl;
    return r;
  }

  for (auto &&it : shared_driver_datas) {
    if (it->is_equal(sn_tag)) {
      *driver = it;
      return 0;
    }
  }

  if (!init) {
    init = true;
    dpdk_thread = std::thread(
      [this]() {
        static const char *ealargs[] = {
            "ceph-osd",
            "-c 0x3", /* This must be the second parameter. It is overwritten by index in main(). */
            "-n 4",
        };

        int r = rte_eal_init(sizeof(ealargs) / sizeof(ealargs[0]), (char **)(void *)(uintptr_t)ealargs);
        if (r < 0) {
          derr << __func__ << " failed to do rte_eal_init" << dendl;
          assert(0);
        }

        request_mempool = rte_mempool_create("nvme_request", 512,
                                             spdk_nvme_request_size(), 128, 0,
                                             NULL, NULL, NULL, NULL,
                                             SOCKET_ID_ANY, 0);
        if (request_mempool == NULL) {
          derr << __func__ << " failed to create memory pool for nvme requests" << dendl;
          assert(0);
        }

        pci_system_init();
        spdk_nvme_retry_count = g_conf->bdev_nvme_retry_count;
        if (spdk_nvme_retry_count < 0)
          spdk_nvme_retry_count = SPDK_NVME_DEFAULT_RETRY_COUNT;

        std::unique_lock<std::mutex> l(probe_queue_lock);
        while (true) {
          if (!probe_queue.empty()) {
            ProbeContext* ctxt = probe_queue.front();
            probe_queue.pop_front();
            r = spdk_nvme_probe(ctxt, probe_cb, attach_cb);
            if (r < 0) {
              assert(!ctxt->driver);
              derr << __func__ << " device probe nvme failed" << dendl;
            }
            ctxt->done = true;
            probe_queue_cond.notify_all();
          } else {
            probe_queue_cond.wait(l);
          }
        }
      }
    );
    dpdk_thread.detach();
  }

  ProbeContext ctx = {sn_tag, this, nullptr, false};
  {
    std::unique_lock<std::mutex> l(probe_queue_lock);
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
  SharedDriverData *driver = task->device->get_driver();
  ++driver->completed_op_seq;
  auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(
      ceph::coarse_real_clock::now(g_ceph_context) - task->start);
  if (task->command == IOCommand::WRITE_COMMAND ||
      task->command == IOCommand::ZERO_COMMAND) {
    if (task->command == IOCommand::WRITE_COMMAND)
      driver->logger->tinc(l_bluestore_nvmedevice_aio_write_lat, dur);
    else
      driver->logger->tinc(l_bluestore_nvmedevice_aio_zero_lat, dur);
    assert(!spdk_nvme_cpl_is_error(completion));
    dout(20) << __func__ << " write/zero op successfully, left "
             << driver->queue_op_seq - driver->completed_op_seq << dendl;
    // check waiting count before doing callback (which may
    // destroy this ioc).
    if (ctx && !--ctx->num_running) {
      ctx->aio_wake();
      if (task->device->aio_callback && ctx->priv) {
        task->device->aio_callback(task->device->aio_callback_priv, ctx->priv);
      }
    }
    task->release_segs();
    delete task;
  } else if (task->command == IOCommand::READ_COMMAND) {
    driver->logger->tinc(l_bluestore_nvmedevice_read_lat, dur);
    --ctx->num_reading;
    assert(!spdk_nvme_cpl_is_error(completion));
    dout(20) << __func__ << " read op successfully" << dendl;
    if (spdk_nvme_cpl_is_error(completion))
      task->return_code = -1; // FIXME
    else
      task->return_code = 0;
    task->fill_cb();
    task->release_segs();
    {
      std::unique_lock<std::mutex> l(ctx->lock);
      ctx->cond.notify_all();
    }
  } else {
    assert(task->command == IOCommand::FLUSH_COMMAND);
    driver->logger->tinc(l_bluestore_nvmedevice_flush_lat, dur);
    dout(20) << __func__ << " flush op successfully" << dendl;
    if (spdk_nvme_cpl_is_error(completion))
      task->return_code = -1; // FIXME
    else
      task->return_code = 0;
    {
      std::unique_lock<std::mutex> l(ctx->lock);
      ctx->cond.notify_all();
    }
  }
}

// ----------------
#undef dout_prefix
#define dout_prefix *_dout << "bdev(" << name << ") "

NVMEDevice::NVMEDevice(aio_callback_t cb, void *cbpriv)
    : buffer_lock("NVMEDevice::buffer_lock"),
      aio_callback(cb),
      aio_callback_priv(cbpriv)
{
}


int NVMEDevice::open(string p)
{
  int r = 0;
  dout(1) << __func__ << " path " << p << dendl;

  string serial_number;
  int fd = ::open(p.c_str(), O_RDONLY);
  if (fd < 0) {
    r = -errno;
    derr << __func__ << " unable to open " << p << ": " << cpp_strerror(r)
	 << dendl;
    return r;
  }
  char buf[100];
  r = ::read(fd, buf, sizeof(buf));
  VOID_TEMP_FAILURE_RETRY(::close(fd));
  fd = -1; // defensive
  if (r <= 0) {
    if (r == 0) {
      r = -EINVAL;
    } else {
      r = -errno;
    }
    derr << __func__ << " unable to read " << p << ": " << cpp_strerror(r) << dendl;
    return r;
  }
  serial_number = string(buf, r);
  r = manager.try_get(serial_number, &driver);
  if (r < 0) {
    derr << __func__ << " failed to get nvme deivce with sn " << serial_number << dendl;
    return r;
  }

  driver->register_device(this);
  block_size = driver->get_block_size();
  size = driver->get_size();
  if (!driver->zero_command_support) {
    zeros = buffer::create_page_aligned(1048576);
    zeros.zero();
  }

  dout(1) << __func__ << " size " << size << " (" << pretty_si_t(size) << "B)"
          << " block_size " << block_size << " (" << pretty_si_t(block_size)
          << "B)" << dendl;

  return 0;
}

void NVMEDevice::close()
{
  dout(1) << __func__ << dendl;

  name.clear();
  driver->remove_device(this);

  dout(1) << __func__ << " end" << dendl;
}

int NVMEDevice::flush()
{
  dout(10) << __func__ << " start" << dendl;
  auto start = ceph::coarse_real_clock::now(g_ceph_context);
  driver->flush_wait();
  auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(
      ceph::coarse_real_clock::now(g_ceph_context) - start);
  driver->logger->tinc(l_bluestore_nvmedevice_flush_lat, dur);
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
    assert(ioc->num_pending.load() == 0);  // we should be only thread doing this
    // Only need to push the first entry
    driver->queue_task(t, pending);
    ioc->nvme_task_first = ioc->nvme_task_last = nullptr;
  }
}

int NVMEDevice::aio_write(
    uint64_t off,
    bufferlist &bl,
    IOContext *ioc,
    bool buffered)
{
  uint64_t len = bl.length();
  dout(20) << __func__ << " " << off << "~" << len << " ioc " << ioc
           << " buffered " << buffered << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  Task *t = new Task(this, IOCommand::WRITE_COMMAND, off, len);

  // TODO: if upper layer alloc memory with known physical address,
  // we can reduce this copy
  t->write_bl = std::move(bl);

  if (buffered) {
    // Only need to push the first entry
    driver->queue_task(t);
  } else {
    t->ctx = ioc;
    Task *first = static_cast<Task*>(ioc->nvme_task_first);
    Task *last = static_cast<Task*>(ioc->nvme_task_last);
    if (last)
      last->next = t;
    if (!first)
      ioc->nvme_task_first = t;
    ioc->nvme_task_last = t;
    ++ioc->num_pending;
  }

  dout(5) << __func__ << " " << off << "~" << len << dendl;

  return 0;
}

int NVMEDevice::aio_zero(
    uint64_t off,
    uint64_t len,
    IOContext *ioc)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  if (driver->zero_command_support) {
    Task *t = new Task(this, IOCommand::ZERO_COMMAND, off, len);
    t->ctx = ioc;
    Task *first = static_cast<Task*>(ioc->nvme_task_first);
    Task *last = static_cast<Task*>(ioc->nvme_task_last);
    if (last)
      last->next = t;
    if (!first)
      ioc->nvme_task_first = t;
    ioc->nvme_task_last = t;
    ++ioc->num_pending;
  } else {
    assert(zeros.length());
    bufferlist bl;
    while (len > 0) {
      bufferlist t;
      t.append(zeros, 0, MIN(zeros.length(), len));
      len -= t.length();
      bl.claim_append(t);
    }
    // note: this works with aio only becaues the actual buffer is
    // this->zeros, which is page-aligned and never freed.
    return aio_write(off, bl, ioc, false);
  }

  return 0;
}

int NVMEDevice::read(uint64_t off, uint64_t len, bufferlist *pbl,
                     IOContext *ioc,
                     bool buffered)
{
  dout(5) << __func__ << " " << off << "~" << len << " ioc " << ioc << dendl;
  assert(off % block_size == 0);
  assert(len % block_size == 0);
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  Task *t = new Task(this, IOCommand::READ_COMMAND, off, len, 1);
  bufferptr p = buffer::create_page_aligned(len);
  int r = 0;
  t->ctx = ioc;
  char *buf = p.c_str();
  t->fill_cb = [buf, t]() {
    t->copy_to_buf(buf, 0, t->len);
  };
  ++ioc->num_reading;
  driver->queue_task(t);

  {
    std::unique_lock<std::mutex> l(ioc->lock);
    while (t->return_code > 0)
      ioc->cond.wait(l);
  }
  pbl->clear();
  pbl->push_back(std::move(p));
  r = t->return_code;
  delete t;
  if (ioc->num_waiting.load()) {
    dout(20) << __func__ << " waking waiter" << dendl;
    std::unique_lock<std::mutex> l(ioc->lock);
    ioc->cond.notify_all();
  }
  return r;
}

int NVMEDevice::read_buffered(uint64_t off, uint64_t len, char *buf)
{
  assert(len > 0);
  assert(off < size);
  assert(off + len <= size);

  uint64_t aligned_off = align_down(off, block_size);
  uint64_t aligned_len = align_up(off+len, block_size) - aligned_off;
  dout(5) << __func__ << " " << off << "~" << len
          << " aligned " << aligned_off << "~" << aligned_len << dendl;
  IOContext ioc(nullptr);
  Task *t = new Task(this, IOCommand::READ_COMMAND, aligned_off, aligned_len, 1);
  int r = 0;
  t->ctx = &ioc;
  t->fill_cb = [buf, t, off, len]() {
    t->copy_to_buf(buf, off-t->offset, len);
  };
  ++ioc.num_reading;
  driver->queue_task(t);

  {
    std::unique_lock<std::mutex> l(ioc.lock);
    while (t->return_code > 0)
      ioc.cond.wait(l);
  }
  r = t->return_code;
  delete t;

  return r;
}

int NVMEDevice::invalidate_cache(uint64_t off, uint64_t len)
{
  dout(5) << __func__ << " " << off << "~" << len << dendl;
  return 0;
}

bool supports_discard()
{
  return false;
}
