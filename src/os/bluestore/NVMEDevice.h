// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#ifndef CEPH_OS_BLUESTORE_NVMEDEVICE
#define CEPH_OS_BLUESTORE_NVMEDEVICE

#include <queue>
#include <map>
#include <pciaccess.h>
#include <limits>

// since _Static_assert introduced in c11
#define _Static_assert static_assert


#include "include/atomic.h"
#include "include/interval_set.h"
#include "common/ceph_time.h"
#include "common/Mutex.h"
#include "BlockDevice.h"

enum class IOCommand {
  READ_COMMAND,
  WRITE_COMMAND,
  ZERO_COMMAND,
  FLUSH_COMMAND
};

class Task;
class PerfCounters;
class SharedDriverData;

class NVMEDevice : public BlockDevice {
  /**
   * points to pinned, physically contiguous memory region;
   * contains 4KB IDENTIFY structure for controller which is
   *  target for CONTROLLER IDENTIFY command during initialization
   */
  SharedDriverData *driver;
  string name;

  uint64_t size;
  uint64_t block_size;

  bool aio_stop;
  bufferptr zeros;

  struct BufferedExtents {
    struct Extent {
      uint64_t x_len;
      uint64_t x_off;
      const char *data;
      uint64_t data_len;
    };
    using Offset = uint64_t;
    map<Offset, Extent> buffered_extents;
    uint64_t left_edge = std::numeric_limits<uint64_t>::max();
    uint64_t right_edge = 0;

    void verify() {
      interval_set<uint64_t> m;
      for (auto && it : buffered_extents) {
        assert(!m.intersects(it.first, it.second.x_len));
        m.insert(it.first, it.second.x_len);
      }
    }

    void insert(uint64_t off, uint64_t len, const char *data) {
      auto it = buffered_extents.lower_bound(off);
      if (it != buffered_extents.begin()) {
        --it;
        if (it->first + it->second.x_len <= off)
          ++it;
      }
      uint64_t end = off + len;
      if (off < left_edge)
        left_edge = off;
      if (end > right_edge)
        right_edge = end;
      while (it != buffered_extents.end()) {
        if (it->first >= end)
          break;
        uint64_t extent_it_end = it->first + it->second.x_len;
        assert(extent_it_end >= off);
        if (it->first <= off) {
          if (extent_it_end > end) {
            //         <-     data    ->
            // <-            it           ->
            it->second.x_len -= (extent_it_end - off);
            buffered_extents[end] = Extent{
                extent_it_end - end, it->second.x_off + it->second.x_len + len, it->second.data, it->second.data_len};
          } else {
            //         <-     data    ->
            // <-     it    ->
            assert(extent_it_end <= end);
            it->second.x_len -= (extent_it_end - off);
          }
          ++it;
        } else {
          assert(it->first > off) ;
          if (extent_it_end > end) {
            //  <-     data    ->
            //      <-           it          ->
            uint64_t overlap = end - it->first;
            buffered_extents[end] = Extent{
                it->second.x_len - overlap, it->second.x_off + overlap, it->second.data, it->second.data_len};
          } else {
            //  <-     data    ->
            //      <- it ->
          }
          buffered_extents.erase(it++);
        }
      }
      buffered_extents[off] = Extent{
          len, 0, data, len};

      if (0)
        verify();
    }

    void memcpy_check(char *dst, uint64_t dst_raw_len, uint64_t dst_off,
                      map<Offset, Extent>::iterator &it, uint64_t src_off, uint64_t copylen) {
      if (0) {
        assert(dst_off + copylen <= dst_raw_len);
        assert(it->second.x_off + src_off + copylen <= it->second.data_len);
      }
      memcpy(dst + dst_off, it->second.data + it->second.x_off + src_off, copylen);
    }

    uint64_t read_overlap(uint64_t off, uint64_t len, char *buf) {
      uint64_t end = off + len;
      if (end <= left_edge || off >= right_edge)
        return 0;

      uint64_t copied = 0;
      auto it = buffered_extents.lower_bound(off);
      if (it != buffered_extents.begin()) {
        --it;
        if (it->first + it->second.x_len <= off)
          ++it;
      }
      uint64_t copy_len;
      while (it != buffered_extents.end()) {
        if (it->first >= end)
          break;
        uint64_t extent_it_end = it->first + it->second.x_len;
        assert(extent_it_end >= off);
        if (it->first >= off) {
          if (extent_it_end > end) {
            //  <-     data    ->
            //      <-           it          ->
            copy_len = len - (it->first - off);
            memcpy_check(buf, len, it->first - off, it, 0, copy_len);
          } else {
            //  <-     data    ->
            //      <- it ->
            copy_len = it->second.x_len;
            memcpy_check(buf, len, it->first - off, it, 0, copy_len);
          }
        } else {
          if (extent_it_end > end) {
            //         <-     data    ->
            // <-           it          ->
            copy_len = len;
            memcpy_check(buf, len, 0, it, off - it->first, copy_len);
          } else {
            //         <-     data    ->
            // <-     it    ->
            assert(extent_it_end <= end);
            copy_len = it->first + it->second.x_len - off;
            memcpy_check(buf, len, 0, it, off - it->first, copy_len);
          }
        }
        copied += copy_len;
        ++it;
      }
      return copied;
    }

    void clear() {
      buffered_extents.clear();
      left_edge = std::numeric_limits<uint64_t>::max();
      right_edge = 0;
    }
  };
  Mutex buffer_lock;
  BufferedExtents buffered_extents;
  Task *buffered_task_head = nullptr;

  static void init();
 public:
  SharedDriverData *get_driver() { return driver; }

 public:
  aio_callback_t aio_callback;
  void *aio_callback_priv;

  NVMEDevice(aio_callback_t cb, void *cbpriv);

  bool supported_bdev_label() override { return false; }

  void aio_submit(IOContext *ioc) override;

  uint64_t get_size() const override {
    return size;
  }
  uint64_t get_block_size() const override {
    return block_size;
  }

  int read(uint64_t off, uint64_t len, bufferlist *pbl,
           IOContext *ioc,
           bool buffered) override;

  int aio_write(uint64_t off, bufferlist& bl,
                IOContext *ioc,
                bool buffered) override ;
  int aio_zero(uint64_t off, uint64_t len,
               IOContext *ioc) override;
  int flush() override;
  int read_buffered(uint64_t off, uint64_t len, char *buf) override;

  // for managing buffered readers/writers
  int invalidate_cache(uint64_t off, uint64_t len) override;
  int open(string path) override;
  void close() override;
};

#endif
