// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "acconfig.h"

#include "include/types.h"
#include "aio/aio.h"

#include <memory>
#include <vector>

struct ioring_data;

struct ioring_queue_t final : public io_queue_t {
  std::unique_ptr<ioring_data> d;
  unsigned iodepth = 0;
  bool hipri = false;
  bool sq_thread = false;

  // Returns true if arch is x86-64 and kernel supports io_uring
  static bool supported();

  ioring_queue_t(unsigned iodepth_, bool hipri_, bool sq_thread_);
  ~ioring_queue_t() final;

  int init(std::vector<int> &fds) final;
  void shutdown() final;

  int submit_batch(aio_iter first, aio_iter last,
                   void *priv, int *retries, int submit_retries, int initial_delay_us) final;
  int get_next_completed(int timeout_ms, aio_t **paio, int max) final;
};
