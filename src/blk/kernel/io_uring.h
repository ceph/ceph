// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "acconfig.h"

#include "include/types.h"
#include "aio/aio.h"

struct ioring_data;

struct ioring_queue_t final : public io_queue_t {
  std::unique_ptr<ioring_data> d;
  unsigned iodepth = 0;

  typedef std::list<aio_t>::iterator aio_iter;

  // Returns true if arch is x86-64 and kernel supports io_uring
  static bool supported();

  ioring_queue_t(unsigned iodepth_);
  ~ioring_queue_t() final;

  int init(std::vector<int> &fds) final;
  void shutdown() final;

  int submit_batch(aio_iter begin, aio_iter end, uint16_t aios_size,
                   void *priv, int *retries) final;
  int get_next_completed(int timeout_ms, aio_t **paio, int max) final;
};
