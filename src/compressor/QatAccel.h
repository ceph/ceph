/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corporation
 *
 * Author: Qiaowei Ren <qiaowei.ren@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_QATACCEL_H
#define CEPH_QATACCEL_H

#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "include/buffer.h"

extern "C" struct QzSession_S; // typedef struct QzSession_S QzSession_T;

struct QzSessionDeleter {
  void operator() (struct QzSession_S *session);
};

class QatAccel {
 public:
  using session_ptr = std::unique_ptr<struct QzSession_S, QzSessionDeleter>;
  QatAccel();
  ~QatAccel();

  bool init(const std::string &alg);

  int compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message);
  int decompress(const bufferlist &in, bufferlist &out, std::optional<int32_t> compressor_message);
  int decompress(bufferlist::const_iterator &p, size_t compressed_len, bufferlist &dst, std::optional<int32_t> compressor_message);

 private:
  // get a session from the pool or create a new one. returns null if session init fails
  session_ptr get_session();

  friend struct cached_session_t;
  std::vector<session_ptr> sessions;
  std::mutex mutex;
  std::string alg_name;
};

#endif
