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

#include <memory>
#include <boost/optional.hpp>
#include "include/buffer.h"

extern "C" struct QzSession_S; //struct QzSession_S comes from QAT libraries

struct QzSessionDeleter {
  void operator() (struct QzSession_S *session);
};

class QatAccel {
  std::unique_ptr<struct QzSession_S, QzSessionDeleter> session;

 public:
  QatAccel();
  ~QatAccel();

  bool init(const std::string &alg);

  int compress(const bufferlist &in, bufferlist &out, boost::optional<int32_t> &compressor_message);
  int decompress(const bufferlist &in, bufferlist &out, boost::optional<int32_t> compressor_message);
  int decompress(bufferlist::const_iterator &p, size_t compressed_len, bufferlist &dst, boost::optional<int32_t> compressor_message);
};

#endif
