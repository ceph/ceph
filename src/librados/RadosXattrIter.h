// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Sebastien Ponce <sebastien.ponce@cern.ch>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBRADOS_XATTRITER_H
#define CEPH_LIBRADOS_XATTRITER_H

#include <string>
#include <map>

#include "include/buffer.h"

namespace librados {

  /**
   * iterator object used in implementation of the extrenal
   * attributes part of the C interface of librados
   */
  struct RadosXattrsIter {
    RadosXattrsIter();
    ~RadosXattrsIter();
    std::map<std::string, bufferlist> attrset;
    std::map<std::string, bufferlist>::iterator i;
    char *val;
  };
};

#endif
