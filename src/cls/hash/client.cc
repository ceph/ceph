// -*- mode:C++; tab-width:8; c-basic-offset:2
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 * Copyright (C) 2025 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "client.h"
#include "include/rados/librados.hpp"
namespace cls::hash {
  int hash_data(librados::ObjectReadOperation& op,
                hash_type_t hash_type,
                uint64_t    offset,
                bufferlist *hash_state_bl,
                bufferlist *out,
                cls_hash_flags_t flags)
  {
    cls_hash_op req;
    req.hash_type = (int32_t)hash_type;
    req.offset = offset;
    req.flags = flags;
    req.hash_state_bl = *hash_state_bl;
    // not sure what is it used for, so don't need to pass an argument for it
    // in any case, keep a static place for an async reply
    static int rval;
    bufferlist in;
    encode(req, in);
    op.exec("hash", "hash_data", in, out, &rval);
    return 0;
  }
} // namespace cls::hash
