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
namespace cls::blake3_hash {
  int blake3_hash_data(librados::ObjectReadOperation& op,
                       bufferlist *blake3_state_bl,
                       bufferlist *out,
                       cls_blake3_flags_t flags)
  {
    cls_blake3_op req;
    req.flags = flags;
    req.blake3_state_bl = *blake3_state_bl;
    // not sure what is it used for, so don't need to pass an argument for it
    // in any case, keep a static place for an async reply
    static int rval;
    bufferlist in;
    encode(req, in);
    op.exec("blake3", "blake3_hash_data", in, out, &rval);
    return 0;
  }
} // namespace cls::blake3_hash
