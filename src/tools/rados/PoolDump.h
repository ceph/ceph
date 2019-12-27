// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef POOL_DUMP_H_
#define POOL_DUMP_H_

#include "include/rados/librados_fwd.hpp"
#include "tools/RadosDump.h"

class PoolDump : public RadosDump
{
  public:
    explicit PoolDump(int file_fd_) : RadosDump(file_fd_, false) {}
    int dump(librados::IoCtx *io_ctx);
    int export_object(librados::IoCtx *io_ctx, std::string oid, std::string nspace, std::string oloc);
    int dump_object(librados::IoCtx *io_ctx, std::string oid, std::string nspace, std::string oloc);
};

#endif // POOL_DUMP_H_
