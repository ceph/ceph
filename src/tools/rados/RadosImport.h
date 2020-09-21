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

#ifndef RADOS_IMPORT_H_
#define RADOS_IMPORT_H_

#include <string>

#include "include/rados/librados.hpp"
#include "include/buffer_fwd.h"

#include "tools/RadosDump.h"

/**
 * Specialization of RadosDump that adds
 * methods for importing objects from a stream
 * to a live cluster.
 */
class RadosImport : public RadosDump
{
  protected:
    uint64_t align;
    int get_object_rados(librados::IoCtx &ioctx, bufferlist &bl, bool no_overwrite);

  public:
    RadosImport(int file_fd_, uint64_t align_, bool dry_run_)
      : RadosDump(file_fd_, dry_run_), align(align_)
    {}

    int import(std::string pool, bool no_overwrite);
    int import(librados::IoCtx &io_ctx, bool no_overwrite);
};

#endif // RADOS_IMPORT_H_
