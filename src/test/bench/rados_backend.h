// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef RADOSBACKENDH
#define RADOSBACKENDH

#include "backend.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"

class RadosBackend : public Backend {
  librados::IoCtx *ioctx;
public:
  explicit RadosBackend(
    librados::IoCtx *ioctx)
    : ioctx(ioctx) {}
  void write(
    const string &oid,
    uint64_t offset,
    const bufferlist &bl,
    Context *on_applied,
    Context *on_commit);

  void read(
    const string &oid,
    uint64_t offset,
    uint64_t length,
    bufferlist *bl,
    Context *on_complete);
};

#endif
