/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_KRBD_H
#define CEPH_KRBD_H

#include "rados/librados.h"

#ifdef __cplusplus
extern "C" {
#endif

struct krbd_ctx;

int krbd_create_from_context(rados_config_t cct, struct krbd_ctx **pctx);
void krbd_destroy(struct krbd_ctx *ctx);

int krbd_map(struct krbd_ctx *ctx, const char *pool, const char *image,
             const char *snap, const char *options, char **pdevnode);

int krbd_unmap(struct krbd_ctx *ctx, const char *devnode,
               const char *options);
int krbd_unmap_by_spec(struct krbd_ctx *ctx, const char *pool,
                       const char *image, const char *snap,
                       const char *options);

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus

namespace ceph {
  class Formatter;
}

int krbd_showmapped(struct krbd_ctx *ctx, ceph::Formatter *f);

#endif /* __cplusplus */

#endif /* CEPH_KRBD_H */
