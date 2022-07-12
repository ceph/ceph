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

/*
 * Don't wait for udev add uevents in krbd_map() and udev remove
 * uevents in krbd_unmap*().  Instead, make do with the respective
 * kernel uevents and return as soon as they are received.
 *
 * systemd-udevd sends out udev uevents after it finishes processing
 * the respective kernel uevents, which mostly boils down to executing
 * all matching udev rules.  With this flag set, on return from
 * krbd_map() systemd-udevd may still be poking at the device: it
 * may still be open with tools such as blkid and various ioctls to
 * be run against it, none of the persistent symlinks to the device
 * node may be there, etc.  udev used to be responsible for creating
 * the device node as well, but that has been handled by devtmpfs in
 * the kernel for many years now, so the device node (as returned
 * through @pdevnode) is guaranteed to be there.
 *
 * If set, krbd_map() and krbd_unmap*() can be invoked from any
 * network namespace that is owned by the initial user namespace
 * (which is a formality because things like loading kernel modules
 * and creating block devices are not namespaced and require global
 * privileges, i.e. capabilities in the initial user namespace).
 * Otherwise, krbd_map() and krbd_unmap*() must be invoked from
 * the initial network namespace.
 *
 * If set, krbd_unmap*() doesn't attempt to settle the udev queue
 * before retrying unmap for the last time.  Some EBUSY errors due
 * to systemd-udevd poking at the device at the time krbd_unmap*()
 * is invoked that are otherwise covered by the retry logic may be
 * returned.
 */
#define KRBD_CTX_F_NOUDEV       (1U << 0)

#ifdef __cplusplus
extern "C" {
#endif

struct krbd_ctx;

int krbd_create_from_context(rados_config_t cct, uint32_t flags,
                             struct krbd_ctx **pctx);
void krbd_destroy(struct krbd_ctx *ctx);

int krbd_map(struct krbd_ctx *ctx,
             const char *pool_name,
             const char *nspace_name,
             const char *image_name,
             const char *snap_name,
             const char *options,
             char **pdevnode);
int krbd_is_mapped(struct krbd_ctx *ctx,
                   const char *pool_name,
                   const char *nspace_name,
                   const char *image_name,
                   const char *snap_name,
                   char **pdevnode);

int krbd_unmap(struct krbd_ctx *ctx, const char *devnode,
               const char *options);
int krbd_unmap_by_spec(struct krbd_ctx *ctx,
                       const char *pool_name,
                       const char *nspace_name,
                       const char *image_name,
                       const char *snap_name,
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
