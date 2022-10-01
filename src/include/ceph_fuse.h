// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Inktank Storage, Inc.
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 */
#ifndef CEPH_FUSE_H
#define CEPH_FUSE_H

/*
 * The API version that we want to use, regardless of what the
 * library version is. Note that this must be defined before
 * fuse.h is included.
 */
#ifndef FUSE_USE_VERSION
#define FUSE_USE_VERSION	35
#endif

#include <fuse.h>
#include "acconfig.h"

/*
 * Redefine the FUSE_VERSION macro defined in "fuse_common.h"
 * header file, because the MINOR numner has been forgotten to
 * update since libfuse 3.2 to 3.8. We need to fetch the MINOR
 * number from pkgconfig file.
 */
#ifdef FUSE_VERSION
#undef FUSE_VERSION
#define FUSE_VERSION FUSE_MAKE_VERSION(CEPH_FUSE_MAJOR_VERSION, CEPH_FUSE_MINOR_VERSION)
#endif

static inline int filler_compat(fuse_fill_dir_t filler,
                                void *buf, const char *name,
                                const struct stat *stbuf,
                                off_t off)
{
  return filler(buf, name, stbuf, off
#if FUSE_VERSION >= FUSE_MAKE_VERSION(3, 0)
                , static_cast<enum fuse_fill_dir_flags>(0)
#endif
        );
}
#endif /* CEPH_FUSE_H */
