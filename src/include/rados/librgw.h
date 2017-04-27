// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_LIBRGW_H
#define CEPH_LIBRGW_H

#ifdef __cplusplus
extern "C" {
#endif

#define LIBRGW_VER_MAJOR 1
#define LIBRGW_VER_MINOR 1
#define LIBRGW_VER_EXTRA 0

#define LIBRGW_VERSION(maj, min, extra) ((maj << 16) + (min << 8) + extra)
#define LIBRGW_VERSION_CODE LIBRGW_VERSION(LIBRGW_VER_MAJOR, LIBRGW_VER_MINOR, LIBRGW_VER_EXTRA)

typedef void* librgw_t;
int librgw_create(librgw_t *rgw, int argc, char **argv);
void librgw_shutdown(librgw_t rgw);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_LIBRGW_H */
