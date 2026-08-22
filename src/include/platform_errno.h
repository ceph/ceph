// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/* XXX: This definitions are placed here so that it's easy to import them into
 * CephFS python bindings. Otherwise, entire src/include/types.h would needed to
 * be imported, which is unneccessary and also complicated.
 */

#pragma once

#if defined(__sun) || defined(_AIX) || defined(__APPLE__) || \
    defined(__FreeBSD__) || defined(_WIN32)
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
/* int32_t rather than __s32, which would need int_types.h and so the
 * generated acconfig.h; this header has to stand on its own to be included
 * from the bindings.  Every platform reaching this branch has __s32 as a
 * typedef of int32_t, so the definitions still match. */
int32_t  ceph_to_hostos_errno(int32_t e);
int32_t  hostos_to_ceph_errno(int32_t e);
#ifdef __cplusplus
}
#endif
#else
#define  ceph_to_hostos_errno(e) (e)
#define  hostos_to_ceph_errno(e) (e)
#endif


