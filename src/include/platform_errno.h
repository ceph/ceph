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
extern "C" {
__s32  ceph_to_hostos_errno(__s32 e);
__s32  hostos_to_ceph_errno(__s32 e);
}
#else
#define  ceph_to_hostos_errno(e) (e)
#define  hostos_to_ceph_errno(e) (e)
#endif


