// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBCEPHFSSQLITE_H
#define CEPH_LIBCEPHFSSQLITE_H
#include <sqlite3.h>

#ifdef _WIN32
#  define LIBCEPHFSSQLITE_API __declspec(dllexport)
#else
#  define LIBCEPHFSSQLITE_API [[gnu::visibility("default")]]
#endif

#ifdef __cplusplus
extern "C" {
#endif


LIBCEPHFSSQLITE_API int sqlite3_cephfssqlite_init(sqlite3* db, char** err, const sqlite3_api_routines* api);


LIBCEPHFSSQLITE_API int cephfssqlite_setcct(class CephContext* cct, char** ident);
#ifdef __cplusplus
}
#endif

#endif
