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

#ifndef LIBCEPHSQLITE_H
#define LIBCEPHSQLITE_H

/* This loadable extension does not generally require using this header. It is
 * here to allow controlling which version of the library is linked in. See
 * also sqlite3_cephsqlite_init below. Additionally, you may specify which
 * CephContext to use rather than the library instantiating its own and using
 * whatever the default credential is.
 */

#include <sqlite3.h>

#ifdef _WIN32
#  define LIBCEPHSQLITE_API __declspec(dllexport)
#else
#  define LIBCEPHSQLITE_API [[gnu::visibility("default")]]
#endif

#ifdef __cplusplus
extern "C" {
#endif
/* This is the SQLite entry point when loaded as a dynamic library. You also
 * need to ensure SQLite calls this method when using libcephsqlite as a static
 * library or a dynamic library linked at compile time. For the latter case,
 * you can do this by:
 *
 *   sqlite3_auto_extension((void (*)())sqlite3_cephsqlite_init);
 *   sqlite3* db = nullptr;
 *   int rc = sqlite3_open_v2(":memory:", &db, SQLITE_OPEN_READWRITE, nullptr);
 *   if (rc == SQLITE_DONE) {
 *     sqlite3_close(db);
 *   } else {
 *     // failure
 *   }
 *
 * The throwaway database created (name == "") is a memory database opened so
 * that SQLite runs the libcephsqlite initialization routine to register the
 * VFS. AFter that's done, the VFS is available for a future database open with
 * the VFS set to "ceph":
 *
 *   sqlite3_open_v2("foo:bar/baz.db", &db, SQLITE_OPEN_READWRITE, "ceph");
 *
 * You MUST do this before calling any other libcephsqlite routine so that
 * sqlite3 can pass its API routines to the libcephsqlite extension.
 */

LIBCEPHSQLITE_API int sqlite3_cephsqlite_init(sqlite3* db, char** err, const sqlite3_api_routines* api);

/* If you prefer to have libcephsqlite use a CephContext managed by your
 * application, use this routine to set that. libcephsqlite can only have one
 * context globally.
 */

LIBCEPHSQLITE_API int cephsqlite_setcct(class CephContext* cct, char** ident);
#ifdef __cplusplus
}
#endif

#endif
