// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sstream>
#include <windows.h>

#include "common/errno.h"
#include "include/dlfcn_compat.h"


void* dlopen(const char *filename, int flags) {
  return LoadLibrary(filename);
}

int dlclose(void* handle) {
  //FreeLibrary returns 0 on error, as opposed to dlclose.
  return !FreeLibrary((HMODULE)handle);
}

void* dlsym(void* handle, const char* symbol) {
  return (void*)GetProcAddress((HMODULE)handle, symbol);
}

dl_errmsg_t dlerror() {
  return win32_lasterror_str();
}

