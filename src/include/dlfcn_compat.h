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

#ifndef DLFCN_COMPAT_H
#define DLFCN_COMPAT_H

#include "acconfig.h"

#define SHARED_LIB_SUFFIX CMAKE_SHARED_LIBRARY_SUFFIX

#ifdef _WIN32
  #include <string>

  using dl_errmsg_t = std::string;

  // The load mode flags will be ignored on Windows. We keep the same
  // values for debugging purposes though.
  #define RTLD_LAZY       0x00001
  #define RTLD_NOW        0x00002
  #define RTLD_BINDING_MASK   0x3
  #define RTLD_NOLOAD     0x00004
  #define RTLD_DEEPBIND   0x00008
  #define RTLD_GLOBAL     0x00100
  #define RTLD_LOCAL      0
  #define RTLD_NODELETE   0x01000

  void* dlopen(const char *filename, int flags);
  int dlclose(void* handle);
  dl_errmsg_t dlerror();
  void* dlsym(void* handle, const char* symbol);
#else
  #include <dlfcn.h>

  using dl_errmsg_t = char*;
#endif /* _WIN32 */

#endif /* DLFCN_H */
