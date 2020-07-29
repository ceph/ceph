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

#include "include/dlfcn_compat.h"


void* dlopen(const char *filename, int flags) {
  return LoadLibrary(filename);
}

int dlclose(void* handle) {
  //FreeLibrary returns 0 on error, as opposed to dlclose.
  return !FreeLibrary(handle);
}

void* dlsym(void* handle, const char* symbol) {
  return (void*)GetProcAddress(handle, symbol);
}

dl_errmsg_t dlerror() {
  DWORD err_code = ::GetLastError();
  // As opposed to dlerror messages, this has to be freed.
  LPSTR msg = NULL;
  DWORD msg_len = FormatMessageA(
    FORMAT_MESSAGE_ALLOCATE_BUFFER |
    FORMAT_MESSAGE_FROM_SYSTEM |
    FORMAT_MESSAGE_IGNORE_INSERTS,
    NULL,
    err_code,
    0,
    (LPSTR) &msg,
    0,
    NULL);
  if (!msg_len) {
    std::ostringstream msg_stream;
    msg_stream << "Unknown error (" << err_code << ").";
    return msg_stream.str();
  }
  std::string msg_s(msg);
  LocalFree(msg);
  return msg_s;
}

