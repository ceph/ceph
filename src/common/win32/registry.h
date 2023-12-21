/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/compat.h"
#include "common/ceph_context.h"


class RegistryKey {
public:
  RegistryKey(CephContext *cct_, HKEY hRootKey, LPCTSTR strKey, bool create_value);
  ~RegistryKey();

  static int remove(CephContext *cct_, HKEY hRootKey, LPCTSTR strKey);

  int flush();

  int set(LPCTSTR lpValue, DWORD data);
  int set(LPCTSTR lpValue, std::string data);

  int get(LPCTSTR lpValue, bool& value);
  int get(LPCTSTR lpValue, DWORD& value);
  int get(LPCTSTR lpValue, std::string& value);

  HKEY hKey = NULL;
  bool missingKey = false;

private:
  CephContext *cct;
};
