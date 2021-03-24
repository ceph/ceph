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

#define dout_context cct
#define dout_subsys ceph_subsys_

#include "common/debug.h"
#include "common/errno.h"
#include "common/win32/registry.h"

RegistryKey::RegistryKey(CephContext *cct_, HKEY hRootKey, LPCTSTR strKey,
                         bool create_value): cct(cct_)
{
  DWORD status = RegOpenKeyEx(hRootKey, strKey, 0, KEY_ALL_ACCESS, &hKey);

  if (status == ERROR_FILE_NOT_FOUND && create_value)
  {
    ldout(cct_, 10) << "Creating registry key: " << strKey << dendl;
    status = RegCreateKeyEx(
        hRootKey, strKey, 0, NULL, REG_OPTION_NON_VOLATILE,
        KEY_ALL_ACCESS, NULL, &hKey, NULL);
  }

  if (ERROR_SUCCESS != status) {
    if (ERROR_FILE_NOT_FOUND == status) {
      missingKey = true;
    } else {
      lderr(cct_) << "Error: " << win32_strerror(status)
                  << ". Could not open registry key: "
                  << strKey << dendl;
    }
  }
}

RegistryKey::~RegistryKey() {
  if (!hKey)
    return;

  DWORD status = RegCloseKey(hKey);
  if (ERROR_SUCCESS != status) {
    derr << "Error: " << win32_strerror(status)
         << ". Could not close registry key." << dendl;
  } else {
    hKey = NULL;
  }
}

int RegistryKey::remove(CephContext *cct_, HKEY hRootKey, LPCTSTR strKey)
{
  DWORD status = RegDeleteKeyEx(hRootKey, strKey, KEY_WOW64_64KEY, 0);

  if (status == ERROR_FILE_NOT_FOUND)
  {
    ldout(cct_, 20) << "Registry key : " << strKey
                    << " does not exist." << dendl;
    return 0;
  }

  if (ERROR_SUCCESS != status) {
    lderr(cct_) << "Error: " << win32_strerror(status)
                << ". Could not delete registry key: "
                << strKey << dendl;
    return -EINVAL;
  }

  return 0;
}

int RegistryKey::flush() {
  DWORD status = RegFlushKey(hKey);
  if (ERROR_SUCCESS != status) {
    derr << "Error: " << win32_strerror(status)
         << ". Could not flush registry key." << dendl;
    return -EINVAL;
  }

  return 0;
}

int RegistryKey::set(LPCTSTR lpValue, DWORD data)
{
  DWORD status = RegSetValueEx(hKey, lpValue, 0, REG_DWORD,
                               (LPBYTE)&data, sizeof(DWORD));
  if (ERROR_SUCCESS != status) {
    derr << "Error: " << win32_strerror(status)
         << ". Could not set registry value: " << (char*)lpValue << dendl;
    return -EINVAL;
  }

  return 0;
}

int RegistryKey::set(LPCTSTR lpValue, std::string data)
{
  DWORD status = RegSetValueEx(hKey, lpValue, 0, REG_SZ,
                               (LPBYTE)data.c_str(), data.length());
  if (ERROR_SUCCESS != status) {
    derr << "Error: " << win32_strerror(status)
         << ". Could not set registry value: "
         << (char*)lpValue << dendl;
    return -EINVAL;
  }
  return 0;
}

int RegistryKey::get(LPCTSTR lpValue, bool& value)
{
  DWORD value_dw = 0;
  int r = get(lpValue, value_dw);
  if (!r) {
    value = !!value_dw;
  }
  return r;
}

int RegistryKey::get(LPCTSTR lpValue, DWORD& value)
{
  DWORD data;
  DWORD size = sizeof(data);
  DWORD type = REG_DWORD;
  DWORD status = RegQueryValueEx(hKey, lpValue, NULL,
                                 &type, (LPBYTE)&data, &size);
  if (ERROR_SUCCESS != status) {
    derr << "Error: " << win32_strerror(status)
         << ". Could not get registry value: "
         << (char*)lpValue << dendl;
    return -EINVAL;
  }
  value = data;

  return 0;
}

int RegistryKey::get(LPCTSTR lpValue, std::string& value)
{
  std::string data{""};
  DWORD size = 0;
  DWORD type = REG_SZ;
  DWORD status = RegQueryValueEx(hKey, lpValue, NULL, &type,
                                 (LPBYTE)data.c_str(), &size);
  if (ERROR_MORE_DATA == status) {
    data.resize(size);
    status = RegQueryValueEx(hKey, lpValue, NULL, &type,
                             (LPBYTE)data.c_str(), &size);
  }

  if (ERROR_SUCCESS != status) {
    derr << "Error: " << win32_strerror(status)
         << ". Could not get registry value: "
         << (char*)lpValue << dendl;
    return -EINVAL;
  }
  value.assign(data.c_str());

  return 0;
}
