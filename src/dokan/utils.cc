/*
 * ceph-dokan - Win32 CephFS client based on Dokan
 *
 * Copyright (C) 2021 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#include "utils.h"

#include <boost/locale/encoding_utf.hpp>

using boost::locale::conv::utf_to_utf;

std::wstring to_wstring(const std::string& str)
{
  return utf_to_utf<wchar_t>(str.c_str(), str.c_str() + str.size());
}

std::string to_string(const std::wstring& str)
{
  return utf_to_utf<char>(str.c_str(), str.c_str() + str.size());
}

void to_filetime(time_t t, LPFILETIME pft)
{
  // Note that LONGLONG is a 64-bit value
  LONGLONG ll = Int32x32To64(t, 10000000) + 116444736000000000;
  pft->dwLowDateTime = (DWORD)ll;
  pft->dwHighDateTime = ll >> 32;
}

void to_unix_time(FILETIME ft, time_t *t)
{
  ULARGE_INTEGER ui;
  ui.LowPart  = ft.dwLowDateTime;
  ui.HighPart = ft.dwHighDateTime;

  *t = (LONGLONG)(ui.QuadPart / 10000000ULL - 11644473600ULL);
}
