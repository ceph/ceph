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

#include "include/compat.h"

std::wstring to_wstring(const std::string& str);
std::string to_string(const std::wstring& str);

void to_filetime(time_t t, LPFILETIME pft);
void to_unix_time(FILETIME ft, time_t *t);
