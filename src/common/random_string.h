// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 * Copyright (C) 2015 Yehuda Sadeh <yehuda@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <string>
#include "include/common_fwd.h"

/* size should be the required string size + 1 */
int gen_rand_base64(CephContext *cct, char *dest, size_t size);
void gen_rand_alphanumeric(CephContext *cct, char *dest, size_t size);
void gen_rand_alphanumeric_lower(CephContext *cct, char *dest, size_t size);
void gen_rand_alphanumeric_upper(CephContext *cct, char *dest, size_t size);
void gen_rand_alphanumeric_no_underscore(CephContext *cct, char *dest, size_t size);
void gen_rand_alphanumeric_plain(CephContext *cct, char *dest, size_t size);
void gen_rand_numeric(CephContext *cct, char *dest, size_t size);

// returns a std::string with 'size' random characters
std::string gen_rand_alphanumeric(CephContext *cct, size_t size);
std::string gen_rand_alphanumeric_lower(CephContext *cct, size_t size);
std::string gen_rand_alphanumeric_upper(CephContext *cct, size_t size);
std::string gen_rand_alphanumeric_no_underscore(CephContext *cct, size_t size);
std::string gen_rand_alphanumeric_plain(CephContext *cct, size_t size);
std::string gen_rand_numeric(CephContext *cct, size_t size);
