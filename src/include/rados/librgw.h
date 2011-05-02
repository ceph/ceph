// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBRGW_H
#define CEPH_LIBRGW_H

#ifdef __cplusplus
extern "C" {
#endif

int librgw_acl_bin2xml(const char *bin, int bin_len, char **xml);
void librgw_free_xml(char *xml);
int librgw_acl_xml2bin(const char *xml, char **bin, int *bin_len);
void librgw_free_bin(char *bin);

#ifdef __cplusplus
}
#endif

#endif
