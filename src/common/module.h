/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MODULE_H
#define CEPH_MODULE_H

#ifdef __cplusplus
extern "C" {
#endif

int module_has_param(const char *module, const char *param);
int module_load(const char *module, const char *options);

#ifdef __cplusplus
}
#endif

#endif /* CEPH_MODULE_H */
