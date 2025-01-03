// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __CEPH_OS_REVERSE_H
#define __CEPH_OS_REVERSE_H

#include "include/int_types.h"

#ifdef __cplusplus
extern "C" {
#endif

extern uint32_t reverse_bits(uint32_t v);
extern uint32_t reverse_nibbles(uint32_t retval);

#ifdef __cplusplus
}
#endif

#endif    
