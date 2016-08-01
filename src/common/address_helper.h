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

#ifndef ADDRESS_HELPER_H_
#define ADDRESS_HELPER_H_

#include "msg/msg_types.h"

int entity_addr_from_url(entity_addr_t *addr /* out */, const char *url);

#endif /* ADDRESS_HELPER_H_ */
