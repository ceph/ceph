// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/encoding.h"
#include "include/denc.h"

int main (void) {
  ceph::buffer::list::const_iterator dummy;
  DECODE_START(42, dummy);
  if (struct_v >= 42)
    /* OK */;
#ifdef SHOULD_FAIL
  if (struct_v >= 43)
    /* NOK */;
#endif
  DECODE_FINISH(dummy);
}
