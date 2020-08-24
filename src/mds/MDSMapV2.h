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

#ifndef CEPH_MDSMAPV2_H
#define CEPH_MDSMAPV2_H

#include "mds/MDSMap.h"

/*
  Use MDSMapV2 for Pacific or later versions.
  Primarily intention is to have newer implementation of
  encoding/decoding.
 */
class MDSMapV2 : public MDSMap {

public:
  MDSMapV2();
  void encode(ceph::buffer::list& bl, uint64_t features, bool encode_ev=true) const;
  void decode(ceph::buffer::list::const_iterator& p);
  static void generate_test_instances(std::list<MDSMap*>& ls);
  ~MDSMapV2();
};
#endif
