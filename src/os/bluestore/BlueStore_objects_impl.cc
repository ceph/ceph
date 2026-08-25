// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/dout.h"
#include "common/debug.h"

#include "BlueStore.h"
#include "BlueStore_objects.h"
#include "BlueStore_objects_impl.h"
#include "os/bluestore/bluestore_types.h"
#include "os/kv.h"

#define dout_context cct
#define dout_subsys ceph_subsys_bluestore

using std::min;
using std::numeric_limits;
using std::less;
using std::list;
using std::map;
using std::max;
using std::ostream;
using std::set;
using std::string;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;

// Extent

void bluestore::Extent::dump(Formatter* f) const
{
  f->dump_unsigned("logical_offset", logical_offset);
  f->dump_unsigned("length", length);
  f->dump_unsigned("blob_offset", blob_offset);
  f->dump_object("blob", *blob);
}

namespace bluestore {
ostream& operator<<(ostream& out, const bluestore::Extent& e)
{
  return out << std::hex << "0x" << e.logical_offset << "~" << e.length
	     << ": 0x" << e.blob_offset << "~" << e.length << std::dec
	     << " " << *e.blob;
}
}