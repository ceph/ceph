// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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



#include "OSDMap.h"




// serialize/unserialize

void OSDMap::encode(bufferlist& blist)
{
  blist.append((char*)&epoch, sizeof(epoch));
  blist.append((char*)&pg_bits, sizeof(pg_bits));
  blist.append((char*)&mkfs, sizeof(mkfs));

  _encode(osds, blist);
  _encode(down_osds, blist);
  _encode(out_osds, blist);
  _encode(overload_osds, blist);

  crush._encode(blist);
}

void OSDMap::decode(bufferlist& blist)
{
  int off = 0;
  blist.copy(off, sizeof(epoch), (char*)&epoch);
  off += sizeof(epoch);
  blist.copy(off, sizeof(pg_bits), (char*)&pg_bits);
  off += sizeof(pg_bits);
  blist.copy(off, sizeof(mkfs), (char*)&mkfs);
  off += sizeof(mkfs);

  _decode(osds, blist, off);
  _decode(down_osds, blist, off);
  _decode(out_osds, blist, off);
  _decode(overload_osds, blist, off);

  crush._decode(blist, off);
}
 
