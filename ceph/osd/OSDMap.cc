// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */


#include "OSDMap.h"




// serialize/unserialize

void OSDMap::encode(bufferlist& blist)
{
  blist.append((char*)&version, sizeof(version));
  blist.append((char*)&pg_bits, sizeof(pg_bits));

  _encode(osds, blist);
  _encode(down_osds, blist);
  //_encode(out_osds, blist);

  crush._encode(blist);
}

void OSDMap::decode(bufferlist& blist)
{
  int off = 0;
  blist.copy(off, sizeof(version), (char*)&version);
  off += sizeof(version);
  blist.copy(off, sizeof(pg_bits), (char*)&pg_bits);
  off += sizeof(pg_bits);

  _decode(osds, blist, off);
  _decode(down_osds, blist, off);
  //_decode(out_osds, blist, off);

  crush._decode(blist, off);
}
 
