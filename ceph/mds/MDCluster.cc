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


#include "MDCluster.h"
#include "CDir.h"
#include "CInode.h"

#include <iostream>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include <sys/types.h>
#include <unistd.h>

#include "config.h"


#define HASHDIR_OID_MULT (0x100 * 0x100000000LL) // 40 bits (~1 trillion)


MDCluster::MDCluster(int num_mds, int num_osd)
{
  this->num_mds = num_mds;
  this->num_osd = num_osd;

  map_osds();
}


void MDCluster::map_osds()
{
  // logs on ~10% of osd
  osd_log_begin = 0;
  osd_log_end = num_osd / 10;
  if (osd_log_end > num_mds)
	osd_log_end = num_mds;

  // metadata on the rest
  osd_meta_begin = osd_log_end;
  osd_meta_end = num_osd;

  dout(15) << "mdcluster: " << num_mds << " mds, " << num_osd << " osd" << endl;
  dout(15) << "mdcluster:  logs on " << (osd_log_end-osd_log_begin) << " osd [" << osd_log_begin << ", " << osd_log_end << ")" << endl;
  dout(15) << "mdcluster:  metadata on " << (osd_meta_end-osd_meta_begin) << " osd [" << osd_meta_begin << ", " << osd_meta_end << ")" << endl;
}



/* hash a directory inode, dentry to a mds server
 */
int MDCluster::hash_dentry( inodeno_t dirino, const string& dn )
{
  static hash<const char*> H;
  unsigned r = dirino;
  
  if (1) {
	r += H(dn.c_str());
  } else {
	for (unsigned i=0; i<dn.length(); i++)
	  r += (dn[i] ^ (r+i));
  }

  r %= num_mds;

  dout(22) << "hash_dentry(" << dirino << ", " << dn << ") -> " << r << endl;
  return r;
}


/* map a directory inode to an osd
 */
int MDCluster::get_meta_osd(inodeno_t ino)
{
  return osd_meta_begin + (ino % (osd_meta_end - osd_meta_begin));
}

object_t MDCluster::get_meta_oid(inodeno_t ino)
{
  return ino;
}


/* map a hashed diretory inode and mds to an osd
 */

int MDCluster::get_hashdir_meta_osd(inodeno_t ino, int mds)
{
  return osd_meta_begin + ((ino+mds) % (osd_meta_end - osd_meta_begin));
}

object_t MDCluster::get_hashdir_meta_oid(inodeno_t ino, int mds)
{
  return ino + (HASHDIR_OID_MULT*mds);
}



/* map an mds log to an osd
 */
int MDCluster::get_log_osd(int mds)
{
  return osd_log_begin + (mds % (osd_log_end - osd_log_begin));
}

object_t MDCluster::get_log_oid(int mds)
{
  return ((object_t)1000*(object_t)getpid()) + (object_t)mds;
}
