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


#include <assert.h>

#include "Filer.h"
#include "OSDMap.h"

//#include "messages/MOSDRead.h"
//#include "messages/MOSDReadReply.h"
//#include "messages/MOSDWrite.h"
//#include "messages/MOSDWriteReply.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDMap.h"

#include "msg/Messenger.h"

#include "include/Context.h"

#include "config.h"
#undef dout
#define dout(x)  if (x <= g_conf.debug || x <= g_conf.debug_filer) cout << MSG_ADDR_NICE(messenger->get_myaddr()) << ".filer "


void Filer::file_to_extents(inode_t inode,
							size_t len,
							off_t offset,
							list<ObjectExtent>& extents) 
{
  /* we want only one extent per object!
   * this means that each extent we read may map into different bits of the 
   * final read buffer.. hence OSDExtent.buffer_extents
   */
  map< object_t, ObjectExtent > object_extents;
  
  assert(inode.layout.object_size >= inode.layout.stripe_size);
  off_t stripes_per_object = inode.layout.object_size / inode.layout.stripe_size;
  
  off_t cur = offset;
  off_t left = len;
  while (left > 0) {
	// layout into objects
	off_t blockno = cur / inode.layout.stripe_size;
	off_t stripeno = blockno / inode.layout.stripe_count;
	off_t stripepos = blockno % inode.layout.stripe_count;
	off_t objectsetno = stripeno / stripes_per_object;
	off_t objectno = objectsetno * inode.layout.stripe_count + stripepos;
	
	// find oid, extent
	ObjectExtent *ex = 0;
	object_t oid = file_to_object( inode.ino, objectno );
	if (object_extents.count(oid)) 
	  ex = &object_extents[oid];
	else {
	  ex = &object_extents[oid];
	  ex->oid = oid;
	  ex->pgid = objecter->osdmap->object_to_pg( oid, inode.layout );
	  //ex->osd = objecter->osdmap->get_pg_acting_primary( ex->pg );
	}
	
	// map range into object
	off_t block_start = (stripeno % stripes_per_object)*inode.layout.stripe_size;
	off_t block_off = cur % inode.layout.stripe_size;
	off_t max = inode.layout.stripe_size - block_off;
	
	off_t x_offset = block_start + block_off;
	off_t x_len;
	if (left > max)
	  x_len = max;
	else
	  x_len = left;
	
	if (ex->start + ex->length == x_offset) {
	  // add to extent
	  ex->length += x_len;
	} else {
	  // new extent
	  assert(ex->length == 0);
	  assert(ex->start == 0);
	  ex->start = x_offset;
	  ex->length = x_len;
	}
	ex->buffer_extents[cur-offset] = x_len;
		
	//cout << "map: ino " << ino << " oid " << ex.oid << " osd " << ex.osd << " offset " << ex.offset << " len " << ex.len << " ... left " << left << endl;
	
	left -= x_len;
	cur += x_len;
  }
  
  // make final list
  for (map<object_t, ObjectExtent>::iterator it = object_extents.begin();
	   it != object_extents.end();
	   it++) {
	extents.push_back(it->second);
  }
}
