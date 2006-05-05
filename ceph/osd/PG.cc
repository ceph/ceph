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



#include "PG.h"
#include "config.h"
#include "OSD.h"

#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << osd->whoami << " " << *this << " "



void PG::generate_content_summary()
{  
  dout(10) << "generating summary" << endl;

  list<object_t> olist;
  osd->store->collection_list(info.pgid, olist);
  
  content_summary = new PGContentSummary;

  for (list<object_t>::iterator it = olist.begin();
	   it != olist.end();
	   it++) {
	ObjectInfo item(*it);
	osd->store->getattr(item.oid, 
						"version",
						&item.version, sizeof(item.version));
	item.osd = osd->whoami;
	content_summary->ls.push_back(item);
  }
}



void PG::plan_recovery()
{
  dout(10) << "plan_recovery " << endl;  
  
  assert(is_active());
  assert(content_summary);
  
  // load local contents
  list<object_t> olist;
  osd->store->collection_list(info.pgid, olist);
  
  // check versions
  map<object_t, version_t> vmap;
  for (list<object_t>::iterator it = olist.begin();
	   it != olist.end();
	   it++) {
	version_t v;
	osd->store->getattr(*it, 
						"version",
						&v, sizeof(v));
	vmap[*it] = v;
  }
  
  // scan summary
  content_summary->remote = 0;
  content_summary->missing = 0;
  for (list<ObjectInfo>::iterator it = content_summary->ls.begin();
		 it != content_summary->ls.end();
		 it++) {
	if (vmap.count(it->oid) &&
		vmap[it->oid] == it->version) {
	  // have latest.
	  vmap.erase(it->oid);
	  continue;
	}

	// need it
	dout(20) << "need " << hex << it->oid << dec 
			 << " v " << it->version << endl;
	objects_missing[it->oid] = it->version;
	recovery_queue[it->version] = *it;
  }

  // hose stray
  for (map<object_t,version_t>::iterator it = vmap.begin();
	   it != vmap.end();
	   it++) {
	dout(20) << "removing stray " << hex << it->first << dec 
			 << " v " << it->second << endl;
	osd->store->remove(it->first);
  }
}

void PG::do_recovery()
{
  dout(0) << "do_recovery - implement me" << endl;
}
