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

#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << whoami << "  " << *this << " "


void PG::pulled(object_t oid, version_t v, PGPeer *p)
{
  dout(10) << "pulled o " << hex << oid << dec << " v " << v << " from osd" << p->get_peer() << endl;

  objects_pulling.erase(oid);

  // update peer state
  p->pulled(oid);
  
  // object is now local
  objects_missing.erase(oid);  
  objects_missing_v.erase(oid);
}


void PG::pushed(object_t oid, version_t v, PGPeer *p)
{
  dout(10) << "pushed o " << hex << oid << dec << " v " << v << " to osd" << p->get_peer() << endl;

  objects_pushing[oid].erase(p);
  if (objects_pushing[oid].empty())
	objects_pushing.erase(oid);

  // update peer state
  p->pushed(oid);
  
  objects_unrep[oid].erase(p->get_peer());
  if (objects_unrep[oid].empty())
	objects_unrep.erase(oid);

  // pg clean now?
  if (objects_unrep.empty() &&
	  objects_stray.empty()) {
	assert(!is_clean());
	mark_clean();
  } else {
	dout(10) << " still " << objects_unrep.size() << " unrep and " << objects_stray.size() << " stray" << endl;
  }
}

void PG::removed(object_t oid, version_t v, PGPeer *p)
{
  dout(10) << "removed o " << hex << oid << dec << " v " << v << " from osd" << p->get_peer() << endl;

  objects_removing[oid].erase(p);
  if (objects_removing[oid].empty())
	objects_removing.erase(oid);

  // update peer state
  p->removed(oid);
  
  objects_stray[oid].erase(p->get_peer());
  if (objects_stray[oid].empty())
	objects_stray.erase(oid);

  // clean now?
  if (objects_unrep.empty() &&
	  objects_stray.empty()) {
	assert(!is_clean());
	mark_clean();
  } else {
	dout(10) << " still " << objects_unrep.size() << " unrep and " << objects_stray.size() << " stray" << endl;
  }
}

/*
bool PG::existant_object_is_clean(object_t o, version_t v)
{
  assert(is_peered() && !is_clean());
  return objects_unrep.count(o) ? false:true;
}

bool PG::nonexistant_object_is_clean(object_t o)
{
  assert(is_peered() && !is_clean());

  // FIXME?

  // removed from peers?
  for (map<int, PGPeer*>::iterator it = peers.begin();
	   it != peers.end();
	   it++) {
	if (it->second->get_role() != 1) continue;  // only care about active set
	if (it->second->is_complete()) continue;
	if (it->second->is_stray(o)) 
	  return false;
  }
  
  return true;
}
*/



void PG::plan_recovery(ObjectStore *store, version_t current_version, 
					   list<PGPeer*>& complete_peers) 
{
  dout(10) << "plan_recovery " << current_version << endl;
  assert(is_peered());

  // choose newest last_complete epoch
  version_t last = last_complete;
  for (map<int, PGPeer*>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) {
	dout(10) << "  osd" << pit->first << " " 
			 << pit->second->objects.size() << " objects, last_complete " << pit->second->last_complete << endl;
	if (pit->second->last_complete > last)
	  last = pit->second->last_complete;
  }
  dout(10) << " combined last_complete epoch is " << last << endl;

  if (last+1 < current_version) {
	dout(1) << "WARNING: last_complete skipped one or more epochs, we're possibly missing something" << endl;
  }
  if (!last) {  // bootstrap!
	dout(1) << "WARNING: no complete peers available (yet), pg is crashed" << endl;
	return;
  }

  // build the object map
  // ... OR of complete OSDs' content
  map<object_t, version_t> master;          // what the current object set is

  map<object_t, version_t> local_objects;
  scan_local_objects(local_objects, store);
  dout(10) << " " << local_objects.size() << " local objects" << endl;

  if (last_complete == last) 
	master = local_objects;
  
  for (map<int, PGPeer*>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) {
	for (map<object_t, version_t>::iterator oit = pit->second->objects.begin();
		 oit != pit->second->objects.end();
		 oit++) {
	  // know this object?
	  if (master.count(oit->first)) {
		if (oit->second > master[oit->first])   // newer
		  master[oit->first] = oit->second;
	  } else {
		// newly seen object!
		master[oit->first] = oit->second;
	  }
	}
  }

  // ok, we have a master list.
  dout(7) << " master list has " << master.size() << " objects" << endl;

  // local cleanup?
  if (!is_complete(current_version)) {
	// just cleanup old local objects
	// FIXME: do this async?

	for (map<object_t, version_t>::iterator it = local_objects.begin();
		 it != local_objects.end();
		 it++) {
	  if (master.count(it->first) && 
		  master[it->first] == it->second) continue;  // same!
	  
	  dout(10) << " local o " << hex << it->first << dec << " v " << it->second << " old, removing" << endl;
	  store->remove(it->first);
	  local_objects.erase(it->first);
	}
  }


  // plan pull -> objects_missing
  // plan push -> objects_unrep
  for (map<object_t, version_t>::iterator it = master.begin();
	   it != master.end();
	   it++) {
	const object_t o = it->first;
	const version_t v = it->second;

	// already have it locally?
	bool local = false;
	if (local_objects.count(o) &&
		local_objects[o] == v) local = true;   // we have it.

	for (map<int, PGPeer*>::iterator pit = peers.begin();
		 pit != peers.end();
		 pit++) {
	  // pull?
	  if (!local &&
		  pit->second->objects.count(o) &&
		  pit->second->objects[o] == v) {
		objects_missing[o].insert(pit->first);
		objects_missing_v[o] = v;
	  }
	  
	  // push? 
	  if (pit->second->get_role() == 1 &&
		  (pit->second->objects.count(o) == 0 ||
		   pit->second->objects[o] < v)) {
		objects_unrep[o].insert(pit->first);
		pit->second->missing.insert(o);
	  }
	}

	assert(local || !objects_missing[o].empty());  // pull
  }

  if (objects_missing.empty()) {
	mark_complete(current_version);
  }

  // plan clean -> objects_stray
  for (map<int, PGPeer*>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) {
	const int role = pit->second->get_role();
	assert(role != 0);  // duh
	
	PGPeer *p = pit->second;
	assert(p->is_active());

	if (p->missing.empty() && p->stray.empty()) {
	  p->state_set(PG_PEER_STATE_COMPLETE);
	  complete_peers.push_back(p);
	}
	
	if (p->is_complete()) {
	  dout(12) << " peer osd" << pit->first << " is complete" << endl;
	} else {
	  dout(12) << " peer osd" << pit->first << " is !complete" << endl;
	}
	
	for (map<object_t, version_t>::iterator oit = pit->second->objects.begin();
		 oit != pit->second->objects.end();
		 oit++) {
	  const object_t o = oit->first;
	  const version_t v = oit->second;

	  if (role < 0) {
		dout(10) << " remote o " << hex << o << dec << " v " << v << " on osd" << p->get_peer() << " stray, removing" << endl;
	  } 
	  else if (master.count(oit->first) == 0) {
		dout(10) << " remote o " << hex << o << dec << " v " << v << " on osd" << p->get_peer() << " deleted/stray, removing" << endl;
	  } 
	  else 
		continue;
	  
	  objects_stray[o][pit->first] = v;
	  p->stray.insert(o);
	}
  }

  if (objects_unrep.empty() && objects_stray.empty())
	mark_clean();

  // clear peer content lists
  for (map<int, PGPeer*>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) 
	pit->second->objects.clear();
}
