
#include "PG.h"
#include "config.h"

#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << whoami << "  " << *this << " "


void PG::mark_peered()
{
  dout(10) << "mark_peered" << endl;
  state_set(PG_STATE_PEERED);
}

void PG::pulled(object_t oid, version_t v, PGPeer *p)
{
  dout(10) << "pulled o " << hex << oid << dec << " v " << v << " from osd" << p->get_peer() << endl;

  local_objects[oid] = v;

  // update peer state
  p->pulled(oid);
  
  objects_loc.erase(oid);  // object is now local

  if (objects_loc.empty()) {
	assert(!is_complete());
	mark_complete();
  }
}

void PG::mark_complete()
{
  dout(10) << "mark_complete" << endl;
  //assert(!is_complete());

  // done pulling objects!
  state_set(PG_STATE_COMPLETE);
  pull_plan.clear();

  // hose any !complete state
  objects.clear();
  objects_loc.clear();
  deleted_objects.clear();

  // plan clean?
  if (!is_clean()) {
	plan_push_cleanup();
  }
}

void PG::mark_clean()
{
  dout(10) << "mark_clean" << endl;
  state_set(PG_STATE_CLEAN);

  // drop residual peers

  // discard peer state
  
}

void PG::pushed(object_t oid, version_t v, PGPeer *p)
{
  dout(10) << "pushed o " << hex << oid << dec << " v " << v << " to osd" << p->get_peer() << endl;

  // update peer state
  p->pushed(oid);
  
  objects_unrep[oid].erase(p->get_peer());
  if (objects_unrep[oid].empty())
	objects_unrep.erase(oid);

  // clean now?
  if (objects_unrep.empty() &&
	  objects_stray.empty()) {
	assert(!is_clean());
	mark_clean();
  }
}

void PG::removed(object_t oid, version_t v, PGPeer *p)
{
  dout(10) << "removed o " << hex << oid << dec << " v " << v << " from osd" << p->get_peer() << endl;

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
  }
}

bool PG::existant_object_is_clean(object_t o, version_t v)
{
  assert(is_peered() && !is_clean());

  // exists on peers too?
  for (map<int, PGPeer*>::iterator it = peers.begin();
	   it != peers.end();
	   it++) {
	if (!it->second->is_active()) continue;
	if (!it->second->has_latest(o, v)) return false;
  }
  return true;
}

bool PG::nonexistant_object_is_clean(object_t o)
{
  assert(is_peered() && !is_clean());

  // removed from peers?
  for (map<int, PGPeer*>::iterator it = peers.begin();
	   it != peers.end();
	   it++) {
	if (!it->second->is_active()) continue;
	if (it->second->peer_state.objects.count(o)) {
	  return false;
	}
  }
  
  return true;	  
}



void PG::analyze_peers(ObjectStore *store) 
{
  dout(10) << "analyze_peers" << endl;

  // compare
  map<object_t, int>       nreps;    // not quite accurate.  for pull.
  
  if (local_objects.empty()) 
	scan_local_objects(store);

  objects = local_objects;  // start w/ local object set.
  
  // newest objects -> objects
  for (map<int, PGPeer*>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) {
	for (map<object_t, version_t>::iterator oit = pit->second->peer_state.objects.begin();
		 oit != pit->second->peer_state.objects.end();
		 oit++) {
	  // know this object?
	  if (objects.count(oit->first)) {
		object_t v = objects[oit->first];
		if (oit->second < v)       // older?
		  continue;                // useless
		else if (oit->second == v) // same?
		  nreps[oit->first]++;     // not quite accurate bc local_objects isn't included in nrep
		else {                     // newer!
		  objects[oit->first] = oit->second;
		  nreps[oit->first] = 0;
		  objects_loc[oit->first] = pit->first; // note location. this will overwrite and be lame.
		}
	  } else {
		// newly seen object!
		objects[oit->first] = oit->second;
		nreps[oit->first] = 0;
		objects_loc[oit->first] = pit->first; // note location. this will overwrite and be lame.
	  }
	}
  }
  
  // remove deleted objects
  assim_deleted_objects(deleted_objects);             // locally
  for (map<int, PGPeer*>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) 
	assim_deleted_objects(pit->second->peer_state.deleted);  // on peers
  
  // plan pull
  // order objects by replication level
  map<int, list<object_t> > byrep;
  for (map<object_t, int>::iterator oit = objects_loc.begin();
	   oit != objects_loc.end();
	   oit++) 
	byrep[nreps[oit->first]].push_back(oit->first);
  // make plan
  pull_plan.clear();
  for (map<int, list<object_t> >::iterator it = byrep.begin();
	   it != byrep.end();
	   it++) {
	for (list<object_t>::iterator oit = it->second.begin();
		 oit != it->second.end();
		 oit++) {
	  dout(10) << " need o " << hex << *oit << dec  << " v " << objects[*oit] << " will proxy+pull" << endl;		  
	  pull_plan.push_front(*oit, objects[*oit], objects_loc[*oit]);
	}
  }
  
  // just cleanup old local objects
  // FIXME: do this async?
  for (map<object_t, version_t>::iterator it = local_objects.begin();
	   it != local_objects.end();
	   it++) {
	if (objects.count(it->first) && objects[it->first] == it->second) continue;  // same!
	
	dout(10) << " local o " << hex << it->first << dec << " v " << it->second << " old, removing" << endl;
	store->remove(it->first);
	local_objects.erase(it->first);
  }

  if (pull_plan.empty()) {
	dout(10) << "nothing to pull, marking complete" << endl;
	mark_complete();
  }
 

}


void PG::plan_push_cleanup()
{
  dout(10) << "plan_push_cleanup" << endl;
  assert(is_complete());
  assert(is_peered());

  // push
  push_plan.clear();
  for (map<object_t, version_t>::iterator oit = local_objects.begin();
	   oit != local_objects.end();
	   oit++) {

	// active replicas
	for (unsigned r = 1; r<acting.size(); r++) {
	  PGPeer *pgp = peers[acting[r]];
	  assert(pgp);

	  if (pgp->peer_state.objects.count(oit->first) == 0 || 
		  oit->second < pgp->peer_state.objects[oit->first]) {
		dout(10) << " o " << hex << oit->first << dec << " v " << oit->second << " old|dne on osd" << pgp->get_peer() << ", pushing" << endl;
		push_plan.push_back(oit->first, oit->second, pgp->get_peer());
		objects_unrep[oit->first].insert(pgp->get_peer());
	  }
	}
  }

  // cleanup
  clean_plan.clear();

  // active and residual replicas
  for (map<int, PGPeer*>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) {
	int role = -1;
	for (unsigned i=0; i<acting.size(); i++)
	  if (acting[i] == pit->first) role = i;
	if (role == 0) continue;   // skip primary
	
	PGPeer *pgp = pit->second;
	for (map<object_t, version_t>::iterator oit = pit->second->peer_state.objects.begin();
		 oit != pit->second->peer_state.objects.end();
		 oit++) {
	  if (role < 0) {
		dout(10) << " remote o " << hex << oit->first << dec << " v " << oit->second << " on osd" << pgp->get_peer() << " stray, removing" << endl;
	  } 
	  else if (local_objects.count(oit->first) == 0) {
		dout(10) << " remote o " << hex << oit->first << dec << " v " << oit->second << " on osd" << pgp->get_peer() << " deleted, removing" << endl;
	  } 
	  else continue;
	  clean_plan.push_back(oit->first, oit->second, pit->first);
	  objects_stray[oit->first].insert(pit->first);
	}
  }

  if (push_plan.empty() && clean_plan.empty()) {
	dout(10) << "nothing to push|clean, marking clean" << endl;
	mark_clean();
  }
}

