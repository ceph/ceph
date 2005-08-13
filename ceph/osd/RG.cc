
#include "RG.h"
#include "config.h"

#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_osd) cout << "osd" << whoami << ".rg" << hex << rgid << dec << " "


void RG::mark_peered()
{
  dout(10) << "mark_peered" << endl;
  state_set(RG_STATE_PEERED);
}

void RG::pulled(object_t oid, version_t v, RGPeer *p)
{
  dout(10) << "pulled o " << hex << oid << dec << " v " << v << " from " << p->get_peer() << endl;

  local_objects[oid] = v;

  // update peer state
  p->pulled(oid);
  
  objects_loc.erase(oid);  // object is now local
  if (objects_loc.empty()) {
	assert(!is_complete());
	mark_complete();
	
	if (!is_clean()) {
	  plan_push();
	  plan_cleanup();
	}
  }
}

void RG::mark_complete()
{
  dout(10) << "mark_complete" << endl;
  assert(!is_complete());

  // done pulling objects!
  state_set(RG_STATE_COMPLETE);
  pull_plan.clear();
  
  // hose any !complete state
  objects.clear();
  objects_loc.clear();
  deleted_objects.clear();
}

void RG::pushed(object_t oid, version_t v, RGPeer *p)
{
  dout(10) << "pushed o " << hex << oid << dec << " v " << v << " from " << p->get_peer() << endl;

  // update peer state
  p->pushed(oid);
  
  // clean now?
}

void RG::removed(object_t oid, version_t v, RGPeer *p)
{
  dout(10) << "removed o " << hex << oid << dec << " v " << v << " from " << p->get_peer() << endl;

  // update peer state
  p->removed(oid);
  
  // clean now?
}




void RG::analyze_peers(ObjectStore *store) 
{
  dout(10) << "analyze_peers" << endl;

  // compare
  map<object_t, int>       nreps;    // not quite accurate.  for pull.
  
  objects = local_objects;  // start w/ local object set.
  
  // newest objects -> objects
  for (map<int, RGPeer*>::iterator pit = peers.begin();
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
  for (map<int, RGPeer*>::iterator pit = peers.begin();
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
	  dout(10) << " rg " << hex << rgid << dec << " o " << *oit << " will proxy+pull" << endl;		  
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
}


void RG::plan_push()
{
  dout(10) << "plan_push" << endl;
  assert(is_complete());
  assert(is_peered());

  // push
  push_plan.clear();
  for (map<object_t, version_t>::iterator oit = local_objects.begin();
	   oit != local_objects.end();
	   oit++) {
	for (map<int, RGPeer*>::iterator pit = peers.begin();
		 pit != peers.end();
		 pit++) {
	  RGPeer *rgp = pit->second;
	  if (rgp->get_role() < 0) continue;

	  if (rgp->peer_state.objects.count(oit->first) == 0 || 
		  oit->second < rgp->peer_state.objects[oit->first]) {
		dout(10) << " remote o " << hex << oit->first << dec << " v " << oit->second << " on osd" << rgp->get_peer() << " old|dne, pushing" << endl;
		push_plan.push_back(oit->first, oit->second, pit->first);
	  }
	}
  }
}

void RG::plan_cleanup()
{
  dout(10) << "plan_cleanup" << endl;
  assert(is_complete());
  assert(is_peered());

  // cleanup
  clean_plan.clear();
  for (map<int, RGPeer*>::iterator pit = peers.begin();
	   pit != peers.end();
	   pit++) {
	RGPeer *rgp = pit->second;
	for (map<object_t, version_t>::iterator oit = pit->second->peer_state.objects.begin();
		 oit != pit->second->peer_state.objects.end();
		 oit++) {
	  if (rgp->get_role() < 0) {
		dout(10) << " remote o " << hex << oit->first << dec << " v " << oit->second << " on osd" << rgp->get_peer() << " stray, removing" << endl;
	  } 
	  else if (local_objects.count(oit->first) == 0) {
		dout(10) << " remote o " << hex << oit->first << dec << " v " << oit->second << " on osd" << rgp->get_peer() << " deleted, removing" << endl;
	  } 
	  else continue;
	  clean_plan.push_back(oit->first, oit->second, pit->first);
	}
  }

}
