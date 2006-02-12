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



#include "MDBalancer.h"
#include "MDS.h"
#include "MDCluster.h"
#include "CInode.h"
#include "CDir.h"
#include "MDCache.h"

#include "include/Context.h"
#include "msg/Messenger.h"
#include "messages/MHeartbeat.h"

#include <vector>
#include <map>
using namespace std;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_mds_balancer) cout << "mds" << mds->get_nodeid() << ".bal "

#define MIN_LOAD    50   //  ??
#define MIN_REEXPORT 5  // will automatically reexport
#define MIN_OFFLOAD 10   // point at which i stop trying, close enough

ostream& operator<<( ostream& out, mds_load_t& load )
{
  return out << "load<" << load.root_pop << "," << load.req_rate << "," << load.rd_rate << "," << load.wr_rate << ">";
}

mds_load_t& operator+=( mds_load_t& l, mds_load_t& r ) 
{
  l.root_pop += r.root_pop;
  l.req_rate += r.req_rate;
  return l;
}

mds_load_t operator/( mds_load_t& a, double d ) 
{
  mds_load_t r;
  r.root_pop = a.root_pop / d;
  r.req_rate = a.req_rate / d;
  return r;
}


int MDBalancer::proc_message(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_HEARTBEAT:
	handle_heartbeat((MHeartbeat*)m);
	break;
	
  default:
	dout(1) << " balancer unknown message " << m->get_type() << endl;
	assert(0);
	break;
  }

  return 0;
}


class C_Bal_SendHeartbeat : public Context {
public:
  MDS *mds;
  C_Bal_SendHeartbeat(MDS *mds) {
	this->mds = mds;
  }
  virtual void finish(int f) {
	mds->balancer->send_heartbeat();
  }
};

void MDBalancer::send_heartbeat()
{
  if (!mds->mdcache->get_root()) {
	dout(5) << "no root on send_heartbeat" << endl;
	mds->mdcache->open_root(new C_Bal_SendHeartbeat(mds));
	return;
  }

  mds_load.clear();
  if (mds->get_nodeid() == 0)
	beat_epoch++;

  // load
  mds_load_t load = mds->get_load();
  mds_load[ mds->get_nodeid() ] = load;

  // import_map
  map<int, float> import_map;

  for (set<CDir*>::iterator it = mds->mdcache->imports.begin();
	   it != mds->mdcache->imports.end();
	   it++) {
	CDir *im = *it;
	if (im->inode->is_root()) continue;
	int from = im->inode->authority();
	import_map[from] += im->popularity[MDS_POP_CURDOM].get();
  }
  mds_import_map[ mds->get_nodeid() ] = import_map;

  
  dout(5) << "mds" << mds->get_nodeid() << " sending heartbeat " << beat_epoch << " " << load << endl;
  for (map<int, float>::iterator it = import_map.begin();
	   it != import_map.end();
	   it++) {
	dout(5) << "  import_map from " << it->first << " -> " << it->second << endl;
  }

  
  int size = mds->get_cluster()->get_num_mds();
  for (int i = 0; i<size; i++) {
	if (i == mds->get_nodeid()) continue;
	MHeartbeat *hb = new MHeartbeat(load, beat_epoch);
	hb->get_import_map() = import_map;
	mds->messenger->send_message(hb,
								 MSG_ADDR_MDS(i), MDS_PORT_BALANCER,
								 MDS_PORT_BALANCER);
  }
}

void MDBalancer::handle_heartbeat(MHeartbeat *m)
{
  dout(5) << "=== got heartbeat " << m->get_beat() << " from " << m->get_source() << " " << m->get_load() << endl;
  
  if (!mds->mdcache->get_root()) {
	dout(10) << "no root on handle" << endl;
	mds->mdcache->open_root(new C_MDS_RetryMessage(mds, m));
	return;
  }
  
  if (m->get_source() == 0) {
	dout(10) << " from mds0, new epoch" << endl;
	beat_epoch = m->get_beat();
	send_heartbeat();

	show_imports();
  }
  
  mds_load[ m->get_source() ] = m->get_load();
  mds_import_map[ m->get_source() ] = m->get_import_map();

  //cout << "  load is " << load << " have " << mds_load.size() << endl;
  
  unsigned cluster_size = mds->get_cluster()->get_num_mds();
  if (mds_load.size() == cluster_size) {
	// let's go!
	export_empties();
	do_rebalance(m->get_beat());
  }
  
  // done
  delete m;
}


void MDBalancer::export_empties() 
{
  dout(5) << "export_empties checking for empty imports" << endl;

  for (set<CDir*>::iterator it = mds->mdcache->imports.begin();
	   it != mds->mdcache->imports.end();
	   it++) {
	CDir *dir = *it;
	
	if (!dir->inode->is_root() && dir->get_size() == 0) 
	  mds->mdcache->export_empty_import(dir);
  }
}



double MDBalancer::try_match(int ex, double& maxex, 
							int im, double& maxim)
{
  if (maxex <= 0 || maxim <= 0) return 0.0;
  
  double howmuch = -1;
  if (maxim < maxex)                // import takes it all
	howmuch = maxim;
  else if (maxim >= maxex)          // export all
	howmuch = maxim;
  if (howmuch <= 0) return 0.0;
  
  dout(5) << "   - mds" << ex << " exports " << howmuch << " to mds" << im << endl;
  
  if (ex == mds->get_nodeid())
	my_targets[im] += howmuch;
  
  exported[ex] += howmuch;
  imported[im] += howmuch;

  maxex -= howmuch;
  maxim -= howmuch;

  return howmuch;
}



void MDBalancer::do_rebalance(int beat)
{
  int cluster_size = mds->get_cluster()->get_num_mds();
  int whoami = mds->get_nodeid();

  // reset
  my_targets.clear();
  imported.clear();
  exported.clear();

  dout(5) << " do_rebalance: cluster loads are" << endl;

  mds_load_t total_load;
  multimap<double,int> load_map;
  for (int i=0; i<cluster_size; i++) {
	dout(5) << "  mds" << i << " load " << mds_load[i] << endl;
	total_load += mds_load[i];

	load_map.insert(pair<double,int>( mds_load[i].root_pop, i ));
  }

  dout(5) << "  total load " << total_load << endl;

  // target load
  target_load = total_load / (double)cluster_size;
  dout(5) << "  target load " << target_load << endl;

  // under or over?
  if (mds_load[whoami].root_pop < target_load.root_pop) {
	dout(5) << "  i am underloaded, doing nothing." << endl;
	show_imports();
	return;
  }  

  dout(5) << "  i am overloaded" << endl;


  // first separate exporters and importers
  multimap<double,int> importers;
  multimap<double,int> exporters;
  set<int>             importer_set;
  set<int>             exporter_set;
  
  for (multimap<double,int>::iterator it = load_map.begin();
	   it != load_map.end();
	   it++) {
	if (it->first < target_load.root_pop) {
	  //dout(5) << "   mds" << it->second << " is importer" << endl;
	  importers.insert(pair<double,int>(it->first,it->second));
	  importer_set.insert(it->second);
	} else {
	  //dout(5) << "   mds" << it->second << " is exporter" << endl;
	  exporters.insert(pair<double,int>(it->first,it->second));
	  exporter_set.insert(it->second);
	}
  }


  // determine load transfer mapping

  if (true) {
	// analyze import_map; do any matches i can

	dout(5) << "  matching exporters to import sources" << endl;

	// big -> small exporters
	for (multimap<double,int>::reverse_iterator ex = exporters.rbegin();
		 ex != exporters.rend();
		 ex++) {
	  double maxex = get_maxex(ex->second);
	  if (maxex <= .001) continue;
	  
	  // check importers. for now, just in arbitrary order (no intelligent matching).
	  for (map<int, float>::iterator im = mds_import_map[ex->second].begin();
		   im != mds_import_map[ex->second].end();
		   im++) {
		double maxim = get_maxim(im->first);
		if (maxim <= .001) continue;
		try_match(ex->second, maxex,
				  im->first, maxim);
		if (maxex <= .001) break;;
	  }
	}
  }


  if (1) {
	if (beat % 2 == 1) {
	  // old way
	  dout(5) << "  matching big exporters to big importers" << endl;
	  // big exporters to big importers
	  multimap<double,int>::reverse_iterator ex = exporters.rbegin();
	  multimap<double,int>::iterator im = importers.begin();
	  while (ex != exporters.rend() &&
			 im != importers.end()) {
		double maxex = get_maxex(ex->second);
		double maxim = get_maxim(im->second);
		if (maxex < .001 || maxim < .001) break;
		try_match(ex->second, maxex,
				  im->second, maxim);
		if (maxex <= .001) ex++;
		if (maxim <= .001) im++;
	  }
	} else {
	  // new way
	  dout(5) << "  matching small exporters to big importers" << endl;
	  // small exporters to big importers
	  multimap<double,int>::iterator ex = exporters.begin();
	  multimap<double,int>::iterator im = importers.begin();
	  while (ex != exporters.end() &&
			 im != importers.end()) {
		double maxex = get_maxex(ex->second);
		double maxim = get_maxim(im->second);
		if (maxex < .001 || maxim < .001) break;
		try_match(ex->second, maxex,
				  im->second, maxim);
		if (maxex <= .001) ex++;
		if (maxim <= .001) im++;
	  }
	}
  }


  // make a sorted list of my imports
  map<double,CDir*>    import_pop_map;
  multimap<int,CDir*>  import_from_map;
  for (set<CDir*>::iterator it = mds->mdcache->imports.begin();
	   it != mds->mdcache->imports.end();
	   it++) {
	double pop = (*it)->popularity[MDS_POP_CURDOM].get();
	if (pop < g_conf.mds_bal_idle_threshold &&
		(*it)->inode != mds->mdcache->get_root()) {
	  dout(5) << " exporting idle import " << **it << endl;
	  mds->mdcache->export_dir(*it, (*it)->inode->authority());
	  continue;
	}
	import_pop_map[ pop ] = *it;
	int from = (*it)->inode->authority();
	dout(15) << "  map: i imported " << **it << " from " << from << endl;
	import_from_map.insert(pair<int,CDir*>(from, *it));
  }
  
  // do my exports!
  set<CDir*> already_exporting;
  for (map<int,double>::iterator it = my_targets.begin();
	   it != my_targets.end();
	   it++) {
	int target = (*it).first;
	double amount = (*it).second;

	if (amount < MIN_OFFLOAD) continue;

	dout(5) << " sending " << amount << " to " << target << endl;
	
	show_imports();

	// search imports from target
	if (import_from_map.count(target)) {
	  dout(5) << " aha, looking through imports from target mds" << target << endl;
	  pair<multimap<int,CDir*>::iterator, multimap<int,CDir*>::iterator> p =
		p = import_from_map.equal_range(target);
	  while (p.first != p.second) {
		CDir *dir = (*p.first).second;
		dout(5) << "considering " << *dir << " from " << (*p.first).first << endl;
		multimap<int,CDir*>::iterator plast = p.first++;
		
		if (dir->inode->is_root()) continue;
		if (dir->is_freezing() || dir->is_frozen()) continue;  // export pbly already in progress
		double pop = dir->popularity[MDS_POP_CURDOM].get();
		assert(dir->inode->authority() == target);  // cuz that's how i put it in the map, dummy
		
		if (pop <= amount) {
		  dout(5) << "reexporting " << *dir << " pop " << pop << " back to " << target << endl;
		  mds->mdcache->export_dir(dir, target);
		  amount -= pop;
		  import_from_map.erase(plast);
		  import_pop_map.erase(pop);
		} else {
		  dout(5) << "can't reexport " << *dir << ", too big " << pop << endl;
		}
		if (amount < MIN_OFFLOAD) break;
	  }
	}
	if (amount < MIN_OFFLOAD) break;
	
	// any other imports
	for (map<double,CDir*>::iterator import = import_pop_map.begin();
		 import != import_pop_map.end();
		 import++) {
	  CDir *imp = (*import).second;
	  if (imp->inode->is_root()) continue;
	  
	  double pop = (*import).first;
	  if (pop < amount ||
		  pop < MIN_REEXPORT) {
		dout(5) << "reexporting " << *imp << " pop " << pop << endl;
		amount -= pop;
		mds->mdcache->export_dir(imp, imp->inode->authority());
	  }
	  if (amount < MIN_OFFLOAD) break;
	}
	if (amount < MIN_OFFLOAD) break;

	// okay, search for fragments of my workload
	set<CDir*> candidates = mds->mdcache->imports;

	list<CDir*> exports;
	double have = 0;
	
	for (set<CDir*>::iterator pot = candidates.begin();
		 pot != candidates.end();
		 pot++) {
	  find_exports(*pot, amount, exports, have, already_exporting);
	  if (have > amount-MIN_OFFLOAD) break;
	}
	
	for (list<CDir*>::iterator it = exports.begin(); it != exports.end(); it++) {
	  dout(5) << " exporting fragment " << **it << " pop " << (*it)->popularity[MDS_POP_CURDOM].get() << endl;
	  mds->mdcache->export_dir(*it, target);
	}
  }

  dout(5) << "rebalance done" << endl;
  show_imports();
  
}



void MDBalancer::find_exports(CDir *dir, 
							  double amount, 
							  list<CDir*>& exports, 
							  double& have,
							  set<CDir*>& already_exporting)
{
  double need = amount - have;
  if (need < amount / 5)
	return;   // good enough!
  double needmax = need * 1.2;
  double needmin = need * .8;
  double midchunk = need * .3;
  double minchunk = need * .001;

  list<CDir*> bigger;
  multimap<double, CDir*> smaller;

  dout(7) << " find_exports in " << *dir << " need " << need << " (" << needmin << " - " << needmax << ")" << endl;

  for (CDir_map_t::iterator it = dir->begin();
	   it != dir->end();
	   it++) {
	CInode *in = it->second->get_inode();
	if (!in) continue;
	if (!in->is_dir()) continue;
	if (!in->dir) continue;  // clearly not popular
	
	if (in->dir->is_export()) continue;
	if (in->dir->is_hashed()) continue;
	if (already_exporting.count(in->dir)) continue;

	if (in->dir->is_frozen()) continue;  // can't export this right now!
	if (in->dir->get_size() == 0) continue;  // don't export empty dirs, even if they're not complete.  for now!
	
	// how popular?
	double pop = in->dir->popularity[MDS_POP_CURDOM].get();
	//cout << "   in " << in->inode.ino << " " << pop << endl;

	if (pop < minchunk) continue;

	// lucky find?
	if (pop > needmin && pop < needmax) {
	  exports.push_back(in->dir);
	  have += pop;
	  return;
	}
	
	if (pop > need)
	  bigger.push_back(in->dir);
	else
	  smaller.insert(pair<double,CDir*>(pop, in->dir));
  }

  // grab some sufficiently big small items
  multimap<double,CDir*>::reverse_iterator it;
  for (it = smaller.rbegin();
	   it != smaller.rend();
	   it++) {

	if ((*it).first < midchunk)
	  break;  // try later
	
	dout(7) << " taking smaller " << *(*it).second << endl;
	
	exports.push_back((*it).second);
	already_exporting.insert((*it).second);
	have += (*it).first;
	if (have > needmin)
	  return;
  }
  
  // apprently not enough; drill deeper into the hierarchy
  for (list<CDir*>::iterator it = bigger.begin();
	   it != bigger.end();
	   it++) {
	dout(7) << " descending into " << **it << endl;
	find_exports(*it, amount, exports, have, already_exporting);
	if (have > needmin)
	  return;
  }

  // ok fine, use smaller bits
  for (;
	   it != smaller.rend();
	   it++) {

	dout(7) << " taking (much) smaller " << *(*it).second << endl;

	exports.push_back((*it).second);
	already_exporting.insert((*it).second);
	have += (*it).first;
	if (have > needmin)
	  return;
  }


}




void MDBalancer::hit_inode(CInode *in)
{
  // hit me
  in->popularity[MDS_POP_JUSTME].hit();
  in->popularity[MDS_POP_NESTED].hit();
  if (in->is_auth()) {
	in->popularity[MDS_POP_CURDOM].hit();
	in->popularity[MDS_POP_ANYDOM].hit();
  }
  
  // hit auth up to import
  CDir *dir = in->get_parent_dir();
  if (dir) hit_recursive(dir);
}


void MDBalancer::hit_dir(CDir *dir) 
{
  // hit me
  dir->popularity[MDS_POP_JUSTME].hit();
  
  hit_recursive(dir);

}



void MDBalancer::hit_recursive(CDir *dir)
{
  bool anydom = dir->is_auth();
  bool curdom = dir->is_auth();


  // replicate?
  float dir_pop = dir->popularity[MDS_POP_CURDOM].get();    // hmm??

  if (dir->is_auth()) {
	if (!dir->is_rep() &&
		dir_pop >= g_conf.mds_bal_replicate_threshold) {
	  // replicate
	  dout(1) << "replicating dir " << *dir << " pop " << dir_pop << endl;
		  
	  dir->dir_rep = CDIR_REP_ALL;
	  mds->mdcache->send_dir_updates(dir, true);
	}
		
	if (dir->is_rep() &&
		dir_pop < g_conf.mds_bal_unreplicate_threshold) {
	  // unreplicate
	  dout(1) << "unreplicating dir " << *dir << " pop " << dir_pop << endl;
	  
	  dir->dir_rep = CDIR_REP_NONE;
	  mds->mdcache->send_dir_updates(dir);
	}
  }


  while (dir) {
	CInode *in = dir->inode;

	dir->popularity[MDS_POP_NESTED].hit();
	in->popularity[MDS_POP_NESTED].hit();
	
	if (anydom) {
	  dir->popularity[MDS_POP_ANYDOM].hit();
	  in->popularity[MDS_POP_ANYDOM].hit();
	}
	
	if (curdom) {
	  dir->popularity[MDS_POP_CURDOM].hit();
	  in->popularity[MDS_POP_CURDOM].hit();
	}
	
	if (dir->is_import()) 
	  curdom = false;   // end of auth domain, stop hitting auth counters.
	dir = dir->inode->get_parent_dir();
  }
}


/*
 * subtract off an exported chunk
 */
void MDBalancer::subtract_export(CDir *dir)
{
  double curdom = -dir->popularity[MDS_POP_CURDOM].get();

  bool in_domain = !dir->is_import();
  
  while (true) {
	CInode *in = dir->inode;
	
	in->popularity[MDS_POP_ANYDOM].adjust(curdom);
	if (in_domain) in->popularity[MDS_POP_CURDOM].adjust(curdom);
	
	dir = in->get_parent_dir();
	if (!dir) break;
	
	if (dir->is_import()) in_domain = false;
	
	dir->popularity[MDS_POP_ANYDOM].adjust(curdom);
	if (in_domain) dir->popularity[MDS_POP_CURDOM].adjust(curdom);
  }
}
	

void MDBalancer::add_import(CDir *dir)
{
  double curdom = dir->popularity[MDS_POP_CURDOM].get();

  bool in_domain = !dir->is_import();
  
  while (true) {
	CInode *in = dir->inode;
	
	in->popularity[MDS_POP_ANYDOM].adjust(curdom);
	if (in_domain) in->popularity[MDS_POP_CURDOM].adjust(curdom);
	
	dir = in->get_parent_dir();
	if (!dir) break;
	
	if (dir->is_import()) in_domain = false;
	
	dir->popularity[MDS_POP_ANYDOM].adjust(curdom);
	if (in_domain) dir->popularity[MDS_POP_CURDOM].adjust(curdom);
  }
 
}






void MDBalancer::show_imports(bool external)
{
  int db = 7; //debug level

  
  if (mds->mdcache->imports.empty() &&
	  mds->mdcache->hashdirs.empty()) {
	dout(db) << "no imports/exports/hashdirs" << endl;
	return;
  }
  dout(db) << "imports/exports/hashdirs:" << endl;

  set<CDir*> ecopy = mds->mdcache->exports;

  set<CDir*>::iterator it = mds->mdcache->hashdirs.begin();
  while (1) {
	if (it == mds->mdcache->hashdirs.end()) it = mds->mdcache->imports.begin();
	if (it == mds->mdcache->imports.end() ) break;
	
	CDir *im = *it;
	
	if (im->is_import()) {
	  dout(db) << "  + import (" << im->popularity[MDS_POP_CURDOM].get() << "/" << im->popularity[MDS_POP_ANYDOM].get() << ")  " << *im << endl;
	  assert( im->is_auth() );
	} 
	else if (im->is_hashed()) {
	  if (im->is_import()) continue;  // if import AND hash, list as import.
	  dout(db) << "  + hash (" << im->popularity[MDS_POP_CURDOM].get() << "/" << im->popularity[MDS_POP_ANYDOM].get() << ")  " << *im << endl;
	}
	
	for (set<CDir*>::iterator p = mds->mdcache->nested_exports[im].begin();
		 p != mds->mdcache->nested_exports[im].end();
		 p++) {
	  CDir *exp = *p;
	  if (exp->is_hashed()) {
		assert(0);  // we don't do it this way actually
		dout(db) << "      - hash (" << exp->popularity[MDS_POP_NESTED].get() << ", " << exp->popularity[MDS_POP_ANYDOM].get() << ")  " << *exp << " to " << exp->dir_auth << endl;
		assert( exp->is_auth() );
	  } else {
		dout(db) << "      - ex (" << exp->popularity[MDS_POP_NESTED].get() << ", " << exp->popularity[MDS_POP_ANYDOM].get() << ")  " << *exp << " to " << exp->dir_auth << endl;
		assert( exp->is_export() );
		assert( !exp->is_auth() );
	  }

	  if ( mds->mdcache->get_auth_container(exp) != im ) {
		dout(1) << "uh oh, auth container is " << mds->mdcache->get_auth_container(exp) << endl;
		dout(1) << "uh oh, auth container is " << *mds->mdcache->get_auth_container(exp) << endl;
		assert( mds->mdcache->get_auth_container(exp) == im );
	  }
	  
	  if (ecopy.count(exp) != 1) {
		dout(1) << "***** nested_export " << *exp << " not in exports" << endl;
		assert(0);
	  }
	  ecopy.erase(exp);
	}

	it++;
  }
  
  if (ecopy.size()) {
	for (set<CDir*>::iterator it = ecopy.begin();
		 it != ecopy.end();
		 it++) 
	  dout(1) << "***** stray item in exports: " << **it << endl;
	assert(ecopy.size() == 0);
  }
}



/*  replicate?

	  float dir_pop = dir->get_popularity();
	  
	  if (dir->is_auth()) {
		if (!dir->is_rep() &&
			dir_pop >= g_conf.mds_bal_replicate_threshold) {
		  // replicate
		  dout(5) << "replicating dir " << *in << " pop " << dir_pop << endl;
		  
		  dir->dir_rep = CDIR_REP_ALL;
		  mds->mdcache->send_dir_updates(dir);
		}
		
		if (dir->is_rep() &&
			dir_pop < g_conf.mds_bal_unreplicate_threshold) {
		  // unreplicate
		  dout(5) << "unreplicating dir " << *in << " pop " << dir_pop << endl;
		  
		  dir->dir_rep = CDIR_REP_NONE;
		  mds->mdcache->send_dir_updates(dir);
		}
	  }

*/
