

#include "MDBalancer.h"
#include "MDS.h"
#include "MDCluster.h"
#include "CInode.h"
#include "CDir.h"
#include "MDCache.h"

#include "messages/MHeartbeat.h"

#include "include/Messenger.h"
#include "include/Context.h"

#include <vector>
#include <map>
using namespace std;

#include "include/config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug) cout << "mds" << mds->get_nodeid() << ".bal "

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
	mds->open_root(new C_Bal_SendHeartbeat(mds));
	return;
  }

  mds_load.clear();
  if (mds->get_nodeid() == 0)
	beat_epoch++;

  mds_load_t load = mds->get_load();
  mds_load[ mds->get_nodeid() ] = load;

  dout(5) << "mds" << mds->get_nodeid() << " sending heartbeat " << beat_epoch << " " << load << endl;
  
  int size = mds->get_cluster()->get_num_mds();
  for (int i = 0; i<size; i++) {
	if (i == mds->get_nodeid()) continue;
	mds->messenger->send_message(new MHeartbeat(load, beat_epoch),
								 i, MDS_PORT_BALANCER,
								 MDS_PORT_BALANCER);
  }
}

void MDBalancer::handle_heartbeat(MHeartbeat *m)
{
  dout(5) << " got heartbeat " << m->get_beat() << " from " << m->get_source() << " " << m->get_load() << endl;
  
  if (!mds->mdcache->get_root()) {
	dout(10) << "no root on handle" << endl;
	mds->open_root(new C_MDS_RetryMessage(mds, m));
	return;
  }
  
  if (m->get_source() == 0) {
	dout(10) << " from mds0, new epoch" << endl;
	beat_epoch = m->get_beat();
	send_heartbeat();

	mds->mdcache->show_imports();
  }
  
  mds_load[ m->get_source() ] = m->get_load();
  //cout << "  load is " << load << " have " << mds_load.size() << endl;
  
  int cluster_size = mds->get_cluster()->get_num_mds();
  if (mds_load.size() == cluster_size) 
	do_rebalance();
  
  // done
  delete m;
}


void MDBalancer::do_rebalance()
{
  int cluster_size = mds->get_cluster()->get_num_mds();
  int whoami = mds->get_nodeid();

  dout(5) << " do_rebalance: cluster loads are" << endl;

  mds_load_t total_load;
  multimap<double,int> load_map;
  for (int i=0; i<cluster_size; i++) {
	dout(5) << "  mds" << i << " load " << mds_load[i] << endl;
	total_load += mds_load[i];

	load_map.insert(pair<double,int>( mds_load[i].root_pop, i ));
  }

  dout(5) << "  total load " << total_load << endl;
  
  double my_load = mds_load[whoami].root_pop;
  mds_load_t target_load = total_load / (double)cluster_size;

  dout(5) << "  target load " << target_load << endl;
  
  if (my_load < target_load.root_pop) {
	dout(5) << "  i am underloaded, doing nothing." << endl;
	mds->mdcache->show_imports();
	return;
  }  

  dout(5) << "  i am overloaded" << endl;
  

  // determine load transfer mapping
  multimap<int,double> my_targets;
  multimap<double,int>::reverse_iterator exporter = load_map.rbegin();
  multimap<double,int>::iterator importer = load_map.begin();
  double imported = 0;
  double exported = 0;
  while (exporter != load_map.rend() &&
		 importer != load_map.end()) {
	double maxex = (*exporter).first - target_load.root_pop - exported;
	double maxim = target_load.root_pop - (*importer).first - imported;
	if (maxex < 0 ||
		maxim < 0) break;

	if (maxim < maxex) {  // import takes it all
	  dout(5) << " - " << (*exporter).second << " exports " << maxim << " to " << (*importer).second << endl;
	  if ((*exporter).second == whoami)
		my_targets.insert(pair<int,double>((*importer).second, maxim));
	  exported += maxim;
	  importer++;
	  imported = 0;
	} 
	else if (maxim > maxex) {         // export all
	  dout(5) << " - " << (*exporter).second << " exports " << maxex << " to " << (*importer).second << endl;
	  if ((*exporter).second == whoami)
		my_targets.insert(pair<int,double>((*importer).second, maxex));
	  imported += maxex;
	  exporter++;
	  exported = 0;
	} else {
	  // wow, perfect match!
	  dout(5) << " - " << (*exporter).second << " exports " << maxex << " to " << (*importer).second << endl;
	  if ((*exporter).second == whoami)
		my_targets.insert(pair<int,double>((*importer).second, maxex));
	  imported = exported = 0;
	  importer++; importer++;
	}
  }

  // make a sorted list of my imports
  map<double,CInode*>    import_pop_map;
  multimap<int,CInode*>  import_from_map;
  for (set<CInode*>::iterator it = mds->mdcache->imports.begin();
	   it != mds->mdcache->imports.end();
	   it++) {
	import_pop_map.insert(pair<double,CInode*>((*it)->popularity.get(), *it));
	int from = (*it)->authority(mds->get_cluster());
	dout(5) << "map i imported " << **it << " from " << from << endl;
	import_from_map.insert(pair<int,CInode*>(from, *it));
  }
  
  // do my exports!
  for (multimap<int,double>::iterator it = my_targets.begin();
	   it != my_targets.end();
	   it++) {
	int target = (*it).first;
	double amount = (*it).second;

	if (amount < MIN_OFFLOAD) continue;

	dout(5) << " sending " << amount << " to " << target << endl;
	
	mds->mdcache->show_imports();

	// search imports from target
	if (import_from_map.count(target)) {
	  dout(5) << " aha, looking through imports from target mds" << target << endl;
	  pair<multimap<int,CInode*>::iterator, multimap<int,CInode*>::iterator> p =
		p = import_from_map.equal_range(target);
	  while (p.first != p.second) {
		CInode *in = (*p.first).second;
		dout(5) << "considering " << *in << " from " << (*p.first).first << endl;
		multimap<int,CInode*>::iterator plast = p.first++;
		
		if (in->is_root()) continue;
		double pop = in->popularity.get();
		assert(in->authority(mds->get_cluster()) == target);  // cuz that's how i put it in the map, dummy

		if (pop <= amount) {
		  dout(5) << "reexporting " << *in << " pop " << pop << " back to " << target << endl;
		  mds->mdcache->export_dir(in, target);
		  amount -= pop;
		  import_from_map.erase(plast);
		  import_pop_map.erase(pop);
		} else {
		  dout(5) << "can't reexport " << *in << ", too big " << pop << endl;
		}
		if (amount < MIN_OFFLOAD) break;
	  }
	}
	if (amount < MIN_OFFLOAD) break;
	
	// any other imports
	for (map<double,CInode*>::iterator import = import_pop_map.begin();
		 import != import_pop_map.end();
		 import++) {
	  CInode *imp = (*import).second;
	  if (imp->is_root()) continue;
	  
	  double pop = (*import).first;
	  if (pop < amount ||
		  pop < MIN_REEXPORT) {
		dout(5) << "reexporting " << *imp << " pop " << pop << endl;
		amount -= pop;
		mds->mdcache->export_dir(imp, imp->authority(mds->get_cluster()));
	  }
	  if (amount < MIN_OFFLOAD) break;
	}
	if (amount < MIN_OFFLOAD) break;

	// okay, search for fragments of my workload
	set<CInode*> candidates = mds->mdcache->imports;

	list<CInode*> exports;
	double have = 0;
	
	for (set<CInode*>::iterator pot = candidates.begin();
		 pot != candidates.end();
		 pot++) {
	  find_exports(*pot, amount, exports, have);
	  if (have > amount-MIN_OFFLOAD) break;
	}
	
	for (list<CInode*>::iterator it = exports.begin(); it != exports.end(); it++) {
	  dout(5) << " exporting fragment " << **it << " pop " << (*it)->popularity.get() << endl;
	  mds->mdcache->export_dir(*it, target);
	}
  }

  dout(5) << "rebalance done" << endl;
  mds->mdcache->show_imports();
  
}



void MDBalancer::find_exports(CInode *idir, 
							  double amount, 
							  list<CInode*>& exports, 
							  double& have)
{
  double need = amount - have;
  if (need < amount / 5)
	return;   // good enough!
  double needmax = need * 1.2;
  double needmin = need * .8;
  double midchunk = need * .3;
  double minchunk = need * .01;

  if (!idir->dir) return;  // clearly nothing to export

  list<CInode*> bigger;
  multimap<double, CInode*> smaller;

  dout(7) << " find_exports in " << *idir << " need " << need << " (" << needmin << " - " << needmax << ")" << endl;

  for (CDir_map_t::iterator it = idir->dir->begin();
	   it != idir->dir->end();
	   it++) {
	CInode *in = it->second->get_inode();

	if (!in->is_dir()) continue;
	if (!in->dir) continue;  // clearly not popular
	if (mds->mdcache->exports.count(in)) continue;  
	if (in->dir->is_freeze_root()) continue;  // can't export this right now!

	double pop = in->popularity.get();

	//cout << "   in " << in->inode.ino << " " << pop << endl;

	if (pop < minchunk) continue;

	// lucky find?
	if (pop > needmin && pop < needmax) {
	  exports.push_back(in);
	  have += pop;
	  return;
	}

	if (pop > need)
	  bigger.push_back(in);
	else
	  smaller.insert(pair<double,CInode*>(pop, in));
  }

  // grab some sufficiently big small items
  multimap<double,CInode*>::reverse_iterator it;
  for (it = smaller.rbegin();
	   it != smaller.rend();
	   it++) {

	if ((*it).first < midchunk)
	  break;  // try later

	dout(7) << " taking smaller " << *(*it).second << endl;

	exports.push_back((*it).second);
	have += (*it).first;
	if (have > needmin)
	  return;
  }
  
  // apprently not enough; drill deeper into the hierarchy
  for (list<CInode*>::iterator it = bigger.begin();
	   it != bigger.end();
	   it++) {
	dout(7) << " descending into " << **it << endl;
	find_exports(*it, amount, exports, have);
	if (have > needmin)
	  return;
  }

  // ok fine, use smaller bits
  for (;
	   it != smaller.rend();
	   it++) {

	dout(7) << " taking (much) smaller " << *(*it).second << endl;

	exports.push_back((*it).second);
	have += (*it).first;
	if (have > needmin)
	  return;
  }


}




void MDBalancer::hit_inode(CInode *in)
{
  // inode
  in->popularity.hit();

  CInode *p = in->get_parent_inode();
  if (p)
	hit_dir(p->dir);
}
	
void MDBalancer::hit_dir(CDir *dir) 
{
  CInode *in = dir->inode;

  while (dir) {
	dir->popularity.hit();
	float dir_pop = dir->popularity.get();

	if (dir->auth) {
	  if (!dir->is_rep() &&
		  dir_pop >= g_conf.mdbal_replicate_threshold) {
		// replicate
		dout(5) << "replicating dir " << *in << " pop " << dir_pop << endl;
		
		dir->dir_rep = CDIR_REP_ALL;
		mds->mdcache->send_dir_updates(dir);
	  }
	  
	  if (dir->is_rep() &&
		  dir_pop < g_conf.mdbal_unreplicate_threshold) {
		// unreplicate
		dout(5) << "unreplicating dir " << *in << " pop " << dir_pop << endl;
		
		dir->dir_rep = CDIR_REP_NONE;
		mds->mdcache->send_dir_updates(dir);
	  }
	}
	
	// next!
	in->popularity.hit();   // hit inode too.

	in = in->get_parent_inode();
	if (!in) break;
	dir = in->dir;
  }

}
