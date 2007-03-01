// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "mdstypes.h"

#include "MDBalancer.h"
#include "MDS.h"
#include "MDSMap.h"
#include "CInode.h"
#include "CDir.h"
#include "MDCache.h"
#include "Migrator.h"

#include "include/Context.h"
#include "msg/Messenger.h"
#include "messages/MHeartbeat.h"

#include <vector>
#include <map>
using namespace std;

#include "config.h"
#undef dout
#define  dout(l)    if (l<=g_conf.debug_mds || l<=g_conf.debug_mds_balancer) cout << g_clock.now() << " mds" << mds->get_nodeid() << ".bal "

#define MIN_LOAD    50   //  ??
#define MIN_REEXPORT 5  // will automatically reexport
#define MIN_OFFLOAD 10   // point at which i stop trying, close enough



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




void MDBalancer::tick()
{
  static int num_bal_times = g_conf.mds_bal_max;
  static utime_t first = g_clock.now();
  utime_t now = g_clock.now();
  utime_t elapsed = now;
  elapsed -= first;

  // balance?
  if (true && 
      mds->get_nodeid() == 0 &&
      (num_bal_times || 
       (g_conf.mds_bal_max_until >= 0 && 
	elapsed.sec() > g_conf.mds_bal_max_until)) && 
      mds->is_active() &&
      now.sec() - last_heartbeat.sec() >= g_conf.mds_bal_interval) {
    last_heartbeat = now;
    send_heartbeat();
    num_bal_times--;
  }
  
  // hash?
  if (true &&
      g_conf.num_mds > 1 &&
      now.sec() - last_hash.sec() > g_conf.mds_bal_hash_interval) {
    last_hash = now;
    do_hashing();
  }
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

mds_load_t MDBalancer::get_load()
{
  mds_load_t load;
  if (mds->mdcache->get_root()) 
    load.root = 
      mds->mdcache->get_root()->popularity[MDS_POP_ANYDOM];
  // +
  //  mds->mdcache->get_root()->popularity[MDS_POP_NESTED];

  load.req_rate = mds->get_req_rate();
  load.queue_len = mds->messenger->get_dispatch_queue_len();
  return load;
}

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
  mds_load_t load = get_load();
  mds_load[ mds->get_nodeid() ] = load;

  // import_map
  map<int, float> import_map;

  for (set<CDir*>::iterator it = mds->mdcache->imports.begin();
       it != mds->mdcache->imports.end();
       it++) {
    CDir *im = *it;
    if (im->inode->is_root()) continue;
    int from = im->inode->authority();
    import_map[from] += im->popularity[MDS_POP_CURDOM].meta_load();
  }
  mds_import_map[ mds->get_nodeid() ] = import_map;

  
  dout(5) << "mds" << mds->get_nodeid() << " sending heartbeat " << beat_epoch << " " << load << endl;
  for (map<int, float>::iterator it = import_map.begin();
       it != import_map.end();
       it++) {
    dout(5) << "  import_map from " << it->first << " -> " << it->second << endl;
  }

  
  set<int> up;
  mds->get_mds_map()->get_up_mds_set(up);
  for (set<int>::iterator p = up.begin(); p != up.end(); ++p) {
    if (*p == mds->get_nodeid()) continue;
    MHeartbeat *hb = new MHeartbeat(load, beat_epoch);
    hb->get_import_map() = import_map;
    mds->messenger->send_message(hb,
                                 mds->mdsmap->get_inst(*p),
				 MDS_PORT_BALANCER, MDS_PORT_BALANCER);
  }
}

void MDBalancer::handle_heartbeat(MHeartbeat *m)
{
  dout(25) << "=== got heartbeat " << m->get_beat() << " from " << m->get_source().num() << " " << m->get_load() << endl;
  
  if (!mds->mdcache->get_root()) {
    dout(10) << "no root on handle" << endl;
    mds->mdcache->open_root(new C_MDS_RetryMessage(mds, m));
    return;
  }

  int who = m->get_source().num();
  
  if (who == 0) {
    dout(20) << " from mds0, new epoch" << endl;
    beat_epoch = m->get_beat();
    send_heartbeat();

    show_imports();
  }
  
  mds_load[ who ] = m->get_load();
  mds_import_map[ who ] = m->get_import_map();

  //cout << "  load is " << load << " have " << mds_load.size() << endl;
  
  unsigned cluster_size = mds->get_mds_map()->get_num_mds();
  if (mds_load.size() == cluster_size) {
    // let's go!
    //export_empties();  // no!
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
      mds->mdcache->migrator->export_empty_import(dir);
  }
}



double MDBalancer::try_match(int ex, double& maxex, 
                             int im, double& maxim)
{
  if (maxex <= 0 || maxim <= 0) return 0.0;
  
  double howmuch = MIN(maxex, maxim);
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



void MDBalancer::do_hashing()
{
  if (hash_queue.empty()) {
    dout(20) << "do_hashing has nothing to do" << endl;
    return;
  }

  dout(0) << "do_hashing " << hash_queue.size() << " dirs marked for possible hashing" << endl;
  
  for (set<inodeno_t>::iterator i = hash_queue.begin();
       i != hash_queue.end();
       i++) {
    inodeno_t dirino = *i;
    CInode *in = mds->mdcache->get_inode(dirino);
    if (!in) continue;
    CDir *dir = in->dir;
    if (!dir) continue;
    if (!dir->is_auth()) continue;

    dout(0) << "do_hashing hashing " << *dir << endl;
    mds->mdcache->migrator->hash_dir(dir);
  }
  hash_queue.clear();
}



void MDBalancer::do_rebalance(int beat)
{
  int cluster_size = mds->get_mds_map()->get_num_mds();
  int whoami = mds->get_nodeid();

  // reset
  my_targets.clear();
  imported.clear();
  exported.clear();

  dout(5) << " do_rebalance: cluster loads are" << endl;

  // rescale!  turn my mds_load back into meta_load units
  double load_fac = 1.0;
  if (mds_load[whoami].mds_load() > 0) {
    load_fac = mds_load[whoami].root.meta_load() / mds_load[whoami].mds_load();
    dout(7) << " load_fac is " << load_fac 
             << " <- " << mds_load[whoami].root.meta_load() << " / " << mds_load[whoami].mds_load()
             << endl;
  }
  
  double total_load = 0;
  multimap<double,int> load_map;
  for (int i=0; i<cluster_size; i++) {
    double l = mds_load[i].mds_load() * load_fac;
    mds_meta_load[i] = l;

    if (whoami == 0)
      dout(-5) << "  mds" << i 
               << " meta load " << mds_load[i] 
               << " = " << mds_load[i].mds_load() 
               << " --> " << l << endl;

    if (whoami == i) my_load = l;
    total_load += l;

    load_map.insert(pair<double,int>( l, i ));
  }

  // target load
  target_load = total_load / (double)cluster_size;
  dout(5) << "do_rebalance:  my load " << my_load 
          << "   target " << target_load 
          << "   total " << total_load 
          << endl;
  
  // under or over?
  if (my_load < target_load) {
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
    if (it->first < target_load) {
      dout(15) << "   mds" << it->second << " is importer" << endl;
      importers.insert(pair<double,int>(it->first,it->second));
      importer_set.insert(it->second);
    } else {
      dout(15) << "   mds" << it->second << " is exporter" << endl;
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
    if ((*it)->is_hashed()) continue;
    double pop = (*it)->popularity[MDS_POP_CURDOM].meta_load();
    if (pop < g_conf.mds_bal_idle_threshold &&
        (*it)->inode != mds->mdcache->get_root()) {
      dout(-5) << " exporting idle import " << **it 
               << " back to mds" << (*it)->inode->authority()
               << endl;
      mds->mdcache->migrator->export_dir(*it, (*it)->inode->authority());
      continue;
    }
    import_pop_map[ pop ] = *it;
    int from = (*it)->inode->authority();
    dout(15) << "  map: i imported " << **it << " from " << from << endl;
    import_from_map.insert(pair<int,CDir*>(from, *it));
  }
  


  // do my exports!
  set<CDir*> already_exporting;
  double total_sent = 0;
  double total_goal = 0;

  for (map<int,double>::iterator it = my_targets.begin();
       it != my_targets.end();
       it++) {

    /*
    double fac = 1.0;
    if (false && total_goal > 0 && total_sent > 0) {
      fac = total_goal / total_sent;
      dout(-5) << " total sent is " << total_sent << " / " << total_goal << " -> fac 1/ " << fac << endl;
      if (fac > 1.0) fac = 1.0;
    }
    fac = .9 - .4 * ((float)g_conf.num_mds / 128.0);  // hack magic fixme
    */
    
    int target = (*it).first;
    double amount = (*it).second;// * load_fac;
    total_goal += amount;

    if (amount < MIN_OFFLOAD) continue;

    dout(-5) << " sending " << amount << " to mds" << target 
      //<< " .. " << (*it).second << " * " << load_fac 
            << " -> " << amount
            << endl;//" .. fudge is " << fudge << endl;
    double have = 0;
    
    show_imports();

    // search imports from target
    if (import_from_map.count(target)) {
      dout(5) << " aha, looking through imports from target mds" << target << endl;
      pair<multimap<int,CDir*>::iterator, multimap<int,CDir*>::iterator> p =
        import_from_map.equal_range(target);
      while (p.first != p.second) {
        CDir *dir = (*p.first).second;
        dout(5) << "considering " << *dir << " from " << (*p.first).first << endl;
        multimap<int,CDir*>::iterator plast = p.first++;
        
        if (dir->inode->is_root()) continue;
        if (dir->is_hashed()) continue;
        if (dir->is_freezing() || dir->is_frozen()) continue;  // export pbly already in progress
        double pop = dir->popularity[MDS_POP_CURDOM].meta_load();
        assert(dir->inode->authority() == target);  // cuz that's how i put it in the map, dummy
        
        if (pop <= amount-have) {
          dout(-5) << "reexporting " << *dir 
                   << " pop " << pop 
                   << " back to mds" << target << endl;
          mds->mdcache->migrator->export_dir(dir, target);
          have += pop;
          import_from_map.erase(plast);
          import_pop_map.erase(pop);
        } else {
          dout(5) << "can't reexport " << *dir << ", too big " << pop << endl;
        }
        if (amount-have < MIN_OFFLOAD) break;
      }
    }
    if (amount-have < MIN_OFFLOAD) {
      total_sent += have;
      continue;
    }
    
    // any other imports
    if (false)
    for (map<double,CDir*>::iterator import = import_pop_map.begin();
         import != import_pop_map.end();
         import++) {
      CDir *imp = (*import).second;
      if (imp->inode->is_root()) continue;
      
      double pop = (*import).first;
      if (pop < amount-have || pop < MIN_REEXPORT) {
        dout(-5) << "reexporting " << *imp 
                 << " pop " << pop 
                 << " back to mds" << imp->inode->authority()
                 << endl;
        have += pop;
        mds->mdcache->migrator->export_dir(imp, imp->inode->authority());
      }
      if (amount-have < MIN_OFFLOAD) break;
    }
    if (amount-have < MIN_OFFLOAD) {
      //fudge = amount-have;
      total_sent += have;
      continue;
    }

    // okay, search for fragments of my workload
    set<CDir*> candidates = mds->mdcache->imports;

    list<CDir*> exports;
    
    for (set<CDir*>::iterator pot = candidates.begin();
         pot != candidates.end();
         pot++) {
      find_exports(*pot, amount, exports, have, already_exporting);
      if (have > amount-MIN_OFFLOAD) {
        break;
      }
    }
    //fudge = amount - have;
    total_sent += have;
    
    for (list<CDir*>::iterator it = exports.begin(); it != exports.end(); it++) {
      dout(-5) << " exporting to mds" << target 
               << " fragment " << **it 
               << " pop " << (*it)->popularity[MDS_POP_CURDOM].meta_load() 
               << endl;
      mds->mdcache->migrator->export_dir(*it, target);
      
      // hack! only do one dir.
      break;
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
  if (need < amount * g_conf.mds_bal_min_start)
    return;   // good enough!
  double needmax = need * g_conf.mds_bal_need_max;
  double needmin = need * g_conf.mds_bal_need_min;
  double midchunk = need * g_conf.mds_bal_midchunk;
  double minchunk = need * g_conf.mds_bal_minchunk;

  list<CDir*> bigger;
  multimap<double, CDir*> smaller;

  double dir_pop = dir->popularity[MDS_POP_CURDOM].meta_load();
  double dir_sum = 0;
  dout(-7) << " find_exports in " << dir_pop << " " << *dir << " need " << need << " (" << needmin << " - " << needmax << ")" << endl;

  for (CDir_map_t::iterator it = dir->begin();
       it != dir->end();
       it++) {
    CInode *in = it->second->get_inode();
    if (!in) continue;
    if (!in->is_dir()) continue;
    if (!in->dir) continue;       // clearly not popular
    
    if (in->dir->is_export()) continue;
    if (in->dir->is_hashed()) continue;
    if (already_exporting.count(in->dir)) continue;

    if (in->dir->is_frozen()) continue;  // can't export this right now!
    //if (in->dir->get_size() == 0) continue;  // don't export empty dirs, even if they're not complete.  for now!
    
    // how popular?
    double pop = in->dir->popularity[MDS_POP_CURDOM].meta_load();
    dir_sum += pop;
    dout(20) << "   pop " << pop << " " << *in->dir << endl;

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
  dout(7) << " .. sum " << dir_sum << " / " << dir_pop << endl;

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
  
  // apprently not enough; drill deeper into the hierarchy (if non-replicated)
  for (list<CDir*>::iterator it = bigger.begin();
       it != bigger.end();
       it++) {
    if ((*it)->is_rep()) continue;
    dout(7) << " descending into " << **it << endl;
    find_exports(*it, amount, exports, have, already_exporting);
    if (have > needmin)
      return;
  }

  // ok fine, use smaller bits
  for (;
       it != smaller.rend();
       it++) {

    dout(7) << " taking (much) smaller " << it->first << " " << *(*it).second << endl;

    exports.push_back((*it).second);
    already_exporting.insert((*it).second);
    have += (*it).first;
    if (have > needmin)
      return;
  }

  // ok fine, drill inot replicated dirs
  for (list<CDir*>::iterator it = bigger.begin();
       it != bigger.end();
       it++) {
    if (!(*it)->is_rep()) continue;
    dout(7) << " descending into replicated " << **it << endl;
    find_exports(*it, amount, exports, have, already_exporting);
    if (have > needmin)
      return;
  }

}




void MDBalancer::hit_inode(CInode *in, int type)
{
  // hit me
  in->popularity[MDS_POP_JUSTME].pop[type].hit();
  in->popularity[MDS_POP_NESTED].pop[type].hit();
  if (in->is_auth()) {
    in->popularity[MDS_POP_CURDOM].pop[type].hit();
    in->popularity[MDS_POP_ANYDOM].pop[type].hit();
  }
  
  // hit auth up to import
  CDir *dir = in->get_parent_dir();
  if (dir) hit_dir(dir, type);
}


void MDBalancer::hit_dir(CDir *dir, int type) 
{
  // hit me
  float v = dir->popularity[MDS_POP_JUSTME].pop[type].hit();

  // hit modify counter, if this was a modify
  if (g_conf.num_mds > 2 &&             // FIXME >2 thing
      !dir->inode->is_root() &&        // not root (for now at least)
      dir->is_auth()) {
    //dout(-20) << "hit_dir " << type << " pop is " << v << "  " << *dir << endl;

    // hash this dir?  (later?)
    if (((v > g_conf.mds_bal_hash_rd && type == META_POP_IRD) ||
         //(v > g_conf.mds_bal_hash_wr && type == META_POP_IWR) ||
         (v > g_conf.mds_bal_hash_wr && type == META_POP_DWR)) &&
        !(dir->is_hashed() || dir->is_hashing()) &&
        hash_queue.count(dir->ino()) == 0) {
      dout(0) << "hit_dir " << type << " pop is " << v << ", putting in hash_queue: " << *dir << endl;
      hash_queue.insert(dir->ino());
    }

  }
  
  hit_recursive(dir, type);
}



void MDBalancer::hit_recursive(CDir *dir, int type)
{
  bool anydom = dir->is_auth();
  bool curdom = dir->is_auth();

  float rd_adj = 0.0;

  // replicate?
  float dir_pop = dir->popularity[MDS_POP_CURDOM].pop[type].get();    // hmm??

  if (dir->is_auth()) {
    if (!dir->is_rep() &&
        dir_pop >= g_conf.mds_bal_replicate_threshold) {
      // replicate
      float rdp = dir->popularity[MDS_POP_JUSTME].pop[META_POP_IRD].get();
      rd_adj = rdp / mds->get_mds_map()->get_num_mds() - rdp; 
      rd_adj /= 2.0;  // temper somewhat

      dout(1) << "replicating dir " << *dir << " pop " << dir_pop << " .. rdp " << rdp << " adj " << rd_adj << endl;
          
      dir->dir_rep = CDIR_REP_ALL;
      mds->mdcache->send_dir_updates(dir, true);

      dir->popularity[MDS_POP_JUSTME].pop[META_POP_IRD].adjust(rd_adj);
      dir->popularity[MDS_POP_CURDOM].pop[META_POP_IRD].adjust(rd_adj);
    }
        
    if (!dir->ino() != 1 &&
        dir->is_rep() &&
        dir_pop < g_conf.mds_bal_unreplicate_threshold) {
      // unreplicate
      dout(1) << "unreplicating dir " << *dir << " pop " << dir_pop << endl;
      
      dir->dir_rep = CDIR_REP_NONE;
      mds->mdcache->send_dir_updates(dir);
    }
  }


  while (dir) {
    CInode *in = dir->inode;

    dir->popularity[MDS_POP_NESTED].pop[type].hit();
    in->popularity[MDS_POP_NESTED].pop[type].hit();
    
    if (rd_adj != 0.0) dir->popularity[MDS_POP_NESTED].pop[META_POP_IRD].adjust(rd_adj);

    if (anydom) {
      dir->popularity[MDS_POP_ANYDOM].pop[type].hit();
      in->popularity[MDS_POP_ANYDOM].pop[type].hit();
    }
    
    if (curdom) {
      dir->popularity[MDS_POP_CURDOM].pop[type].hit();
      in->popularity[MDS_POP_CURDOM].pop[type].hit();
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
  meta_load_t curdom = dir->popularity[MDS_POP_CURDOM];
  
  bool in_domain = !dir->is_import();
  
  while (true) {
    CInode *in = dir->inode;
    
    in->popularity[MDS_POP_ANYDOM] -= curdom;
    if (in_domain) in->popularity[MDS_POP_CURDOM] -= curdom;
    
    dir = in->get_parent_dir();
    if (!dir) break;
    
    if (dir->is_import()) in_domain = false;
    
    dir->popularity[MDS_POP_ANYDOM] -= curdom;
    if (in_domain) dir->popularity[MDS_POP_CURDOM] -= curdom;
  }
}
    

void MDBalancer::add_import(CDir *dir)
{
  meta_load_t curdom = dir->popularity[MDS_POP_CURDOM];

  bool in_domain = !dir->is_import();
  
  while (true) {
    CInode *in = dir->inode;
    
    in->popularity[MDS_POP_ANYDOM] += curdom;
    if (in_domain) in->popularity[MDS_POP_CURDOM] += curdom;
    
    dir = in->get_parent_dir();
    if (!dir) break;
    
    if (dir->is_import()) in_domain = false;
    
    dir->popularity[MDS_POP_ANYDOM] += curdom;
    if (in_domain) dir->popularity[MDS_POP_CURDOM] += curdom;
  }
 
}






void MDBalancer::show_imports(bool external)
{
  mds->mdcache->show_imports();
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
