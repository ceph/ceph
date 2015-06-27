// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
#include "mon/MonClient.h"
#include "MDSMap.h"
#include "CInode.h"
#include "CDir.h"
#include "MDCache.h"
#include "Migrator.h"

#include "include/Context.h"
#include "msg/Messenger.h"
#include "messages/MHeartbeat.h"
#include "messages/MMDSLoadTargets.h"

#include <fstream>
#include <iostream>
#include <vector>
#include <map>
#include <string.h>
using std::map;
using std::vector;

#include "common/config.h"

#define dout_subsys ceph_subsys_mds_balancer
#undef DOUT_COND
#define DOUT_COND(cct, l) l<=cct->_conf->debug_mds || l <= cct->_conf->debug_mds_balancer
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".bal "

#define MIN_LOAD    50   //  ??
#define MIN_REEXPORT 5  // will automatically reexport

// these strings are pasted together to make a Lua executable string
// -- this is very ugly. :(
static const char *LUA_IMPORT = "package.path = package.path .. ';";
static const char *LUA_INIT = "?.lua;'\n require \"libmantle\"\n"
  "log, whoami, MDSs, authmetaload, nfiles, allmetaload, targets = libmantle.parse_args(arg)\n"
  "for i=1,#MDSs do\n"
  "  MDSs[i][\"load\"] = ";
static const char *LUA_LOAD = "\n"
  "end\n"
  "total = libmantle.get_total(MDSs)\n"
  "libmantle.print_metrics(arg[1], MDSs)\n";
static const char *LUA_WHEN = "\n"
  "   E, I = libmantle.get_exporters_importers(log, MDSs)\n"
  " else\n"
  "   return libmantle.get_empty_targets(log, targets)\n"
  " end\n";
static const char *LUA_WHERE = "\n"
  "return libmantle.convert_targets(log, targets)";

static const char *LUA_INIT_HOWMUCH = "?.lua;'\n require \"libbinpacker\"\n"
  "strategies = ";
static const char *LUA_HOWMUCH = "\n"
  "return libbinpacker.pack(strategies, arg)";

/* This function DOES put the passed message before returning */
int MDBalancer::proc_message(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_HEARTBEAT:
    handle_heartbeat(static_cast<MHeartbeat*>(m));
    break;

  default:
    dout(1) << " balancer unknown message " << m->get_type() << dendl;
    assert(0);
    m->put();
    break;
  }

  return 0;
}




void MDBalancer::tick()
{
  static int num_bal_times = g_conf->mds_bal_max;
  static utime_t first = ceph_clock_now(g_ceph_context);
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t elapsed = now;
  elapsed -= first;

  // sample?
  if ((double)now - (double)last_sample > g_conf->mds_bal_sample_interval) {
    dout(15) << "tick last_sample now " << now << dendl;
    last_sample = now;
  }

  // balance?
  if (last_heartbeat == utime_t())
    last_heartbeat = now;
  if (mds->get_nodeid() == 0 &&
      g_conf->mds_bal_interval > 0 &&
      (num_bal_times ||
       (g_conf->mds_bal_max_until >= 0 &&
	elapsed.sec() > g_conf->mds_bal_max_until)) &&
      mds->is_active() &&
      now.sec() - last_heartbeat.sec() >= g_conf->mds_bal_interval) {
    last_heartbeat = now;
    send_heartbeat();
    num_bal_times--;
  }

  // hash?
  if ((g_conf->mds_bal_frag || g_conf->mds_thrash_fragments) &&
      g_conf->mds_bal_fragment_interval > 0 &&
      now.sec() - last_fragment.sec() > g_conf->mds_bal_fragment_interval) {
    last_fragment = now;
    do_fragmenting();
  }
}




class C_Bal_SendHeartbeat : public MDSInternalContext {
public:
  C_Bal_SendHeartbeat(MDS *mds_) : MDSInternalContext(mds_) { }
  virtual void finish(int f) {
    mds->balancer->send_heartbeat();
  }
};


double mds_load_t::mds_load()
{
  switch(g_conf->mds_bal_mode) {
  case 0:
    return
      .8 * auth.meta_load() +
      .2 * all.meta_load() +
      req_rate +
      10.0 * queue_len;

  case 1:
    return req_rate + 10.0*queue_len;

  case 2:
    return cpu_load_avg;

  }
  assert(0);
  return 0;
}

mds_load_t MDBalancer::get_load(utime_t now)
{
  mds_load_t load(now);
  if (mds->mdcache->get_root()) {
    list<CDir*> ls;
    mds->mdcache->get_root()->get_dirfrags(ls);
    for (list<CDir*>::iterator p = ls.begin();
	 p != ls.end();
	 ++p) {
      load.auth.add(now, mds->mdcache->decayrate, (*p)->pop_auth_subtree_nested);
      load.all.add(now, mds->mdcache->decayrate, (*p)->pop_nested);
    }
  } else {
    dout(20) << "get_load no root, no load" << dendl;
  }

  load.req_rate = mds->get_req_rate();
  load.queue_len = mds->messenger->get_dispatch_queue_len();

  // use instantaneous CPU util. (better for flash crowds/hotspots) instead of load average
  ifstream cpu("/proc/stat");
  if (cpu.is_open()) {
    // TODO: this should be cleaned up
    map<string, double> procstat;
    string temp;
    cpu >> temp;
    cpu >> temp;
    procstat.insert(pair<string,double>("usr", atof(temp.c_str())));
    cpu >> temp;
    procstat.insert(pair<string,double>("nice", atof(temp.c_str())));
    cpu >> temp;
    procstat.insert(pair<string,double>("sys", atof(temp.c_str())));
    cpu >> temp;
    procstat.insert(pair<string,double>("idle", atof(temp.c_str())));
    cpu >> temp;
    procstat.insert(pair<string,double>("iowait", atof(temp.c_str())));
    cpu >> temp;
    procstat.insert(pair<string,double>("irq", atof(temp.c_str())));
    cpu >> temp;
    procstat.insert(pair<string,double>("softirq", atof(temp.c_str())));
    cpu >> temp;
    procstat.insert(pair<string,double>("steal", atof(temp.c_str())));
    cpu.close();
    
    double cpu_total = 0;
    double cpu_work = 0;
    for (map<string, double>:: iterator it = procstat.begin();
         it != procstat.end();
         it++) {
      cpu_total += it->second;
      if (!it->first.compare("usr") ||
          !it->first.compare("sys") ||
          !it->first.compare("nice"))
        cpu_work += it->second;
    }

    // save it if it is a legit sample.
    double cpu_work_period = cpu_work - cpu_work_prev;
    double cpu_total_period = cpu_total - cpu_total_prev;
    if (cpu_total_period > 100 && cpu_work_period > 100) {
      cpu_load_avg = 100*cpu_work_period/cpu_total_period;
      cpu_work_prev = cpu_work;
      cpu_total_prev = cpu_total;
      dout(15) << "get_load cpu=" << cpu_work_period << "/" << cpu_total_period << "=" << load.cpu_load_avg << dendl;
    }
    load.cpu_load_avg = cpu_load_avg;
  }

  dout(15) << "get_load " << load << dendl;
  return load;
}

void MDBalancer::send_heartbeat()
{
  utime_t now = ceph_clock_now(g_ceph_context);
  
  if (mds->mdsmap->is_degraded()) {
    dout(10) << "send_heartbeat degraded" << dendl;
    return;
  }

  if (!mds->mdcache->is_open()) {
    dout(5) << "not open" << dendl;
    mds->mdcache->wait_for_open(new C_Bal_SendHeartbeat(mds));
    return;
  }

  mds_load.clear();
  if (mds->get_nodeid() == 0)
    beat_epoch++;

  // my load
  mds_load_t load = get_load(now);
  map<mds_rank_t, mds_load_t>::value_type val(mds->get_nodeid(), load);
  mds_load.insert(val);

  // import_map -- how much do i import from whom
  map<mds_rank_t, float> import_map;
  set<CDir*> authsubs;
  mds->mdcache->get_auth_subtrees(authsubs);
  for (set<CDir*>::iterator it = authsubs.begin();
       it != authsubs.end();
       ++it) {
    CDir *im = *it;
    mds_rank_t from = im->inode->authority().first;
    if (from == mds->get_nodeid()) continue;
    if (im->get_inode()->is_stray()) continue;
    import_map[from] += im->pop_auth_subtree.meta_load(now, mds->mdcache->decayrate);
  }
  mds_import_map[ mds->get_nodeid() ] = import_map;


  dout(5) << "mds." << mds->get_nodeid() << " epoch " << beat_epoch << " load " << load << dendl;
  for (map<mds_rank_t, float>::iterator it = import_map.begin();
       it != import_map.end();
       ++it) {
    dout(5) << "  import_map from " << it->first << " -> " << it->second << dendl;
  }


  set<mds_rank_t> up;
  mds->get_mds_map()->get_up_mds_set(up);
  for (set<mds_rank_t>::iterator p = up.begin(); p != up.end(); ++p) {
    if (*p == mds->get_nodeid())
      continue;
    MHeartbeat *hb = new MHeartbeat(load, beat_epoch);
    hb->get_import_map() = import_map;
    mds->messenger->send_message(hb,
                                 mds->mdsmap->get_inst(*p));
  }
}

/* This function DOES put the passed message before returning */
void MDBalancer::handle_heartbeat(MHeartbeat *m)
{
  typedef map<mds_rank_t, mds_load_t> mds_load_map_t;

  mds_rank_t who = mds_rank_t(m->get_source().num());
  dout(25) << "=== got heartbeat " << m->get_beat() << " from " << m->get_source().num() << " " << m->get_load() << dendl;

  if (!mds->is_active())
    goto out;

  if (!mds->mdcache->is_open()) {
    dout(10) << "opening root on handle_heartbeat" << dendl;
    mds->mdcache->wait_for_open(new C_MDS_RetryMessage(mds, m));
    return;
  }

  if (mds->mdsmap->is_degraded()) {
    dout(10) << " degraded, ignoring" << dendl;
    goto out;
  }

  if (who == 0) {
    dout(20) << " from mds0, new epoch" << dendl;
    beat_epoch = m->get_beat();
    send_heartbeat();

    show_imports();
  }

  {
    // set mds_load[who]
    mds_load_map_t::value_type val(who, m->get_load());
    pair < mds_load_map_t::iterator, bool > rval (mds_load.insert(val));
    if (!rval.second) {
      rval.first->second = val.second;
    }
  }
  mds_import_map[ who ] = m->get_import_map();

  //dout(0) << "  load is " << load << " have " << mds_load.size() << dendl;

  {
    unsigned cluster_size = mds->get_mds_map()->get_num_in_mds();
    if (mds_load.size() == cluster_size) {
      dump_subtree_loads();

      // let's go!
      if (g_conf->mds_bal_metaload != "" && g_conf->mds_bal_mdsload != "" &&
          g_conf->mds_bal_when != "" && g_conf->mds_bal_where != "" &&
          g_conf->mds_bal_dir != "") 
        custom_balancer();
      else
        prep_rebalance(m->get_beat());
    }
  }

  // done
 out:
  m->put();
}



void MDBalancer::dump_subtree_loads() {
  utime_t now = ceph_clock_now(g_ceph_context);
  
  // get the auth load  
  subtrees.clear();
  if (mds->mdcache->get_root()) {
    list<CDir*> ls;
    mds->mdcache->get_root()->get_dirfrags(ls);
    double authload = 0;
    for (list<CDir*>::iterator p = ls.begin();
	 p != ls.end();
	 ++p) {
      string path;
      (*p)->get_inode()->make_path_string_projected(path);
      authload += (*p)->pop_auth_subtree_nested.meta_load(now, mds->mdcache->decayrate);
      dout(5) << "AUTH LOAD: [IRD,IWR metaload]=" << (*p)->pop_auth_subtree_nested  
              << " fragsize=" << (*p)->get_frag_size() << " path=/root" << path << dendl;
    }
  }

  // get the rest of the load
  set<CDir*> roots;
  mds->mdcache->get_fullauth_subtrees(roots);
  total_meta_load = 0;
  for (set<CDir*>::iterator it = roots.begin();
       it != roots.end();
       ++it) {
    CDir *dir = *it;
    string path;
    dir->get_inode()->make_path_string_projected(path);
    subtrees.clear();
    if (path.find("~") == 0) continue;

    dout(10) << " inserting load of root dir=" << *dir << dendl;
    subtrees.push_back(dir); // don't immediately recurse, could be dirfrag
    total_meta_load += dir->pop_auth_subtree.meta_load(now, mds->mdcache->decayrate);
    subtree_loads(dir, 1);

    // print out the results
    dout(10) << " printing out " << subtrees.size() << " subtrees" << dendl;
    for (vector<CDir*>::iterator popit = subtrees.begin();
         popit != subtrees.end();
         ++popit) {
      CDir *popdir = *popit;
      string path;
      popdir->get_inode()->make_path_string_projected(path);
      dout(2) << "/root" << path
              << " " << popdir->pop_auth_subtree
              << " df=" << popdir->dirfrag()
              << " size=" << popdir->get_frag_size() << dendl;
    }
    subtrees.clear();
  }
}



void MDBalancer::subtree_loads(CDir *dir, int depth) 
{
  if (depth > g_conf->mds_bal_debug_dfs_depth) return;

  // breadth first traversal, break when we have enough samples
  utime_t now = ceph_clock_now(g_ceph_context);
  for (CDir::map_t::iterator it = dir->begin();
       it != dir->end();
       ++it) {
    CInode *in = it->second->get_linkage()->get_inode();
    if (!in) continue;
    if (!in->is_dir()) continue;
    if (in->is_stray()) continue;
    
    list<CDir*> dfls;
    in->get_dirfrags(dfls);
    for (list<CDir*>::iterator p = dfls.begin();
         p != dfls.end();
         ++p) {
      CDir *subdir = *p;
      double metaload = subdir->pop_auth_subtree.meta_load(now, mds->mdcache->decayrate);
      if (metaload >= g_conf->mds_bal_debug_dfs_metaload) {
        dout(10) << " pushing back load=" << metaload << " dir=" << *subdir << dendl;
        subtrees.push_back(subdir);
        if (subtrees.size() > (size_t) g_conf->mds_bal_debug_dfs) return;
      }
    }
  }

  // recurse down since we don't have enough samples (mds_bal_debug_dfs)
  for (CDir::map_t::iterator it = dir->begin();
       it != dir->end();
       ++it) {
    CInode *in = it->second->get_linkage()->get_inode();
    if (!in) continue;
    if (!in->is_dir()) continue;
    if (in->is_stray()) continue;
    
    list<CDir*> dfls;
    in->get_dirfrags(dfls);
    for (list<CDir*>::iterator p = dfls.begin();
         p != dfls.end();
         ++p) {
      subtree_loads(*p, depth+1);
      if (subtrees.size() > (size_t) g_conf->mds_bal_debug_dfs) return;
    }
  }
}



void MDBalancer::export_empties()
{
  dout(5) << "export_empties checking for empty imports" << dendl;

  for (map<CDir*,set<CDir*> >::iterator it = mds->mdcache->subtrees.begin();
       it != mds->mdcache->subtrees.end();
       ++it) {
    CDir *dir = it->first;
    if (!dir->is_auth() ||
	dir->is_ambiguous_auth() ||
	dir->is_freezing() ||
	dir->is_frozen())
      continue;

    if (!dir->inode->is_base() &&
	!dir->inode->is_stray() &&
	dir->get_num_head_items() == 0)
      mds->mdcache->migrator->export_empty_import(dir);
  }
}



double MDBalancer::try_match(mds_rank_t ex, double& maxex,
                             mds_rank_t im, double& maxim)
{
  if (maxex <= 0 || maxim <= 0) return 0.0;

  double howmuch = MIN(maxex, maxim);
  if (howmuch <= 0) return 0.0;

  dout(5) << "   - mds." << ex << " exports " << howmuch << " to mds." << im << dendl;

  if (ex == mds->get_nodeid())
    my_targets[im] += howmuch;

  exported[ex] += howmuch;
  imported[im] += howmuch;

  maxex -= howmuch;
  maxim -= howmuch;

  return howmuch;
}

void MDBalancer::queue_split(CDir *dir)
{
  split_queue.insert(dir->dirfrag());
}

void MDBalancer::queue_merge(CDir *dir)
{
  merge_queue.insert(dir->dirfrag());
}

void MDBalancer::do_fragmenting()
{
  if (split_queue.empty() && merge_queue.empty()) {
    dout(20) << "do_fragmenting has nothing to do" << dendl;
    return;
  }

  if (!split_queue.empty()) {
    dout(10) << "do_fragmenting " << split_queue.size() << " dirs marked for possible splitting" << dendl;

    set<dirfrag_t> q;
    q.swap(split_queue);

    for (set<dirfrag_t>::iterator i = q.begin();
	 i != q.end();
	 ++i) {
      CDir *dir = mds->mdcache->get_dirfrag(*i);
      if (!dir ||
	  !dir->is_auth())
	continue;

      dout(10) << "do_fragmenting splitting " << *dir << dendl;
      mds->mdcache->split_dir(dir, g_conf->mds_bal_split_bits);
    }
  }

  if (!merge_queue.empty()) {
    dout(10) << "do_fragmenting " << merge_queue.size() << " dirs marked for possible merging" << dendl;

    set<dirfrag_t> q;
    q.swap(merge_queue);

    for (set<dirfrag_t>::iterator i = q.begin();
	 i != q.end();
	 ++i) {
      CDir *dir = mds->mdcache->get_dirfrag(*i);
      if (!dir ||
	  !dir->is_auth() ||
	  dir->get_frag() == frag_t())  // ok who's the joker?
	continue;

      dout(10) << "do_fragmenting merging " << *dir << dendl;

      CInode *diri = dir->get_inode();

      frag_t fg = dir->get_frag();
      while (fg != frag_t()) {
	frag_t sibfg = fg.get_sibling();
	list<CDir*> sibs;
	bool complete = diri->get_dirfrags_under(sibfg, sibs);
	if (!complete) {
	  dout(10) << "  not all sibs under " << sibfg << " in cache (have " << sibs << ")" << dendl;
	  break;
	}
	bool all = true;
	for (list<CDir*>::iterator p = sibs.begin(); p != sibs.end(); ++p) {
	  CDir *sib = *p;
	  if (!sib->is_auth() || !sib->should_merge()) {
	    all = false;
	    break;
	  }
	}
	if (!all) {
	  dout(10) << "  not all sibs under " << sibfg << " " << sibs << " should_merge" << dendl;
	  break;
	}
	dout(10) << "  all sibs under " << sibfg << " " << sibs << " should merge" << dendl;
	fg = fg.parent();
      }

      if (fg != dir->get_frag())
	mds->mdcache->merge_dir(diri, fg);
    }
  }
}



void MDBalancer::prep_rebalance(int beat)
{
  if (g_conf->mds_thrash_exports) {
    //we're going to randomly export to all the mds in the cluster
    my_targets.clear();
    set<mds_rank_t> up_mds;
    mds->get_mds_map()->get_up_mds_set(up_mds);
    for (set<mds_rank_t>::iterator i = up_mds.begin();
	 i != up_mds.end();
	 ++i)
      my_targets[*i] = 0.0;
  } else {
    int cluster_size = mds->get_mds_map()->get_num_in_mds();
    mds_rank_t whoami = mds->get_nodeid();
    rebalance_time = ceph_clock_now(g_ceph_context);

    // reset
    my_targets.clear();
    imported.clear();
    exported.clear();

    dout(5) << " prep_rebalance: cluster loads are" << dendl;

    mds->mdcache->migrator->clear_export_queue();

    // rescale!  turn my mds_load back into meta_load units
    double load_fac = 1.0;
    map<mds_rank_t, mds_load_t>::iterator m = mds_load.find(whoami);
    if ((m != mds_load.end()) && (m->second.mds_load() > 0)) {
      double metald = m->second.auth.meta_load(rebalance_time, mds->mdcache->decayrate);
      double mdsld = m->second.mds_load();
      load_fac = metald / mdsld;
      dout(7) << " load_fac is " << load_fac
	      << " <- " << m->second.auth << " " << metald
	      << " / " << mdsld
	      << dendl;
    }

    double total_load = 0;
    multimap<double,mds_rank_t> load_map;
    for (mds_rank_t i=mds_rank_t(0); i < mds_rank_t(cluster_size); i++) {
      map<mds_rank_t, mds_load_t>::value_type val(i, mds_load_t(ceph_clock_now(g_ceph_context)));
      std::pair < map<mds_rank_t, mds_load_t>::iterator, bool > r(mds_load.insert(val));
      mds_load_t &load(r.first->second);

      double l = load.mds_load() * load_fac;
      mds_meta_load[i] = l;

      if (whoami == 0)
	dout(0) << "  mds." << i
		<< " " << load
		<< " = " << load.mds_load()
		<< " ~ " << l << dendl;

      if (whoami == i) my_load = l;
      total_load += l;

      load_map.insert(pair<double,mds_rank_t>( l, i ));
    }

    // target load
    target_load = total_load / (double)cluster_size;
    dout(5) << "prep_rebalance:  my load " << my_load
	    << "   target " << target_load
	    << "   total " << total_load
	    << dendl;

    // under or over?
    if (my_load < target_load * (1.0 + g_conf->mds_bal_min_rebalance)) {
      dout(5) << "  i am underloaded or barely overloaded, doing nothing." << dendl;
      last_epoch_under = beat_epoch;
      show_imports();
      return;
    }

    last_epoch_over = beat_epoch;

    // am i over long enough?
    if (last_epoch_under && beat_epoch - last_epoch_under < 2) {
      dout(5) << "  i am overloaded, but only for " << (beat_epoch - last_epoch_under) << " epochs" << dendl;
      return;
    }

    dout(5) << "  i am sufficiently overloaded" << dendl;


    // first separate exporters and importers
    multimap<double,mds_rank_t> importers;
    multimap<double,mds_rank_t> exporters;
    set<mds_rank_t>             importer_set;
    set<mds_rank_t>             exporter_set;

    for (multimap<double,mds_rank_t>::iterator it = load_map.begin();
	 it != load_map.end();
	 ++it) {
      if (it->first < target_load) {
	dout(15) << "   mds." << it->second << " is importer" << dendl;
	importers.insert(pair<double,mds_rank_t>(it->first,it->second));
	importer_set.insert(it->second);
      } else {
	dout(15) << "   mds." << it->second << " is exporter" << dendl;
	exporters.insert(pair<double,mds_rank_t>(it->first,it->second));
	exporter_set.insert(it->second);
      }
    }


    // determine load transfer mapping

    if (true) {
      // analyze import_map; do any matches i can

      dout(15) << "  matching exporters to import sources" << dendl;

      // big -> small exporters
      for (multimap<double,mds_rank_t>::reverse_iterator ex = exporters.rbegin();
	   ex != exporters.rend();
	   ++ex) {
	double maxex = get_maxex(ex->second);
	if (maxex <= .001) continue;

	// check importers. for now, just in arbitrary order (no intelligent matching).
	for (map<mds_rank_t, float>::iterator im = mds_import_map[ex->second].begin();
	     im != mds_import_map[ex->second].end();
	     ++im) {
	  double maxim = get_maxim(im->first);
	  if (maxim <= .001) continue;
	  try_match(ex->second, maxex,
		    im->first, maxim);
	  if (maxex <= .001) break;
	}
      }
    }


    if (1) {
      if (beat % 2 == 1) {
	// old way
	dout(15) << "  matching big exporters to big importers" << dendl;
	// big exporters to big importers
	multimap<double,mds_rank_t>::reverse_iterator ex = exporters.rbegin();
	multimap<double,mds_rank_t>::iterator im = importers.begin();
	while (ex != exporters.rend() &&
	       im != importers.end()) {
	  double maxex = get_maxex(ex->second);
	  double maxim = get_maxim(im->second);
	  if (maxex < .001 || maxim < .001) break;
	  try_match(ex->second, maxex,
		    im->second, maxim);
	  if (maxex <= .001) ++ex;
	  if (maxim <= .001) ++im;
	}
      } else {
	// new way
	dout(15) << "  matching small exporters to big importers" << dendl;
	// small exporters to big importers
	multimap<double,mds_rank_t>::iterator ex = exporters.begin();
	multimap<double,mds_rank_t>::iterator im = importers.begin();
	while (ex != exporters.end() &&
	       im != importers.end()) {
	  double maxex = get_maxex(ex->second);
	  double maxim = get_maxim(im->second);
	  if (maxex < .001 || maxim < .001) break;
	  try_match(ex->second, maxex,
		    im->second, maxim);
	  if (maxex <= .001) ++ex;
	  if (maxim <= .001) ++im;
	}
      }
    }
  }
  try_rebalance();
}



void MDBalancer::custom_balancer() 
{
  rebalance_time = ceph_clock_now(g_ceph_context);

  dout(10) << "Preparing transfer to Lua and clearing mytargets=" << my_targets.size()
           << " imported=" << imported.size() << " exported=" << exported.size() 
           << dendl;
  my_targets.clear();
  imported.clear();
  exported.clear();
  mds->mdcache->migrator->clear_export_queue();

  // initialize Lua and construct balancer script
  lua_State *L = luaL_newstate();
  luaL_openlibs(L);
  lua_newtable(L);
  string script = LUA_IMPORT + g_conf->mds_bal_dir + LUA_INIT +
                  format_policy(g_conf->mds_bal_mdsload) + LUA_LOAD +
                  format_policy(g_conf->mds_bal_when) + LUA_WHEN +
                  format_policy(g_conf->mds_bal_where) + LUA_WHERE;

  // push metrics to Lua
  vector<pair<int, string> > args = extract_metrics();
  push_lua_metrics(L, args);
  lua_setglobal(L, "arg");

  // execute script
  if (luaL_dostring(L, script.c_str()) > 0) {
    dout(0) << " script failed: " << lua_tostring(L, lua_gettop(L)) << dendl;
    dump_balancer(script);
    lua_close(L);
    return;
  }

  // parse response
  std::istringstream targets(lua_tostring(L, lua_gettop(L)));
  lua_close(L);
  string target;
  for (mds_rank_t m = mds_rank_t(0); 
       std::getline(targets, target, ' ');
       m++)
    my_targets[m] = atof(target.c_str());
  dout(2) << " done executing, made targets=" << my_targets << dendl; 

  try_rebalance();
}



vector<pair<int, string> > MDBalancer::extract_metrics() 
{
  // pull out some important metrics and push them to lua
  vector<pair<int, string> > args;
  stringstream whoami, authmetaload, files, totalmetaload;
  whoami        << ((int) mds->get_nodeid()) + 1; // current MDS
  files         << nfiles;                        // # of files in a directory
  totalmetaload << total_meta_load;               // current metadata load on all subtrees
  authmetaload  << get_current_authmetaload();    // current metadata load on authority
  args.push_back(pair<int, string>(LUAARG_STRING, g_conf->log_file));
  args.push_back(pair<int, string>(LUAARG_NUMBER, whoami.str()));
  args.push_back(pair<int, string>(LUAARG_NUMBER, authmetaload.str()));
  args.push_back(pair<int, string>(LUAARG_NUMBER, files.str()));
  args.push_back(pair<int, string>(LUAARG_NUMBER, totalmetaload.str()));

  // pass per-MDS sextuplets to Lua balancer
  int cluster_size = mds->get_mds_map()->get_num_in_mds();
  for (mds_rank_t i=mds_rank_t(0); i < mds_rank_t(cluster_size); i++) {
    map<mds_rank_t, mds_load_t>::value_type val(i, mds_load_t(ceph_clock_now(g_ceph_context)));
    std::pair < map<mds_rank_t, mds_load_t>::iterator, bool > r(mds_load.insert(val));
    mds_load_t &load(r.first->second);
    dout(10) << "mds " << i << " has load < auth all req q cpu mem >:  "
             << load.auth.meta_load() << " " << load.all.meta_load() << " "
             << load.req_rate << " "  << load.queue_len << " " 
             << load.cpu_load_avg << " " << "-1" << dendl;

    // auth/all metadata loads are slightly stale, since they are updated with get_load. 
    stringstream authmetaload, allmetaload, req_rate, queue_len, cpu_load_avg, mem;
    authmetaload << load.auth.meta_load();      
    allmetaload  << load.all.meta_load();      
    req_rate     << load.req_rate;
    queue_len    << load.queue_len;
    cpu_load_avg << load.cpu_load_avg;
    mem          << "-1";                      // TODO: add memory pressure
    args.push_back(pair<int, string>(LUAARG_NUMBER, authmetaload.str()));
    args.push_back(pair<int, string>(LUAARG_NUMBER, allmetaload.str()));
    args.push_back(pair<int, string>(LUAARG_NUMBER, req_rate.str()));
    args.push_back(pair<int, string>(LUAARG_NUMBER, queue_len.str()));
    args.push_back(pair<int, string>(LUAARG_NUMBER, cpu_load_avg.str()));
    args.push_back(pair<int, string>(LUAARG_NUMBER, mem.str())); 
  }

  return args;
}



void MDBalancer::push_lua_args(lua_State *L, vector<pair<int, string> >& args) 
{
  int index = 1;                                
  for (vector<pair<int, string> >::iterator arg = args.begin();
       arg != args.end();
       arg++) {
    lua_pushnumber(L, index++);                   // - index of value (e.g., log file)
    if (arg->first == LUAARG_NUMBER) {            // - value of log file (e.g., the string)
      dout(10) << "pushing number: " << *arg << dendl;
      lua_pushnumber(L, atof(arg->second.c_str()));
    }
    else if (arg->first == LUAARG_STRING) {
      dout(10) << "pushing string: " << *arg << dendl;
      lua_pushstring(L, arg->second.c_str());
    }
    lua_settable(L, -3);                          // - pop 2; t[index] = val
  }
}



double MDBalancer::get_current_authmetaload() 
{
  double total = 0;
  list<CDir*> ls;
  mds->mdcache->get_root()->get_dirfrags(ls);
  for (list<CDir*>::iterator p = ls.begin();
       p != ls.end();
       p++) {
    dout(10) << "   - for p=" << **p << "total=" << total << dendl;
    total += (*p)->pop_auth_subtree_nested.meta_load(ceph_clock_now(g_ceph_context), mds->mdcache->decayrate);
  }
  return total;
}



void MDBalancer::dump_balancer(string script)
{
  std::istringstream script_stream(script);
  string line; 
  for (int i = 0; std::getline(script_stream, line); i++)
    dout(10) << i << ". " << line << dendl;
}



string MDBalancer::format_policy(string s) 
{
  string ret = s;
  replace(ret.begin(), ret.end(), '_', ' ');
  size_t pos = 0;
  while((pos = ret.find("\\n", pos)) != string::npos)
    ret.replace(pos, 2, " \n");
  return ret; 
}



void MDBalancer::try_rebalance()
{
  static int MIN_OFFLOAD = g_conf->mds_bal_minoffload;
  if (!check_targets())
    return;

  if (g_conf->mds_thrash_exports) {
    dout(5) << "mds_thrash is on; not performing standard rebalance operation!"
	    << dendl;
    return;
  }

  // make a sorted list of my imports
  map<double,CDir*>    import_pop_map;
  multimap<mds_rank_t,CDir*>  import_from_map;
  set<CDir*> fullauthsubs;

  mds->mdcache->get_fullauth_subtrees(fullauthsubs);
  for (set<CDir*>::iterator it = fullauthsubs.begin();
       it != fullauthsubs.end();
       ++it) {
    CDir *im = *it;
    if (im->get_inode()->is_stray()) continue;

    double pop = im->pop_auth_subtree.meta_load(rebalance_time, mds->mdcache->decayrate);
    if (g_conf->mds_bal_idle_threshold > 0 &&
	pop < g_conf->mds_bal_idle_threshold &&
	im->inode != mds->mdcache->get_root() &&
	im->inode->authority().first != mds->get_nodeid()) {
      dout(0) << " exporting idle (" << pop << ") import " << *im
	      << " back to mds." << im->inode->authority().first
	      << dendl;
      mds->mdcache->migrator->export_dir_nicely(im, im->inode->authority().first);
      continue;
    }

    import_pop_map[ pop ] = im;
    mds_rank_t from = im->inode->authority().first;
    dout(15) << "  map: i imported " << *im << " from " << from << dendl;
    import_from_map.insert(pair<mds_rank_t,CDir*>(from, im));
  }



  // do my exports!
  set<CDir*> already_exporting;

  for (map<mds_rank_t,double>::iterator it = my_targets.begin();
       it != my_targets.end();
       ++it) {
    mds_rank_t target = (*it).first;
    double amount = (*it).second;

    if (amount < MIN_OFFLOAD) continue;

    // this threshold was too strict for some Mantle experiments
    //if (amount / target_load < .2) continue;

    dout(5) << "want to send " << amount << " to mds." << target
      //<< " .. " << (*it).second << " * " << load_fac
	    << " -> " << amount
	    << dendl;//" .. fudge is " << fudge << dendl;
    double have = 0;


    show_imports();

    // search imports from target
    if (import_from_map.count(target)) {
      dout(5) << " aha, looking through imports from target mds." << target << dendl;
      pair<multimap<mds_rank_t,CDir*>::iterator, multimap<mds_rank_t,CDir*>::iterator> p =
	import_from_map.equal_range(target);
      while (p.first != p.second) {
	CDir *dir = (*p.first).second;
	dout(5) << "considering " << *dir << " from " << (*p.first).first << dendl;
	multimap<mds_rank_t,CDir*>::iterator plast = p.first++;

	if (dir->inode->is_base() ||
	    dir->inode->is_stray())
	  continue;
	if (dir->is_freezing() || dir->is_frozen()) continue;  // export pbly already in progress
	double pop = dir->pop_auth_subtree.meta_load(rebalance_time, mds->mdcache->decayrate);
	assert(dir->inode->authority().first == target);  // cuz that's how i put it in the map, dummy

	if (pop <= amount-have) {
	  dout(0) << "reexporting " << *dir
		  << " pop " << pop
		  << " back to mds." << target << dendl;
	  mds->mdcache->migrator->export_dir_nicely(dir, target);
	  have += pop;
	  import_from_map.erase(plast);
	  import_pop_map.erase(pop);
	} else {
	  dout(5) << "can't reexport " << *dir << ", too big " << pop << dendl;
	}
	if (amount-have < MIN_OFFLOAD) break;
      }
    }
    if (amount-have < MIN_OFFLOAD) {
      continue;
    }

    // any other imports
    if (false)
      for (map<double,CDir*>::iterator import = import_pop_map.begin();
	   import != import_pop_map.end();
	   import++) {
	CDir *imp = (*import).second;
	if (imp->inode->is_base() ||
	    imp->inode->is_stray())
	  continue;

	double pop = (*import).first;
	if (pop < amount-have || pop < MIN_REEXPORT) {
	  dout(0) << "reexporting " << *imp
		  << " pop " << pop
		  << " back to mds." << imp->inode->authority()
		  << dendl;
	  have += pop;
	  mds->mdcache->migrator->export_dir_nicely(imp, imp->inode->authority().first);
	}
	if (amount-have < MIN_OFFLOAD) break;
      }
    if (amount-have < MIN_OFFLOAD) {
      //fudge = amount-have;
      continue;
    }

    // okay, search for fragments of my workload
    set<CDir*> candidates;
    mds->mdcache->get_fullauth_subtrees(candidates);

    list<CDir*> exports;

    // MSEVILLA: should we move this entire auth?
    // can I put myself (irrespective if I am a whole directory or dirfrag)
    multimap<double, CDir*> smaller;
    for (set<CDir*>::iterator root = candidates.begin();
	 root != candidates.end();
	 ++root) {
      CDir *subdir = *root;
      string path;
      subdir->get_inode()->make_path_string_projected(path);
      if (path.find("~") == 0) continue;
      if (subdir->get_inode()->is_stray()) continue;
      if (!subdir->is_auth()) continue;
      if (already_exporting.count(subdir)) continue;
      if (subdir->is_frozen()) continue;
      double rootpop = subdir->pop_auth_subtree.meta_load(rebalance_time, mds->mdcache->decayrate);
      
      dout(7) << "consider exporting rootpop=" << rootpop 
              << " amount-have=" << amount << "-" << have << "=" << amount-have 
              << " root=" << *subdir << dendl;
      if (rootpop < amount - have) {
        dout(7) << "it's small enough, let's kick it off to the fragment selector." << dendl;
        if (already_exporting.count(subdir)) continue;
        smaller.insert(pair<double,CDir*>(rootpop, subdir));
      }
    }
    if (g_conf->mds_bal_dir.compare("") && g_conf->mds_bal_howmuch.compare(""))
      dirfrag_selector(smaller, amount, exports, have, already_exporting); 
    else {
      // Use the old balancer policy (send off biggest dirfrags)
      for (multimap<double,CDir*>::reverse_iterator it = smaller.rbegin();
           it != smaller.rend();
           ++it) {
        exports.push_back((*it).second);
        have += (*it).first;
        if (have > amount - MIN_OFFLOAD)
          break;
      }
    }
    smaller.clear();

    // Ok, drill down       
    if (have < amount-MIN_OFFLOAD) {
      for (set<CDir*>::iterator pot = candidates.begin();
           pot != candidates.end();
           ++pot) {
        // if we aren't already exporting 
        if (already_exporting.count(*pot)) continue;
        if (find(exports.begin(), exports.end(), *pot) == exports.end())
          find_exports(*pot, amount, exports, have, already_exporting);
        if (have > amount-MIN_OFFLOAD)
          break;
      }
    }
    //fudge = amount - have;

    for (list<CDir*>::iterator it = exports.begin(); it != exports.end(); ++it) {
      dout(0) << "   - exporting "
	       << (*it)->pop_auth_subtree
	       << " "
	       << (*it)->pop_auth_subtree.meta_load(rebalance_time, mds->mdcache->decayrate)
	       << " to mds." << target
	       << " " << **it
	       << dendl;
      mds->mdcache->migrator->export_dir_nicely(*it, target);
    }
  }

  dout(5) << "rebalance done, nfiles=" << nfiles << dendl;
  show_imports();
}


/* returns true if all my_target MDS are in the MDSMap.
 */
bool MDBalancer::check_targets()
{
  // get MonMap's idea of my_targets
  const set<mds_rank_t>& map_targets = mds->mdsmap->get_mds_info(mds->whoami).export_targets;

  bool send = false;
  bool ok = true;

  // make sure map targets are in the old_prev_targets map
  for (set<mds_rank_t>::iterator p = map_targets.begin(); p != map_targets.end(); ++p) {
    if (old_prev_targets.count(*p) == 0)
      old_prev_targets[*p] = 0;
    if (my_targets.count(*p) == 0)
      old_prev_targets[*p]++;
  }

  // check if the current MonMap has all our targets
  set<mds_rank_t> need_targets;
  for (map<mds_rank_t,double>::iterator i = my_targets.begin();
       i != my_targets.end();
       ++i) {
    need_targets.insert(i->first);
    old_prev_targets[i->first] = 0;

    if (!map_targets.count(i->first)) {
      dout(20) << " target mds." << i->first << " not in map's export_targets" << dendl;
      send = true;
      ok = false;
    }
  }

  set<mds_rank_t> want_targets = need_targets;
  map<mds_rank_t, int>::iterator p = old_prev_targets.begin();
  while (p != old_prev_targets.end()) {
    if (map_targets.count(p->first) == 0 &&
	need_targets.count(p->first) == 0) {
      old_prev_targets.erase(p++);
      continue;
    }
    dout(20) << " target mds." << p->first << " has been non-target for " << p->second << dendl;
    if (p->second < g_conf->mds_bal_target_removal_min)
      want_targets.insert(p->first);
    if (p->second >= g_conf->mds_bal_target_removal_max)
      send = true;
    ++p;
  }

  dout(10) << "check_targets have " << map_targets << " need " << need_targets << " want " << want_targets << dendl;

  if (send) {
    MMDSLoadTargets* m = new MMDSLoadTargets(mds_gid_t(mds->monc->get_global_id()), want_targets);
    mds->monc->send_mon_message(m);
  }
  return ok;
}

void MDBalancer::find_exports(CDir *dir,
                              double amount,
                              list<CDir*>& exports,
                              double& have,
                              set<CDir*>& already_exporting)
{
  double need = amount - have;
  if (need < amount * g_conf->mds_bal_min_start)
    return;   // good enough!
  double needmax = need * g_conf->mds_bal_need_max;
  double needmin = need * g_conf->mds_bal_need_min;
  double midchunk = need * g_conf->mds_bal_midchunk;
  double minchunk = need * g_conf->mds_bal_minchunk;

  list<CDir*> bigger_rep, bigger_unrep;
  multimap<double, CDir*> smaller;

  double dir_pop = dir->pop_auth_subtree.meta_load(rebalance_time, mds->mdcache->decayrate);
  dout(7) << " find_exports in " << dir_pop << " " << *dir << " need " << need << " (" << needmin << " - " << needmax << ")" << dendl;

  double subdir_sum = 0;
  for (CDir::map_t::iterator it = dir->begin();
       it != dir->end();
       ++it) {
    CInode *in = it->second->get_linkage()->get_inode();
    if (!in) continue;
    if (!in->is_dir()) continue;

    list<CDir*> dfls;
    in->get_dirfrags(dfls);
    for (list<CDir*>::iterator p = dfls.begin();
	 p != dfls.end();
	 ++p) {
      CDir *subdir = *p;
      if (!subdir->is_auth()) continue;
      if (already_exporting.count(subdir)) continue;

      if (subdir->is_frozen()) continue;  // can't export this right now!

      // how popular?
      double pop = subdir->pop_auth_subtree.meta_load(rebalance_time, mds->mdcache->decayrate);
      subdir_sum += pop;
      dout(15) << "   subdir pop " << pop << " " << *subdir << dendl;

      if (pop < minchunk) continue;

      // lucky find?
      if (pop > needmin && pop < needmax) {
	exports.push_back(subdir);
	already_exporting.insert(subdir);
	have += pop;
	return;
      }

      if (pop > need) {
	if (subdir->is_rep())
	  bigger_rep.push_back(subdir);
	else
	  bigger_unrep.push_back(subdir);
      } else
	smaller.insert(pair<double,CDir*>(pop, subdir));
    }
  }
  dout(15) << "   sum " << subdir_sum << " / " << dir_pop << dendl;

  // grab some sufficiently big small items
  multimap<double,CDir*>::reverse_iterator it;
  if (g_conf->mds_bal_dir.compare("") && g_conf->mds_bal_howmuch.compare("")) {
    dirfrag_selector(smaller, amount, exports, have, already_exporting); 
    if (have > needmin)
      return;
    it = smaller.rbegin();
  } else {
    for (it = smaller.rbegin();
         it != smaller.rend();
         ++it) {

      if ((*it).first < midchunk)
        break;  // try later

      dout(7) << "   taking smaller " << *(*it).second << dendl;

      exports.push_back((*it).second);
      already_exporting.insert((*it).second);
      have += (*it).first;
      if (have > needmin)
        return;
    }
  }

  // apprently not enough; drill deeper into the hierarchy (if non-replicated)
  for (list<CDir*>::iterator it = bigger_unrep.begin();
       it != bigger_unrep.end();
       ++it) {
    dout(15) << "   descending into " << **it << dendl;
    find_exports(*it, amount, exports, have, already_exporting);
    if (have > needmin)
      return;
  }

  // ok fine, use smaller bits
  if (!g_conf->mds_bal_dir.compare("") || !g_conf->mds_bal_howmuch.compare("")) {
    for (;
         it != smaller.rend();
         ++it) {
      dout(7) << "   taking (much) smaller " << it->first << " " << *(*it).second << dendl;

      exports.push_back((*it).second);
      already_exporting.insert((*it).second);
      have += (*it).first;
      if (have > needmin)
        return;
    }
  }

  // ok fine, drill into replicated dirs
  for (list<CDir*>::iterator it = bigger_rep.begin();
       it != bigger_rep.end();
       ++it) {
    dout(7) << "   descending into replicated " << **it << dendl;
    find_exports(*it, amount, exports, have, already_exporting);
    if (have > needmin)
      return;
  }

}



void MDBalancer::dirfrag_selector(multimap<double, CDir*> smaller,
                                           double amount,
                                           list<CDir*>& exports,
                                           double& have,
                                           set<CDir*>& already_exporting)
{
  multimap<double,CDir*>::reverse_iterator it;
  if (smaller.size() <= 0) {
    it = smaller.rbegin();
    dout(10) << "not enough fragments to select from, size=" << smaller.size() << dendl;
  } else {
    lua_State *L = luaL_newstate();
    luaL_openlibs(L);
    lua_newtable(L);

    // construct the Lua script
    char script[strlen(LUA_IMPORT) +
                strlen(g_conf->mds_bal_dir.c_str())+
                strlen(LUA_INIT_HOWMUCH) +
                strlen(g_conf->mds_bal_howmuch.c_str()) + 
                strlen(LUA_HOWMUCH)];
    strcpy(script, LUA_IMPORT);
    strcat(script, g_conf->mds_bal_dir.c_str());
    strcat(script, LUA_INIT_HOWMUCH);
    strcat(script, g_conf->mds_bal_howmuch.c_str());
    strcat(script, LUA_HOWMUCH);

    // pull out the important metrics
    const char *log = g_conf->log_file.c_str();

    dout(10) << "sending frags to Lua fragselector"
             << " log=" << log 
             << " target=" << amount 
             << dendl;
    vector<CDir *> frags;
    int index = 1;
    lua_pushnumber(L, index++);
    lua_pushstring(L, log);
    lua_settable(L, -3);
    lua_pushnumber(L, index++);
    lua_pushnumber(L, amount);
    lua_settable(L, -3);
    for (it = smaller.rbegin();
       it != smaller.rend();
       ++it) {
      dout(10) << "pushing dirfrag: " << (*it).first << dendl;
      lua_pushnumber(L, index++);
      lua_pushnumber(L, (*it).first);
      lua_settable(L, -3);
      frags.push_back((*it).second);
    }

    dout(20) << " kicking control off to Lua so it can select the dirfrags" << dendl;
    lua_setglobal(L, "arg");
    if (luaL_dostring(L, script) > 0) {
      dout(0) << " script failed: " << lua_tostring(L, lua_gettop(L)) << dendl;
      dump_balancer(script);
      lua_close(L);
      return;
    }

    std::istringstream selected_dirfrags(lua_tostring(L, lua_gettop(L)));
    lua_close(L);
    string dirfrag;
    while (std::getline(selected_dirfrags, dirfrag, ' ')) {
      int df_index = atof(dirfrag.c_str()) - 1;
      if (df_index < 0) continue;
      double pop = frags[df_index]->pop_auth_subtree.meta_load(rebalance_time, mds->mdcache->decayrate);

      dout(10) << " exporting df=" << df_index << " dirfrag=" << dirfrag << " pop=" << pop << " frag=" << *frags[df_index] << dendl;
      exports.push_back(frags[df_index]);
      already_exporting.insert(frags[df_index]);
      have += pop;
    }
  }
}



void MDBalancer::hit_nfiles(double n) 
{
  nfiles += n;
}

void MDBalancer::hit_nfiles_dir(CInode *in)
{
  if (in != NULL) {
    if (in->is_dir()) { 
      list<CDir*> dirfrags;
      in->get_dirfrags(dirfrags);
      dout(20) << "found " << dirfrags.size() << " dirfrags for " << *in << dendl;
      for (list<CDir*>::iterator dirfrags_it = dirfrags.begin();
           dirfrags_it != dirfrags.end();
           ++dirfrags_it) {
        CDir *dir = *dirfrags_it;
        string path;
        dir->get_inode()->make_path_string_projected(path);
        for (CDir::map_t::iterator direntry_it = dir->begin();
             direntry_it != dir->end();
             ++direntry_it) {
          hit_nfiles_dir(direntry_it->second->get_linkage()->get_inode());
        }
      }
    } else {
      dout(20) << "decrementing nfiles for " << *in << dendl;
      hit_nfiles(-1);
    }
  }
}

void MDBalancer::hit_inode(utime_t now, CInode *in, int type, int who)
{
  // hit inode
  in->pop.get(type).hit(now, mds->mdcache->decayrate);

  if (in->get_parent_dn())
    hit_dir(now, in->get_parent_dn()->get_dir(), type, who);
}
/*
  // hit me
  in->popularity[MDS_POP_JUSTME].pop[type].hit(now);
  in->popularity[MDS_POP_NESTED].pop[type].hit(now);
  if (in->is_auth()) {
    in->popularity[MDS_POP_CURDOM].pop[type].hit(now);
    in->popularity[MDS_POP_ANYDOM].pop[type].hit(now);

    dout(20) << "hit_inode " << type << " pop "
	     << in->popularity[MDS_POP_JUSTME].pop[type].get(now) << " me, "
	     << in->popularity[MDS_POP_NESTED].pop[type].get(now) << " nested, "
	     << in->popularity[MDS_POP_CURDOM].pop[type].get(now) << " curdom, "
	     << in->popularity[MDS_POP_CURDOM].pop[type].get(now) << " anydom"
	     << " on " << *in
	     << dendl;
  } else {
    dout(20) << "hit_inode " << type << " pop "
	     << in->popularity[MDS_POP_JUSTME].pop[type].get(now) << " me, "
	     << in->popularity[MDS_POP_NESTED].pop[type].get(now) << " nested, "
      	     << " on " << *in
	     << dendl;
  }

  // hit auth up to import
  CDir *dir = in->get_parent_dir();
  if (dir) hit_dir(now, dir, type);
*/


void MDBalancer::hit_dir(utime_t now, CDir *dir, int type, int who, double amount)
{
  // hit me
  double v = dir->pop_me.get(type).hit(now, amount);

  //if (dir->ino() == inodeno_t(0x10000000000))
  //dout(0) << "hit_dir " << type << " pop " << v << " in " << *dir << dendl;

  // split/merge
  if (g_conf->mds_bal_frag && g_conf->mds_bal_fragment_interval > 0 &&
      !dir->inode->is_base() &&        // not root/base (for now at least)
      dir->is_auth()) {

    dout(20) << "hit_dir " << type << " pop is " << v << ", frag " << dir->get_frag()
	     << " size " << dir->get_frag_size() << dendl;

    // split
    if (g_conf->mds_bal_split_size > 0 &&
	(dir->should_split() ||
	 (v > g_conf->mds_bal_split_rd && type == META_POP_IRD) ||
	 (v > g_conf->mds_bal_split_wr && type == META_POP_IWR)) &&
	split_queue.count(dir->dirfrag()) == 0) {
      dout(10) << "hit_dir " << type << " pop is " << v << ", putting in split_queue: " << *dir << dendl;
      split_queue.insert(dir->dirfrag());
    }

    // merge?
    if (dir->get_frag() != frag_t() && dir->should_merge() &&
	merge_queue.count(dir->dirfrag()) == 0) {
      dout(10) << "hit_dir " << type << " pop is " << v << ", putting in merge_queue: " << *dir << dendl;
      merge_queue.insert(dir->dirfrag());
    }
  }

  // replicate?
  if (type == META_POP_IRD && who >= 0) {
    dir->pop_spread.hit(now, mds->mdcache->decayrate, who);
  }

  double rd_adj = 0;
  if (type == META_POP_IRD &&
      dir->last_popularity_sample < last_sample) {
    float dir_pop = dir->pop_auth_subtree.get(type).get(now, mds->mdcache->decayrate);    // hmm??
    dir->last_popularity_sample = last_sample;
    float pop_sp = dir->pop_spread.get(now, mds->mdcache->decayrate);
    dir_pop += pop_sp * 10;

    //if (dir->ino() == inodeno_t(0x10000000002))
    if (pop_sp > 0) {
      dout(20) << "hit_dir " << type << " pop " << dir_pop << " spread " << pop_sp
	      << " " << dir->pop_spread.last[0]
	      << " " << dir->pop_spread.last[1]
	      << " " << dir->pop_spread.last[2]
	      << " " << dir->pop_spread.last[3]
	      << " in " << *dir << dendl;
    }

    if (dir->is_auth() && !dir->is_ambiguous_auth()) {
      if (!dir->is_rep() &&
	  dir_pop >= g_conf->mds_bal_replicate_threshold) {
	// replicate
	float rdp = dir->pop_me.get(META_POP_IRD).get(now, mds->mdcache->decayrate);
	rd_adj = rdp / mds->get_mds_map()->get_num_in_mds() - rdp;
	rd_adj /= 2.0;  // temper somewhat

	dout(0) << "replicating dir " << *dir << " pop " << dir_pop << " .. rdp " << rdp << " adj " << rd_adj << dendl;

	dir->dir_rep = CDir::REP_ALL;
	mds->mdcache->send_dir_updates(dir, true);

	// fixme this should adjust the whole pop hierarchy
	dir->pop_me.get(META_POP_IRD).adjust(rd_adj);
	dir->pop_auth_subtree.get(META_POP_IRD).adjust(rd_adj);
      }

      if (dir->ino() != 1 &&
	  dir->is_rep() &&
	  dir_pop < g_conf->mds_bal_unreplicate_threshold) {
	// unreplicate
	dout(0) << "unreplicating dir " << *dir << " pop " << dir_pop << dendl;

	dir->dir_rep = CDir::REP_NONE;
	mds->mdcache->send_dir_updates(dir);
      }
    }
  }

  // adjust ancestors
  bool hit_subtree = dir->is_auth();         // current auth subtree (if any)
  bool hit_subtree_nested = dir->is_auth();  // all nested auth subtrees

  while (1) {
    dir->pop_nested.get(type).hit(now, amount);
    if (rd_adj != 0.0)
      dir->pop_nested.get(META_POP_IRD).adjust(now, mds->mdcache->decayrate, rd_adj);

    if (hit_subtree) {
      dir->pop_auth_subtree.get(type).hit(now, amount);
      if (rd_adj != 0.0)
	dir->pop_auth_subtree.get(META_POP_IRD).adjust(now, mds->mdcache->decayrate, rd_adj);
    }

    if (hit_subtree_nested) {
      dir->pop_auth_subtree_nested.get(type).hit(now, mds->mdcache->decayrate, amount);
      if (rd_adj != 0.0)
	dir->pop_auth_subtree_nested.get(META_POP_IRD).adjust(now, mds->mdcache->decayrate, rd_adj);
    }

    if (dir->is_subtree_root())
      hit_subtree = false;                // end of auth domain, stop hitting auth counters.

    if (dir->inode->get_parent_dn() == 0) break;
    dir = dir->inode->get_parent_dn()->get_dir();
  }
}


/*
 * subtract off an exported chunk.
 *  this excludes *dir itself (encode_export_dir should have take care of that)
 *  we _just_ do the parents' nested counters.
 *
 * NOTE: call me _after_ forcing *dir into a subtree root,
 *       but _before_ doing the encode_export_dirs.
 */
void MDBalancer::subtract_export(CDir *dir, utime_t now)
{
  dirfrag_load_vec_t subload = dir->pop_auth_subtree;

  while (true) {
    dir = dir->inode->get_parent_dir();
    if (!dir) break;

    dir->pop_nested.sub(now, mds->mdcache->decayrate, subload);
    dir->pop_auth_subtree_nested.sub(now, mds->mdcache->decayrate, subload);
  }
}


void MDBalancer::add_import(CDir *dir, utime_t now)
{
  dirfrag_load_vec_t subload = dir->pop_auth_subtree;

  while (true) {
    dir = dir->inode->get_parent_dir();
    if (!dir) break;

    dir->pop_nested.add(now, mds->mdcache->decayrate, subload);
    dir->pop_auth_subtree_nested.add(now, mds->mdcache->decayrate, subload);
  }
}


void MDBalancer::show_imports(bool external)
{
  mds->mdcache->show_subtrees();
}
