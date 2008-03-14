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

#include "OSDMap.h"

#include "config.h"
#define dout generic_dout
#define derr generic_derr


void OSDMap::build_simple(epoch_t e, ceph_fsid &fsid,
			  int num_osd, int pg_bits, int mds_local_osd)
{
  dout(10) << "build_simple on " << num_osd
	   << " osds with " << pg_bits << " pg bits per osd" << dendl;
  epoch = e;
  set_fsid(fsid);
  ctime = g_clock.now();

  set_max_osd(num_osd);
  //pg_num = num_osd << pg_bits;  // not here.. must be >0 'up' osds!

  // crush map
  map<int,double> weights;
  build_simple_crush_map(crush, num_osd, weights);

  for (int i=0; i<num_osd; i++) {
    set_state(i, CEPH_OSD_EXISTS|CEPH_OSD_CLEAN);
    set_offload(i, CEPH_OSD_OUT);
  }
  
  if (mds_local_osd) {
    set_max_osd(mds_local_osd+num_osd);

    // add mds local osds, but don't put them in the crush mapping func
    for (int i=0; i<mds_local_osd; i++) {
      set_max_osd(i+num_osd);
      set_state(i, CEPH_OSD_EXISTS);
      set_offload(i, CEPH_OSD_OUT);
    }
  }
}

void OSDMap::build_simple_crush_map(CrushWrapper& crush, int num_osd, map<int,double>& weights)
{
  // new
  crush.create();

  int ndom = MAX(g_conf.osd_max_rep, g_conf.osd_max_raid_width);
  if (num_osd >= ndom*2) {
    int ritems[ndom];
    int rweights[ndom];

    int nper = ((num_osd - 1) / ndom) + 1;
    derr(0) << ndom << " failure domains, " << nper << " osds each" << dendl;

    int o = 0;
    for (int i=0; i<ndom; i++) {
      int items[nper];
      //int w[nper];
      int j;
      rweights[i] = 0;
      for (j=0; j<nper; j++, o++) {
	if (o == num_osd) break;
	dout(20) << "added osd" << o << dendl;
	items[j] = o;
	//w[j] = weights[o] ? (0x10000 - (int)(weights[o] * 0x10000)):0x10000;
	//rweights[i] += w[j];
	rweights[i] += 0x10000;
      }

      crush_bucket_uniform *domain = crush_make_uniform_bucket(1, j, items, 0x10000);
      ritems[i] = crush_add_bucket(crush.crush, 0, (crush_bucket*)domain);
      dout(20) << "added domain bucket i " << ritems[i] << " of size " << j << dendl;
    }
    
    // root
    crush_bucket_list *root = crush_make_list_bucket(2, ndom, ritems, rweights);
    int rootid = crush_add_bucket(crush.crush, 0, (crush_bucket*)root);
    
    // rules
    // replication
    for (int pool=0; pool<1; pool++) {
      // size 1..ndom
      crush_rule *rule = crush_make_rule(4, pool, CEPH_PG_TYPE_REP, 1, ndom);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, CRUSH_CHOOSE_N, 1); // choose N domains
      crush_rule_set_step(rule, 2, CRUSH_RULE_CHOOSE_FIRSTN, 1, 0);  // and 1 device in each
      crush_rule_set_step(rule, 3, CRUSH_RULE_EMIT, 0, 0);
      crush_add_rule(crush.crush, rule, -1);
    }

    // raid
    for (int pool=0; pool<1; pool++) {
      crush_rule *rule = crush_make_rule(4, pool, CEPH_PG_TYPE_RAID4, g_conf.osd_min_raid_width, g_conf.osd_max_raid_width);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_INDEP, CRUSH_CHOOSE_N, 1);
      crush_rule_set_step(rule, 2, CRUSH_RULE_CHOOSE_INDEP, 1, 0);
      crush_rule_set_step(rule, 3, CRUSH_RULE_EMIT, 0, 0);
      crush_add_rule(crush.crush, rule, -1);
    }
    
  } else {
    // one bucket

    int items[num_osd];
    for (int i=0; i<num_osd; i++) 
      items[i] = i;
    
    crush_bucket_uniform *b = crush_make_uniform_bucket(1, num_osd, items, 0x10000);
    int root = crush_add_bucket(crush.crush, 0, (crush_bucket*)b);
    
    // replication
    for (int pool=0; pool<1; pool++) {
      crush_rule *rule = crush_make_rule(3, pool, CEPH_PG_TYPE_REP, 1, g_conf.osd_max_rep);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, root, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, CRUSH_CHOOSE_N, 0);
      crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
      crush_add_rule(crush.crush, rule, -1);
    }

    // raid4
    for (int pool=0; pool<1; pool++) {
      crush_rule *rule = crush_make_rule(3, pool, CEPH_PG_TYPE_RAID4, g_conf.osd_min_raid_width, g_conf.osd_max_raid_width);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, root, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_INDEP, CRUSH_CHOOSE_N, 0);
      crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
      crush_add_rule(crush.crush, rule, -1);
    }
  }
  
  crush.finalize();

  // mark all in
  for (int i=0; i<num_osd; i++)
    crush.set_offload(i, CEPH_OSD_IN);

  dout(20) << "crush max_devices " << crush.crush->max_devices << dendl;
}
