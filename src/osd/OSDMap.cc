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



void OSDMap::print(ostream& out)
{
  out << "epoch " << get_epoch() << "\n"
      << "fsid " << get_fsid() << "\n"
      << "ctime " << get_ctime() << "\n"
      << "mtime " << get_mtime() << "\n"
      << std::endl;
  out << "pg_num " << get_pg_num() << "\n"
      << "pgp_num " << get_pgp_num() << "\n"
      << "lpg_num " << get_lpg_num() << "\n"
      << "lpgp_num " << get_lpgp_num() << "\n"
      << "last_pg_change " << get_last_pg_change() << "\n"
      << std::endl;
  out << "max_osd " << get_max_osd() << "\n";
  for (int i=0; i<get_max_osd(); i++) {
    if (exists(i)) {
      out << "osd" << i;
      out << (is_up(i) ? " up":" down");
      if (is_up(i))
	out << " " << get_addr(i);
      osd_info_t& info = get_info(i);
      out << " (up_from " << info.up_from
	  << " up_thru " << info.up_thru
	  << " down_at " << info.down_at
	  << " last_clean " << info.last_clean_first << "-" << info.last_clean_last << ")";
      out << (is_in(i) ? " in":" out");
      if (is_in(i))
	out << " weight " << get_weightf(i);
      out << "\n";
    }
  }
  out << std::endl;
  
  for (hash_map<entity_addr_t,utime_t>::iterator p = blacklist.begin();
       p != blacklist.end();
       p++)
    out << "blacklist " << p->first << " expires " << p->second << "\n";
  
  // ignore pg_swap_primary
  
  out << "max_snap " << get_max_snap() << "\n"
      << "removed_snaps " << get_removed_snaps() << "\n"
      << std::endl;
}

void OSDMap::print_summary(ostream& out)
{
  out << "e" << get_epoch() << ": "
      << get_num_osds() << " osds: "
      << get_num_up_osds() << " up, " 
      << get_num_in_osds() << " in";
  if (blacklist.size())
    out << ", " << blacklist.size() << " blacklisted";
}


void OSDMap::build_simple(epoch_t e, ceph_fsid_t &fsid,
			  int num_osd, int num_dom, int pg_bits, int lpg_bits,
			  int mds_local_osd)
{
  dout(10) << "build_simple on " << num_osd
	   << " osds with " << pg_bits << " pg bits per osd, "
	   << lpg_bits << " lpg bits" << dendl;
  epoch = e;
  set_fsid(fsid);
  mtime = ctime = g_clock.now();

  set_max_osd(num_osd);
  pg_num = pgp_num = num_osd << pg_bits;
  lpg_num = lpgp_num = lpg_bits ? (1 << (lpg_bits-1)) : 0;
  
  // crush map
  build_simple_crush_map(crush, num_osd, num_dom);

  for (int i=0; i<num_osd; i++) {
    set_state(i, CEPH_OSD_EXISTS);
    set_weight(i, CEPH_OSD_OUT);
  }
  
  if (mds_local_osd) {
    set_max_osd(mds_local_osd+num_osd);

    // add mds local osds, but don't put them in the crush mapping func
    for (int i=0; i<mds_local_osd; i++) {
      set_state(i+num_osd, CEPH_OSD_EXISTS);
      set_weight(i+num_osd, CEPH_OSD_OUT);
    }
  }
}

void OSDMap::build_simple_crush_map(CrushWrapper& crush, int num_osd,
				    int num_dom)
{
  // new
  crush.create();

  crush.set_type_name(1, "domain");
  crush.set_type_name(2, "pool");

  int npools = 3;

  int minrep = g_conf.osd_min_rep;
  int ndom = num_dom;
  if (!ndom)
    ndom = MAX(g_conf.osd_max_rep, g_conf.osd_max_raid_width);
  if (num_osd >= ndom*3 &&
      num_osd > 8) {
    int ritems[ndom];
    int rweights[ndom];

    int nper = ((num_osd - 1) / ndom) + 1;
    derr(0) << ndom << " failure domains, " << nper << " osds each" << dendl;
    
    int o = 0;
    for (int i=0; i<ndom; i++) {
      int items[nper], weights[nper];      
      int j;
      rweights[i] = 0;
      for (j=0; j<nper; j++, o++) {
	if (o == num_osd) break;
	dout(20) << "added osd" << o << dendl;
	items[j] = o;
	weights[j] = 0x10000;
	//w[j] = weights[o] ? (0x10000 - (int)(weights[o] * 0x10000)):0x10000;
	//rweights[i] += w[j];
	rweights[i] += 0x10000;
      }

      crush_bucket *domain = crush_make_bucket(CRUSH_BUCKET_UNIFORM, 1, j, items, weights);
      ritems[i] = crush_add_bucket(crush.crush, 0, domain);
      dout(20) << "added domain bucket i " << ritems[i] << " of size " << j << dendl;

      char bname[10];
      sprintf(bname, "dom%d", i);
      crush.set_item_name(ritems[i], bname);
    }
    
    // root
    crush_bucket *root = crush_make_bucket(CRUSH_BUCKET_STRAW, 2, ndom, ritems, rweights);
    int rootid = crush_add_bucket(crush.crush, 0, root);
    crush.set_item_name(rootid, "root");

    // rules
    // replication
    for (int pool=0; pool<npools; pool++) {
      // size minrep..ndom
      crush_rule *rule = crush_make_rule(3, pool, CEPH_PG_TYPE_REP, minrep, g_conf.osd_max_rep);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_LEAF_FIRSTN, CRUSH_CHOOSE_N, 1); // choose N domains
      crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
      int rno = crush_add_rule(crush.crush, rule, -1);
      crush.set_rule_name(rno, get_pool_name(pool));
    }

    // raid
    if (false && g_conf.osd_min_raid_width <= g_conf.osd_max_raid_width)
      for (int pool=0; pool<npools; pool++) {
	crush_rule *rule = crush_make_rule(3, pool, CEPH_PG_TYPE_RAID4, g_conf.osd_min_raid_width, g_conf.osd_max_raid_width);
	crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
	crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_LEAF_INDEP, CRUSH_CHOOSE_N, 1);
	crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
	crush_add_rule(crush.crush, rule, -1);
      }
    
  } else {
    // one bucket

    int items[num_osd];
    int weights[num_osd];
    for (int i=0; i<num_osd; i++) {
      items[i] = i;
      weights[i] = 0x10000;
    }

    crush_bucket *b = crush_make_bucket(CRUSH_BUCKET_STRAW, 1, num_osd, items, weights);
    int rootid = crush_add_bucket(crush.crush, 0, b);
    crush.set_item_name(rootid, "root");

    // replication
    for (int pool=0; pool<npools; pool++) {
      crush_rule *rule = crush_make_rule(3, pool, CEPH_PG_TYPE_REP, g_conf.osd_min_rep, g_conf.osd_max_rep);
      crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
      crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_FIRSTN, CRUSH_CHOOSE_N, 0);
      crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
      int rno = crush_add_rule(crush.crush, rule, -1);
      crush.set_rule_name(rno, get_pool_name(pool));
    }

    // raid4
    if (false && g_conf.osd_min_raid_width <= g_conf.osd_max_raid_width)
      for (int pool=0; pool<npools; pool++) {
	crush_rule *rule = crush_make_rule(3, pool, CEPH_PG_TYPE_RAID4, g_conf.osd_min_raid_width, g_conf.osd_max_raid_width);
	crush_rule_set_step(rule, 0, CRUSH_RULE_TAKE, rootid, 0);
	crush_rule_set_step(rule, 1, CRUSH_RULE_CHOOSE_INDEP, CRUSH_CHOOSE_N, 0);
	crush_rule_set_step(rule, 2, CRUSH_RULE_EMIT, 0, 0);
	crush_add_rule(crush.crush, rule, -1);
      }
  }

  crush.finalize();

  dout(20) << "crush max_devices " << crush.crush->max_devices << dendl;
}

