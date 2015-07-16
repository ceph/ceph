// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <memory>
#include "include/memory.h"
#include <errno.h>
#include <map>
#include <list>
#include <string>
#include <sstream>

#include "acconfig.h"

#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#ifdef HAVE_SYS_MOUNT_H
#include <sys/mount.h>
#endif

#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif

#include "messages/MMonHealth.h"
#include "include/types.h"
#include "include/Context.h"
#include "include/assert.h"
#include "common/Formatter.h"
#include "common/errno.h"

#include "mon/Monitor.h"
#include "mon/QuorumService.h"
#include "mon/DataHealthService.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)
static ostream& _prefix(std::ostream *_dout, const Monitor *mon,
                        const DataHealthService *svc) {
  assert(mon != NULL);
  assert(svc != NULL);
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name() << ")." << svc->get_name()
                << "(" << svc->get_epoch() << ") ";
}

void DataHealthService::start_epoch()
{
  dout(10) << __func__ << " epoch " << get_epoch() << dendl;
  // we are not bound by election epochs, but we should clear the stats
  // everytime an election is triggerd.  As far as we know, a monitor might
  // have been running out of disk space and someone fixed it.  We don't want
  // to hold the cluster back, even confusing the user, due to some possibly
  // outdated stats.
  stats.clear();
  last_warned_percent = 0;
}

void DataHealthService::get_health(
    Formatter *f,
    list<pair<health_status_t,string> >& summary,
    list<pair<health_status_t,string> > *detail)
{
  dout(10) << __func__ << dendl;
  if (f) {
    f->open_object_section("data_health");
    f->open_array_section("mons");
  }

  for (map<entity_inst_t,DataStats>::iterator it = stats.begin();
       it != stats.end(); ++it) {
    string mon_name = mon->monmap->get_name(it->first.addr);
    DataStats& stats = it->second;

    health_status_t health_status = HEALTH_OK;
    string health_detail;
    if (stats.fs_stats.avail_percent <= g_conf->mon_data_avail_crit) {
      health_status = HEALTH_ERR;
      health_detail = "low disk space, shutdown imminent";
    } else if (stats.fs_stats.avail_percent <= g_conf->mon_data_avail_warn) {
      health_status = HEALTH_WARN;
      health_detail = "low disk space";
    }

    if (stats.store_stats.bytes_total >= g_conf->mon_data_size_warn) {
      if (health_status > HEALTH_WARN)
        health_status = HEALTH_WARN;
      if (!health_detail.empty())
        health_detail.append("; ");
      stringstream ss;
      ss << "store is getting too big! "
         << prettybyte_t(stats.store_stats.bytes_total)
         << " >= " << prettybyte_t(g_conf->mon_data_size_warn);
      health_detail.append(ss.str());
    }

    if (health_status != HEALTH_OK) {
      stringstream ss;
      ss << "mon." << mon_name << " " << health_detail;
      summary.push_back(make_pair(health_status, ss.str()));
      ss << " -- " <<  stats.fs_stats.avail_percent << "% avail";
      if (detail)
	detail->push_back(make_pair(health_status, ss.str()));
    }

    if (f) {
      f->open_object_section("mon");
      f->dump_string("name", mon_name.c_str());
      // leave this unenclosed by an object section to avoid breaking backward-compatibility
      stats.dump(f);
      f->dump_stream("health") << health_status;
      if (health_status != HEALTH_OK)
        f->dump_string("health_detail", health_detail);
      f->close_section();
    }
  }

  if (f) {
    f->close_section(); // mons
    f->close_section(); // data_health
  }
}

int DataHealthService::update_store_stats(DataStats &ours)
{
  map<string,uint64_t> extra;
  uint64_t store_size = mon->store->get_estimated_size(extra);
  assert(store_size > 0);

  ours.store_stats.bytes_total = store_size;
  ours.store_stats.bytes_sst = extra["sst"];
  ours.store_stats.bytes_log = extra["log"];
  ours.store_stats.bytes_misc = extra["misc"];
  ours.last_update = ceph_clock_now(g_ceph_context);

  return 0;
}


int DataHealthService::update_stats()
{
  entity_inst_t our_inst = mon->messenger->get_myinst();
  DataStats& ours = stats[our_inst];

  int err = get_fs_stats(ours.fs_stats, g_conf->mon_data.c_str());
  if (err < 0) {
    derr << __func__ << " get_fs_stats error: " << cpp_strerror(err) << dendl;
    return err;
  }
  dout(0) << __func__ << " avail " << ours.fs_stats.avail_percent << "%"
          << " total " << prettybyte_t(ours.fs_stats.byte_total)
          << ", used " << prettybyte_t(ours.fs_stats.byte_used)
          << ", avail " << prettybyte_t(ours.fs_stats.byte_avail) << dendl;
  ours.last_update = ceph_clock_now(g_ceph_context);

  return update_store_stats(ours);
}

void DataHealthService::share_stats()
{
  dout(10) << __func__ << dendl;
  if (!in_quorum())
    return;

  assert(!stats.empty());
  entity_inst_t our_inst = mon->messenger->get_myinst();
  assert(stats.count(our_inst) > 0);
  DataStats &ours = stats[our_inst];
  const set<int>& quorum = mon->get_quorum();
  for (set<int>::const_iterator it = quorum.begin();
       it != quorum.end(); ++it) {
    if (mon->monmap->get_name(*it) == mon->name)
      continue;
    entity_inst_t inst = mon->monmap->get_inst(*it);
    MMonHealth *m = new MMonHealth(HealthService::SERVICE_HEALTH_DATA,
                                   MMonHealth::OP_TELL);
    m->data_stats = ours;
    dout(20) << __func__ << " send " << *m << " to " << inst << dendl;
    mon->messenger->send_message(m, inst);
  }
}

void DataHealthService::service_tick()
{
  dout(10) << __func__ << dendl;

  int err = update_stats();
  if (err < 0) {
    derr << "something went wrong obtaining our disk stats: "
         << cpp_strerror(err) << dendl;
    force_shutdown();
    return;
  }
  if (in_quorum())
    share_stats();

  DataStats &ours = stats[mon->messenger->get_myinst()];

  if (ours.fs_stats.avail_percent <= g_conf->mon_data_avail_crit) {
    derr << "reached critical levels of available space on local monitor storage"
         << " -- shutdown!" << dendl;
    force_shutdown();
    return;
  }

  // we must backoff these warnings, and track how much data is being
  // consumed in-between reports to assess if it's worth to log this info,
  // otherwise we may very well contribute to the consumption of the
  // already low available disk space.
  if (ours.fs_stats.avail_percent <= g_conf->mon_data_avail_warn) {
    if (ours.fs_stats.avail_percent != last_warned_percent)
      mon->clog->warn()
	<< "reached concerning levels of available space on local monitor storage"
	<< " (" << ours.fs_stats.avail_percent << "% free)\n";
    last_warned_percent = ours.fs_stats.avail_percent;
  } else {
    last_warned_percent = 0;
  }
}

void DataHealthService::handle_tell(MonOpRequestRef op)
{
  op->mark_event("datahealth:handle_tell");
  MMonHealth *m = static_cast<MMonHealth*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  assert(m->get_service_op() == MMonHealth::OP_TELL);

  stats[m->get_source_inst()] = m->data_stats;
}

bool DataHealthService::service_dispatch_op(MonOpRequestRef op)
{
  op->mark_event("datahealth:service_dispatch_op");
  MMonHealth *m = static_cast<MMonHealth*>(op->get_req());
  dout(10) << __func__ << " " << *m << dendl;
  assert(m->get_service_type() == get_type());
  if (!in_quorum()) {
    dout(1) << __func__ << " not in quorum -- drop message" << dendl;
    return false;
  }

  switch (m->service_op) {
    case MMonHealth::OP_TELL:
      // someone is telling us their stats
      handle_tell(op);
      break;
    default:
      dout(0) << __func__ << " unknown op " << m->service_op << dendl;
      assert(0 == "Unknown service op");
      break;
  }
  return true;
}
