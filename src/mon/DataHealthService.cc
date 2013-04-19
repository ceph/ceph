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
#include <tr1/memory>
#include <errno.h>
#include <map>
#include <list>
#include <string>
#include <sstream>
#include <sys/vfs.h>

#include "messages/MMonHealth.h"
#include "include/types.h"
#include "include/Context.h"
#include "include/assert.h"
#include "common/Formatter.h"

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

health_status_t DataHealthService::get_health(
    Formatter *f,
    list<pair<health_status_t,string> > *detail)
{
  dout(10) << __func__ << dendl;
  if (f) {
    f->open_object_section("data_health");
    f->open_array_section("mons");
  }

  health_status_t overall_status = HEALTH_OK;

  for (map<entity_inst_t,DataStats>::iterator it = stats.begin();
       it != stats.end(); ++it) {
    string mon_name = mon->monmap->get_name(it->first.addr);
    DataStats& stats = it->second;

    health_status_t health_status = HEALTH_OK;
    string health_detail;
    if (stats.latest_avail_percent <= g_conf->mon_data_avail_crit) {
      health_status = HEALTH_ERR;
      health_detail = "shutdown iminent!";
    } else if (stats.latest_avail_percent <= g_conf->mon_data_avail_warn) {
      health_status = HEALTH_WARN;
      health_detail = "low disk space!";
    }

    if (overall_status > health_status)
      overall_status = health_status;

    if (detail && health_status != HEALTH_OK) {
      stringstream ss;
      ss << "mon." << mon_name << " addr " << it->first.addr
          << " has " << stats.latest_avail_percent
          << "\% avail disk space -- " << health_detail;
      detail->push_back(make_pair(health_status, ss.str()));
    }

    if (f) {
      f->open_object_section(mon_name.c_str());
      f->dump_string("name", mon_name.c_str());
      f->dump_int("kb_total", stats.kb_total);
      f->dump_int("kb_used", stats.kb_used);
      f->dump_int("kb_avail", stats.kb_avail);
      f->dump_int("avail_percent", stats.latest_avail_percent);
      f->dump_stream("last_updated") << stats.last_update;
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

  return overall_status;
}

int DataHealthService::update_stats()
{
  struct statfs stbuf;
  int err = ::statfs(g_conf->mon_data.c_str(), &stbuf);
  if (err < 0) {
    derr << __func__ << " statfs error: " << cpp_strerror(errno) << dendl;
    return -errno;
  }

  entity_inst_t our_inst = mon->monmap->get_inst(mon->name);
  DataStats& ours = stats[our_inst];

  ours.kb_total = stbuf.f_blocks * stbuf.f_bsize / 1024;
  ours.kb_used = (stbuf.f_blocks - stbuf.f_bfree) * stbuf.f_bsize / 1024;
  ours.kb_avail = stbuf.f_bavail * stbuf.f_bsize / 1024;
  ours.latest_avail_percent = (((float)ours.kb_avail/ours.kb_total)*100);
  dout(0) << __func__ << " avail " << ours.latest_avail_percent << "%"
          << " total " << ours.kb_total << " used " << ours.kb_used << " avail " << ours.kb_avail
          << dendl;
  ours.last_update = ceph_clock_now(g_ceph_context);
  return 0;
}

void DataHealthService::share_stats()
{
  dout(10) << __func__ << dendl;
  if (!in_quorum())
    return;

  assert(!stats.empty());
  entity_inst_t our_inst = mon->monmap->get_inst(mon->rank);
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

  DataStats &ours = stats[mon->monmap->get_inst(mon->name)];

  if (ours.latest_avail_percent <= g_conf->mon_data_avail_crit) {
    derr << "reached critical levels of available space on data store"
         << " -- shutdown!" << dendl;
    force_shutdown();
    return;
  }

  // we must backoff these warnings, and track how much data is being
  // consumed in-between reports to assess if it's worth to log this info,
  // otherwise we may very well contribute to the consumption of the
  // already low available disk space.
  if (ours.latest_avail_percent <= g_conf->mon_data_avail_warn) {
    if (ours.latest_avail_percent != last_warned_percent)
      mon->clog.warn()
	<< "reached concerning levels of available space on data store"
	<< " (" << ours.latest_avail_percent << "\% free)\n";
    last_warned_percent = ours.latest_avail_percent;
  } else {
    last_warned_percent = 0;
  }
}

void DataHealthService::handle_tell(MMonHealth *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  assert(m->get_service_op() == MMonHealth::OP_TELL);

  stats[m->get_source_inst()] = m->data_stats;
}

bool DataHealthService::service_dispatch(MMonHealth *m)
{
  dout(10) << __func__ << " " << *m << dendl;
  assert(m->get_service_type() == get_type());
  if (!in_quorum()) {
    dout(1) << __func__ << " not in quorum -- drop message" << dendl;
    m->put();
    return false;
  }

  switch (m->service_op) {
    case MMonHealth::OP_TELL:
      // someone is telling us their stats
      handle_tell(m);
      break;
    default:
      dout(0) << __func__ << " unknown op " << m->service_op << dendl;
      assert(0 == "Unknown service op");
      break;
  }
  m->put();
  return true;
}
