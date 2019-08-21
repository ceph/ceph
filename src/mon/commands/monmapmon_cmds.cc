// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 SUSE LLC <contact@suse.com> 
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "mon/Monitor.h"
#include "mon/MonmapMonitor.h"
#include "mon/MonOpRequest.h"

#include "mon/commands/Command.h"
#include "mon/commands/monmapmon_cmds.h"

#include "messages/MMonCommand.h"

#include "include/types.h"
#include "include/ceph_assert.h"
#include "include/Context.h"

#include "common/dout.h"
#include "common/cmdparse.h"

#define dout_subsys ceph_subsys_mon

bool MonMonStatCommand::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const MonMap &monmap)
{

  ceph_assert(handles_command(prefix));

  mon->monmap->print_summary(ss);
  ss << ", election epoch " << mon->get_epoch() << ", leader "
    << mon->get_leader() << " " << mon->get_leader_name()
    << ", quorum " << mon->get_quorum() << " " << mon->get_quorum_names();
  rdata.append(ss);
  ss.str("");
  reply_with_data(op, 0, ss, rdata, service->get_last_committed());
  return true;
}


bool MonMonGetMap::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const MonMap &monmap)
{

  ceph_assert(handles_command(prefix));

  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());

  epoch_t epoch;
  int64_t epochnum;
  cmd_getval(cct, cmdmap, "epoch", epochnum, (int64_t)0);
  epoch = epochnum;

  MonMap *p = mon->monmap;
  if (epoch) {
    bufferlist bl;
    int r = service->get_version(epoch, bl);
    if (r == -ENOENT) {
      ss << "there is no map for epoch " << epoch;
      reply(op, -ENOENT, ss, service->get_last_committed());
    }
    ceph_assert(r == 0);
    ceph_assert(bl.length() > 0);
    p = new MonMap;
    p->decode(bl);
  }

  ceph_assert(p);

  if (prefix == "mon getmap") {
    p->encode(rdata, m->get_connection()->get_features());
    ss << "got monmap epoch " << p->get_epoch();
  } else if (prefix == "mon dump") {
    stringstream ds;
    if (f) {
      f->open_object_section("monmap");
      p->dump(f.get());
      f->open_array_section("quorum");
      for (set<int>::iterator q = mon->get_quorum().begin();
	  q != mon->get_quorum().end(); ++q) {
	f->dump_int("mon", *q);
      }
      f->close_section();
      f->close_section();
      f->flush(ds);
    } else {
      p->print(ds);
    }
    rdata.append(ds);
    ss << "dumped monmap epoch " << p->get_epoch();
  }
  if (p != mon->monmap) {
    delete p;
    p = nullptr;
  }

  reply_with_data(op, 0, ss, rdata, service->get_last_committed());
  return true;
}

bool MonMonFeatureLs::do_preprocess(
    MonOpRequestRef op,
    const string &prefix,
    const cmdmap_t &cmdmap,
    stringstream &ss,
    bufferlist rdata,
    FormatterRef f,
    const MonMap &monmap)
{

  ceph_assert(handles_command(prefix));

  bool list_with_value = false;
  string with_value;
  if (cmd_getval(g_ceph_context, cmdmap, "with_value", with_value) &&
      with_value == "--with-value") {
    list_with_value = true;
  }

  // list features
  mon_feature_t supported = ceph::features::mon::get_supported();
  mon_feature_t persistent = ceph::features::mon::get_persistent();
  mon_feature_t required = monmap.get_required_features();

  stringstream ds;
  auto print_feature = [&](mon_feature_t& m_features, const char* m_str) {
    if (f) {
      if (list_with_value)
	m_features.dump_with_value(f.get(), m_str);
      else
	m_features.dump(f.get(), m_str);
    } else {
      if (list_with_value)
	m_features.print_with_value(ds);
      else
	m_features.print(ds);
    }
  };

  mon_feature_t mon_persistent = monmap.persistent_features;
  mon_feature_t mon_optional = monmap.optional_features;

  if (f) {
    f->open_object_section("features");

    f->open_object_section("all");
    print_feature(supported, "supported");
    print_feature(persistent, "persistent");
    f->close_section(); // all

    f->open_object_section("monmap");
    print_feature(mon_persistent, "persistent");
    print_feature(mon_optional, "optional");
    print_feature(required, "required");
    f->close_section(); // monmap 

    f->close_section(); // features
    f->flush(ds);

  } else {
    ds << "all features" << std::endl
      << "\tsupported: ";
    print_feature(supported, nullptr);
    ds << std::endl
      << "\tpersistent: ";
    print_feature(persistent, nullptr);
    ds << std::endl
      << std::endl;

    ds << "on current monmap (epoch "
      << monmap.get_epoch() << ")" << std::endl
      << "\tpersistent: ";
    print_feature(mon_persistent, nullptr);
    ds << std::endl
      // omit optional features in plain-text
      // makes it easier to read, and they're, currently, empty.
      << "\trequired: ";
    print_feature(required, nullptr);
    ds << std::endl;
  }
  rdata.append(ds);

  reply_with_data(op, 0, ss, rdata, service->get_last_committed());
  return true;
}

