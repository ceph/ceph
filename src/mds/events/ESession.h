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

#ifndef CEPH_MDS_ESESSION_H
#define CEPH_MDS_ESESSION_H

#include "common/config.h"
#include "include/types.h"

#include "../LogEvent.h"

class ESession : public LogEvent {
 protected:
  entity_inst_t client_inst;
  bool open;    // open or close
  version_t cmapv{0};  // client map version

  interval_set<inodeno_t> inos_to_free;
  version_t inotablev{0};

  interval_set<inodeno_t> inos_to_purge;
  
  // Client metadata stored during open
  client_metadata_t client_metadata;

  bool killed;

 public:
  ESession() : LogEvent(EVENT_SESSION), open(false), killed(false) { }
  ESession(const entity_inst_t& inst, bool o, version_t v,
	   const client_metadata_t& cm, bool k = false) :
    LogEvent(EVENT_SESSION),
    client_inst(inst), open(o), cmapv(v), inotablev(0),
    client_metadata(cm), killed(k) { }
  ESession(const entity_inst_t& inst, bool o, version_t v,
	   const interval_set<inodeno_t>& to_free, version_t iv,
	   const interval_set<inodeno_t>& to_purge, bool k = false) :
    LogEvent(EVENT_SESSION), client_inst(inst), open(o), cmapv(v),
    inos_to_free(to_free), inotablev(iv), inos_to_purge(to_purge), killed(k) {}

  void encode(bufferlist& bl, uint64_t features) const override;
  void decode(bufferlist::const_iterator& bl) override;
  void dump(Formatter *f) const override;
  static void generate_test_instances(std::list<ESession*>& ls);

  void print(std::ostream& out) const override {
    if (open)
      out << "ESession " << client_inst << " open cmapv " << cmapv;
    else
      out << "ESession " << client_inst << " close cmapv " << cmapv;
    if (inos_to_free.size() || inos_to_purge.size())
      out << " (" << inos_to_free.size() << " to free, v" << inotablev
	  << ", " << inos_to_purge.size() << " to purge)";
  }
  
  void update_segment() override;
  void replay(MDSRank *mds) override;
  entity_inst_t get_client_inst() const {return client_inst;}
};
WRITE_CLASS_ENCODER_FEATURES(ESession)

#endif
