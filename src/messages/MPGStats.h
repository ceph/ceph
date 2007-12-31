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

#ifndef __MPGSTATS_H
#define __MPGSTATS_H

#include "osd/osd_types.h"

class MPGStats : public Message {
public:
  map<pg_t,pg_stat_t> pg_stat;
  osd_stat_t osd_stat;
  
  MPGStats() : Message(MSG_PGSTATS) {}

  const char *get_type_name() { return "pg_stats"; }
  void print(ostream& out) {
    out << "pg_stats";
  }

  void encode_payload() {
    ::_encode(osd_stat, payload);
    ::_encode(pg_stat, payload);
  }
  void decode_payload() {
    int off = 0;
    ::_decode(osd_stat, payload, off);
    ::_decode(pg_stat, payload, off);
  }
};

#endif
