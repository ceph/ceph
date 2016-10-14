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


#ifndef CEPH_MOSDPGREMOVE_H
#define CEPH_MOSDPGREMOVE_H

#include "common/hobject.h"
#include "msg/Message.h"


class MOSDPGRemove : public Message {

  static const int HEAD_VERSION = 2;
  static const int COMPAT_VERSION = 1;

  epoch_t epoch;

 public:
  vector<spg_t> pg_list;

  epoch_t get_epoch() { return epoch; }

  MOSDPGRemove() :
    Message(MSG_OSD_PG_REMOVE, HEAD_VERSION, COMPAT_VERSION) {}
  MOSDPGRemove(epoch_t e, vector<spg_t>& l) :
    Message(MSG_OSD_PG_REMOVE, HEAD_VERSION, COMPAT_VERSION) {
    this->epoch = e;
    pg_list.swap(l);
  }
private:
  ~MOSDPGRemove() {}

public:  
  const char *get_type_name() const { return "PGrm"; }

  void encode_payload(uint64_t features) {
    ::encode(epoch, payload);

    vector<pg_t> _pg_list;
    _pg_list.reserve(pg_list.size());
    vector<shard_id_t> _shard_list;
    _shard_list.reserve(pg_list.size());
    for (vector<spg_t>::iterator i = pg_list.begin(); i != pg_list.end(); ++i) {
      _pg_list.push_back(i->pgid);
      _shard_list.push_back(i->shard);
    }
    ::encode(_pg_list, payload);
    ::encode(_shard_list, payload);
  }
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(epoch, p);
    vector<pg_t> _pg_list;
    ::decode(_pg_list, p);

    vector<shard_id_t> _shard_list(_pg_list.size(), shard_id_t::NO_SHARD);
    if (header.version >= 2) {
      _shard_list.clear();
      ::decode(_shard_list, p);
    }
    assert(_shard_list.size() == _pg_list.size());
    pg_list.reserve(_shard_list.size());
    for (unsigned i = 0; i < _shard_list.size(); ++i) {
      pg_list.push_back(spg_t(_pg_list[i], _shard_list[i]));
    }
  }
  void print(ostream& out) const {
    out << "osd pg remove(" << "epoch " << epoch << "; ";
    for (vector<spg_t>::const_iterator i = pg_list.begin();
         i != pg_list.end();
         ++i) {
      out << "pg" << *i << "; ";
    }
    out << ")";
  }
};

#endif
