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

#ifndef __MCLIENTRENEWAL_H
#define __MCLIENTRENEWAL_H

#include "msg/Message.h"

class MClientRenewal : public Message {
 private:
  set<cap_id_t> caps_to_renew;
 public:
  MClientRenewal() : Message(MSG_CLIENT_RENEWAL) { }
  void add_cap_set(set<cap_id_t>& capset) {
    for (set<cap_id_t>::iterator si = capset.begin();
	 si != capset.end();
	 si++)
      caps_to_renew.insert(*si);
  }
  set<cap_id_t>& get_cap_set() {
    return caps_to_renew;
  }

  virtual void encode_payload() {
    _encode(caps_to_renew, payload);
  }
  virtual void decode_payload() {
    int off = 0;
    _decode(caps_to_renew, payload, off);
  }
  virtual char *get_type_name() { return "client_renewal"; }
  void print(ostream& out) {
    out << "client_renewal_request()";
  }
};

#endif
