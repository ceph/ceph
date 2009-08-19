// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __AUTHTYPES_H
#define __AUTHTYPES_H

#include "config.h"


class EntitySecret {
protected:
  bufferlist secret;

public:
  EntitySecret(bufferlist& bl) { secret = bl; }

  void encode(bufferlist& bl) const {
    ::encode(secret, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(secret, bl);
  }

  bufferlist& get_secret() { return secret; }
};
WRITE_CLASS_ENCODER(EntitySecret);

class ServiceSecret : public EntitySecret {
  utime_t created;

public:
  void encode(bufferlist& bl) const {
    ::encode(secret, bl);
    ::encode(created, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(secret, bl);
    ::decode(created, bl);
  }
};
WRITE_CLASS_ENCODER(ServiceSecret);

struct EntityName {
  uint32_t entity_type;
  string name;

  void encode(bufferlist& bl) const {
    ::encode(entity_type, bl);
    ::encode(name, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(entity_type, bl);
    ::decode(name, bl);
  }
};
WRITE_CLASS_ENCODER(EntityName);

struct SessionKey {
  bufferlist key;

  void encode(bufferlist& bl) const {
    ::encode(key, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(key, bl);
  }
};
WRITE_CLASS_ENCODER(SessionKey);

#endif
