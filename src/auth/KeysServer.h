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

#ifndef __KEYSSERVER_H
#define __KEYSSERVER_H

#include "config.h"

#include "Crypto.h"
#include "common/Timer.h"
#include "Auth.h"

#define KEY_ROTATE_TIME 5

struct RotatingSecret {
  CryptoKey secret;
  utime_t expiration;

  void encode(bufferlist& bl) const {
    ::encode(secret, bl);
    ::encode(expiration, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(secret, bl);
    ::decode(expiration, bl);
  }
};
WRITE_CLASS_ENCODER(RotatingSecret);


class KeysServer {
 class C_RotateTimeout : public Context {
  protected:
    KeysServer *server;
    double timeout;
  public:
    C_RotateTimeout(KeysServer *s, double to) :
                                        server(s), timeout(to) {
    }
    void finish(int r) {
      if (r >= 0) server->rotate_timeout(timeout);
    }
  };


  /* for each service type */
  map<uint32_t, RotatingSecret> rotating_secrets;

  /* for each entity */
  map<EntityName, CryptoKey> secrets;

  Mutex rotating_lock;
  Mutex secrets_lock;

  SafeTimer timer;
  Context *rotate_event;

  bool generate_secret(CryptoKey& secret);
  void _rotate_secret(uint32_t service_id);
  void generate_all_rotating_secrets();
public:
  KeysServer();

  bool get_secret(EntityName& name, CryptoKey& secret);
  void rotate_timeout(double timeout);

  /* get current secret for specific service type */
  bool get_service_secret(uint32_t service_id, RotatingSecret& service_key);

  bool generate_secret(EntityName& name, CryptoKey& secret);

  void encode(bufferlist& bl) const {
    ::encode(secrets, bl);
    ::encode(rotating_secrets, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(secrets, bl);
    ::decode(rotating_secrets, bl);
  }
};
WRITE_CLASS_ENCODER(KeysServer);





#endif
