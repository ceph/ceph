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

#define KEY_ROTATE_TIME 20
#define KEY_ROTATE_NUM 3


struct KeysServerData {
  version_t version;
  version_t rotating_ver;

  /* for each entity */
  map<EntityName, CryptoKey> secrets;

  /* for each service type */
  map<uint32_t, RotatingSecrets> rotating_secrets;

  KeysServerData() : version(0), rotating_ver(0) {}

  void encode(bufferlist& bl) const {
    ::encode(version, bl);
    ::encode(rotating_ver, bl);
    ::encode(secrets, bl);
    ::encode(rotating_secrets, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(version, bl);
    ::decode(rotating_ver, bl);
    ::decode(secrets, bl);
    ::decode(rotating_secrets, bl);
  }

  bool contains(EntityName& name) {
    return (secrets.find(name) != secrets.end());
  }

  void add_secret(const EntityName& name, CryptoKey& secret) {
    secrets[name] = secret;
  }

  void remove_secret(const EntityName& name) {
    map<EntityName, CryptoKey>::iterator iter = secrets.find(name);
    if (iter == secrets.end())
      return;
    secrets.erase(iter);
  }

  void add_rotating_secret(uint32_t service_id, ExpiringCryptoKey& key) {
    rotating_secrets[service_id].add(key);
  }

  bool get_service_secret(uint32_t service_id, RotatingSecrets& secret);
  bool get_secret(EntityName& name, CryptoKey& secret);

  map<EntityName, CryptoKey>::iterator secrets_begin() { return secrets.begin(); }
  map<EntityName, CryptoKey>::iterator secrets_end() { return secrets.end(); }
  map<EntityName, CryptoKey>::iterator find_name(EntityName& name) { return secrets.find(name); }
};
WRITE_CLASS_ENCODER(KeysServerData);

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

  KeysServerData data;

  Mutex lock;

  SafeTimer timer;
  Context *rotate_event;

  bool generate_secret(CryptoKey& secret);
  void _rotate_secret(uint32_t service_id, int factor);
  void _generate_all_rotating_secrets(bool init);
public:
  KeysServer();

  bool get_secret(EntityName& name, CryptoKey& secret);
  int start_server(bool init);
  void rotate_timeout(double timeout);

  /* get current secret for specific service type */
  bool get_service_secret(uint32_t service_id, RotatingSecrets& service_key);

  bool generate_secret(EntityName& name, CryptoKey& secret);

  void encode(bufferlist& bl) const {
    ::encode(data, bl);
  }
  void decode(bufferlist::iterator& bl) {
    ::decode(data, bl);
  }
  bool contains(EntityName& name);
  void list_secrets(stringstream& ss);
  version_t get_ver() {
    Mutex::Locker l(lock);
    return data.version;    
  }

  void set_ver(version_t ver) {
    Mutex::Locker l(lock);
    data.version = ver;
  }

  void add_secret(const EntityName& name, CryptoKey& secret) {
    Mutex::Locker l(lock);
    data.add_secret(name, secret);
  }

  void remove_secret(const EntityName& name) {
    Mutex::Locker l(lock);
    data.remove_secret(name);
  }

  void add_rotating_secret(uint32_t service_id, ExpiringCryptoKey& key) {
    Mutex::Locker l(lock);
    data.add_rotating_secret(service_id, key);
  }
  void clone_to(KeysServerData& dst) {
    Mutex::Locker l(lock);
    dst = data;
  }

  bool updated_rotating(bufferlist& rotating_bl, version_t& rotating_ver);
  void decode_rotating(bufferlist& rotating_bl);

  bool get_rotating_encrypted(EntityName& name, bufferlist& enc_bl);
};
WRITE_CLASS_ENCODER(KeysServer);





#endif
