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

#ifndef CEPH_ROTATINGKEYRING_H
#define CEPH_ROTATINGKEYRING_H

#include "common/config.h"
#include "common/Mutex.h"

#include "auth/Crypto.h"
#include "auth/Auth.h"

/*
 * mediate access to a service's keyring and rotating secrets
 */

class KeyRing;

class RotatingKeyRing : public KeyStore {
  CephContext *cct;
  uint32_t service_id;
  RotatingSecrets secrets;
  KeyRing *keyring;
  mutable Mutex lock;

public:
  RotatingKeyRing(CephContext *cct_, uint32_t s, KeyRing *kr) :
    cct(cct_),
    service_id(s),
    keyring(kr),
    lock("RotatingKeyRing::lock") {}

  bool need_new_secrets() const;
  bool need_new_secrets(utime_t now) const;
  void set_secrets(RotatingSecrets& s);
  void dump_rotating() const;
  bool get_secret(const EntityName& name, CryptoKey& secret) const;
  bool get_service_secret(uint32_t service_id, uint64_t secret_id,
			  CryptoKey& secret) const;
  KeyRing *get_keyring();
};

#endif
