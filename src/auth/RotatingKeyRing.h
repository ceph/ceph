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

#ifndef __ROTATINGKEYRING_H
#define __ROTATINGKEYRING_H

#include "config.h"

#include "auth/Crypto.h"
#include "auth/Auth.h"

/*
 * mediate access to a service's keyring and rotating secrets
 */

class KeyRing;

class RotatingKeyRing : public KeyStore {
  uint32_t service_id;
  RotatingSecrets secrets;
  KeyRing *keyring;
  Mutex lock;

public:
  RotatingKeyRing(uint32_t s, KeyRing *kr) :
    service_id(s),
    keyring(kr),
    lock("RotatingKeyRing::lock") {}

  bool need_new_secrets();
  bool need_new_secrets(utime_t now);
  void set_secrets(RotatingSecrets& s);
  void dump_rotating();
  bool get_secret(EntityName& name, CryptoKey& secret);
  bool get_service_secret(uint32_t service_id, uint64_t secret_id, CryptoKey& secret);
};

#endif
