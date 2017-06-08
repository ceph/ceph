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

#include "common/debug.h"
#include "AuthSessionHandler.h"
#include "cephx/CephxSessionHandler.h"
#include "none/AuthNoneSessionHandler.h"
#include "unknown/AuthUnknownSessionHandler.h"
#include "common/config.h"

#define dout_subsys ceph_subsys_auth


AuthSessionHandler *get_auth_session_handler(CephContext *cct, int protocol, CryptoKey key, uint64_t features)
{

  // Should add code to only print the SHA1 hash of the key, unless in secure debugging mode

  ldout(cct,10) << "In get_auth_session_handler for protocol " << protocol << dendl;
 
  switch (protocol) {
  case CEPH_AUTH_CEPHX:
    return new CephxSessionHandler(cct, key, features);
  case CEPH_AUTH_NONE:
    return new AuthNoneSessionHandler(cct, key);
  case CEPH_AUTH_UNKNOWN:
    return new AuthUnknownSessionHandler(cct, key);
  }
  return NULL;
}


void AuthSessionHandler::print_auth_session_handler_stats() {
  ldout(cct,10) << "Auth Session Handler Stats " << this << dendl;
  ldout(cct,10) << "    Messages Signed    = " << messages_signed << dendl;
  ldout(cct,10) << "    Signatures Checked = " << signatures_checked << dendl;
  ldout(cct,10) << "        Signatures Matched = " << signatures_matched << dendl;
  ldout(cct,10) << "        Signatures Did Not Match = " << signatures_failed << dendl;
  ldout(cct,10) << "    Messages Encrypted = " << messages_encrypted << dendl;
  ldout(cct,10) << "    Messages Decrypted = " << messages_decrypted << dendl;
}
