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

#include "CephxSessionHandler.h"
#include "CephxProtocol.h"

#include <errno.h>
#include <sstream>

#include "common/config.h"
#include "include/assert.h"
#include "include/ceph_features.h"

#define dout_subsys ceph_subsys_auth

int CephxSessionHandler::_calc_signature(Message *m, uint64_t *psig)
{
  ceph_msg_header& header = m->get_header();
  ceph_msg_footer& footer = m->get_footer();

  bufferlist bl_plaintext;
  ::encode(header.crc, bl_plaintext);
  ::encode(footer.front_crc, bl_plaintext);
  ::encode(footer.middle_crc, bl_plaintext);
  ::encode(footer.data_crc, bl_plaintext);

  bufferlist bl_encrypted;
  std::string error;
  if (encode_encrypt(cct, bl_plaintext, key, bl_encrypted, error)) {
    ldout(cct, 0) << "error encrypting message signature: " << error << dendl;
    ldout(cct, 0) << "no signature put on message" << dendl;
    return SESSION_SIGNATURE_FAILURE;
  }

  bufferlist::iterator ci = bl_encrypted.begin();
  // Skip the magic number up front. PLR
  ci.advance(4);
  ::decode(*psig, ci);
  ldout(cct, 10) << __func__ << " seq " << m->get_seq()
		 << " front_crc_ = " << footer.front_crc
		 << " middle_crc = " << footer.middle_crc
		 << " data_crc = " << footer.data_crc
		 << " sig = " << *psig
		 << dendl;
  return 0;
}

int CephxSessionHandler::sign_message(Message *m)
{
  // If runtime signing option is off, just return success without signing.
  if (!cct->_conf->cephx_sign_messages) {
    return 0;
  }

  uint64_t sig;
  int r = _calc_signature(m, &sig);
  if (r < 0)
    return r;

  ceph_msg_footer& f = m->get_footer();
  f.sig = sig;
  f.flags = (unsigned)f.flags | CEPH_MSG_FOOTER_SIGNED;
  messages_signed++;
  ldout(cct, 20) << "Putting signature in client message(seq # " << m->get_seq()
		 << "): sig = " << sig << dendl;
  return 0;
}

int CephxSessionHandler::check_message_signature(Message *m)
{
  // If runtime signing option is off, just return success without checking signature.
  if (!cct->_conf->cephx_sign_messages) {
    return 0;
  }
  if ((features & CEPH_FEATURE_MSG_AUTH) == 0) {
    // it's fine, we didn't negotiate this feature.
    return 0;
  }

  uint64_t sig;
  int r = _calc_signature(m, &sig);
  if (r < 0)
    return r;

  signatures_checked++;

  if (sig != m->get_footer().sig) {
    // Should have been signed, but signature check failed.  PLR
    if (!(m->get_footer().flags & CEPH_MSG_FOOTER_SIGNED)) {
      ldout(cct, 0) << "SIGN: MSG " << m->get_seq() << " Sender did not set CEPH_MSG_FOOTER_SIGNED." << dendl;
    }
    ldout(cct, 0) << "SIGN: MSG " << m->get_seq() << " Message signature does not match contents." << dendl;
    ldout(cct, 0) << "SIGN: MSG " << m->get_seq() << "Signature on message:" << dendl;
    ldout(cct, 0) << "SIGN: MSG " << m->get_seq() << "    sig: " << m->get_footer().sig << dendl;
    ldout(cct, 0) << "SIGN: MSG " << m->get_seq() << "Locally calculated signature:" << dendl;
    ldout(cct, 0) << "SIGN: MSG " << m->get_seq() << "    sig_check:" << sig << dendl;

    // For the moment, printing an error message to the log and
    // returning failure is sufficient.  In the long term, we should
    // probably have code parsing the log looking for this kind of
    // security failure, particularly when there are large numbers of
    // them, since the latter is a potential sign of an attack.  PLR

    signatures_failed++;
    ldout(cct, 0) << "Signature failed." << dendl;
    return (SESSION_SIGNATURE_FAILURE);
  }

  // If we get here, the signature checked.  PLR
  signatures_matched++;

  return 0;
}

