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

#define dout_subsys ceph_subsys_auth

int CephxSessionHandler::sign_message(Message *m)
{
  bufferlist bl_plaintext, bl_encrypted;
  ceph_msg_footer en_footer;
  ceph_msg_header header = m->get_header();
  std::string error;

  // If runtime signing option is off, just return success without signing.

  if (!(cct->_conf->cephx_sign_messages) ) {
    return(0);
  }

  en_footer = m->get_footer();

  ::encode(header.crc, bl_plaintext);
  ::encode(en_footer.front_crc, bl_plaintext);
  ::encode(en_footer.middle_crc, bl_plaintext);
  ::encode(en_footer.data_crc, bl_plaintext);

  ldout(cct, 10) <<  "sign_message: seq # " << header.seq << " CRCs are: header " << header.crc << " front " << en_footer.front_crc << " middle " << en_footer.middle_crc << " data " << en_footer.data_crc  << "; key = " << key << dendl;

  encode_encrypt(cct,bl_plaintext, key, bl_encrypted,error);
  if (!error.empty()) {
    ldout(cct, 0) << "error encrypting message signature: " << error << dendl;
    ldout(cct, 0) << "no signature put on message" << dendl;
    return SESSION_SIGNATURE_FAILURE;
  } 
  bufferlist::iterator ci = bl_encrypted.begin();
  uint32_t magic;
  // Skip the magic number up front. PLR
  try {
    ::decode(magic, ci);
  } catch (buffer::error& e) {
    ldout(cct, 0) << "failed to decode magic number on msg " << dendl;
    return SESSION_SIGNATURE_FAILURE;
  }
  try {
    ::decode(en_footer.sig, ci);
  } catch (buffer::error& e) {
    ldout(cct, 0) << "failed to decode signature on msg " << dendl;
    return SESSION_SIGNATURE_FAILURE;
  }

  // There's potentially an issue with whether the encoding and decoding done here will work
  // properly when a big endian and little endian machine are talking.  We think it's OK,
  // but it should be tested to be sure.  PLR

  // Receiver won't trust this flag to decide if msg should have been signed.  It's primarily
  // to debug problems where sender and receiver disagree on need to sign msg.  PLR

  en_footer.flags = (unsigned)en_footer.flags | CEPH_MSG_FOOTER_SIGNED;
  m->set_footer(en_footer);
  messages_signed++;
  ldout(cct, 20) << "Putting signature in client message(seq # " << header.seq << "): sig = " << en_footer.sig << dendl;
  return 0;
}

int CephxSessionHandler::check_message_signature(Message *m)
{
  bufferlist bl_plaintext, bl_ciphertext;
  std::string sig_error;
  ceph_msg_footer footer;
  ceph_msg_header header;

  // If runtime signing option is off, just return success without checking signature.

  if (!(cct->_conf->cephx_sign_messages) ) {
    return(0);
  }

  signatures_checked++;
  header = m->get_header();
  footer = m->get_footer();

  ldout(cct, 10) << "check_message_signature: seq # = " << m->get_seq() << " front_crc_ = " << footer.front_crc << " middle_crc = " << footer.middle_crc << " data_crc = " << footer.data_crc << "; key = " << key << dendl;

  ::encode((__le32)header.crc, bl_plaintext);
  ::encode((__le32)footer.front_crc, bl_plaintext);
  ::encode((__le32)footer.middle_crc, bl_plaintext);
  ::encode((__le32)footer.data_crc, bl_plaintext);

  // Encrypt the buffer containing the checksums to calculate the signature. PLR

  encode_encrypt(cct,bl_plaintext, key, bl_ciphertext, sig_error);

  // If the encryption was error-free, grab the signature from the message and compare it. PLR

  if (!sig_error.empty()) {
    ldout(cct, 0) << "error in encryption for checking message signature: " << sig_error << dendl;
    return (SESSION_SIGNATURE_FAILURE);
  } 

    bufferlist::iterator ci = bl_ciphertext.begin();
    uint32_t magic; 
    uint64_t sig_check = 0;
    // Skip the magic number at the front. PLR
    try {
      ::decode(magic, ci);
    } catch (buffer::error& e) {
      ldout(cct, 0) << "Failed to decode magic number on msg." << dendl;
      return (SESSION_SIGNATURE_FAILURE);
    }
    try {
      ::decode(sig_check, ci);
    } catch (buffer::error& e) {
      ldout(cct, 0) << "Failed to decode sig check on msg." << dendl;
      return (SESSION_SIGNATURE_FAILURE);
    }

    // There's potentially an issue with whether the encoding and decoding done here will work
    // properly when a big endian and little endian machine are talking.  We think it's OK,
    // but it should be tested to be sure.  PLR

    if (sig_check != footer.sig ) {
      // Should have been signed, but signature check failed.  PLR
      if (!(footer.flags & CEPH_MSG_FOOTER_SIGNED)) {
        ldout(cct, 0) << "SIGN: MSG " << header.seq << " Sender did not set CEPH_MSG_FOOTER_SIGNED." << dendl;
      }
      ldout(cct, 0) << "SIGN: MSG " << header.seq << " Message signature does not match contents." << dendl;
      ldout(cct, 0) << "SIGN: MSG " << header.seq << "Signature on message:" << dendl;
      ldout(cct, 0) << "SIGN: MSG " << header.seq << "    sig: " << footer.sig << dendl;
      ldout(cct, 0) << "SIGN: MSG " << header.seq << "Locally calculated signature:" << dendl;
      ldout(cct, 0) << "SIGN: MSG " << header.seq << "    sig_check:" << sig_check << dendl;

      // For the moment, printing an error message to the log and returning failure is sufficient.
      // In the long term, we should probably have code parsing the log looking for this kind
      // of security failure, particularly when there are large numbers of them, since the latter
      // is a potential sign of an attack.  PLR

      signatures_failed++;
      ldout(cct, 0) << "Signature failed." << dendl;
      return (SESSION_SIGNATURE_FAILURE);
    }

  // If we get here, the signature checked.  PLR

  signatures_matched++;;

  return (0);
}

