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


#include "CephxServiceHandler.h"
#include "CephxProtocol.h"
#include "CephxKeyServer.h"
#include <errno.h>
#include <sstream>

#include "include/random.h"
#include "common/config.h"
#include "common/debug.h"

#define dout_subsys ceph_subsys_auth
#undef dout_prefix
#define dout_prefix *_dout << "cephx server " << entity_name << ": "

using std::dec;
using std::hex;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

int CephxServiceHandler::do_start_session(
  bool is_new_global_id,
  bufferlist *result_bl,
  AuthCapsInfo *caps)
{
  global_id_status = is_new_global_id ? global_id_status_t::NEW_PENDING :
					global_id_status_t::RECLAIM_PENDING;

  uint64_t min = 1; // always non-zero
  uint64_t max = std::numeric_limits<uint64_t>::max();
  server_challenge = ceph::util::generate_random_number<uint64_t>(min, max);
  ldout(cct, 10) << "start_session server_challenge "
		 << hex << server_challenge << dec << dendl;

  CephXServerChallenge ch;
  ch.server_challenge = server_challenge;
  encode(ch, *result_bl);
  return 0;
}

int CephxServiceHandler::verify_old_ticket(
  const CephXAuthenticate& req,
  CephXServiceTicketInfo& old_ticket_info,
  bool& should_enc_ticket)
{
  ldout(cct, 20) << " checking old_ticket: secret_id="
		 << req.old_ticket.secret_id
		 << " len=" << req.old_ticket.blob.length()
		 << ", old_ticket_may_be_omitted="
		 << req.old_ticket_may_be_omitted << dendl;
  ceph_assert(global_id_status != global_id_status_t::NONE);
  if (global_id_status == global_id_status_t::NEW_PENDING) {
    // old ticket is not needed
    if (req.old_ticket.blob.length()) {
      ldout(cct, 0) << " superfluous ticket presented" << dendl;
      return -EINVAL;
    }
    if (req.old_ticket_may_be_omitted) {
      ldout(cct, 10) << " new global_id " << global_id
		     << " (unexposed legacy client)" << dendl;
      global_id_status = global_id_status_t::NEW_NOT_EXPOSED;
    } else {
      ldout(cct, 10) << " new global_id " << global_id << dendl;
      global_id_status = global_id_status_t::NEW_OK;
    }
    return 0;
  }

  if (!req.old_ticket.blob.length()) {
    // old ticket is needed but not presented
    if (cct->_conf->auth_allow_insecure_global_id_reclaim &&
	req.old_ticket_may_be_omitted) {
      ldout(cct, 10) << " allowing reclaim of global_id " << global_id
		     << " with no ticket presented (legacy client, auth_allow_insecure_global_id_reclaim=true)"
		     << dendl;
      global_id_status = global_id_status_t::RECLAIM_INSECURE;
      return 0;
    }
    ldout(cct, 0) << " attempt to reclaim global_id " << global_id
		  << " without presenting ticket" << dendl;
    return -EACCES;
  }

  if (!cephx_decode_ticket(cct, key_server, CEPH_ENTITY_TYPE_AUTH,
			   req.old_ticket, old_ticket_info)) {
    if (cct->_conf->auth_allow_insecure_global_id_reclaim &&
	req.old_ticket_may_be_omitted) {
      ldout(cct, 10) << " allowing reclaim of global_id " << global_id
		     << " using bad ticket (legacy client, auth_allow_insecure_global_id_reclaim=true)"
		     << dendl;
      global_id_status = global_id_status_t::RECLAIM_INSECURE;
      return 0;
    }
    ldout(cct, 0) << " attempt to reclaim global_id " << global_id
		  << " using bad ticket" << dendl;
    return -EACCES;
  }
  ldout(cct, 20) << " decoded old_ticket: global_id="
		 << old_ticket_info.ticket.global_id << dendl;
  if (global_id != old_ticket_info.ticket.global_id) {
    if (cct->_conf->auth_allow_insecure_global_id_reclaim &&
	req.old_ticket_may_be_omitted) {
      ldout(cct, 10) << " allowing reclaim of global_id " << global_id
		     << " using mismatching ticket (legacy client, auth_allow_insecure_global_id_reclaim=true)"
		     << dendl;
      global_id_status = global_id_status_t::RECLAIM_INSECURE;
      return 0;
    }
    ldout(cct, 0) << " attempt to reclaim global_id " << global_id
		  << " using mismatching ticket" << dendl;
    return -EACCES;
  }
  ldout(cct, 10) << " allowing reclaim of global_id " << global_id
		 << " (valid ticket presented, will encrypt new ticket)"
		 << dendl;
  global_id_status = global_id_status_t::RECLAIM_OK;
  should_enc_ticket = true;
  return 0;
}

int CephxServiceHandler::handle_request(
  bufferlist::const_iterator& indata,
  size_t connection_secret_required_len,
  bufferlist *result_bl,
  AuthCapsInfo *caps,
  CryptoKey *psession_key,
  std::string *pconnection_secret)
{
  int ret = 0;

  struct CephXRequestHeader cephx_header;
  try {
    decode(cephx_header, indata);
  } catch (ceph::buffer::error& e) {
    ldout(cct, 0) << __func__ << " failed to decode CephXRequestHeader: "
		  << e.what() << dendl;
    return -EPERM;
  }

  switch (cephx_header.request_type) {
  case CEPHX_GET_AUTH_SESSION_KEY:
    {
      ldout(cct, 10) << "handle_request get_auth_session_key for "
		     << entity_name << dendl;

      CephXAuthenticate req;
      try {
	decode(req, indata);
      } catch (ceph::buffer::error& e) {
	ldout(cct, 0) << __func__ << " failed to decode CephXAuthenticate: "
		      << e.what() << dendl;
	ret = -EPERM;
	break;
      }

      EntityAuth eauth;
      if (!key_server->get_auth(entity_name, eauth)) {
        ldout(cct, 0) << "couldn't find entity name: " << entity_name << dendl;
	ret = -EACCES;
	break;
      }

      if (!server_challenge) {
	ret = -EACCES;
	break;
      }      

      uint64_t expected_key;
      CryptoKey *used_key = &eauth.key;
      std::string error;
      cephx_calc_client_server_challenge(cct, eauth.key, server_challenge,
					 req.client_challenge, &expected_key, error);
      if ((!error.empty() || req.key != expected_key) &&
	  !eauth.pending_key.empty()) {
	ldout(cct, 10) << "normal key failed for " << entity_name
		       << ", trying pending_key" << dendl;
	// try pending_key instead
	error.clear();
	cephx_calc_client_server_challenge(cct, eauth.pending_key,
					   server_challenge,
					   req.client_challenge, &expected_key,
					   error);
	if (error.empty()) {
	  used_key = &eauth.pending_key;
	  key_server->note_used_pending_key(entity_name, eauth.pending_key);
	}
      }
      if (!error.empty()) {
	ldout(cct, 0) << " cephx_calc_client_server_challenge error: " << error << dendl;
	ret = -EACCES;
	break;
      }

      ldout(cct, 20) << " checking key: req.key=" << hex << req.key
	       << " expected_key=" << expected_key << dec << dendl;
      if (req.key != expected_key) {
        ldout(cct, 0) << " unexpected key: req.key=" << hex << req.key
		<< " expected_key=" << expected_key << dec << dendl;
        ret = -EACCES;
	break;
      }

      CryptoKey session_key;
      CephXSessionAuthInfo info;
      bool should_enc_ticket = false;

      CephXServiceTicketInfo old_ticket_info;
      ret = verify_old_ticket(req, old_ticket_info, should_enc_ticket);
      if (ret) {
	ldout(cct, 0) << " could not verify old ticket" << dendl;
	break;
      }

      double ttl;
      if (!key_server->get_service_secret(CEPH_ENTITY_TYPE_AUTH,
					  info.service_secret, info.secret_id,
					  ttl)) {
        ldout(cct, 0) << " could not get service secret for auth subsystem" << dendl;
        ret = -EIO;
        break;
      }

      info.service_id = CEPH_ENTITY_TYPE_AUTH;
      info.ticket.name = entity_name;
      info.ticket.global_id = global_id;
      info.ticket.init_timestamps(ceph_clock_now(), ttl);
      info.validity.set_from_double(ttl);

      key_server->generate_secret(session_key);

      info.session_key = session_key;
      if (psession_key) {
	*psession_key = session_key;
      }

      vector<CephXSessionAuthInfo> info_vec;
      info_vec.push_back(info);

      build_cephx_response_header(cephx_header.request_type, 0, *result_bl);
      if (!cephx_build_service_ticket_reply(
	    cct, *used_key, info_vec, should_enc_ticket,
	    old_ticket_info.session_key, *result_bl)) {
	ret = -EIO;
	break;
      }

      if (!key_server->get_service_caps(entity_name, CEPH_ENTITY_TYPE_MON,
					*caps)) {
        ldout(cct, 0) << " could not get mon caps for " << entity_name << dendl;
        ret = -EACCES;
	break;
      } else {
        char *caps_str = caps->caps.c_str();
        if (!caps_str || !caps_str[0]) {
          ldout(cct,0) << "mon caps null for " << entity_name << dendl;
          ret = -EACCES;
	  break;
        }

	if (req.other_keys) {
	  // nautilus+ client
	  // generate a connection_secret
	  bufferlist cbl;
	  if (pconnection_secret) {
	    pconnection_secret->resize(connection_secret_required_len);
	    if (connection_secret_required_len) {
	      cct->random()->get_bytes(pconnection_secret->data(),
				       connection_secret_required_len);
	    }
	    std::string err;
	    if (encode_encrypt(cct, *pconnection_secret, session_key, cbl,
			       err)) {
	      lderr(cct) << __func__ << " failed to encrypt connection secret, "
			 << err << dendl;
	      ret = -EACCES;
	      break;
	    }
	  }
	  encode(cbl, *result_bl);
	  // provide requested service tickets at the same time
	  vector<CephXSessionAuthInfo> info_vec;
	  for (uint32_t service_id = 1; service_id <= req.other_keys;
	       service_id <<= 1) {
	    // skip CEPH_ENTITY_TYPE_AUTH: auth ticket is already encoded
	    // (possibly encrypted with the old session key)
	    if ((req.other_keys & service_id) &&
		service_id != CEPH_ENTITY_TYPE_AUTH) {
	      ldout(cct, 10) << " adding key for service "
			     << ceph_entity_type_name(service_id) << dendl;
	      CephXSessionAuthInfo svc_info;
	      key_server->build_session_auth_info(
		service_id,
		info.ticket,
		svc_info);
	      info_vec.push_back(svc_info);
	    }
	  }
	  bufferlist extra;
	  if (!info_vec.empty()) {
	    CryptoKey no_key;
	    cephx_build_service_ticket_reply(
	      cct, session_key, info_vec, false, no_key, extra);
	  }
	  encode(extra, *result_bl);
	}

	// caller should try to finish authentication
	ret = 1;
      }
    }
    break;

  case CEPHX_GET_PRINCIPAL_SESSION_KEY:
    {
      ldout(cct, 10) << "handle_request get_principal_session_key" << dendl;

      bufferlist tmp_bl;
      CephXServiceTicketInfo auth_ticket_info;
      // note: no challenge here.
      if (!cephx_verify_authorizer(
	    cct, *key_server, indata, 0, auth_ticket_info, nullptr,
	    nullptr,
	    &tmp_bl)) {
        ret = -EACCES;
	break;
      }

      CephXServiceTicketRequest ticket_req;
      try {
	decode(ticket_req, indata);
      } catch (ceph::buffer::error& e) {
	ldout(cct, 0) << __func__
		      << " failed to decode CephXServiceTicketRequest: "
		      << e.what() << dendl;
	ret = -EPERM;
	break;
      }
      ldout(cct, 10) << " ticket_req.keys = " << ticket_req.keys << dendl;

      ret = 0;
      vector<CephXSessionAuthInfo> info_vec;
      int found_services = 0;
      int service_err = 0;
      for (uint32_t service_id = 1; service_id <= ticket_req.keys;
	   service_id <<= 1) {
        // skip CEPH_ENTITY_TYPE_AUTH: auth ticket must be obtained with
        // CEPHX_GET_AUTH_SESSION_KEY
        if ((ticket_req.keys & service_id) &&
            service_id != CEPH_ENTITY_TYPE_AUTH) {
	  ldout(cct, 10) << " adding key for service "
			 << ceph_entity_type_name(service_id) << dendl;
          CephXSessionAuthInfo info;
          int r = key_server->build_session_auth_info(
	    service_id,
	    auth_ticket_info.ticket,  // parent ticket (client's auth ticket)
	    info);
	  // tolerate missing MGR rotating key for the purposes of upgrades.
          if (r < 0) {
	    ldout(cct, 10) << "   missing key for service "
			   << ceph_entity_type_name(service_id) << dendl;
	    service_err = r;
	    continue;
	  }
          info_vec.push_back(info);
	  ++found_services;
        }
      }
      if (!found_services && service_err) {
	ldout(cct, 10) << __func__ << " did not find any service keys" << dendl;
	ret = service_err;
      }
      CryptoKey no_key;
      build_cephx_response_header(cephx_header.request_type, ret, *result_bl);
      cephx_build_service_ticket_reply(cct, auth_ticket_info.session_key,
				       info_vec, false, no_key, *result_bl);
    }
    break;

  case CEPHX_GET_ROTATING_KEY:
    {
      ldout(cct, 10) << "handle_request getting rotating secret for "
		     << entity_name << dendl;
      build_cephx_response_header(cephx_header.request_type, 0, *result_bl);
      if (!key_server->get_rotating_encrypted(entity_name, *result_bl)) {
        ret = -EACCES;
        break;
      }
    }
    break;

  default:
    ldout(cct, 10) << "handle_request unknown op " << cephx_header.request_type << dendl;
    return -EINVAL;
  }
  return ret;
}

void CephxServiceHandler::build_cephx_response_header(int request_type, int status, bufferlist& bl)
{
  struct CephXResponseHeader header;
  header.request_type = request_type;
  header.status = status;
  encode(header, bl);
}
