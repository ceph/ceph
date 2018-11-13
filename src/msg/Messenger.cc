// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <netdb.h>

#include "include/types.h"
#include "include/random.h"

#include "Messenger.h"

#include "msg/simple/SimpleMessenger.h"
#include "msg/async/AsyncMessenger.h"
#ifdef HAVE_XIO
#include "msg/xio/XioMessenger.h"
#endif

Messenger *Messenger::create_client_messenger(CephContext *cct, string lname)
{
  std::string public_msgr_type = cct->_conf->ms_public_type.empty() ? cct->_conf.get_val<std::string>("ms_type") : cct->_conf->ms_public_type;
  auto nonce = ceph::util::generate_random_number<uint64_t>();
  return Messenger::create(cct, public_msgr_type, entity_name_t::CLIENT(),
			   std::move(lname), nonce, 0);
}

Messenger *Messenger::create(CephContext *cct, const string &type,
			     entity_name_t name, string lname,
			     uint64_t nonce, uint64_t cflags)
{
  int r = -1;
  if (type == "random") {
    r = ceph::util::generate_random_number(0, 1);
  }
  if (r == 0 || type == "simple")
    return new SimpleMessenger(cct, name, std::move(lname), nonce);
  else if (r == 1 || type.find("async") != std::string::npos)
    return new AsyncMessenger(cct, name, type, std::move(lname), nonce);
#ifdef HAVE_XIO
  else if ((type == "xio") &&
	   cct->check_experimental_feature_enabled("ms-type-xio"))
    return new XioMessenger(cct, name, std::move(lname), nonce, cflags);
#endif
  lderr(cct) << "unrecognized ms_type '" << type << "'" << dendl;
  return nullptr;
}

/**
 * Get the default crc flags for this messenger.
 * but not yet dispatched.
 */
static int get_default_crc_flags(const ConfigProxy&);

Messenger::Messenger(CephContext *cct_, entity_name_t w)
  : trace_endpoint("0.0.0.0", 0, "Messenger"),
    my_name(w),
    default_send_priority(CEPH_MSG_PRIO_DEFAULT),
    started(false),
    magic(0),
    socket_priority(-1),
    cct(cct_),
    crcflags(get_default_crc_flags(cct->_conf)),
    auth_ah_service_registry(
      new AuthAuthorizeHandlerRegistry(
	cct,
	cct->_conf->auth_supported.empty() ?
	cct->_conf->auth_service_required :
	cct->_conf->auth_supported)),
    auth_ah_cluster_registry(
      new AuthAuthorizeHandlerRegistry(
	cct,
	cct->_conf->auth_supported.empty() ?
	cct->_conf->auth_cluster_required :
	cct->_conf->auth_supported))
{}

void Messenger::set_endpoint_addr(const entity_addr_t& a,
                                  const entity_name_t &name)
{
  size_t hostlen;
  if (a.get_family() == AF_INET)
    hostlen = sizeof(struct sockaddr_in);
  else if (a.get_family() == AF_INET6)
    hostlen = sizeof(struct sockaddr_in6);
  else
    hostlen = 0;

  if (hostlen) {
    char buf[NI_MAXHOST] = { 0 };
    getnameinfo(a.get_sockaddr(), hostlen, buf, sizeof(buf),
                NULL, 0, NI_NUMERICHOST);

    trace_endpoint.copy_ip(buf);
  }
  trace_endpoint.set_port(a.get_port());
}

/**
 * Get the default crc flags for this messenger.
 * but not yet dispatched.
 *
 * Pre-calculate desired software CRC settings.  CRC computation may
 * be disabled by default for some transports (e.g., those with strong
 * hardware checksum support).
 */
int get_default_crc_flags(const ConfigProxy& conf)
{
  int r = 0;
  if (conf->ms_crc_data)
    r |= MSG_CRC_DATA;
  if (conf->ms_crc_header)
    r |= MSG_CRC_HEADER;
  return r;
}

int Messenger::bindv(const entity_addrvec_t& addrs)
{
  lderr(cct) << __func__ << " " << addrs << " fallback to legacy "
	     << addrs.legacy_addr() << dendl;
  return bind(addrs.legacy_addr());
}

void Messenger::ms_deliver_get_auth_allowed_methods(
    int peer_type, std::vector<uint32_t> &allowed_methods)
{
  for (const auto &dispatcher : dispatchers) {
    if (dispatcher->ms_get_auth_allowed_methods(peer_type, allowed_methods))
      return;
  }
  if (allowed_methods.empty()) {
    if (get_mytype() == CEPH_ENTITY_TYPE_MON &&
        peer_type != CEPH_ENTITY_TYPE_MON) {
      allowed_methods.push_back(CEPH_AUTH_NONE);
      return;
    }
    std::string method;
    if (!cct->_conf->auth_supported.empty()) {
      method = cct->_conf->auth_supported;
    } else if (peer_type == CEPH_ENTITY_TYPE_OSD ||
               peer_type == CEPH_ENTITY_TYPE_MDS ||
               peer_type == CEPH_ENTITY_TYPE_MON ||
               peer_type == CEPH_ENTITY_TYPE_MGR) {
      method = cct->_conf->auth_cluster_required;
    } else {
      method = cct->_conf->auth_client_required;
    }
    AuthMethodList auth_list(cct, method);
    for (auto pt : auth_list.get_supported_set()) {
      allowed_methods.push_back(pt);
    }
  }
}

bool Messenger::ms_deliver_verify_authorizer(
  Connection *con,
  int peer_type,
  int protocol,
  bufferlist& authorizer,
  bufferlist& authorizer_reply,
  bool& isvalid,
  CryptoKey& session_key,
  std::unique_ptr<AuthAuthorizerChallenge> *challenge)
{
  if (authorizer.length() == 0) {
    for (auto dis : dispatchers) {
      if (!dis->require_authorizer) {
	//ldout(cct,10) << __func__ << " tolerating missing authorizer" << dendl;
	isvalid = true;
	return true;
      }
    }
  }
  AuthAuthorizeHandler *ah = 0;
  switch (peer_type) {
  case CEPH_ENTITY_TYPE_MDS:
  case CEPH_ENTITY_TYPE_MON:
  case CEPH_ENTITY_TYPE_OSD:
    ah = auth_ah_cluster_registry->get_handler(protocol);
    break;
  default:
    ah = auth_ah_service_registry->get_handler(protocol);
  }
  if (get_mytype() == CEPH_ENTITY_TYPE_MON &&
      peer_type != CEPH_ENTITY_TYPE_MON) {
    // the monitor doesn't do authenticators for msgr1.
    isvalid = true;
    return true;
  }
  if (!ah) {
    lderr(cct) << __func__ << " no AuthAuthorizeHandler found for protocol "
	       << protocol << dendl;
    isvalid = false;
    return false;
  }

  for (auto dis : dispatchers) {
    KeyStore *ks = dis->ms_get_auth1_authorizer_keystore();
    if (ks) {
      isvalid = ah->verify_authorizer(
	cct,
	ks,
	authorizer,
	authorizer_reply,
	con->peer_name,
	con->peer_global_id,
	con->peer_caps_info,
	session_key,
	challenge);
      if (isvalid) {
	dis->ms_handle_authentication(con);
      }
      return true;
    }
  }
  return false;
}
