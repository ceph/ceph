// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <netdb.h>

#include "include/types.h"
#include "include/random.h"

#include "Messenger.h"

#include "msg/async/AsyncMessenger.h"

Messenger *Messenger::create_client_messenger(CephContext *cct, std::string lname)
{
  std::string public_msgr_type = cct->_conf->ms_public_type.empty() ? cct->_conf.get_val<std::string>("ms_type") : cct->_conf->ms_public_type;
  auto nonce = get_random_nonce();
  return Messenger::create(cct, public_msgr_type, entity_name_t::CLIENT(),
			   std::move(lname), nonce);
}

uint64_t Messenger::get_pid_nonce()
{
  uint64_t nonce = getpid();
  if (nonce == 1) {
    // we're running in a container; use a random number instead!
    nonce = ceph::util::generate_random_number<uint64_t>();
  }
  return nonce;
}

uint64_t Messenger::get_random_nonce()
{
  return ceph::util::generate_random_number<uint64_t>();
}

Messenger *Messenger::create(CephContext *cct, const std::string &type,
			     entity_name_t name, std::string lname,
			     uint64_t nonce)
{
  int r = -1;
  if (type == "random") {
    r = 0;
    //r = ceph::util::generate_random_number(0, 1);
  }
  if (r == 0 || type.find("async") != std::string::npos)
    return new AsyncMessenger(cct, name, type, std::move(lname), nonce);
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
    auth_registry(cct)
{
  auth_registry.refresh_config();
  dispatch_throttle_bytes = cct->_conf.get_val<Option::size_t>("ms_dispatch_throttle_bytes");
  dispatch_throttle_log_interval = cct->_conf.get_val<std::chrono::seconds>("ms_dispatch_throttle_log_interval");
}

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
  return bind(addrs.legacy_addr());
}

