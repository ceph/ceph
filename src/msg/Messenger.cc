// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <thread>
#include "include/types.h"
#include "Messenger.h"

#include "msg/simple/SimpleMessenger.h"
#include "msg/async/AsyncMessenger.h"
#ifdef HAVE_XIO
#include "msg/xio/XioMessenger.h"
#endif

Messenger *Messenger::create_client_messenger(CephContext *cct, string lname)
{
  uint64_t nonce = 0;
  get_random_bytes((char*)&nonce, sizeof(nonce));
  return Messenger::create(cct, cct->_conf->ms_type, entity_name_t::CLIENT(),
			   lname, nonce, 0);
}

Messenger *Messenger::create(CephContext *cct, const string &type,
			     entity_name_t name, string lname,
			     uint64_t nonce, uint64_t features)
{
  int r = -1;
  if (type == "random") {
    thread_local unsigned seed = (unsigned) time(nullptr) +
      (unsigned) std::hash<std::thread::id>()(std::this_thread::get_id());
    r = rand_r(&seed) % 2; // random does not include xio
  }
  if (r == 0 || type == "simple")
    return new SimpleMessenger(cct, name, lname, nonce, features);
  else if (r == 1 || type == "async")
    return new AsyncMessenger(cct, name, lname, nonce, features);
#ifdef HAVE_XIO
  else if ((type == "xio") &&
	   cct->check_experimental_feature_enabled("ms-type-xio"))
    return new XioMessenger(cct, name, lname, nonce, features);
#endif
  lderr(cct) << "unrecognized ms_type '" << type << "'" << dendl;
  return nullptr;
}

/*
 * Pre-calculate desired software CRC settings.  CRC computation may
 * be disabled by default for some transports (e.g., those with strong
 * hardware checksum support).
 */
int Messenger::get_default_crc_flags(md_config_t * conf)
{
  int r = 0;
  if (conf->ms_crc_data)
    r |= MSG_CRC_DATA;
  if (conf->ms_crc_header)
    r |= MSG_CRC_HEADER;
  return r;
}
