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

#include "AuthServiceHandler.h"
#include "cephx/CephxServiceHandler.h"
#ifdef HAVE_GSSAPI
#include "krb/KrbServiceHandler.hpp"
#endif
#include "none/AuthNoneServiceHandler.h"
#include "common/dout.h"

#define dout_subsys ceph_subsys_auth


std::ostream& operator<<(std::ostream& os,
			 global_id_status_t global_id_status)
{
  switch (global_id_status) {
  case global_id_status_t::NONE:
    return os << "none";
  case global_id_status_t::NEW_PENDING:
    return os << "new_pending";
  case global_id_status_t::NEW_OK:
    return os << "new_ok";
  case global_id_status_t::NEW_NOT_EXPOSED:
    return os << "new_not_exposed";
  case global_id_status_t::RECLAIM_PENDING:
    return os << "reclaim_pending";
  case global_id_status_t::RECLAIM_OK:
    return os << "reclaim_ok";
  case global_id_status_t::RECLAIM_INSECURE:
    return os << "reclaim_insecure";
  default:
    ceph_abort();
  }
}

int AuthServiceHandler::start_session(const EntityName& entity_name,
				      uint64_t global_id,
				      bool is_new_global_id,
				      ceph::buffer::list *result,
				      AuthCapsInfo *caps)
{
  ceph_assert(!this->entity_name.get_type() && !this->global_id &&
	      global_id_status == global_id_status_t::NONE);

  ldout(cct, 10) << __func__ << " entity_name=" << entity_name
		 << " global_id=" << global_id << " is_new_global_id="
		 << is_new_global_id << dendl;
  this->entity_name = entity_name;
  this->global_id = global_id;

  return do_start_session(is_new_global_id, result, caps);
}

AuthServiceHandler *get_auth_service_handler(int type, CephContext *cct, KeyServer *ks)
{
  switch (type) {
  case CEPH_AUTH_CEPHX:
    return new CephxServiceHandler(cct, ks);
  case CEPH_AUTH_NONE:
    return new AuthNoneServiceHandler(cct);
#ifdef HAVE_GSSAPI
  case CEPH_AUTH_GSS: 
    return new KrbServiceHandler(cct, ks);
#endif
  default:
    return nullptr;
  }
}
