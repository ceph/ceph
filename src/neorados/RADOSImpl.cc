// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#include <boost/system/system_error.hpp>

#include "common/common_init.h"

#include "global/global_init.h"

#include "RADOSImpl.h"

namespace neorados {
namespace detail {

RADOS::RADOS(boost::asio::io_context& ioctx,
	     boost::intrusive_ptr<CephContext> cct)
  : Dispatcher(cct.get()),
    ioctx(ioctx),
    cct(cct),
    monclient(cct.get(), ioctx),
    mgrclient(cct.get(), nullptr, &monclient.monmap) {
  auto err = monclient.build_initial_monmap();
  if (err < 0)
    throw std::system_error(ceph::to_error_code(err));

  messenger.reset(Messenger::create_client_messenger(cct.get(), "radosclient"));
  if (!messenger)
    throw std::bad_alloc();

  // Require OSDREPLYMUX feature. This means we will fail to talk to
  // old servers. This is necessary because otherwise we won't know
  // how to decompose the reply data into its constituent pieces.
  messenger->set_default_policy(
    Messenger::Policy::lossy_client(CEPH_FEATURE_OSDREPLYMUX));

  objecter.reset(new Objecter(cct.get(), messenger.get(), &monclient,
			      ioctx,
			      cct->_conf->rados_mon_op_timeout,
			      cct->_conf->rados_osd_op_timeout));

  objecter->set_balanced_budget();
  monclient.set_messenger(messenger.get());
  mgrclient.set_messenger(messenger.get());
  objecter->init();
  messenger->add_dispatcher_head(&mgrclient);
  messenger->add_dispatcher_tail(objecter.get());
  messenger->start();
  monclient.set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD | CEPH_ENTITY_TYPE_MGR);
  err = monclient.init();
  if (err) {
    throw boost::system::system_error(ceph::to_error_code(err));
  }
  err = monclient.authenticate(cct->_conf->client_mount_timeout);
  if (err) {
    throw boost::system::system_error(ceph::to_error_code(err));
  }
  messenger->set_myname(entity_name_t::CLIENT(monclient.get_global_id()));
  // Detect older cluster, put mgrclient into compatible mode
  mgrclient.set_mgr_optional(
      !get_required_monitor_features().contains_all(
        ceph::features::mon::FEATURE_LUMINOUS));

  // MgrClient needs this (it doesn't have MonClient reference itself)
  monclient.sub_want("mgrmap", 0, 0);
  monclient.renew_subs();

  mgrclient.init();
  objecter->set_client_incarnation(0);
  objecter->start();

  messenger->add_dispatcher_tail(this);

  std::unique_lock l(lock);
  instance_id = monclient.get_global_id();
}

RADOS::~RADOS() {
  if (objecter && objecter->initialized) {
    objecter->shutdown();
  }

  mgrclient.shutdown();
  monclient.shutdown();

  if (messenger) {
    messenger->shutdown();
    messenger->wait();
  }
}

bool RADOS::ms_dispatch(Message *m)
{
  switch (m->get_type()) {
  // OSD
  case CEPH_MSG_OSD_MAP:
    m->put();
    return true;
  }
  return false;
}

void RADOS::ms_handle_connect(Connection *con) {}
bool RADOS::ms_handle_reset(Connection *con) {
  return false;
}
void RADOS::ms_handle_remote_reset(Connection *con) {}
bool RADOS::ms_handle_refused(Connection *con) {
  return false;
}

} // namespace detail
} // namespace neorados
