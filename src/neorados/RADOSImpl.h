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
#ifndef CEPH_LIBRADOS_RADOSCLIENT_H
#define CEPH_LIBRADOS_RADOSCLIENT_H

#include <functional>
#include <memory>
#include <string>

#include <boost/asio.hpp>
#include <boost/intrusive_ptr.hpp>

#include "common/ceph_context.h"
#include "common/ceph_mutex.h"

#include "mon/MonClient.h"

#include "mgr/MgrClient.h"

#include "osdc/Objecter.h"

namespace neorados {
  class RADOS;
namespace detail {

class RADOS : public Dispatcher
{
  friend ::neorados::RADOS;
  struct MsgDeleter {
    void operator()(Messenger* p) const {
      if (p) {
	p->shutdown();
	p->wait();
      }
      delete p;
    }
  };

  struct ObjDeleter {
    void operator()(Objecter* p) const {
      if (p) {
	p->shutdown();
      }
      delete p;
    }
  };

  template<typename T>
  struct scoped_shutdown {
    T& m;
    scoped_shutdown(T& m) : m(m) {}

    ~scoped_shutdown() {
      m.shutdown();
    }
  };

  boost::asio::io_context& ioctx;
  ceph::mutex lock = ceph::make_mutex("RADOS_unleashed::_::RADOSImpl");
  int instance_id = -1;

  std::unique_ptr<Messenger, MsgDeleter> messenger;

  MonClient monclient;
  scoped_shutdown<MonClient> moncsd;

  MgrClient mgrclient;
  scoped_shutdown<MgrClient> mgrcsd;

  std::unique_ptr<Objecter, ObjDeleter> objecter;


public:

  RADOS(boost::asio::io_context& ioctx, boost::intrusive_ptr<CephContext> cct);
  ~RADOS();
  bool ms_dispatch(Message *m) override;
  void ms_handle_connect(Connection *con) override;
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override;
  bool ms_handle_refused(Connection *con) override;
  mon_feature_t get_required_monitor_features() const {
    return monclient.with_monmap(std::mem_fn(&MonMap::get_required_features));
  }
  int get_instance_id() const {
    return instance_id;
  }
};
}
}

#endif
