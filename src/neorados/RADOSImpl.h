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
#ifndef CEPH_NEORADOS_RADOSIMPL_H
#define CEPH_NEORADOS_RADOSIMPL_H

#include <atomic>
#include <memory>

#include <boost/intrusive_ptr.hpp>

#include "common/async/service.h"

#include "common/ceph_context.h"
#include "common/ceph_mutex.h"

#include "librados/RadosClient.h"

#include "mon/MonClient.h"

#include "mgr/MgrClient.h"

#include "osdc/Objecter.h"

namespace boost::asio { class io_context; }

namespace neorados {

class RADOS;

namespace detail {

class NeoClient;

class RADOS : public Dispatcher {
  friend ::neorados::RADOS;
  friend NeoClient;

  boost::asio::io_context& ioctx;
  boost::intrusive_ptr<CephContext> cct;

  ceph::mutex lock = ceph::make_mutex("neorados::detail::RADOSImpl");
  int instance_id = -1;

  std::unique_ptr<Messenger> messenger;

  MonClient monclient;
  MgrClient mgrclient;

  std::unique_ptr<Objecter> objecter;
  std::atomic<bool> finished = false;

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
  void shutdown();
};

class Client : public std::enable_shared_from_this<Client> {
public:
  Client(boost::asio::io_context& ioctx,
         boost::intrusive_ptr<CephContext> cct,
         MonClient& monclient, Objecter* objecter)
    : ioctx(ioctx), cct(cct), monclient(monclient), objecter(objecter) {
  }
  virtual ~Client() {}

  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;

  boost::asio::io_context& ioctx;

  boost::intrusive_ptr<CephContext> cct;
  MonClient& monclient;
  Objecter* objecter;

  mon_feature_t get_required_monitor_features() const {
    return monclient.with_monmap(std::mem_fn(&MonMap::get_required_features));
  }

  virtual int get_instance_id() const = 0;
};

class NeoClient : public Client,
		  public ceph::async::service_list_base_hook {
public:

  NeoClient(std::unique_ptr<RADOS>&& rados)
    : Client(rados->ioctx, rados->cct, rados->monclient,
	     rados->objecter.get()),
      svc(boost::asio::use_service<ceph::async::service<NeoClient>>(
	  boost::asio::query(ioctx.get_executor(),
			     boost::asio::execution::context))),
      rados(std::move(rados)) {
    svc.add(*this);
  }

  ~NeoClient() {
    svc.remove(*this);
  }

  int get_instance_id() const override {
    return rados->instance_id;
  }

  void service_shutdown() {
    // In case the last owner of a reference is an op we're about to
    // cancel. (This can happen if the `RADOS` object
    auto service_ref = shared_from_this();
    rados->shutdown();
  }

private:
  friend ceph::async::service<RADOS>;
  async::service<NeoClient>& svc;
  std::unique_ptr<RADOS> rados;
};

class RadosClient : public Client {
public:
  RadosClient(librados::RadosClient* rados_client)
    : Client(rados_client->poolctx, {rados_client->cct},
             rados_client->monclient, rados_client->objecter),
      rados_client(rados_client) {
  }

  int get_instance_id() const override {
    return rados_client->instance_id;
  }

public:
  librados::RadosClient* rados_client;
};

} // namespace detail
} // namespace neorados

#endif
