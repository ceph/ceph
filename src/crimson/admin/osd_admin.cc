// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/ceph_context.h"

#include <iostream>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include "seastar/core/future.hh"
#include "seastar/core/thread.hh"
#include "crimson/admin/admin_socket.h"
#include "crimson/admin/osd_admin.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/exceptions.h"
#include "common/config.h"
#include "crimson/common/log.h"

// for CINIT_FLAGS
//#include "common/common_init.h"

#include <iostream>

#ifndef WITH_SEASTAR
#error "this is a Crimson-specific implementation of some OSD APIs"
#endif

using ceph::bufferlist;
using ceph::common::local_conf;
using ceph::osd::OSD;

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace ceph::osd {

/*!
  the hooks and states needed to handle OSD asok requests
*/
class OsdAdminImp {
  friend class OsdAdmin;
  friend class OsdAdminHookBase;

  OSD* m_osd;
  CephContext* m_cct;
  ceph::common::ConfigProxy& m_conf;

  //  shared-ownership of the socket server itself, to guarantee its existence until we have
  //  a chance to remove our registration:
  AsokRegistrationRes m_socket_server;

  /*!
       Common code for all OSD admin hooks.
       Adds access to the owning OSD.
  */
  class OsdAdminHookBase : public AdminSocketHook {
  protected:
    OsdAdminImp& m_osd_admin;

    /// the specific command implementation
    virtual seastar::future<> exec_command(Formatter* formatter, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) const = 0;

    explicit OsdAdminHookBase(OsdAdminImp& master) :
      m_osd_admin{master}
    {}
  };

  /*!
       An OSD admin hook: OSD status
   */
  class OsdStatusHook : public OsdAdminHookBase {
  public:
    explicit OsdStatusHook(OsdAdminImp& master) : OsdAdminHookBase(master) {};
    seastar::future<> exec_command(Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) const final {

      f->dump_stream("cluster_fsid") << m_osd_admin.osd_superblock().cluster_fsid;
      f->dump_stream("osd_fsid") << m_osd_admin.osd_superblock().osd_fsid;
      f->dump_unsigned("whoami", m_osd_admin.osd_superblock().whoami);
      // \todo f->dump_string("state", get_state_name(get_state()));       // RRR where should I look for the data?
      f->dump_unsigned("oldest_map", m_osd_admin.osd_superblock().oldest_map);
      f->dump_unsigned("newest_map", m_osd_admin.osd_superblock().newest_map);
      // \todo f->dump_unsigned("num_pgs", num_pgs);  -> where to find the data?
      return seastar::now();
    }
  };

  /*!
       An OSD admin hook: send beacon
   */
  class SendBeaconHook : public OsdAdminHookBase {
  public:
    explicit SendBeaconHook(OsdAdminImp& master) : OsdAdminHookBase(master) {};
    seastar::future<> exec_command(Formatter* f, [[maybe_unused]] std::string_view command,
                                   [[maybe_unused]] const cmdmap_t& cmdmap,
	                           [[maybe_unused]] std::string_view format, [[maybe_unused]] bufferlist& out) const final
    {
      // \todo if (!is_active()) -> where to find the data?
      // \todo   return seastar::now();

      return m_osd_admin.m_osd->send_beacon();
    }
  };

  /*!
       A test hook that throws or returns an exceptional future
   */
  class TestThrowHook : public OsdAdminHookBase {
  public:
    explicit TestThrowHook(OsdAdminImp& master) : OsdAdminHookBase(master) {};
    seastar::future<> exec_command(Formatter* f, std::string_view command,
                                   [[maybe_unused]] const cmdmap_t& cmdmap,
                                   [[maybe_unused]] std::string_view format, [[maybe_unused]] bufferlist& out) const final {

      if (command == "fthrow")
        return seastar::make_exception_future<>(ceph::osd::no_message_available{});
      throw(std::invalid_argument("TestThrowHook"));
    }
  };

  /*!
       provide the hooks with access to OSD internals
  */
  const OSDSuperblock& osd_superblock() {
    return m_osd->superblock;
  }

  OsdStatusHook   osd_status_hook;
  SendBeaconHook  send_beacon_hook;
  TestThrowHook   osd_test_throw_hook;

public:

  OsdAdminImp(OSD* osd, CephContext* cct, ceph::common::ConfigProxy& conf)
    : m_osd{osd}
    , m_cct{cct}
    , m_conf{conf}
    , osd_status_hook{*this}
    , send_beacon_hook{*this}
    , osd_test_throw_hook{*this}
  {
  }

  ~OsdAdminImp() {
    // our registration with the admin_socket server was already removed by
    // 'OsdAdmin' - our 'pimpl' owner. Thus no need for:
    //   unregister_admin_commands();
  }

  seastar::future<> register_admin_commands() {
    static const std::vector<AsokServiceDef> hooks_tbl{
        AsokServiceDef{"status",      "status",        &osd_status_hook,      "OSD status"}
      , AsokServiceDef{"send_beacon", "send_beacon",   &send_beacon_hook,     "send OSD beacon to mon immediately"}
      , AsokServiceDef{"throw",       "throw",         &osd_test_throw_hook,  ""}  // dev tool
      , AsokServiceDef{"fthrow",      "fthrow",        &osd_test_throw_hook,  ""}  // dev tool
    };

    return m_cct->get_admin_socket()->register_server(AdminSocket::hook_server_tag{this}, hooks_tbl).
      then([this](AsokRegistrationRes rr) {
        m_socket_server = rr;
      });
  }

  seastar::future<> unregister_admin_commands()
  {
    if (!m_socket_server.has_value()) {
      logger().warn("{}: OSD asok APIs removed already", __func__);
      return seastar::now();
    }

    AdminSocketRef srv{std::move(m_socket_server.value())};

    // note that unregister_server() closes a seastar::gate (i.e. - it blocks)
    auto admin_if = m_cct->get_admin_socket();
    assert(admin_if);
    return admin_if->unregister_server(AdminSocket::hook_server_tag{this}, std::move(srv));
  }
};

//
//  some Pimpl details:
//
OsdAdmin::OsdAdmin(OSD* osd, CephContext* cct, ceph::common::ConfigProxy& conf)
  : m_imp{ std::make_unique<ceph::osd::OsdAdminImp>(osd, cct, conf) }
{}

seastar::future<>  OsdAdmin::register_admin_commands()
{
  return m_imp->register_admin_commands();
}

seastar::future<> OsdAdmin::unregister_admin_commands()
{
  return m_imp->unregister_admin_commands();
}

OsdAdmin::~OsdAdmin()
{
  // relinquish control over the actual implementation object, as that one should only be
  // destructed after the relevant seastar::gate closes
  std::ignore = seastar::do_with(std::move(m_imp), [](auto&& imp) {
    // test using sleep(). Change from 1ms to 1s:
    return seastar::sleep(1ms).
    then([imp_ptr = imp.get()]() {
       return imp_ptr->unregister_admin_commands();
    });
  });
}

} // namespace
