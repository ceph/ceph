// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/admin/osd_admin.h"

#include <fmt/format.h>
#include <seastar/core/do_with.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>

#include "common/config.h"
#include "crimson/admin/admin_socket.h"
#include "crimson/common/log.h"
#include "crimson/osd/exceptions.h"
#include "crimson/osd/osd.h"

using crimson::osd::OSD;

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

namespace crimson::admin {

using crimson::common::local_conf;

/**
 * the hooks and states needed to handle OSD asok requests
 */
class OsdAdminImp {
  friend class OsdAdmin;
  friend class OsdAdminHookBase;

  OSD* m_osd;

  //  shared-ownership of the socket server itself, to guarantee its existence
  //  until we have a chance to remove our registration:
  AsokRegistrationRes m_socket_server;

  /**
   * Common code for all OSD admin hooks.
   * Adds access to the owning OSD.
   */
  class OsdAdminHookBase : public AdminSocketHook {
   protected:
    explicit OsdAdminHookBase(OsdAdminImp& master) : m_osd_admin{ master } {}
    struct tell_result_t {
      int ret = 0;
      string err;
    };
    /// the specific command implementation
    virtual seastar::future<tell_result_t> tell(const cmdmap_t& cmdmap,
						Formatter* f) const = 0;
    seastar::future<bufferlist> call(std::string_view command,
				     std::string_view format,
				     const cmdmap_t& cmdmap) const final
    {
      unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
      auto ret = seastar::do_with(std::move(f), [cmdmap, this] (auto& f) {
	Formatter* formatter = f.get();
	return tell(cmdmap, formatter).then([formatter](auto result) {
	  bufferlist out;
	  if (auto& [ret, err] = result; ret < 0) {
	    out.append(fmt::format("ERROR: {}\n", cpp_strerror(ret)));
	    out.append(err);
	  } else {
	    formatter->flush(out);
	  }
	  return seastar::make_ready_future<bufferlist>(std::move(out));
        });
      });
      return ret;
    }
    OsdAdminImp& m_osd_admin;
  };

  /**
   * An OSD admin hook: OSD status
   */
  class OsdStatusHook : public OsdAdminHookBase {
   public:
    explicit OsdStatusHook(OsdAdminImp& master) : OsdAdminHookBase(master){};
    seastar::future<tell_result_t> tell(const cmdmap_t&,
					Formatter* f) const final
    {
      f->open_object_section("status");
      f->dump_stream("cluster_fsid")
        << m_osd_admin.osd_superblock().cluster_fsid;
      f->dump_stream("osd_fsid") << m_osd_admin.osd_superblock().osd_fsid;
      f->dump_unsigned("whoami", m_osd_admin.osd_superblock().whoami);
      f->dump_string("state", m_osd_admin.osd_state_name());
      f->dump_unsigned("oldest_map", m_osd_admin.osd_superblock().oldest_map);
      f->dump_unsigned("newest_map", m_osd_admin.osd_superblock().newest_map);
      f->dump_unsigned("num_pgs", m_osd_admin.m_osd->pg_map.get_pgs().size());
      f->close_section();
      return seastar::make_ready_future<tell_result_t>();
    }
  };

  /**
   * An OSD admin hook: send beacon
   */
  class SendBeaconHook : public OsdAdminHookBase {
   public:
    explicit SendBeaconHook(OsdAdminImp& master) : OsdAdminHookBase(master){};
    seastar::future<tell_result_t> tell(const cmdmap_t&, Formatter*) const final
    {
      if (m_osd_admin.get_osd_state().is_active()) {
        return m_osd_admin.m_osd->send_beacon().then([] {
          return seastar::make_ready_future<tell_result_t>();
        });
      } else {
        return seastar::make_ready_future<tell_result_t>();
      }
    }
  };

  /**
   * A CephContext admin hook: listing the configuration values
   */
  class ConfigShowHook : public OsdAdminHookBase {
   public:
    explicit ConfigShowHook(OsdAdminImp& master)
        : OsdAdminHookBase(master) {}
    seastar::future<tell_result_t> tell(const cmdmap_t&, Formatter* f) const final
    {
      f->open_object_section("config_show");
      local_conf().show_config(f);
      f->close_section();
      return seastar::make_ready_future<tell_result_t>();
    }
  };

  /**
   * A CephContext admin hook: fetching the value of a specific
   * configuration item
   */
  class ConfigGetHook : public OsdAdminHookBase {
   public:
    explicit ConfigGetHook(OsdAdminImp& master)
        : OsdAdminHookBase(master) {}
    seastar::future<tell_result_t> tell(const cmdmap_t& cmdmap,
					ceph::Formatter* f) const final
    {
      std::string var;
      if (!cmd_getval(cmdmap, "var", var)) {
        // should have been caught by 'validate()'
        return seastar::make_ready_future<tell_result_t>(
	  tell_result_t{-EINVAL, "syntax error: 'config get <var>'"});
      }
      try {
	f->open_object_section("config_get");
	std::string conf_val =
	  local_conf().get_val<std::string>(var);
	f->dump_string(var.c_str(), conf_val.c_str());
	f->close_section();
	return seastar::make_ready_future<tell_result_t>();
      } catch (const boost::bad_get&) {
	return seastar::make_ready_future<tell_result_t>(
	  tell_result_t{-EINVAL, fmt::format("unrecognized option {}", var)});
      }
    }
  };

  /**
   * A CephContext admin hook: setting the value of a specific configuration
   * item (a real example: {"prefix": "config set", "var":"debug_osd", "val":
   * ["30/20"]} )
   */
  class ConfigSetHook : public OsdAdminHookBase {
   public:
    explicit ConfigSetHook(OsdAdminImp& master)
        : OsdAdminHookBase(master) {}

    seastar::future<tell_result_t> tell(const cmdmap_t& cmdmap,
					ceph::Formatter* f) const final
    {
      std::string var;
      std::vector<std::string> new_val;
      if (!cmd_getval(cmdmap, "var", var) ||
          !cmd_getval(cmdmap, "val", new_val)) {
	return seastar::make_ready_future<tell_result_t>(
	  tell_result_t{-EINVAL, "syntax error: 'config set <var> <value>'"});
      }
      // val may be multiple words
      string valstr = str_join(new_val, " ");
      return local_conf().set_val(var, valstr).then([f] {
	f->open_object_section("config_set");
        f->dump_string("success", "");
	f->close_section();
	return seastar::make_ready_future<tell_result_t>();
      }).handle_exception_type([](std::invalid_argument& e) {
	return seastar::make_ready_future<tell_result_t>(
          tell_result_t{-EINVAL, e.what()});
      });
    }
  };

  /**
   * A CephContext admin hook: calling assert (if allowed by
   * 'debug_asok_assert_abort')
   */
  class AssertAlwaysHook : public OsdAdminHookBase {
   public:
    explicit AssertAlwaysHook(OsdAdminImp& master)
        : OsdAdminHookBase(master) {}
    seastar::future<tell_result_t> tell(const cmdmap_t&,
					ceph::Formatter*) const final
    {
      if (local_conf().get_val<bool>("debug_asok_assert_abort")) {
	ceph_assert_always(0);
	return seastar::make_ready_future<tell_result_t>();
      } else {
	return seastar::make_ready_future<tell_result_t>(
	  tell_result_t{-EPERM, "configuration set to disallow asok assert"});
      }
    }
  };

  /**
   * provide the hooks with access to OSD internals
   */
  const OSDSuperblock& osd_superblock() const
  {
    return m_osd->superblock;
  }

  OSDState get_osd_state() const
  {
    return m_osd->state;
  }

  string_view osd_state_name() const
  {
    return m_osd->state.to_string();
  }

  OsdStatusHook osd_status_hook{*this};
  SendBeaconHook send_beacon_hook{*this};
  ConfigShowHook config_show_hook{*this};
  ConfigGetHook config_get_hook{*this};
  ConfigSetHook config_set_hook{*this};
  AssertAlwaysHook assert_hook{*this};

 public:
  OsdAdminImp(OSD* osd)
      : m_osd{ osd }
  {}

  ~OsdAdminImp() = default;

  seastar::future<> register_admin_commands()
  {
    static const std::vector<AsokServiceDef> hooks_tbl{
      // clang-format off
        AsokServiceDef{"status",      "status",        &osd_status_hook,
                      "OSD status"}
      , AsokServiceDef{"send_beacon", "send_beacon",   &send_beacon_hook,
                      "send OSD beacon to mon immediately"}
      , AsokServiceDef{"config show", "config show", &config_show_hook,
                      "dump current config settings" }
      , AsokServiceDef{"config get", "config get name=var,type=CephString",
		       &config_get_hook,
		       "config get <field>: get the config value" }
      , AsokServiceDef{"config set",
		       "config set name=var,type=CephString name=val,type=CephString,n=N",
		       &config_set_hook,
		       "config set <field> <val> [<val> ...]: set a config variable" }
      , AsokServiceDef{ "assert", "assert", &assert_hook, "asserts" }
      // clang-format on
    };

    return m_osd->asok
      ->register_server(AdminSocket::hook_server_tag{ this }, hooks_tbl)
      .then([this](AsokRegistrationRes res) { m_socket_server = res; });
  }

  seastar::future<> unregister_admin_commands()
  {
    if (!m_socket_server.has_value()) {
      logger().warn("{}: OSD asok APIs already removed", __func__);
      return seastar::now();
    }

    AdminSocketRef srv{ std::move(m_socket_server.value()) };
    m_socket_server.reset();

    return m_osd->asok->unregister_server(AdminSocket::hook_server_tag{ this },
                                          std::move(srv));
  }
};

//
//  some PIMPL details:
//
OsdAdmin::OsdAdmin(OSD* osd)
    : m_imp{ std::make_unique<crimson::admin::OsdAdminImp>(osd) }
{}

seastar::future<> OsdAdmin::register_admin_commands()
{
  return m_imp->register_admin_commands();
}

seastar::future<> OsdAdmin::unregister_admin_commands()
{
  return seastar::do_with(std::move(m_imp), [](auto& detached_imp) {
    return detached_imp->unregister_admin_commands();
  });
}

OsdAdmin::~OsdAdmin() = default;
}  // namespace crimson::admin
