// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/admin/osd_admin.h"
#include <string>
#include <string_view>

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
using namespace crimson::common;

namespace crimson::admin {

using crimson::common::local_conf;

template <class Hook, class... Args>
std::unique_ptr<AdminSocketHook> make_asok_hook(Args&&... args)
{
  return std::make_unique<Hook>(std::forward<Args>(args)...);
}

/**
 * An OSD admin hook: OSD status
 */
class OsdStatusHook : public AdminSocketHook {
public:
  explicit OsdStatusHook(crimson::osd::OSD& osd) :
    AdminSocketHook("status", "status", "OSD status"),
    osd(osd)
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("status");
    osd.dump_status(f.get());
    f->close_section();
    return seastar::make_ready_future<tell_result_t>(f.get());
  }
private:
  crimson::osd::OSD& osd;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<OsdStatusHook>(crimson::osd::OSD& osd);

/**
 * An OSD admin hook: send beacon
 */
class SendBeaconHook : public AdminSocketHook {
public:
  explicit SendBeaconHook(crimson::osd::OSD& osd) :
    AdminSocketHook("send_beacon",
		    "send_beacon",
		    "send OSD beacon to mon immediately"),
    osd(osd)
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    return osd.send_beacon().then([] {
      return seastar::make_ready_future<tell_result_t>();
    });
  }
private:
  crimson::osd::OSD& osd;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<SendBeaconHook>(crimson::osd::OSD& osd);

/**
 * A CephContext admin hook: listing the configuration values
 */
class ConfigShowHook : public AdminSocketHook {
public:
  explicit ConfigShowHook() :
  AdminSocketHook("config show",
		   "config show",
		   "dump current config settings")
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->open_object_section("config_show");
    local_conf().show_config(f.get());
    f->close_section();
    return seastar::make_ready_future<tell_result_t>(f.get());
  }
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<ConfigShowHook>();

/**
 * A CephContext admin hook: fetching the value of a specific
 * configuration item
 */
class ConfigGetHook : public AdminSocketHook {
public:
  ConfigGetHook() :
    AdminSocketHook("config get",
		     "config get name=var,type=CephString",
		     "config get <field>: get the config value")
  {}
  seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    std::string var;
    if (!cmd_getval(cmdmap, "var", var)) {
      // should have been caught by 'validate()'
      return seastar::make_ready_future<tell_result_t>(
        tell_result_t{-EINVAL, "syntax error: 'config get <var>'"});
    }
    try {
      unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
      f->open_object_section("config_get");
      std::string conf_val =
        local_conf().get_val<std::string>(var);
      f->dump_string(var.c_str(), conf_val.c_str());
      f->close_section();
      return seastar::make_ready_future<tell_result_t>(f.get());
    } catch (const boost::bad_get&) {
      return seastar::make_ready_future<tell_result_t>(
        tell_result_t{-EINVAL, fmt::format("unrecognized option {}", var)});
    }
  }
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<ConfigGetHook>();

/**
 * A CephContext admin hook: setting the value of a specific configuration
 * item (a real example: {"prefix": "config set", "var":"debug_osd", "val":
 * ["30/20"]} )
 */
class ConfigSetHook : public AdminSocketHook {
public:
  ConfigSetHook()  :
    AdminSocketHook("config set",
		     "config set name=var,type=CephString name=val,type=CephString,n=N",
		     "config set <field> <val> [<val> ...]: set a config variable")
  {}
  seastar::future<tell_result_t> call(const cmdmap_t& cmdmap,
				      std::string_view format,
				      ceph::bufferlist&&) const final
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
    return local_conf().set_val(var, valstr).then([format] {
      unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
      f->open_object_section("config_set");
      f->dump_string("success", "");
      f->close_section();
      return seastar::make_ready_future<tell_result_t>(f.get());
    }).handle_exception_type([](std::invalid_argument& e) {
      return seastar::make_ready_future<tell_result_t>(
        tell_result_t{-EINVAL, e.what()});
    });
  }
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<ConfigSetHook>();

/**
 * send the latest pg stats to mgr
 */
class FlushPgStatsHook : public AdminSocketHook {
public:
  explicit FlushPgStatsHook(crimson::osd::OSD& osd) :
    AdminSocketHook("flush_pg_stats",
		    "flush_pg_stats",
		    "flush pg stats"),
    osd{osd}
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
  {
    uint64_t seq = osd.send_pg_stats();
    unique_ptr<Formatter> f{Formatter::create(format, "json-pretty", "json-pretty")};
    f->dump_unsigned("stat_seq", seq);
    return seastar::make_ready_future<tell_result_t>(tell_result_t(f.get()));
  }

private:
  crimson::osd::OSD& osd;
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<FlushPgStatsHook>(crimson::osd::OSD& osd);

/**
 * A CephContext admin hook: calling assert (if allowed by
 * 'debug_asok_assert_abort')
 */
class AssertAlwaysHook : public AdminSocketHook {
public:
  AssertAlwaysHook()  :
    AdminSocketHook("assert",
		     "assert",
		     "asserts")
  {}
  seastar::future<tell_result_t> call(const cmdmap_t&,
				      std::string_view format,
				      ceph::bufferlist&& input) const final
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
template std::unique_ptr<AdminSocketHook> make_asok_hook<AssertAlwaysHook>();

} // namespace crimson::admin
