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

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

namespace crimson::admin {

using crimson::common::local_conf;

template <class Hook, class... Args>
std::unique_ptr<AdminSocketHook> make_asok_hook(Args&&... args)
{
  return std::make_unique<Hook>(std::forward<Args>(args)...);
}

class OsdAdminHookBase : public AdminSocketHook {
protected:
  OsdAdminHookBase(std::string_view prefix,
		   std::string_view desc,
		   std::string_view help)
    : AdminSocketHook(prefix, desc, help) {}
  struct tell_result_t {
    int ret = 0;
    std::string err;
  };
  /// the specific command implementation
  virtual seastar::future<tell_result_t> tell(const cmdmap_t& cmdmap,
					      Formatter* f) const = 0;
  seastar::future<bufferlist> call(std::string_view prefix,
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
};

/**
 * An OSD admin hook: OSD status
 */
class OsdStatusHook : public OsdAdminHookBase {
public:
  explicit OsdStatusHook(crimson::osd::OSD& osd) :
    OsdAdminHookBase("status", "status", "OSD status"),
    osd(osd)
  {}
  seastar::future<tell_result_t> tell(const cmdmap_t&,
				      Formatter* f) const final
  {
    f->open_object_section("status");
    osd.dump_status(f);
    f->close_section();
    return seastar::make_ready_future<tell_result_t>();
  }
private:
  crimson::osd::OSD& osd;
};
template std::unique_ptr<AdminSocketHook>
make_asok_hook<OsdStatusHook>(crimson::osd::OSD& osd);

/**
 * An OSD admin hook: send beacon
 */
class SendBeaconHook : public OsdAdminHookBase {
public:
  explicit SendBeaconHook(crimson::osd::OSD& osd) :
  OsdAdminHookBase("send_beacon",
		   "send_beacon",
		   "send OSD beacon to mon immediately"),
  osd(osd)
  {}
  seastar::future<tell_result_t> tell(const cmdmap_t&, Formatter*) const final
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
class ConfigShowHook : public OsdAdminHookBase {
public:
  explicit ConfigShowHook() :
  OsdAdminHookBase("config show",
		   "config show",
		   "dump current config settings")
  {}
  seastar::future<tell_result_t> tell(const cmdmap_t&, Formatter* f) const final
  {
    f->open_object_section("config_show");
    local_conf().show_config(f);
    f->close_section();
    return seastar::make_ready_future<tell_result_t>();
  }
};
template std::unique_ptr<AdminSocketHook> make_asok_hook<ConfigShowHook>();

/**
 * A CephContext admin hook: fetching the value of a specific
 * configuration item
 */
class ConfigGetHook : public OsdAdminHookBase {
public:
  ConfigGetHook() :
    OsdAdminHookBase("config get",
		     "config get name=var,type=CephString",
		     "config get <field>: get the config value")
  {}
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
template std::unique_ptr<AdminSocketHook> make_asok_hook<ConfigGetHook>();

/**
 * A CephContext admin hook: setting the value of a specific configuration
 * item (a real example: {"prefix": "config set", "var":"debug_osd", "val":
 * ["30/20"]} )
 */
class ConfigSetHook : public OsdAdminHookBase {
public:
  ConfigSetHook()  :
    OsdAdminHookBase("config set",
		     "config set name=var,type=CephString name=val,type=CephString,n=N",
		     "config set <field> <val> [<val> ...]: set a config variable")
  {}
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
template std::unique_ptr<AdminSocketHook> make_asok_hook<ConfigSetHook>();

/**
 * A CephContext admin hook: calling assert (if allowed by
 * 'debug_asok_assert_abort')
 */
class AssertAlwaysHook : public OsdAdminHookBase {
public:
  AssertAlwaysHook()  :
    OsdAdminHookBase("assert",
		     "assert",
		     "asserts")
  {}
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
template std::unique_ptr<AdminSocketHook> make_asok_hook<AssertAlwaysHook>();

} // namespace crimson::admin
