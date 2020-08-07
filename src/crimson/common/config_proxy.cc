// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "config_proxy.h"

#if __has_include(<filesystem>)
#include <filesystem>
#else
#include <experimental/filesystem>
#endif

#include "crimson/common/buffer_io.h"

#if defined(__cpp_lib_filesystem)
namespace fs = std::filesystem;
#elif defined(__cpp_lib_experimental_filesystem)
namespace fs = std::experimental::filesystem;
#endif

namespace crimson::common {

ConfigProxy::ConfigProxy(const EntityName& name, std::string_view cluster)
{
  if (seastar::this_shard_id() != 0) {
    return;
  }
  // set the initial value on CPU#0
  values.reset(seastar::make_lw_shared<ConfigValues>());
  values.get()->name = name;
  values.get()->cluster = cluster;
  // and the only copy of md_config_impl<> is allocated on CPU#0
  local_config.reset(new md_config_t{*values, obs_mgr, true});
  if (name.is_mds()) {
    local_config->set_val_default(*values, obs_mgr,
				  "keyring", "$mds_data/keyring");
  } else if (name.is_osd()) {
    local_config->set_val_default(*values, obs_mgr,
				  "keyring", "$osd_data/keyring");
  }
}

seastar::future<> ConfigProxy::start()
{
  // populate values and config to all other shards
  if (!values) {
    return seastar::make_ready_future<>();
  }
  return container().invoke_on_others([this](auto& proxy) {
    return values.copy().then([config=local_config.get(),
			       &proxy](auto foreign_values) {
      proxy.values.reset();
      proxy.values = std::move(foreign_values);
      proxy.remote_config = config;
      return seastar::make_ready_future<>();
    });
  });
}

void ConfigProxy::show_config(ceph::Formatter* f) const {
  get_config().show_config(*values, f);
}

seastar::future<> ConfigProxy::parse_config_files(const std::string& conf_files)
{
  auto conffile_paths =
    get_config().get_conffile_paths(*values,
                                    conf_files.empty() ? nullptr : conf_files.c_str(),
                                    &std::cerr,
                                    CODE_ENVIRONMENT_DAEMON);
  return seastar::do_with(std::move(conffile_paths), [this] (auto& paths) {
    return seastar::repeat([path=paths.begin(), e=paths.end(), this]() mutable {
      if (path == e) {
        // tried all conffile, none of them works
        return seastar::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::yes);
      }
      return crimson::read_file(*path++).then([this](auto&& buf) {
        return do_change([buf=std::move(buf), this](ConfigValues& values) {
          if (get_config().parse_buffer(values, obs_mgr, buf.get(), buf.size(), &std::cerr)) {
            throw std::invalid_argument("parse error");
          }
        }).then([] {
          // this one works!
	  return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        });
      }).handle_exception_type([] (const fs::filesystem_error&) {
        return seastar::make_ready_future<seastar::stop_iteration>(
          seastar::stop_iteration::no);
      }).handle_exception_type([] (const std::invalid_argument&) {
        return seastar::make_ready_future<seastar::stop_iteration>(
         seastar::stop_iteration::no);
      });
    });
  });
}

ConfigProxy::ShardedConfig ConfigProxy::sharded_conf;
}
