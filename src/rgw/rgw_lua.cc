// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <lua.hpp>
#include "services/svc_zone.h"
#include "rgw_lua_utils.h"
#include "rgw_sal_rados.h"
#include "rgw_lua.h"
#ifdef WITH_RADOSGW_LUA_PACKAGES
#include <filesystem>
#include <boost/process.hpp>
#endif

#define dout_subsys ceph_subsys_rgw

namespace rgw::lua {

bool verify(const std::string& script, std::string& err_msg) 
{
  lua_state_guard lguard(0, nullptr); // no memory limit, sice we don't execute the script
  auto L = lguard.get();
  try {
    open_standard_libs(L);
    if (luaL_loadstring(L, script.c_str()) != LUA_OK) {
      err_msg.assign(lua_tostring(L, -1));
      return false;
    }
  } catch (const std::runtime_error& e) {
    err_msg = e.what();
    return false;
  }
  err_msg = "";
  return true;
}

std::string script_oid(context ctx, const std::string& tenant) {
  static const std::string SCRIPT_OID_PREFIX("script.");
  return SCRIPT_OID_PREFIX + to_string(ctx) + "." + tenant;
}

std::string script_meta_key(context ctx, const std::string& tenant) {
  static const std::string SCRIPT_META_PREFIX("lua_script_meta.");
  return SCRIPT_META_PREFIX + to_string(ctx) + "." + tenant;
} 

// std::time_t curr_time() {
//   auto now = std::chrono::system_clock::now();
//   return std::chrono::system_clock::to_time_t(now);
// }

int read_script(
  const DoutPrefixProvider *dpp, 
  sal::LuaManager* manager, 
  const std::string& tenant, 
  optional_yield y, 
  context ctx,
  rgw::lua::LuaRuntimeMeta& scripts_meta
) {
  return manager ? manager->get_script(
    dpp, y, script_meta_key(ctx, tenant), script_oid(ctx, tenant), scripts_meta, ctx) : -ENOENT;
}

int write_script(
  const DoutPrefixProvider *dpp, 
  sal::LuaManager* manager, 
  const std::string& tenant, 
  optional_yield y, 
  context ctx, 
  const std::string& script,
  const std::optional<std::uint8_t> optional_priority,
  const std::optional<std::string> optional_name
) {
  if (!manager) {
    return -ENOENT;
  }

  if (!optional_name) {
    auto new_script = LuaScriptMeta(0, ctx, "", script);
    auto empty_lua_runtime = LuaRuntimeMeta();
    return manager->put_script(
      dpp, y, script_oid(ctx, tenant), new_script, empty_lua_runtime);
  }
  
  int priority = optional_priority ? optional_priority.value() : rgw::lua::MAX_LUA_PRIORITY;
  auto new_script = LuaScriptMeta(priority, ctx, optional_name.value(), script);
  LuaRuntimeMeta scripts_meta;
  if (manager->get_script(dpp, y, script_meta_key(ctx, tenant), "", scripts_meta, ctx) < 0) {
    scripts_meta = LuaRuntimeMeta();
  }
  return manager->put_script(dpp, y, script_meta_key(ctx, tenant), new_script, scripts_meta);
}

int delete_script(
  const DoutPrefixProvider *dpp, 
  sal::LuaManager* manager, 
  const std::string& tenant, 
  optional_yield y, 
  context ctx, 
  std::optional<std::string> optional_name
) {
  if (!manager) {
    return -ENOENT;
  }

  if (!optional_name) {
    auto blank_runtime = LuaRuntimeMeta();
    return manager->del_script(dpp, y, script_oid(ctx, tenant), "", optional_name, blank_runtime);
  }

  LuaRuntimeMeta scripts_meta;
  if (manager->get_script(dpp, y, script_meta_key(ctx, tenant), "", scripts_meta, ctx) < 0) {
    return -ENOENT;
  }

  return manager->del_script(dpp, y, script_oid(ctx, tenant), script_meta_key(ctx, tenant), optional_name, scripts_meta);
}

#ifdef WITH_RADOSGW_LUA_PACKAGES

namespace bp = boost::process;

int add_package(const DoutPrefixProvider* dpp, rgw::sal::Driver* driver, optional_yield y, const std::string& package_name, bool allow_compilation)
{
  // verify that luarocks can load this package
  const auto p = bp::search_path("luarocks");
  if (p.empty()) {
    return -ECHILD;
  }
  bp::ipstream is;
  const auto cmd = p.string() + " search --porcelain" + (allow_compilation ? " " : " --binary ") + package_name;
  bp::child c(cmd,
      bp::std_in.close(),
      bp::std_err > bp::null,
      bp::std_out > is);

  std::string line;
  bool package_found = false;
  while (c.running() && std::getline(is, line) && !line.empty()) {
    package_found = true;
  }
  c.wait();
  auto ret = c.exit_code();
  if (ret) {
    return -ret;
  }

  if (!package_found) {
    return -EINVAL;
  }

  //replace previous versions of the package
  const std::string package_name_no_version = package_name.substr(0, package_name.find(" "));
  ret = remove_package(dpp, driver, y, package_name_no_version);
  if (ret < 0) {
    return ret;
  }

  return driver->get_lua_manager("")->add_package(dpp, y, package_name);
}

int remove_package(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y, const std::string& package_name)
{
  return driver->get_lua_manager("")->remove_package(dpp, y, package_name);
}

namespace bp = boost::process;

int list_packages(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y, packages_t& packages)
{
  return driver->get_lua_manager("")->list_packages(dpp, y, packages);
}

namespace fs = std::filesystem;

// similar to to the "mkdir -p" command
int create_directory_p(const DoutPrefixProvider *dpp, const fs::path& p) {
  std::error_code ec;
  fs::path total_path;
  for (const auto& pp : p) {
    total_path /= pp;
    auto should_create = !fs::exists(total_path, ec);
    if (ec) {
      ldpp_dout(dpp, 1) << "cannot check if " << total_path << 
        " directory exists. error: " << ec.message() << dendl;
      return -ec.value();
    }
    if (should_create) {
      if (!create_directory(total_path, ec)) {
        ldpp_dout(dpp, 1) << "failed to create  " << total_path << 
          " directory. error: " << ec.message() << dendl;
        return -ec.value();
      }
    }
  }
  return 0;
}

void get_luarocks_config(const bp::filesystem::path& process,
    const std::string& luarocks_path,
    const bp::environment& env, std::string& output) {
  bp::ipstream is;
  auto cmd = process.string();
  cmd.append(" config");
  output.append("Lua CMD: ");
  output.append(cmd);

  try {
    bp::child c(cmd, env, bp::std_in.close(), (bp::std_err & bp::std_out) > is, bp::start_dir(luarocks_path));
    std::string line;
    do {
      if (!line.empty()) {
        output.append("\n\t").append(line);
      }
    } while (c.running() && std::getline(is, line));

    c.wait();
    output.append("\n\t").append("exit code: ").append(std::to_string(c.exit_code()));
  } catch (const std::runtime_error& err) {
    output.append("\n\t").append(err.what());
  }
}

int install_packages(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
                     optional_yield y, const std::string& luarocks_path,
                     packages_t& failed_packages, std::string& install_dir) {

  packages_t packages;
  auto ret = list_packages(dpp, driver, y, packages);
  if (ret == -ENOENT) {
    // allowlist is empty 
    return 0;
  }
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "Lua ERROR: failed to get package list. error: " << ret << dendl;
    return ret;
  }
  // verify that luarocks exists
  const auto p = bp::search_path("luarocks");
  if (p.empty()) {
    ldpp_dout(dpp, 1) << "Lua ERROR: failed to find luarocks" << dendl;
    return -ECHILD;
  }

  // create the luarocks parent directory
  auto rc = create_directory_p(dpp, luarocks_path);
  if (rc < 0) {
    ldpp_dout(dpp, 1) << "Lua ERROR: failed to recreate luarocks directory: " <<
      luarocks_path << ". error: " << rc << dendl; 
    return rc;
  }
  

  // create a temporary sub-directory to install all luarocks packages
  std::string tmp_path_template = luarocks_path;// fs::temp_directory_path();
  tmp_path_template.append("/XXXXXX");
  const auto tmp_luarocks_path = mkdtemp(tmp_path_template.data());
  if (!tmp_luarocks_path) { 
    const auto rc = -errno;
    ldpp_dout(dpp, 1) << "Lua ERROR: failed to create temporary directory from template: " << 
      tmp_path_template << ". error: " << rc << dendl;
    return rc;
  }
  install_dir.assign(tmp_luarocks_path);

  // get a handle to the current environment
  auto env = boost::this_process::environment();
  bp::environment _env = env;
  _env["HOME"] = luarocks_path;

  if (dpp->get_cct()->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
    std::string output;
    get_luarocks_config(p, luarocks_path, _env, output);
    ldpp_dout(dpp, 20) << output << dendl;
  }

  // the lua rocks install dir will be created by luarocks the first time it is called
  for (const auto& package : packages) {
    bp::ipstream is;
    auto cmd = p.string();
    cmd.append(" install --no-doc --lua-version ").
      append(CEPH_LUA_VERSION).
      append(" --tree ").
      append(install_dir).
      append(" --deps-mode one ").
      append(package);
    bp::child c(cmd, _env, bp::std_in.close(), (bp::std_err & bp::std_out) > is, bp::start_dir(luarocks_path));

    if (dpp->get_cct()->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      // TODO: yield when reading output
      std::string lines = std::string("Lua CMD: ");
      lines.append(cmd);
      std::string line;
      do {
        if (!line.empty()) {
          lines.append("\n\t").append(line);
        }
      } while (c.running() && std::getline(is, line));
      ldpp_dout(dpp, 20) << lines << dendl;
    }

    c.wait();
    if (c.exit_code()) {
      failed_packages.insert(package);
    }
  }
  
  return 0;
}

int reload_packages(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y)
{
  return driver->get_lua_manager("")->reload_packages(dpp, y);
}

#endif // WITH_RADOSGW_LUA_PACKAGES

}

