// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "radosgw-admin/script.h"

#include <fcntl.h>
#include <iostream>
#include <string>
#include <variant>
#include <unistd.h>

#include "common/errno.h"
#include "common/safe_io.h"
#include "include/buffer.h"
#include "rgw_lua.h"
#include "rgw_sal.h"

using namespace rgw_admin;
using namespace std;

namespace {

static const string LUA_CONTEXT_LIST(
  "prerequest, postauth, postrequest, background, getdata, putdata");

static int read_input(const string& infile, bufferlist& bl)
{
  int fd = 0;
  if (infile.size()) {
    fd = open(infile.c_str(), O_RDONLY);
    if (fd < 0) {
      int err = -errno;
      cerr << "error reading input file " << infile << std::endl;
      return err;
    }
  }

  constexpr auto READ_CHUNK = 8196;
  int r;
  int err;

  do {
    char buf[READ_CHUNK];

    r = safe_read(fd, buf, READ_CHUNK);
    if (r < 0) {
      err = -errno;
      cerr << "error while reading input" << std::endl;
      goto out;
    }
    bl.append(buf, r);
  } while (r > 0);
  err = 0;

 out:
  if (infile.size()) {
    close(fd);
  }
  return err;
}

} // anonymous namespace

int rgw_admin_script(const DoutPrefixProvider* dpp,
                     rgw::sal::Driver* driver,
                     const rgw_admin_script_options& o)
{
  auto& command = o.command;
  auto& tenant = o.tenant;
  auto& infile = o.infile;
  auto& str_script_ctx = o.str_script_ctx;
#ifdef WITH_RADOSGW_LUA_PACKAGES
  auto& script_package = o.script_package;
  const int allow_compilation = o.allow_compilation;
#endif

  if (command == OPT::SCRIPT_PUT) {
    if (!str_script_ctx.has_value()) {
      cerr << "ERROR: context was not provided (via --context)" << std::endl;
      return EINVAL;
    }
    if (infile.empty()) {
      cerr << "ERROR: infile was not provided (via --infile)" << std::endl;
      return EINVAL;
    }
    bufferlist bl;
    auto rc = read_input(infile, bl);
    if (rc < 0) {
      cerr << "ERROR: failed to read script: '" << infile << "'. error: " << rc << std::endl;
      return -rc;
    }
    const std::string script = bl.to_str();
    std::string err_msg;
    if (!rgw::lua::verify(script, err_msg)) {
      cerr << "ERROR: script: '" << infile << "' has error: " << std::endl << err_msg << std::endl;
      return EINVAL;
    }
    const rgw::lua::context script_ctx =
      rgw::lua::to_context(str_script_ctx.value());
    if (script_ctx == rgw::lua::context::none) {
      cerr << "ERROR: invalid script context: " << str_script_ctx.value()
           << ". must be one of: " << LUA_CONTEXT_LIST << std::endl;
      return EINVAL;
    }
    if (script_ctx == rgw::lua::context::background && !tenant.empty()) {
      cerr << "ERROR: cannot specify tenant in background context" << std::endl;
      return EINVAL;
    }
    auto lua_manager = driver->get_lua_manager("");
    rc = rgw::lua::write_script(dpp, lua_manager.get(), tenant, null_yield,
                                script_ctx, script);
    if (rc < 0) {
      cerr << "ERROR: failed to put script. error: " << rc << std::endl;
      return -rc;
    }
  }

  if (command == OPT::SCRIPT_GET) {
    if (!str_script_ctx.has_value()) {
      cerr << "ERROR: context was not provided (via --context)" << std::endl;
      return EINVAL;
    }
    const rgw::lua::context script_ctx =
      rgw::lua::to_context(str_script_ctx.value());
    if (script_ctx == rgw::lua::context::none) {
      cerr << "ERROR: invalid script context: " << str_script_ctx.value()
           << ". must be one of: " << LUA_CONTEXT_LIST << std::endl;
      return EINVAL;
    }
    auto lua_manager = driver->get_lua_manager("");
    std::string script;
    const auto rc = rgw::lua::read_script(dpp, lua_manager.get(), tenant,
                                          null_yield, script_ctx, script);
    if (rc == -ENOENT) {
      std::cout << "no script exists for context: " << str_script_ctx.value()
                << (tenant.empty() ? "" : (" in tenant: " + tenant))
                << std::endl;
    } else if (rc < 0) {
      cerr << "ERROR: failed to read script. error: " << rc << std::endl;
      return -rc;
    } else {
      std::cout << script << std::endl;
    }
  }

  if (command == OPT::SCRIPT_RM) {
    if (!str_script_ctx.has_value()) {
      cerr << "ERROR: context was not provided (via --context)" << std::endl;
      return EINVAL;
    }
    const rgw::lua::context script_ctx =
      rgw::lua::to_context(str_script_ctx.value());
    if (script_ctx == rgw::lua::context::none) {
      cerr << "ERROR: invalid script context: " << str_script_ctx.value()
           << ". must be one of: " << LUA_CONTEXT_LIST << std::endl;
      return EINVAL;
    }
    auto lua_manager = driver->get_lua_manager("");
    const auto rc = rgw::lua::delete_script(dpp, lua_manager.get(), tenant,
                                           null_yield, script_ctx);
    if (rc < 0) {
      cerr << "ERROR: failed to remove script. error: " << rc << std::endl;
      return -rc;
    }
  }

  if (command == OPT::SCRIPT_PACKAGE_ADD) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    if (!script_package.has_value()) {
      cerr << "ERROR: Lua package name was not provided (via --package)" << std::endl;
      return EINVAL;
    }
    const auto rc = rgw::lua::add_package(dpp, driver, null_yield,
                                          script_package.value(),
                                          bool(allow_compilation));
    if (rc < 0) {
      cerr << "ERROR: failed to add Lua package: " << script_package.value()
           << " .error: " << rc << std::endl;
      return -rc;
    }
#else
    cerr << "ERROR: adding Lua packages is not permitted" << std::endl;
    return EPERM;
#endif
  }

  if (command == OPT::SCRIPT_PACKAGE_RM) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    if (!script_package.has_value()) {
      cerr << "ERROR: Lua package name was not provided (via --package)" << std::endl;
      return EINVAL;
    }
    const auto rc = rgw::lua::remove_package(dpp, driver, null_yield,
                                           script_package.value());
    if (rc == -ENOENT) {
      cerr << "WARNING: package " << script_package.value()
           << " did not exists or already removed" << std::endl;
      return 0;
    }
    if (rc < 0) {
      cerr << "ERROR: failed to remove Lua package: " << script_package.value()
           << " .error: " << rc << std::endl;
      return -rc;
    }
#else
    cerr << "ERROR: removing Lua packages in not permitted" << std::endl;
    return EPERM;
#endif
  }

  if (command == OPT::SCRIPT_PACKAGE_LIST) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    rgw::lua::packages_t packages;
    const auto rc = rgw::lua::list_packages(dpp, driver, null_yield, packages);
    if (rc == -ENOENT) {
      std::cout << "no Lua packages in allowlist" << std::endl;
    } else if (rc < 0) {
      cerr << "ERROR: failed to read Lua packages allowlist. error: " << rc << std::endl;
      return rc;
    } else {
      for (const auto& package : packages) {
        std::cout << package << std::endl;
      }
    }
#else
    cerr << "ERROR: listing Lua packages in not permitted" << std::endl;
    return EPERM;
#endif
  }

  if (command == OPT::SCRIPT_PACKAGE_RELOAD) {
#ifdef WITH_RADOSGW_LUA_PACKAGES
    const auto rc = rgw::lua::reload_packages(dpp, driver, null_yield);
    if (rc < 0) {
      cerr << "ERROR: failed to reload Lua packages. error: " << rc << std::endl;
      return rc;
    }
#else
    cerr << "ERROR: reloading Lua packages in not permitted" << std::endl;
    return EPERM;
#endif
  }

  return 0;
}
