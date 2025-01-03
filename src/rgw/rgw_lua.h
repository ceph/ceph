#pragma once

#include <string>
#include <set>
#include "rgw_lua_version.h"
#include "common/async/yield_context.h"
#include "common/dout.h"
#include "rgw_sal_fwd.h"

class DoutPrefixProvider;
class lua_State;
class rgw_user;
class DoutPrefixProvider;
namespace rgw::sal {
  class RadosStore;
  class LuaManager;
}

namespace rgw::lua {

enum class context {
  preRequest,
  postRequest,
  background,
  getData,
  putData,
  none
};

// get context enum from string 
// the expected string the same as the enum (case insensitive)
// return "none" if not matched
context to_context(const std::string& s);

// verify a lua script
bool verify(const std::string& script, std::string& err_msg);

// driver a lua script in a context
int write_script(const DoutPrefixProvider *dpp, rgw::sal::LuaManager* manager, const std::string& tenant, optional_yield y, context ctx, const std::string& script);

// read the stored lua script from a context
int read_script(const DoutPrefixProvider *dpp, rgw::sal::LuaManager* manager, const std::string& tenant, optional_yield y, context ctx, std::string& script);

// delete the stored lua script from a context
int delete_script(const DoutPrefixProvider *dpp, rgw::sal::LuaManager* manager, const std::string& tenant, optional_yield y, context ctx);

using packages_t = std::set<std::string>;

#ifdef WITH_RADOSGW_LUA_PACKAGES

// add a lua package to the allowlist
int add_package(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y, const std::string& package_name, bool allow_compilation);

// remove a lua package from the allowlist
int remove_package(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y, const std::string& package_name);

// list lua packages in the allowlist
int list_packages(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y, packages_t& packages);

// reload lua packages
int reload_packages(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver, optional_yield y);

// install all packages from the allowlist
// return (by reference) the list of packages that failed to install
// and return (by reference) the temporary director in which the packages were installed
int install_packages(const DoutPrefixProvider *dpp, rgw::sal::Driver* driver,
                     optional_yield y, const std::string& luarocks_path,
                     packages_t& failed_packages,
                     std::string& install_dir);

#endif
}

