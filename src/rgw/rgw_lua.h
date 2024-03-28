#pragma once

#include <string>
#include <set>
#include "rgw_lua_version.h"
#include "common/async/yield_context.h"
#include "common/dout.h"
#include "rgw_sal_fwd.h"
#include "rgw_lua_utils.h"
#include "rgw_lua.h"

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
context to_context(const std::string& s) 
{
  if (strcasecmp(s.c_str(), "prerequest") == 0) {
    return context::preRequest;
  }
  if (strcasecmp(s.c_str(), "postrequest") == 0) {
    return context::postRequest;
  }
  if (strcasecmp(s.c_str(), "background") == 0) {
    return context::background;
  }
  if (strcasecmp(s.c_str(), "getdata") == 0) {
    return context::getData;
  }
  if (strcasecmp(s.c_str(), "putdata") == 0) {
    return context::putData;
  }
  return context::none;
}

std::string to_string(context ctx) 
{
  switch (ctx) {
    case context::preRequest:
      return "prerequest";
    case context::postRequest:
      return "postrequest";
    case context::background:
      return "background";
    case context::getData:
      return "getdata";
    case context::putData:
      return "putdata";
    case context::none:
      break;
  }
  return "none";
}

// Metadata maintained about each lua script
struct LuaScriptMeta {
  
  // The priority of the script. Scripts are executed from lowest priority to highest 
  int priority;

  // The context for which the script is run
  rgw::lua::context ctx;

  // script name provided by the user
  std::string name;

  // Actual script contents
  std::string script;

  LuaScriptMeta(
    int priority, 
    rgw::lua::context ctx, 
    std::string name,
    std::string script
  ): priority(priority), ctx(ctx), name(name), script(script) {}

  LuaScriptMeta(std::string script, context ctx): priority(rgw::lua::MAX_LUA_PRIORITY), ctx(ctx), name(""), script(script) {}
  LuaScriptMeta() {}

  void print() {
    std::cout << "Script Name: " << name << std::endl;
    std::cout << "Priority: " << priority << std::endl;
    std::cout << "Context: " << to_string(ctx) << std::endl;
    std::cout << "Script Contents:" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << script << std::endl;
    std::cout << "==========================================" << std::endl << std::endl;
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(priority, bl);
    encode(to_string(ctx), bl);
    encode(name, bl);
    encode(script, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    std::string stored_ctx;
    
    DECODE_START(1, bl);
    decode(priority, bl);
    decode(stored_ctx, bl);
    this->ctx = to_context(stored_ctx);
    decode(name, bl);
    decode(script, bl);
    DECODE_FINISH(bl);
  }

};
WRITE_CLASS_ENCODER(LuaScriptMeta)

// Array of LuaScriptMeta for all scripts
struct LuaRuntimeMeta {
  std::vector<LuaScriptMeta> scripts;

  LuaRuntimeMeta() {
    this->scripts = std::vector<LuaScriptMeta>();
  }

  LuaRuntimeMeta(LuaScriptMeta script) {
    this->scripts = std::vector<LuaScriptMeta>(1, script);
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(this->scripts.size(), bl);
    for (auto script : scripts) {
      // script.encode(bl);
      encode(script, bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    int num_scripts;
    rgw::lua::LuaScriptMeta script;

    DECODE_START(1, bl);
    decode(num_scripts, bl);
    this->scripts = std::vector<LuaScriptMeta>(num_scripts, LuaScriptMeta());

    for (int idx = 0; idx < num_scripts; idx++) {
      decode(script, bl);
      this->scripts[idx] = script;
    }
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(LuaRuntimeMeta)

// verify a lua script
bool verify(const std::string& script, std::string& err_msg);

// driver a lua script in a context
int write_script(const DoutPrefixProvider *dpp, rgw::sal::LuaManager* manager, const std::string& tenant, optional_yield y, context ctx, const std::string& script, std::optional<std::uint8_t> optional_priority, std::optional<std::string> optional_name);

// read the stored lua script from a context
int read_script(const DoutPrefixProvider *dpp, rgw::sal::LuaManager* manager, const std::string& tenant, optional_yield y, context ctx, rgw::lua::LuaRuntimeMeta& scripts_meta);

// delete the stored lua script from a context
int delete_script(const DoutPrefixProvider *dpp, sal::LuaManager* manager, const std::string& tenant, optional_yield y, context ctx, std::optional<std::string> optional_name);

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

