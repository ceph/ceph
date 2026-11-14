#pragma once

#include <set>
#include <string>
#include <variant>
#include <vector>

namespace rgw::lua {

enum class context {
  preRequest,
  postAuth,
  postRequest,
  background,
  getData,
  putData,
  none
};

// Can hold Lua script or bytecode
using LuaCodeType = std::variant<std::string, std::vector<char>>;

using packages_t = std::set<std::string>;
}
