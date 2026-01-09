#pragma once

#include <string>
#include <vector>

namespace rgw::lua {

// Can hold Lua script or bytecode
using LuaCodeType = std::variant<std::string, std::vector<char>>;
}
