#ifndef CLS_LUA_CLIENT_HPP
#define CLS_LUA_CLIENT_HPP
#include <string>

#include "include/rados/librados.hpp"

namespace cls_lua_client {
  int exec(librados::IoCtx& ioctx, const std::string& oid,
      const std::string& script, const std::string& handler,
      librados::bufferlist& inbl, librados::bufferlist& outbl);
}

#endif
