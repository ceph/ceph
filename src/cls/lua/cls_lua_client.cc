#include <string>
#include <vector>
#include "include/encoding.h"
#include "include/rados/librados.hpp"  // for IoCtx
#include "cls_lua_client.h"
#include "cls_lua_ops.h"

using std::string;
using std::vector;
using librados::IoCtx;
using librados::bufferlist;

namespace cls_lua_client {
  /*
   * Currently the return code and return bufferlist are not wrapped in a
   * protocol that allows object class vs Lua to be distinguished. For
   * instance, -EOPNOTSUPP might refer to cls_lua not being found, but would
   * also be returned when cls_lua is found, but a Lua handler is not found.
   */
  int exec(IoCtx& ioctx, const string& oid, const string& script,
      const string& handler, bufferlist& input, bufferlist& output)
  {
    cls_lua_eval_op op;

    op.script = script;
    op.handler = handler;
    op.input = input;

    bufferlist inbl;
    encode(op, inbl);

    return ioctx.exec(oid, "lua", "eval_bufferlist", inbl, output);
  }
}
