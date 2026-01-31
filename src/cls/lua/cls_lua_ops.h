#ifndef CEPH_CLS_LUA_OPS_H
#define CEPH_CLS_LUA_OPS_H

#include <string>

#include "include/encoding.h"
#include "include/rados/cls_traits.h"

struct cls_lua_eval_op {
  std::string script;
  std::string handler;
  bufferlist input;

  void encode(bufferlist &bl) const {
    ENCODE_START(1, 1, bl);
    encode(script, bl);
    encode(handler, bl);
    encode(input, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
    DECODE_START(1, bl);
    decode(script, bl);
    decode(handler, bl);
    decode(input, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(cls_lua_eval_op)

namespace cls::lua {
struct ClassId {
  static constexpr auto name = "lua";
};
namespace method {
constexpr auto eval_json = ClsMethod<RdWrTag, ClassId>("eval_json");
constexpr auto eval_bufferlist = ClsMethod<RdWrTag, ClassId>("eval_bufferlist");
}
}

#endif
