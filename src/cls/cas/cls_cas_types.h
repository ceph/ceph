#pragma once
#include "include/rados/cls_traits.h"

namespace cls::cas {
struct ClassId {
  static constexpr auto name = "cas";
};
namespace method {
  constexpr auto chunk_create_or_get_ref = ClsMethod<RdWrTag, ClassId>("chunk_create_or_get_ref");
  constexpr auto chunk_get_ref = ClsMethod<RdWrTag, ClassId>("chunk_get_ref");
  constexpr auto chunk_put_ref = ClsMethod<RdWrTag, ClassId>("chunk_put_ref");
  constexpr auto references_chunk = ClsMethod<RdTag, ClassId>("references_chunk");
}
}