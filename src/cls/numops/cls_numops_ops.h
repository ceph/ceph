#pragma once

#include "include/rados/cls_traits.h"

namespace rados::cls::numops {
struct ClassId {
  static constexpr auto name = "numops";
};
namespace method {
constexpr auto add = ClsMethod<RdWrTag, ClassId>("add");
constexpr auto mul = ClsMethod<RdWrTag, ClassId>("mul");
}
}