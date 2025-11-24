#pragma once

#include "include/rados/cls_traits.h"

namespace cls::sdk {
struct ClassId {
  static constexpr auto name = "sdk";
};
namespace method {
constexpr auto test_coverage_write = ClsMethod<RdWrTag, ClassId>("test_coverage_write");
constexpr auto test_coverage_replay = ClsMethod<RdWrTag, ClassId>("test_coverage_replay");
}
}