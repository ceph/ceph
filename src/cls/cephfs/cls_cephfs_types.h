#pragma once
#include "include/rados/cls_traits.h"
namespace cls::cephfs {
struct ClassId {
  static constexpr auto name = "cephfs";
};
namespace method {
constexpr auto accumulate_inode_metadata = ClsMethod<RdWrTag, ClassId>("accumulate_inode_metadata");
}
}