#pragma once

#include "include/rados/cls_traits.h"

namespace cls::journal {
struct ClassId {
  static constexpr auto name = "journal";
};
namespace method {
constexpr auto create = ClsMethod<RdWrTag, ClassId>("create");
constexpr auto get_order = ClsMethod<RdTag, ClassId>("get_order");
constexpr auto get_splay_width = ClsMethod<RdTag, ClassId>("get_splay_width");
constexpr auto get_pool_id = ClsMethod<RdTag, ClassId>("get_pool_id");
constexpr auto get_minimum_set = ClsMethod<RdTag, ClassId>("get_minimum_set");
constexpr auto set_minimum_set = ClsMethod<RdWrTag, ClassId>("set_minimum_set");
constexpr auto get_active_set = ClsMethod<RdTag, ClassId>("get_active_set");
constexpr auto set_active_set = ClsMethod<RdWrTag, ClassId>("set_active_set");
constexpr auto get_client = ClsMethod<RdTag, ClassId>("get_client");
constexpr auto client_register = ClsMethod<RdWrTag, ClassId>("client_register");
constexpr auto client_update_data = ClsMethod<RdWrTag, ClassId>("client_update_data");
constexpr auto client_update_state = ClsMethod<RdWrTag, ClassId>("client_update_state");
constexpr auto client_unregister = ClsMethod<RdWrTag, ClassId>("client_unregister");
constexpr auto client_commit = ClsMethod<RdWrTag, ClassId>("client_commit");
constexpr auto client_list = ClsMethod<RdTag, ClassId>("client_list");
constexpr auto get_next_tag_tid = ClsMethod<RdTag, ClassId>("get_next_tag_tid");
constexpr auto get_tag = ClsMethod<RdTag, ClassId>("get_tag");
constexpr auto tag_create = ClsMethod<RdWrTag, ClassId>("tag_create");
constexpr auto tag_list = ClsMethod<RdTag, ClassId>("tag_list");
constexpr auto guard_append = ClsMethod<RdWrTag, ClassId>("guard_append");
constexpr auto append = ClsMethod<RdWrTag, ClassId>("append");
}
}