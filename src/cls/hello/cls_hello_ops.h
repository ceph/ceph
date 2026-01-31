#pragma once

#include "include/rados/cls_traits.h"

namespace cls::hello {
struct ClassId {
  static constexpr auto name = "hello";
};
namespace method {
constexpr auto say_hello = ClsMethod<RdTag, ClassId>("say_hello");
constexpr auto record_hello = ClsMethod<WrPromoteTag, ClassId>("record_hello");
constexpr auto write_return_data = ClsMethod<WrTag, ClassId>("write_return_data");
constexpr auto writes_dont_return_data = ClsMethod<WrTag, ClassId>("writes_dont_return_data");
constexpr auto write_too_much_return_data = ClsMethod<WrTag, ClassId>("write_too_much_return_data");
constexpr auto replay = ClsMethod<RdTag, ClassId>("replay");
constexpr auto turn_it_to_11 = ClsMethod<RdWrPromoteTag, ClassId>("turn_it_to_11");
constexpr auto bad_reader = ClsMethod<WrTag, ClassId>("bad_reader");
constexpr auto bad_writer = ClsMethod<RdTag, ClassId>("bad_writer");
}
}
