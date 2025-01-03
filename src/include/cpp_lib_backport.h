#pragma once

#include <cstring>
#include <type_traits>

namespace std {

#ifndef __cpp_lib_bit_cast
#define __cpp_lib_bit_cast 201806L

/// Create a value of type `To` from the bits of `from`.
template<typename To, typename From>
requires (sizeof(To) == sizeof(From)) &&
         std::is_trivially_copyable_v<From> &&
         std::is_trivially_copyable_v<To>
[[nodiscard]] constexpr To
bit_cast(const From& from) noexcept {
#if __has_builtin(__builtin_bit_cast)
  return __builtin_bit_cast(To, from);
#else
  static_assert(std::is_trivially_constructible_v<To>);
  To to;
  std::memcpy(&to, &from, sizeof(To));
  return to;
#endif
}

#endif // __cpp_lib_bit_cast

} // namespace std
