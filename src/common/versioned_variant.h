// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <concepts>
#include <limits>
#include <list>
#include <variant>

#include <boost/mp11/algorithm.hpp> // for mp_with_index
#include "include/encoding.h"

/// \file
/// \brief Contains binary encoding strategies for std::variant.

namespace ceph {

// null encoding for std::monostate
inline void encode(const std::monostate&, bufferlist& bl) {}
inline void decode(std::monostate&, bufferlist::const_iterator& p) {}

// largest value that can be represented by `__u8 struct_v`
inline constexpr size_t max_version = std::numeric_limits<__u8>::max();

/// \namespace versioned_variant
/// \brief A backward-compatible binary encoding for std::variant.
///
/// The variant index is encoded in struct_v so the correct decoder can be
/// selected. This means that existing variant types cannot be changed or
/// removed without breaking the decode of earlier ceph versions. New types
/// can only be added to the end of the variant.
///
/// In addition to struct_v, the variant index is also encoded in compatv. As
/// the variant is extended, this means that existing decoders can continue to
/// decode the types they recognize, but reject the encodings of new types they
/// don't.
///
/// The variant types themselves are free to change their encodings, provided
/// they manage their own versioning. The types must be default-constructible
/// so they can be constructed before decode.
///
/// The contained encode/decode functions won't be found by argument-dependent
/// lookup, so you must either qualify the calls with `versioned_variant::` or
/// add `using namespace versioned_variant` to the calling scope.
namespace versioned_variant {

// Requirements for the list of types for versioned std::variant encoding.
template <typename ...Ts>
concept valid_types = requires {
    sizeof...(Ts) > 0; // variant cannot be empty
    sizeof...(Ts) <= max_version; // index must fit in u8
    requires (std::default_initializable<Ts> && ...); // default-constructible
  };

/// \brief A versioned_variant encoder.
///
/// Example:
/// \code
/// struct example {
///   std::variant<int, bool> value;
///
///   void encode(bufferlist& bl) const {
///     ENCODE_START(0, 0, bl);
///     ceph::versioned_variant::encode(value, bl);
///     ...
/// \endcode
template <typename ...Ts> requires valid_types<Ts...>
void encode(const std::variant<Ts...>& v, bufferlist& bl, uint64_t features=0)
{
  // encode the variant index in struct_v and compatv
  const uint8_t ver = static_cast<uint8_t>(v.index());
  ENCODE_START(ver, ver, bl);
  // use the variant type's encoder
  std::visit([&bl] (const auto& value) mutable {
      encode(value, bl);
    }, v);
  ENCODE_FINISH(bl);
}

/// \brief A versioned_variant decoder.
///
/// Example:
/// \code
/// struct example {
///   std::variant<int, bool> value;
///
///   void decode(bufferlist::const_iterator& bl) const {
///     DECODE_START(0, bl);
///     ceph::versioned_variant::decode(value, bl);
///     ...
/// \endcode
template <typename ...Ts> requires valid_types<Ts...>
void decode(std::variant<Ts...>& v, bufferlist::const_iterator& p)
{
  constexpr uint8_t max_version = sizeof...(Ts) - 1;
  DECODE_START(max_version, p);
  // use struct_v as an index into the variant after converting it into a
  // compile-time index I
  const uint8_t index = struct_v;
  boost::mp11::mp_with_index<sizeof...(Ts)>(index, [&v, &p] (auto I) {
      // default-construct the type at index I and call its decoder
      decode(v.template emplace<I>(), p);
    });
  DECODE_FINISH(p);
}

} // namespace versioned_variant


/// \namespace converted_variant
/// \brief A std::variant<T, ...> encoding that is backward-compatible with T.
///
/// The encoding works the same as versioned_variant, except that a block of
/// version numbers are reserved for the first type T to allow its encoding
/// to continue evolving. T must itself use versioned encoding (ie
/// ENCODE_START/FINISH).
///
/// This encoding strategy allows a serialized type T to be transparently
/// converted into a variant that can represent other types too.
namespace converted_variant {

// For converted variants, reserve the first 128 versions for the original
// type. Variant types after the first use the version numbers above this.
inline constexpr uint8_t converted_max_version = 128;

// Requirements for the list of types for converted std::variant encoding.
template <typename ...Ts>
concept valid_types = requires {
    sizeof...(Ts) > 0; // variant cannot be empty
    sizeof...(Ts) <= (max_version - converted_max_version); // index must fit in u8
    requires (std::default_initializable<Ts> && ...); // default-constructible
  };

/// \brief A converted_variant encoder.
///
/// Example:
/// \code
/// struct example {
///   std::variant<int, bool> value; // replaced `int value`
///
///   void encode(bufferlist& bl) const {
///     ENCODE_START(1, 0, bl);
///     ceph::converted_variant::encode(value, bl);
///     ...
/// \endcode
template <typename ...Ts> requires valid_types<Ts...>
void encode(const std::variant<Ts...>& v, bufferlist& bl, uint64_t features=0)
{
  const uint8_t index = static_cast<uint8_t>(v.index());
  if (index == 0) {
    // encode the first type with its own versioning scheme
    encode(std::get<0>(v), bl);
    return;
  }

  // encode the variant index in struct_v and compatv
  const uint8_t ver = converted_max_version + index;
  ENCODE_START(ver, ver, bl);
  // use the variant type's encoder
  std::visit([&bl] (const auto& value) mutable {
      encode(value, bl);
    }, v);
  ENCODE_FINISH(bl);
}

/// \brief A converted_variant decoder.
///
/// Example:
/// \code
/// struct example {
///   std::variant<int, bool> value; // replaced `int value`
///
///   void decode(bufferlist::const_iterator& bl) {
///     DECODE_START(1, bl);
///     ceph::converted_variant::decode(value, bl);
///     ...
/// \endcode
template <typename ...Ts> requires valid_types<Ts...>
void decode(std::variant<Ts...>& v, bufferlist::const_iterator& p)
{
  // save the iterator position so the first type can restart decode
  const bufferlist::const_iterator prev = p;

  constexpr uint8_t max_version = converted_max_version + sizeof...(Ts) - 1;
  DECODE_START(max_version, p);
  if (struct_v <= converted_max_version) {
    p = prev; // rewind and use type 0's DECODE_START/FINISH
    decode(v.template emplace<0>(), p);
    return;
  }

  // use struct_v as an index into the variant after converting it into a
  // compile-time index I
  const uint8_t index = struct_v - converted_max_version;
  boost::mp11::mp_with_index<sizeof...(Ts)>(index, [&v, &p] (auto I) {
      // default-construct the type at index I and call its decoder
      decode(v.template emplace<I>(), p);
    });
  DECODE_FINISH(p);
}

} // namespace converted_variant


/// \brief Generate a list with a default-constructed variant of each type.
///
/// This can be used in generate_test_instances() for types that contain
/// variants to ensure that an encoding of each type is present in the
/// ceph-object-corpus. This allows the ceph-dencoder tests to catch any
/// breaking changes to the variant types that are present in encodings.
template <typename ...Ts>
void generate_test_instances(std::list<std::variant<Ts...>>& instances)
{
  // use an immediately-invoked lambda to get a parameter pack of variant indices
  [&instances] <std::size_t ...I> (std::index_sequence<I...>) {
    // use a fold expression to call emplace_back() for each index in the pack
    // use in_place_index to default-construct a variant of the type at index I
    (instances.emplace_back(std::in_place_index<I>), ...);
  } (std::make_index_sequence<sizeof...(Ts)>{});
}

} // namespace ceph
