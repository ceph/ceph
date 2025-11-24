#pragma once
#include <type_traits>
#include "cls_flags.h"

template <int Flags>
struct MethodTag {};

// Tags representing flags.
using RdTag       = MethodTag<CLS_METHOD_RD>;
using WrTag       = MethodTag<CLS_METHOD_WR>;
using PromoteTag  = MethodTag<CLS_METHOD_PROMOTE>;

// Combinations of flags.
using RdWrTag     = MethodTag<CLS_METHOD_RD | CLS_METHOD_WR>;
using RdPromoteTag = MethodTag<CLS_METHOD_RD | CLS_METHOD_PROMOTE>;
using WrPromoteTag = MethodTag<CLS_METHOD_WR | CLS_METHOD_PROMOTE>;
using RdWrPromoteTag = MethodTag<CLS_METHOD_RD | CLS_METHOD_WR | CLS_METHOD_PROMOTE>;

template <typename Tag, typename ClassDef>
struct ClsMethod {
  const char* cls;
  const char* name;

  constexpr ClsMethod(const char* n) : cls(ClassDef::name), name(n) {}
};

// Traits to map Tags to properties
template <typename T> struct FlagTraits;

template <int Flags>
struct FlagTraits<MethodTag<Flags>> {

  static constexpr int value = Flags;
  static constexpr bool is_readonly = (Flags & CLS_METHOD_WR) == 0;
};

template <typename Tag>
constexpr bool is_safe_for_ro_v = FlagTraits<Tag>::is_readonly;