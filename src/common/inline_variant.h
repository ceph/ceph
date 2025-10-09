// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef INLINE_VARIANT_H
#define INLINE_VARIANT_H

#include <utility>
#include <variant>

template <class... Functions>
struct overloaded : Functions... { using Functions::operator()...; };

template <typename Variant, typename... Functions>
auto match(Variant const& variant, Functions... functions)
{
    return std::visit(overloaded{std::forward<Functions>(functions)...}, variant);
}

#endif
