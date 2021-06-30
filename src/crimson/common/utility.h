// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <type_traits>

template <class T>
void assert_moveable(T& t) {
    // It's fine
}
template <class T>
void assert_moveable(const T& t) {
    static_assert(always_false<T>::value, "unable to move-out from T");
}

