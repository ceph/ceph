// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "boost/intrusive_ptr.hpp"

template<typename T> using Ref = boost::intrusive_ptr<T>;

// Forward-declare intrusive_ptr ADL functions for PG in its own namespace.
// Required by Boost 1.89+ constexpr intrusive_ptr — with constexpr,
// ordinary unqualified lookup at the template definition point takes
// precedence over ADL.  Since boost/intrusive_ptr.hpp is included early
// (via Seastar headers), we place the declarations in crimson::osd so
// that ADL on crimson::osd::PG* finds them.
namespace crimson::osd {
class PG;
void intrusive_ptr_add_ref(const PG* p) noexcept;
void intrusive_ptr_release(const PG* p) noexcept;
} // namespace crimson::osd
