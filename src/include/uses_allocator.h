// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

// Derived from:
/* uses_allocator.h                  -*-C++-*-
 *
 * Copyright (C) 2016 Pablo Halpern <phalpern@halpernwightsoftware.com>
 * Distributed under the Boost Software License - Version 1.0
 */
// Downloaded from https://github.com/phalpern/uses-allocator.git

#pragma once

#include <memory>
#include <tuple>
#include <type_traits>
#include <utility>

namespace ceph {

namespace internal {
template <class T, class Tuple, std::size_t... Indexes>
T make_from_tuple_imp(Tuple&& t, std::index_sequence<Indexes...>)
{
  return T(std::get<Indexes>(std::forward<Tuple>(t))...);
}
} // namespace internal

template<class T, class Tuple>
T make_from_tuple(Tuple&& args_tuple)
{
    using namespace internal;
    using Indices = std::make_index_sequence<std::tuple_size_v<
                                               std::decay_t<Tuple>>>;
    return make_from_tuple_imp<T>(std::forward<Tuple>(args_tuple), Indices{});
}

////////////////////////////////////////////////////////////////////////

// Forward declaration
template <class T, class Alloc, class... Args>
auto uses_allocator_construction_args(const Alloc& a, Args&&... args);

namespace internal {

template <class T, class A>
struct has_allocator : std::uses_allocator<T, A> { };

// Specialization of `has_allocator` for `std::pair`
template <class T1, class T2, class A>
struct has_allocator<std::pair<T1, T2>, A>
  : std::integral_constant<bool, has_allocator<T1, A>::value ||
                                 has_allocator<T2, A>::value>
{
};

template <bool V> using boolean_constant = std::integral_constant<bool, V>;

template <class T> struct is_pair : std::false_type { };

template <class T1, class T2>
struct is_pair<std::pair<T1, T2>> : std::true_type { };

// Return a tuple of arguments appropriate for uses-allocator construction
// with allocator `Alloc` and ctor arguments `Args`.
// This overload is handles types for which `has_allocator<T, Alloc>` is false.
template <class T, class Unused1, class Unused2, class Alloc, class... Args>
auto uses_allocator_args_imp(Unused1      /* is_pair */,
                             std::false_type   /* has_allocator */,
                             Unused2      /* uses prefix allocator arg */,
                             const Alloc& /* ignored */,
                             Args&&... args)
{
    // Allocator is ignored
    return std::forward_as_tuple(std::forward<Args>(args)...);
}

// Return a tuple of arguments appropriate for uses-allocator construction
// with allocator `Alloc` and ctor arguments `Args`.
// This overload handles non-pair `T` for which `has_allocator<T, Alloc>` is
// true and constructor `T(allocator_arg_t, a, args...)` is valid.
template <class T, class Alloc, class... Args>
auto uses_allocator_args_imp(std::false_type /* is_pair */,
                             std::true_type  /* has_allocator */,
                             std::true_type  /* uses prefix allocator arg */,
                             const Alloc& a,
                             Args&&... args)
{
    // Allocator added to front of argument list, after `allocator_arg`.
  return std::tuple<std::allocator_arg_t, const Alloc&,
                    Args&&...>(std::allocator_arg, a, std::forward<Args>(args)...);
}

// Return a tuple of arguments appropriate for uses-allocator construction
// with allocator `Alloc` and ctor arguments `Args`.
// This overload handles non-pair `T` for which `has_allocator<T, Alloc>` is
// true and constructor `T(allocator_arg_t, a, args...)` NOT valid.
// This function will produce invalid results unless `T(args..., a)` is valid.
template <class T1, class Alloc, class... Args>
auto uses_allocator_args_imp(std::false_type /* is_pair */,
                             std::true_type  /* has_allocator */,
                             std::false_type /* prefix allocator arg */,
                             const Alloc& a,
                             Args&&... args)
{
    // Allocator added to end of argument list
    return std::forward_as_tuple(std::forward<Args>(args)..., a);
}

// Return a tuple of arguments appropriate for uses-allocator construction
// with allocator `Alloc` and ctor arguments `Args`.
// This overload handles specializations of `T` = `std::pair` for which
// `has_allocator<T, Alloc>` is true for either or both of the elements and
// piecewise_construct arguments are passed in.
template <class T, class Alloc, class Tuple1, class Tuple2>
auto uses_allocator_args_imp(std::true_type  /* is_pair */,
                             std::true_type  /* has_allocator */,
                             std::false_type /* prefix allocator arg */,
                             const Alloc& a,
                             std::piecewise_construct_t,
                             Tuple1&& x, Tuple2&& y)
{
    using T1 = typename T::first_type;
    using T2 = typename T::second_type;

    return std::make_tuple(
      std::piecewise_construct,
      std::apply([&a](auto&&... args1) -> auto {
                   return uses_allocator_construction_args<T1>(
                     a, std::forward<decltype(args1)>(args1)...);
                 }, std::forward<Tuple1>(x)),
      std::apply([&a](auto&&... args2) -> auto {
                   return uses_allocator_construction_args<T2>(
                     a, std::forward<decltype(args2)>(args2)...);
                 }, std::forward<Tuple2>(y))
      );
}

// Return a tuple of arguments appropriate for uses-allocator construction
// with allocator `Alloc` and ctor arguments `Args`.
// This overload handles specializations of `T` = `std::pair` for which
// `has_allocator<T, Alloc>` is true for either or both of the elements and
// no other constructor arguments are passed in.
template <class T, class Alloc>
auto uses_allocator_args_imp(std::true_type  /* is_pair */,
                             std::true_type  /* has_allocator */,
                             std::false_type /* prefix allocator arg */,
                             const Alloc& a)
{
    // using T1 = typename T::first_type;
    // using T2 = typename T::second_type;

    // return std::make_tuple(
    //     piecewise_construct,
    //     uses_allocator_construction_args<T1>(a),
    //     uses_allocator_construction_args<T2>(a));
  return uses_allocator_construction_args<T>(a, std::piecewise_construct,
                                             std::tuple<>{}, std::tuple<>{});
}

// Return a tuple of arguments appropriate for uses-allocator construction
// with allocator `Alloc` and ctor arguments `Args`.
// This overload handles specializations of `T` = `std::pair` for which
// `has_allocator<T, Alloc>` is true for either or both of the elements and
// a single argument of type const-lvalue-of-pair is passed in.
template <class T, class Alloc, class U1, class U2>
auto uses_allocator_args_imp(std::true_type  /* is_pair */,
                             std::true_type  /* has_allocator */,
                             std::false_type /* prefix allocator arg */,
                             const Alloc& a,
                             const std::pair<U1, U2>& arg)
{
    // using T1 = typename T::first_type;
    // using T2 = typename T::second_type;

    // return std::make_tuple(
    //     piecewise_construct,
    //     uses_allocator_construction_args<T1>(a, arg.first),
    //     uses_allocator_construction_args<T2>(a, arg.second));
  return uses_allocator_construction_args<T>(a, std::piecewise_construct,
                                               std::forward_as_tuple(arg.first),
                                             std::forward_as_tuple(arg.second));
}

// Return a tuple of arguments appropriate for uses-allocator construction
// with allocator `Alloc` and ctor arguments `Args`.
// This overload handles specializations of `T` = `std::pair` for which
// `has_allocator<T, Alloc>` is true for either or both of the elements and
// a single argument of type rvalue-of-pair is passed in.
template <class T, class Alloc, class U1, class U2>
auto uses_allocator_args_imp(std::true_type  /* is_pair */,
                             std::true_type  /* has_allocator */,
                             std::false_type /* prefix allocator arg */,
                             const Alloc& a,
                             std::pair<U1, U2>&& arg)
{
    // using T1 = typename T::first_type;
    // using T2 = typename T::second_type;

    // return std::make_tuple(
    //     piecewise_construct,
    //     uses_allocator_construction_args<T1>(a, forward<U1>(arg.first)),
    //     uses_allocator_construction_args<T2>(a, forward<U2>(arg.second)));
  return uses_allocator_construction_args<T>(a, std::piecewise_construct,
                                             std::forward_as_tuple(std::forward<U1>(arg.first)),
                                             std::forward_as_tuple(std::forward<U2>(arg.second)));
}

// Return a tuple of arguments appropriate for uses-allocator construction
// with allocator `Alloc` and ctor arguments `Args`.
// This overload handles specializations of `T` = `std::pair` for which
// `has_allocator<T, Alloc>` is true for either or both of the elements and
// two additional constructor arguments are passed in.
template <class T, class Alloc, class U1, class U2>
auto uses_allocator_args_imp(std::true_type  /* is_pair */,
                             std::true_type  /* has_allocator */,
                             std::false_type /* prefix allocator arg */,
                             const Alloc& a,
                             U1&& arg1, U2&& arg2)
{
    // using T1 = typename T::first_type;
    // using T2 = typename T::second_type;

    // return std::make_tuple(
    //     piecewise_construct,
    //     uses_allocator_construction_args<T1>(a, forward<U1>(arg1)),
    //     uses_allocator_construction_args<T2>(a, forward<U2>(arg2)));
  return uses_allocator_construction_args<T>(
    a, std::piecewise_construct,
    std::forward_as_tuple(std::forward<U1>(arg1)),
    std::forward_as_tuple(std::forward<U2>(arg2)));
}

} // close namespace internal

template <class T, class Alloc, class... Args>
auto uses_allocator_construction_args(const Alloc& a, Args&&... args)
{
    using namespace internal;
    return uses_allocator_args_imp<T>(is_pair<T>(),
                                      has_allocator<T, Alloc>(),
                                      std::is_constructible<T, std::allocator_arg_t,
                                                            Alloc, Args...>(),
                                      a, std::forward<Args>(args)...);
}

template <class T, class Alloc, class... Args>
T make_obj_using_allocator(const Alloc& a, Args&&... args)
{
  return make_from_tuple<T>(
    uses_allocator_construction_args<T>(a, std::forward<Args>(args)...));
}

template <class T, class Alloc, class... Args>
T* uninitialized_construct_using_allocator(T* p,
                                           const Alloc& a,
                                           Args&&... args)
{
  return std::apply([p](auto&&... args2){
                      return ::new(static_cast<void*>(p))
                        T(std::forward<decltype(args2)>(args2)...);
                    }, uses_allocator_construction_args<T>(
		      a, std::forward<Args>(args)...));
}

} // namespace ceph
