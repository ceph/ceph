// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 SUSE LINUX GmbH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#ifndef CEPH_RANDOM_H
#define CEPH_RANDOM_H 1

#include <mutex>
#include <random>
#include <type_traits>

#include "boost/optional.hpp"

// Basic random number facility, adapted from N3551:
namespace ceph {
namespace util {

inline namespace version_1_0 {

namespace detail {

template <typename EngineT>
EngineT& engine();

template <typename MutexT, typename EngineT>
void randomize_rng(const int seed, MutexT& m, EngineT& e)
{
  std::lock_guard<MutexT> lg(m);
  e.seed(seed);
}

template <typename MutexT, typename EngineT>
void randomize_rng(MutexT& m, EngineT& e)
{
  thread_local std::random_device rd;
 
  std::lock_guard<MutexT> lg(m);
  e.seed(rd());
}

template <typename EngineT = std::default_random_engine>
void randomize_rng(const int n)
{
  detail::engine<EngineT>().seed(n);
}

template <typename EngineT = std::default_random_engine>
void randomize_rng()
{
  thread_local std::random_device rd;
  detail::engine<EngineT>().seed(rd());
}

template <typename EngineT>
EngineT& engine()
{
  thread_local boost::optional<EngineT> rng_engine;

  if (!rng_engine) {
    rng_engine.emplace(EngineT());
    randomize_rng<EngineT>();
  }

  return *rng_engine;
}

} // namespace detail

namespace detail {

template <typename NumberT,
          typename DistributionT,
          typename EngineT>
NumberT generate_random_number(const NumberT min, const NumberT max,
                               EngineT& e)
{
  thread_local DistributionT d { min, max };

  using param_type = typename DistributionT::param_type;
  return d(e, param_type { min, max });
}

template <typename NumberT,
          typename MutexT,
          typename DistributionT,
          typename EngineT>
NumberT generate_random_number(const NumberT min, const NumberT max,
                               MutexT& m, EngineT& e)
{
  thread_local DistributionT d { min, max };
 
  using param_type = typename DistributionT::param_type;
 
  std::lock_guard<MutexT> lg(m);
  return d(e, param_type { min, max });
}

template <typename NumberT,
          typename DistributionT,
          typename EngineT>
NumberT generate_random_number(const NumberT min, const NumberT max)
{
  return detail::generate_random_number<NumberT, DistributionT, EngineT>
          (min, max, detail::engine<EngineT>());
}

template <typename MutexT, typename EngineT,
          int min = 0,
          int max = std::numeric_limits<int>::max(), 
          typename DistributionT = std::uniform_int_distribution<int>>
int generate_random_number(MutexT& m, EngineT& e)
{
  return detail::generate_random_number<int, MutexT, DistributionT, EngineT>
          (min, max, m, e);
}

} // namespace detail

template <typename EngineT = std::default_random_engine>
void randomize_rng()
{
  detail::randomize_rng<EngineT>();
}

template <typename IntegerT = int,
          typename DistributionT = std::uniform_int_distribution<IntegerT>,
          typename EngineT = std::default_random_engine>
IntegerT generate_random_number()
{
  using limits = std::numeric_limits<IntegerT>;
  return detail::generate_random_number<IntegerT, DistributionT, EngineT>
          (limits::min(), limits::max());
}

template <typename IntegerT>
IntegerT generate_random_number(const IntegerT min, const IntegerT max,
                                std::enable_if_t<std::is_integral<IntegerT>::value>* = nullptr)
{
  return detail::generate_random_number<IntegerT,
                                        std::uniform_int_distribution<IntegerT>,
                                        std::default_random_engine>
                                       (min, max); 
}

namespace detail {

template <typename IntegerT, typename MutexT, typename EngineT>
int generate_random_number(const IntegerT min, const IntegerT max,
                           MutexT& m, EngineT& e,
                           std::enable_if_t<std::is_integral<IntegerT>::value>* = nullptr)
{
  return detail::generate_random_number<IntegerT, MutexT,
                                        std::uniform_int_distribution<IntegerT>,
                                        EngineT>
                                       (min, max, m, e);
}

template <typename IntegerT, typename MutexT, typename EngineT>
int generate_random_number(const IntegerT max,
                           MutexT& m, EngineT& e,
                           std::enable_if_t<std::is_integral<IntegerT>::value>* = nullptr)
{
  constexpr IntegerT zero = 0;
  return generate_random_number(zero, max, m, e);
}

} // namespace detail

template <typename IntegerT>
int generate_random_number(const IntegerT max,
                           std::enable_if_t<std::is_integral<IntegerT>::value>* = nullptr)
{
  constexpr IntegerT zero = 0;   
  return generate_random_number(zero, max);
}

template <typename RealT>
RealT generate_random_number(const RealT min, const RealT max, 
                             std::enable_if_t<std::is_floating_point<RealT>::value>* = nullptr)
{
  return detail::generate_random_number<RealT,
                                        std::uniform_real_distribution<RealT>, 
                                        std::default_random_engine>
                                       (min, max);
} 

namespace detail {

template <typename RealT, typename MutexT>
RealT generate_random_number(const RealT max, MutexT& m,
                             std::enable_if_t<std::is_floating_point<RealT>::value>* = nullptr)
{
  constexpr RealT zero = 0.0;
  return generate_random_number(zero, max, m);
}

template <typename RealT, typename MutexT, typename EngineT>
RealT generate_random_number(const RealT min, const RealT max, MutexT& m, EngineT& e,
                             std::enable_if_t<std::is_floating_point<RealT>::value>* = nullptr)
{
  return detail::generate_random_number<RealT, MutexT, 
                                        std::uniform_real_distribution<RealT>,
                                        EngineT>
                                       (min, max, m, e);
}


template <typename RealT, typename MutexT, typename EngineT>
RealT generate_random_number(const RealT max, MutexT& m, EngineT& e,
                             std::enable_if_t<std::is_floating_point<RealT>::value>* = nullptr)
{
  constexpr RealT zero = 0.0;
  return generate_random_number(zero, max, m, e);
}

} // namespace detail

template <typename RealT>
RealT generate_random_number(const RealT max,
                             std::enable_if_t<std::is_floating_point<RealT>::value>* = nullptr)
{
  constexpr RealT zero = 0.0;
  return generate_random_number(zero, max);
}

// Function object:
template <typename NumberT>
class random_number_generator final
{
  std::mutex l;
  std::random_device rd;
  std::default_random_engine e;
 
  public:
  using number_type = NumberT;
 
  public:
  random_number_generator() {
    detail::randomize_rng(l, e);
  }
 
  explicit random_number_generator(const int seed) {
    detail::randomize_rng(seed, l, e);
  }

  random_number_generator(random_number_generator&& rhs)
   : e(std::move(rhs.e))
  {}
 
  public:
  random_number_generator(const random_number_generator&)            = delete;
  random_number_generator& operator=(const random_number_generator&) = delete;
 
  public:
  NumberT operator()() { 
    return detail::generate_random_number(l, e); 
  }
 
  NumberT operator()(const NumberT max) { 
    return detail::generate_random_number(max, l, e); 
  }
 
  NumberT operator()(const NumberT min, const NumberT max) { 
    return detail::generate_random_number(min, max, l, e); 
  }
 
  public:
  void seed(const int n) { 
    detail::randomize_rng(n, l, e); 
  }
};

} // inline namespace version_1_0

}} // namespace ceph::util

#endif
