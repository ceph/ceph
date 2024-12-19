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
#include <optional>
#include <random>
#include <type_traits>

// Workaround for https://gcc.gnu.org/bugzilla/show_bug.cgi?id=85494
#ifdef __MINGW32__
#include <boost/random/random_device.hpp>

using random_device_t = boost::random::random_device;
#else
using random_device_t = std::random_device;
#endif

// Basic random number facility (see N3551 for inspiration):
namespace ceph::util {

inline namespace version_1_0_3 {

namespace detail {

template <typename T0, typename T1>
using larger_of = typename std::conditional<
                    sizeof(T0) >= sizeof(T1), 
                    T0, T1>
                  ::type;

// avoid mixing floating point and integers:
template <typename NumberT0, typename NumberT1>
using has_compatible_numeric_types =
            std::disjunction<
                std::conjunction<
                    std::is_floating_point<NumberT0>, std::is_floating_point<NumberT1>
                >,
                std::conjunction<
                    std::is_integral<NumberT0>, std::is_integral<NumberT1>
                >
            >;


// Select the larger of type compatible numeric types:
template <typename NumberT0, typename NumberT1>
using select_number_t = std::enable_if_t<detail::has_compatible_numeric_types<NumberT0, NumberT1>::value,
                                         detail::larger_of<NumberT0, NumberT1>>;

} // namespace detail

namespace detail {

// Choose default distribution for appropriate types:
template <typename NumberT, 
          bool IsIntegral>
struct select_distribution
{
 using type = std::uniform_int_distribution<NumberT>;
};

template <typename NumberT>
struct select_distribution<NumberT, false>
{
 using type = std::uniform_real_distribution<NumberT>;
};

template <typename NumberT>
using default_distribution = typename
    select_distribution<NumberT, std::is_integral<NumberT>::value>::type;

} // namespace detail

namespace detail {

template <typename EngineT>
EngineT& engine();

template <typename MutexT, typename EngineT, 
          typename SeedT = typename EngineT::result_type>
void randomize_rng(const SeedT seed, MutexT& m, EngineT& e)
{
  std::lock_guard<MutexT> lg(m);
  e.seed(seed);
}

template <typename MutexT, typename EngineT>
void randomize_rng(MutexT& m, EngineT& e)
{
  random_device_t rd;
 
  std::lock_guard<MutexT> lg(m);
  e.seed(rd());
}

template <typename EngineT = std::default_random_engine,
          typename SeedT = typename EngineT::result_type>
void randomize_rng(const SeedT n)
{
  detail::engine<EngineT>().seed(n);
}

template <typename EngineT = std::default_random_engine>
void randomize_rng()
{
  random_device_t rd;
  detail::engine<EngineT>().seed(rd());
}

template <typename EngineT>
EngineT& engine()
{
  thread_local std::optional<EngineT> rng_engine;

  if (!rng_engine) {
    rng_engine.emplace(EngineT());
    randomize_rng<EngineT>();
  }

  return *rng_engine;
}

} // namespace detail

namespace detail {

template <typename NumberT,
          typename DistributionT = detail::default_distribution<NumberT>,
          typename EngineT>
NumberT generate_random_number(const NumberT min, const NumberT max,
                               EngineT& e)
{
  DistributionT d { min, max };

  using param_type = typename DistributionT::param_type;
  return d(e, param_type { min, max });
}

template <typename NumberT,
          typename MutexT,
          typename DistributionT = detail::default_distribution<NumberT>,
          typename EngineT>
NumberT generate_random_number(const NumberT min, const NumberT max,
                               MutexT& m, EngineT& e)
{
  DistributionT d { min, max };
 
  using param_type = typename DistributionT::param_type;
 
  std::lock_guard<MutexT> lg(m);
  return d(e, param_type { min, max });
}

template <typename NumberT,
          typename DistributionT = detail::default_distribution<NumberT>,
          typename EngineT>
NumberT generate_random_number(const NumberT min, const NumberT max)
{
  return detail::generate_random_number<NumberT, DistributionT, EngineT>
          (min, max, detail::engine<EngineT>());
}

template <typename MutexT, 
          typename EngineT,
          typename NumberT = int,
          typename DistributionT = detail::default_distribution<NumberT>>
NumberT generate_random_number(MutexT& m, EngineT& e)
{
  return detail::generate_random_number<NumberT, MutexT, DistributionT, EngineT>
          (0, std::numeric_limits<NumberT>::max(), m, e);
}

template <typename NumberT, typename MutexT, typename EngineT>
NumberT generate_random_number(const NumberT max, MutexT& m, EngineT& e)
{
  return generate_random_number<NumberT>(0, max, m, e);
}

} // namespace detail

template <typename EngineT = std::default_random_engine>
void randomize_rng()
{
  detail::randomize_rng<EngineT>();
}

template <typename NumberT = int,
          typename DistributionT = detail::default_distribution<NumberT>,
          typename EngineT = std::default_random_engine>
NumberT generate_random_number()
{
  return detail::generate_random_number<NumberT, DistributionT, EngineT>
          (0, std::numeric_limits<NumberT>::max());
}

template <typename NumberT0, typename NumberT1,
          typename NumberT = detail::select_number_t<NumberT0, NumberT1>
         >
NumberT generate_random_number(const NumberT0 min, const NumberT1 max)
{
  return detail::generate_random_number<NumberT,
                                        detail::default_distribution<NumberT>,
                                        std::default_random_engine>
                                       (static_cast<NumberT>(min), static_cast<NumberT>(max)); 
}

template <typename NumberT0, typename NumberT1,
          typename DistributionT,
          typename EngineT,
          typename NumberT = detail::select_number_t<NumberT0, NumberT1>
		 >
NumberT generate_random_number(const NumberT min, const NumberT max,
                               EngineT& e)
{
 return detail::generate_random_number<NumberT,
                       DistributionT,
                       EngineT>(static_cast<NumberT>(min), static_cast<NumberT>(max), e);
}

template <typename NumberT>
NumberT generate_random_number(const NumberT max)
{
 return generate_random_number<NumberT>(0, max);
}

// Function object:
template <typename NumberT>
class random_number_generator final
{
  std::mutex l;
  random_device_t rd;
  std::default_random_engine e;

  using seed_type = typename decltype(e)::result_type;
 
  public:
  using number_type         = NumberT;
  using random_engine_type  = decltype(e);
  using random_device_type  = decltype(rd);

  public:
  random_device_type& random_device() noexcept { return rd; } 
  random_engine_type& random_engine() noexcept { return e; }
 
  public:
  random_number_generator() {
    detail::randomize_rng(l, e);
  }
 
  explicit random_number_generator(const seed_type seed) {
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
    return detail::generate_random_number<NumberT>(max, l, e); 
  }
 
  NumberT operator()(const NumberT min, const NumberT max) { 
    return detail::generate_random_number<NumberT>(min, max, l, e); 
  }
 
  public:
  void seed(const seed_type n) { 
    detail::randomize_rng(n, l, e); 
  }
};

template <typename NumberT>
random_number_generator(const NumberT max) -> random_number_generator<NumberT>;

} // inline namespace version_*

} // namespace ceph::util

#endif
