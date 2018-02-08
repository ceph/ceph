// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_INTARITH_H
#define CEPH_INTARITH_H

#include <limits>
#include <type_traits>

#ifndef DIV_ROUND_UP
#define DIV_ROUND_UP(n, d)  (((n) + (d) - 1) / (d))
#endif

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> div_round_up(T n, U d) {
  return (n + d - 1) / d;
}


#ifndef ROUND_UP_TO
#define ROUND_UP_TO(n, d) ((n)%(d) ? ((n)+(d)-(n)%(d)) : (n))
#endif

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> round_up_to(T n, U d) {
  return (n % d ? (n + d - n % d) : n);
}

#ifndef SHIFT_ROUND_UP
#define SHIFT_ROUND_UP(x,y) (((x)+(1<<(y))-1) >> (y))
#endif

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> shift_round_up(T x, U y) {
  return (x + (1 << y) - 1) >> y;
}

/*
 * Macro to determine if value is a power of 2
 */
#define ISP2(x)		(((x) & ((x) - 1)) == 0)

template<typename T>
constexpr inline bool isp2(T x) {
  return (x & (x - 1)) == 0;
}

/*
 * Macros for various sorts of alignment and rounding.  The "align" must
 * be a power of 2.  Often times it is a block, sector, or page.
 */

/*
 * return x rounded down to an align boundary
 * eg, P2ALIGN(1200, 1024) == 1024 (1*align)
 * eg, P2ALIGN(1024, 1024) == 1024 (1*align)
 * eg, P2ALIGN(0x1234, 0x100) == 0x1200 (0x12*align)
 * eg, P2ALIGN(0x5600, 0x100) == 0x5600 (0x56*align)
 */
#define P2ALIGN(x, align)		((x) & -(align))

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> p2align(T x, U align) {
  return x & -align;
}

/*
 * return x % (mod) align
 * eg, P2PHASE(0x1234, 0x100) == 0x34 (x-0x12*align)
 * eg, P2PHASE(0x5600, 0x100) == 0x00 (x-0x56*align)
 */
#define P2PHASE(x, align)		((x) & ((align) - 1))

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> p2phase(T x, U align) {
  return x & (align - 1);
}

/*
 * return how much space is left in this block (but if it's perfectly
 * aligned, return 0).
 * eg, P2NPHASE(0x1234, 0x100) == 0xcc (0x13*align-x)
 * eg, P2NPHASE(0x5600, 0x100) == 0x00 (0x56*align-x)
 */
#define P2NPHASE(x, align)		(-(x) & ((align) - 1))

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> p2nphase(T x, U align) {
  return -x & (align - 1);
}

/*
 * return x rounded up to an align boundary
 * eg, P2ROUNDUP(0x1234, 0x100) == 0x1300 (0x13*align)
 * eg, P2ROUNDUP(0x5600, 0x100) == 0x5600 (0x56*align)
 */
#define P2ROUNDUP(x, align)		(-(-(x) & -(align)))

template<typename T, typename U>
constexpr inline std::make_unsigned_t<std::common_type_t<T, U>> p2roundup(T x, U align) {
  return (-(-(x) & -(align)));
}

// count trailing zeros.
// NOTE: the builtin is nondeterministic on 0 input
template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) <= sizeof(unsigned)),
  unsigned>::type ctz(T v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctz(v);
}

template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) > sizeof(unsigned int) &&
   sizeof(T) <= sizeof(unsigned long)),
  unsigned>::type ctz(T v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctzl(v);
}

template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) > sizeof(unsigned long) &&
   sizeof(T) <= sizeof(unsigned long long)),
  unsigned>::type ctz(T v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_ctzll(v);
}

// count leading zeros
// NOTE: the builtin is nondeterministic on 0 input
template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) <= sizeof(unsigned)),
  unsigned>::type clz(T v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clz(v);
}

template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) > sizeof(unsigned int) &&
   sizeof(T) <= sizeof(unsigned long)),
  unsigned>::type clz(T v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clzl(v);
}

template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) > sizeof(unsigned long) &&
   sizeof(T) <= sizeof(unsigned long long)),
  unsigned>::type clz(T v) {
  if (v == 0)
    return sizeof(v) * 8;
  return __builtin_clzll(v);
}

// count bits (set + any 0's that follow)
template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) <= sizeof(unsigned)),
  unsigned>::type cbits(T v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clz(v);
}

template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) > sizeof(unsigned int) &&
   sizeof(T) <= sizeof(unsigned long)),
  unsigned>::type cbits(T v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clzl(v);
}

template<class T>
  inline typename std::enable_if<
  (std::is_integral<T>::value &&
   sizeof(T) > sizeof(unsigned long) &&
   sizeof(T) <= sizeof(unsigned long long)),
  unsigned>::type cbits(T v) {
  if (v == 0)
    return 0;
  return (sizeof(v) * 8) - __builtin_clzll(v);
}

namespace ceph::math {

template<class UnsignedValueT>
class p2_t {
  typedef uint8_t exponent_type;
  typedef UnsignedValueT value_type;

  static_assert(std::is_unsigned_v<value_type>);
  static_assert(std::numeric_limits<exponent_type>::max() >
		std::numeric_limits<value_type>::digits);

  value_type value;

  struct _check_skipper_t {};
  p2_t(const value_type value, _check_skipper_t)
    : value(value) {
  }

public:
  p2_t(const value_type value)
    : value(value) {
    // 0 isn't a power of two. Additional validation is necessary as
    // the isp2 routine doesn't sanitize that case.
    //assert(value != 0);
    assert(isp2(value));
  }

  static p2_t<value_type> from_p2(const value_type p2) {
    return p2_t(p2, _check_skipper_t());
  }

  static p2_t<value_type> from_exponent(const exponent_type exponent) {
    return p2_t(1 << exponent, _check_skipper_t());
  }

  exponent_type get_exponent() const {
    return ctz(value);
  }

  value_type get_value() const {
    return value;
  }

  operator value_type() const {
    return value;
  }

  friend value_type operator/(const value_type& l,
                              const p2_t<value_type>& r) {
    return l >> (cbits(r.value) - 1);
  }

  friend value_type operator%(const value_type& l,
                              const p2_t<value_type>& r) {
    return l & (r.value - 1);
  }
};

} // namespace ceph::math

#endif
