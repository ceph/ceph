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

#pragma once

#include <boost/container/flat_map.hpp>

#include "denc_flat_map.h"
#include "encoding.h"

namespace boost::container {

template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode(const boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist& bl)
{
  using ceph::encode;

  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (typename boost::container::flat_map<T,U,Comp>::const_iterator p
	 = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode(const boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist& bl,
	 uint64_t features)
{
  using ceph::encode;

  __u32 n = (__u32)(m.size());
  encode(n, bl);
  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  decode(boost::container::flat_map<T,U,Comp,Alloc>& m, bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  m.clear();
  m.reserve(n);
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U, class Comp, class Alloc>
inline void decode_noclear(boost::container::flat_map<T,U,Comp,Alloc>& m,
			   bufferlist::const_iterator& p)
{
  using ceph::decode;

  __u32 n;
  decode(n, p);
  m.reserve(m.size() + n);
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode_nohead(const boost::container::flat_map<T,U,Comp,Alloc>& m,
		bufferlist& bl)
{
  using ceph::encode;

  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl);
    encode(p->second, bl);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
  inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  encode_nohead(const boost::container::flat_map<T,U,Comp,Alloc>& m,
		bufferlist& bl, uint64_t features)
{
  using ceph::encode;

  for (auto p = m.begin(); p != m.end(); ++p) {
    encode(p->first, bl, features);
    encode(p->second, bl, features);
  }
}
template<class T, class U, class Comp, class Alloc,
	 typename t_traits=denc_traits<T>, typename u_traits=denc_traits<U>>
inline std::enable_if_t<!t_traits::supported || !u_traits::supported>
  decode_nohead(int n, boost::container::flat_map<T,U,Comp,Alloc>& m,
		bufferlist::const_iterator& p)
{
  using ceph::decode;

  m.clear();
  while (n--) {
    T k;
    decode(k, p);
    decode(m[k], p);
  }
}

} // namespace boost::container
