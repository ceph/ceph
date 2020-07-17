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


#ifndef CEPH_INTERVAL_SET_H
#define CEPH_INTERVAL_SET_H

#include <iterator>
#include <map>
#include <ostream>

#include "encoding.h"

/*
 * *** NOTE ***
 *
 * This class is written to work with a variety of map-like containers,
 * *include* ones that invalidate iterators when they are modified (e.g.,
 * flat_map and btree_map).
 */

template<typename T, template<typename, typename, typename ...> class C = std::map>
class interval_set {
 public:
  using Map = C<T, T>;
  using value_type = typename Map::value_type;
  using offset_type = T;
  using length_type = T;
  using reference = value_type&;
  using const_reference = const value_type&;
  using size_type = typename Map::size_type;

  class const_iterator;

  class iterator : public std::iterator <std::forward_iterator_tag, T>
  {
    public:
        explicit iterator(typename Map::iterator iter)
          : _iter(iter)
        { }

        // For the copy constructor and assignment operator, the compiler-generated functions, which
        // perform simple bitwise copying, should be fine.

        bool operator==(const iterator& rhs) const {
          return (_iter == rhs._iter);
        }

        bool operator!=(const iterator& rhs) const {
          return (_iter != rhs._iter);
        }

        // Dereference this iterator to get a pair.
        reference operator*() const {
          return *_iter;
        }

        // Return the interval start.
        offset_type get_start() const {
          return _iter->first;
        }

        // Return the interval length.
        length_type get_len() const {
          return _iter->second;
        }

        offset_type get_end() const {
          return _iter->first + _iter->second;
        }

        // Set the interval length.
        void set_len(const length_type& len) {
          _iter->second = len;
        }

        // Preincrement
        iterator& operator++()
        {
          ++_iter;
          return *this;
        }

        // Postincrement
        iterator operator++(int)
        {
          iterator prev(_iter);
          ++_iter;
          return prev;
        }

    friend class interval_set::const_iterator;

    protected:
        typename Map::iterator _iter;
    friend class interval_set;
  };

  class const_iterator : public std::iterator <std::forward_iterator_tag, T>
  {
    public:
        explicit const_iterator(typename Map::const_iterator iter)
          : _iter(iter)
        { }

        const_iterator(const iterator &i)
	  : _iter(i._iter)
        { }

        // For the copy constructor and assignment operator, the compiler-generated functions, which
        // perform simple bitwise copying, should be fine.

        bool operator==(const const_iterator& rhs) const {
          return (_iter == rhs._iter);
        }

        bool operator!=(const const_iterator& rhs) const {
          return (_iter != rhs._iter);
        }

        // Dereference this iterator to get a pair.
        const_reference operator*() const {
          return *_iter;
        }

        // Return the interval start.
        offset_type get_start() const {
          return _iter->first;
        }
        offset_type get_end() const {
          return _iter->first + _iter->second;
        }

        // Return the interval length.
        length_type get_len() const {
          return _iter->second;
        }

        // Preincrement
        const_iterator& operator++()
        {
          ++_iter;
          return *this;
        }

        // Postincrement
        const_iterator operator++(int)
        {
          const_iterator prev(_iter);
          ++_iter;
          return prev;
        }

    protected:
        typename Map::const_iterator _iter;
  };

  interval_set() = default;
  interval_set(Map&& other) {
    m.swap(other);
    for (const auto& p : m) {
      _size += p.second;
    }
  }

  size_type num_intervals() const
  {
    return m.size();
  }

  iterator begin() {
    return iterator(m.begin());
  }

  iterator lower_bound(T start) {
    return iterator(find_inc_m(start));
  }

  iterator end() {
    return iterator(m.end());
  }

  const_iterator begin() const {
    return const_iterator(m.begin());
  }

  const_iterator lower_bound(T start) const {
    return const_iterator(find_inc(start));
  }

  const_iterator end() const {
    return const_iterator(m.end());
  }

  // helpers
 private:
  auto find_inc(T start) const {
    auto p = m.lower_bound(start);  // p->first >= start
    if (p != m.begin() &&
        (p == m.end() || p->first > start)) {
      --p;   // might overlap?
      if (p->first + p->second <= start)
        ++p; // it doesn't.
    }
    return p;
  }
  
  auto find_inc_m(T start) {
    auto p = m.lower_bound(start);
    if (p != m.begin() &&
        (p == m.end() || p->first > start)) {
      --p;   // might overlap?
      if (p->first + p->second <= start)
        ++p; // it doesn't.
    }
    return p;
  }
  
  auto find_adj(T start) const {
    auto p = m.lower_bound(start);
    if (p != m.begin() &&
        (p == m.end() || p->first > start)) {
      --p;   // might touch?
      if (p->first + p->second < start)
        ++p; // it doesn't.
    }
    return p;
  }
  
  auto find_adj_m(T start) {
    auto p = m.lower_bound(start);
    if (p != m.begin() &&
        (p == m.end() || p->first > start)) {
      --p;   // might touch?
      if (p->first + p->second < start)
        ++p; // it doesn't.
    }
    return p;
  }

  void intersection_size_asym(const interval_set &s, const interval_set &l) {
    auto ps = s.m.begin();
    ceph_assert(ps != s.m.end());
    auto offset = ps->first;
    bool first = true;
    auto mi = m.begin();

    while (1) {
      if (first)
        first = false;
      auto pl = l.find_inc(offset);
      if (pl == l.m.end())
        break;
      while (ps != s.m.end() && ps->first + ps->second <= pl->first)
        ++ps;
      if (ps == s.m.end())
        break;
      offset = pl->first + pl->second;
      if (offset <= ps->first) {
        offset = ps->first;
        continue;
      }

      if (*ps == *pl) {
        do {
          mi = m.insert(mi, *ps);
          _size += ps->second;
          ++ps;
          ++pl;
        } while (ps != s.m.end() && pl != l.m.end() && *ps == *pl);
        if (ps == s.m.end())
          break;
        offset = ps->first;
        continue;
      }

      auto start = std::max<T>(ps->first, pl->first);
      auto en = std::min<T>(ps->first + ps->second, offset);
      ceph_assert(en > start);
      mi = m.emplace_hint(mi, start, en - start);
      _size += mi->second;
      if (ps->first + ps->second <= offset) {
        ++ps;
        if (ps == s.m.end())
          break;
        offset = ps->first;
      }
    }
  }

  bool subset_size_sym(const interval_set &b) const {
    auto pa = m.begin(), pb = b.m.begin();
    const auto a_end = m.end(), b_end = b.m.end();

    while (pa != a_end && pb != b_end) {
      while (pb->first + pb->second <= pa->first) {
        ++pb;
        if (pb == b_end)
          return false;
      }

      if (*pa == *pb) {
        do {
          ++pa;
          ++pb;
        } while (pa != a_end && pb != b_end && *pa == *pb);
        continue;
      }

      // interval begins before other
      if (pa->first < pb->first)
        return false;
      // interval is longer than other
      if (pa->first + pa->second > pb->first + pb->second)
        return false;

      ++pa;
    }

    return pa == a_end;
  }
  
 public:
  bool operator==(const interval_set& other) const {
    return _size == other._size && m == other.m;
  }

  uint64_t size() const {
    return _size;
  }

  void bound_encode(size_t& p) const {
    denc_traits<Map>::bound_encode(m, p);
  }
  void encode(ceph::buffer::list::contiguous_appender& p) const {
    denc(m, p);
  }
  void decode(ceph::buffer::ptr::const_iterator& p) {
    denc(m, p);
    _size = 0;
    for (const auto& p : m) {
      _size += p.second;
    }
  }
  void decode(ceph::buffer::list::iterator& p) {
    denc(m, p);
    _size = 0;
    for (const auto& p : m) {
      _size += p.second;
    }
  }

  void encode_nohead(ceph::buffer::list::contiguous_appender& p) const {
    denc_traits<Map>::encode_nohead(m, p);
  }
  void decode_nohead(int n, ceph::buffer::ptr::const_iterator& p) {
    denc_traits<Map>::decode_nohead(n, m, p);
    _size = 0;
    for (const auto& p : m) {
      _size += p.second;
    }
  }

  void clear() {
    m.clear();
    _size = 0;
  }

  bool contains(T i, T *pstart=0, T *plen=0) const {
    auto p = find_inc(i);
    if (p == m.end()) return false;
    if (p->first > i) return false;
    if (p->first+p->second <= i) return false;
    ceph_assert(p->first <= i && p->first+p->second > i);
    if (pstart)
      *pstart = p->first;
    if (plen)
      *plen = p->second;
    return true;
  }
  bool contains(T start, T len) const {
    auto p = find_inc(start);
    if (p == m.end()) return false;
    if (p->first > start) return false;
    if (p->first+p->second <= start) return false;
    ceph_assert(p->first <= start && p->first+p->second > start);
    if (p->first+p->second < start+len) return false;
    return true;
  }
  bool intersects(T start, T len) const {
    interval_set a;
    a.insert(start, len);
    interval_set i;
    i.intersection_of( *this, a );
    if (i.empty()) return false;
    return true;
  }

  // outer range of set
  bool empty() const {
    return m.empty();
  }
  offset_type range_start() const {
    ceph_assert(!empty());
    auto p = m.begin();
    return p->first;
  }
  offset_type range_end() const {
    ceph_assert(!empty());
    auto p = m.end();
    p--;
    return p->first+p->second;
  }

  // interval start after p (where p not in set)
  bool starts_after(T i) const {
    ceph_assert(!contains(i));
    auto p = find_inc(i);
    if (p == m.end()) return false;
    return true;
  }
  offset_type start_after(T i) const {
    ceph_assert(!contains(i));
    auto p = find_inc(i);
    return p->first;
  }

  // interval end that contains start
  offset_type end_after(T start) const {
    ceph_assert(contains(start));
    auto p = find_inc(start);
    return p->first+p->second;
  }
  
  void insert(T val) {
    insert(val, 1);
  }

  void insert(T start, T len, T *pstart=0, T *plen=0) {
    //cout << "insert " << start << "~" << len << endl;
    ceph_assert(len > 0);
    _size += len;
    auto p = find_adj_m(start);
    if (p == m.end()) {
      m[start] = len;                  // new interval
      if (pstart)
	*pstart = start;
      if (plen)
	*plen = len;
    } else {
      if (p->first < start) {
        
        if (p->first + p->second != start) {
          //cout << "p is " << p->first << "~" << p->second << ", start is " << start << ", len is " << len << endl;
          ceph_abort();
        }
        
        p->second += len;               // append to end
        
        auto n = p;
        ++n;
	if (pstart)
	  *pstart = p->first;
        if (n != m.end() && 
            start+len == n->first) {   // combine with next, too!
          p->second += n->second;
	  if (plen)
	    *plen = p->second;
          m.erase(n);
        } else {
	  if (plen)
	    *plen = p->second;
	}
      } else {
        if (start+len == p->first) {
	  if (pstart)
	    *pstart = start;
	  if (plen)
	    *plen = len + p->second;
	  T psecond = p->second;
          m.erase(p);
          m[start] = len + psecond;  // append to front
        } else {
          ceph_assert(p->first > start+len);
	  if (pstart)
	    *pstart = start;
	  if (plen)
	    *plen = len;
          m[start] = len;              // new interval
        }
      }
    }
  }

  void swap(interval_set& other) {
    m.swap(other.m);
    std::swap(_size, other._size);
  }    
  
  void erase(iterator &i) {
    _size -= i.get_len();
    m.erase(i._iter);
  }

  void erase(T val) {
    erase(val, 1);
  }

  void erase(T start, T len, 
    std::function<bool(T, T)> claim = {}) {
    auto p = find_inc_m(start);

    _size -= len;

    ceph_assert(p != m.end());
    ceph_assert(p->first <= start);

    T before = start - p->first;
    ceph_assert(p->second >= before+len);
    T after = p->second - before - len;
    if (before) {
      if (claim && claim(p->first, before)) {
	_size -= before;
	m.erase(p);
      } else {
	p->second = before;        // shorten bit before
      }
    } else {
      m.erase(p);
    }
    if (after) {
      if (claim && claim(start + len, after)) {
	_size -= after;
      } else {
	m[start + len] = after;
      }
    }
  }

  void subtract(const interval_set &a) {
    for (const auto& [start, len] : a.m) {
      erase(start, len);
    }
  }

  void insert(const interval_set &a) {
    for (const auto& [start, len] : a.m) {
      insert(start, len);
    }
  }


  void intersection_of(const interval_set &a, const interval_set &b) {
    ceph_assert(&a != this);
    ceph_assert(&b != this);
    clear();

    const interval_set *s, *l;

    if (a.size() < b.size()) {
      s = &a;
      l = &b;
    } else {
      s = &b;
      l = &a;
    }

    if (!s->size())
      return;

    /*
     * Use the lower_bound algorithm for larger size ratios
     * where it performs better, but not for smaller size
     * ratios where sequential search performs better.
     */
    if (l->size() / s->size() >= 10) {
      intersection_size_asym(*s, *l);
      return;
    }

    auto pa = a.m.begin();
    auto pb = b.m.begin();
    auto mi = m.begin();

    while (pa != a.m.end() && pb != b.m.end()) {
      // passing?
      if (pa->first + pa->second <= pb->first) 
        { pa++;  continue; }
      if (pb->first + pb->second <= pa->first) 
        { pb++;  continue; }

      if (*pa == *pb) {
        do {
          mi = m.insert(mi, *pa);
          _size += pa->second;
          ++pa;
          ++pb;
        } while (pa != a.m.end() && pb != b.m.end() && *pa == *pb);
        continue;
      }

      T start = std::max(pa->first, pb->first);
      T en = std::min(pa->first+pa->second, pb->first+pb->second);
      ceph_assert(en > start);
      mi = m.emplace_hint(mi, start, en - start);
      _size += mi->second;
      if (pa->first+pa->second > pb->first+pb->second)
        pb++;
      else
        pa++; 
    }
  }
  void intersection_of(const interval_set& b) {
    interval_set a;
    swap(a);
    intersection_of(a, b);
  }

  void union_of(const interval_set &a, const interval_set &b) {
    ceph_assert(&a != this);
    ceph_assert(&b != this);
    clear();
    
    //cout << "union_of" << endl;

    // a
    m = a.m;
    _size = a._size;

    // - (a*b)
    interval_set ab;
    ab.intersection_of(a, b);
    subtract(ab);

    // + b
    insert(b);
    return;
  }
  void union_of(const interval_set &b) {
    interval_set a;
    swap(a);    
    union_of(a, b);
  }
  void union_insert(T off, T len) {
    interval_set a;
    a.insert(off, len);
    union_of(a);
  }

  bool subset_of(const interval_set &big) const {
    if (!size())
      return true;
    if (size() > big.size())
      return false;
    if (range_end() > big.range_end())
      return false;

    /*
     * Use the lower_bound algorithm for larger size ratios
     * where it performs better, but not for smaller size
     * ratios where sequential search performs better.
     */
    if (big.size() / size() < 10)
      return subset_size_sym(big);

    for (const auto& [start, len] : m) {
      if (!big.contains(start, len)) return false;
    }
    return true;
  }  

  /*
   * build a subset of @other, starting at or after @start, and including
   * @len worth of values, skipping holes.  e.g.,
   *  span_of([5~10,20~5], 8, 5) -> [8~2,20~3]
   */
  void span_of(const interval_set &other, T start, T len) {
    clear();
    auto p = other.find_inc(start);
    if (p == other.m.end())
      return;
    if (p->first < start) {
      if (p->first + p->second < start)
	return;
      if (p->first + p->second < start + len) {
	T howmuch = p->second - (start - p->first);
	insert(start, howmuch);
	len -= howmuch;
	p++;
      } else {
	insert(start, len);
	return;
      }
    }
    while (p != other.m.end() && len > 0) {
      if (p->second < len) {
	insert(p->first, p->second);
	len -= p->second;
	p++;
      } else {
	insert(p->first, len);
	return;
      }
    }
  }

  /*
   * Move contents of m into another Map. Use that instead of
   * encoding interval_set into bufferlist then decoding it back into Map.
   */
  Map detach() && {
    return std::move(m);
  }

private:
  // data
  uint64_t _size = 0;
  Map m;   // map start -> len
};

// declare traits explicitly because (1) it's templatized, and (2) we
// want to include _nohead variants.
template<typename T, template<typename, typename, typename ...> class C>
struct denc_traits<interval_set<T, C>> {
private:
  using container_t = interval_set<T, C>;
public:
  static constexpr bool supported = true;
  static constexpr bool bounded = false;
  static constexpr bool featured = false;
  static constexpr bool need_contiguous = denc_traits<T, C<T,T>>::need_contiguous;
  static void bound_encode(const container_t& v, size_t& p) {
    v.bound_encode(p);
  }
  static void encode(const container_t& v,
		     ceph::buffer::list::contiguous_appender& p) {
    v.encode(p);
  }
  static void decode(container_t& v, ceph::buffer::ptr::const_iterator& p) {
    v.decode(p);
  }
  template<typename U=T>
    static typename std::enable_if<sizeof(U) && !need_contiguous>::type
  decode(container_t& v, ceph::buffer::list::iterator& p) {
    v.decode(p);
  }
  static void encode_nohead(const container_t& v,
			    ceph::buffer::list::contiguous_appender& p) {
    v.encode_nohead(p);
  }
  static void decode_nohead(size_t n, container_t& v,
			    ceph::buffer::ptr::const_iterator& p) {
    v.decode_nohead(n, p);
  }
};


template<typename T, template<typename, typename, typename ...> class C>
inline std::ostream& operator<<(std::ostream& out, const interval_set<T,C> &s) {
  out << "[";
  bool first = true;
  for (const auto& [start, len] : s) {
    if (!first) out << ",";
    out << start << "~" << len;
    first = false;
  }
  out << "]";
  return out;
}


#endif
