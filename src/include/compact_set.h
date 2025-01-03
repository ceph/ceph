/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_COMPACT_SET_H
#define CEPH_COMPACT_SET_H

#include "buffer.h"
#include "encoding.h"

#include <memory>
#include <set>

template <class T, class Set>
class compact_set_base {
protected:
  std::unique_ptr<Set> set;
  void alloc_internal() {
    if (!set)
      set.reset(new Set);
  }
  void free_internal() {
    set.reset();
  }
  template <class It>
  class iterator_base {
  private:
    const compact_set_base* set;
    It it;
    iterator_base() : set(0) { }
    iterator_base(const compact_set_base* s) : set(s) { }
    iterator_base(const compact_set_base* s, const It& i) : set(s), it(i) { }
    friend class compact_set_base;
  public:
    iterator_base(const iterator_base& o) {
      set = o.set;
      it = o.it;
    }
    bool operator==(const iterator_base& o) const {
      return (set == o.set) && (!set->set || it == o.it);
    }
    bool operator!=(const iterator_base& o) const {
      return !(*this == o);;
    }
    iterator_base& operator=(const iterator_base& o) {
      set->set = o.set;
      it = o.it;
      return *this;
    }
    iterator_base& operator++() {
      ++it;
      return *this;
    }
    iterator_base operator++(int) {
      iterator_base tmp = *this;
      ++it;
      return tmp;
    }
    iterator_base& operator--() {
      --it;
      return *this;
    }
    const T& operator*() {
      return *it;
    }
  };
public:
  class const_iterator : public iterator_base<typename Set::const_iterator> {
    public:
      const_iterator() { }
      const_iterator(const iterator_base<typename Set::const_iterator>& o)
	: iterator_base<typename Set::const_iterator>(o) { }
      const_iterator(const compact_set_base* s) : iterator_base<typename Set::const_iterator>(s) { }
      const_iterator(const compact_set_base* s, const typename Set::const_iterator& i)
	: iterator_base<typename Set::const_iterator>(s, i) { }
  };
  class iterator : public iterator_base<typename Set::iterator> {
    public:
      iterator() { }
      iterator(const iterator_base<typename Set::iterator>& o)
	: iterator_base<typename Set::iterator>(o) { }
      iterator(compact_set_base* s) : iterator_base<typename Set::iterator>(s) { }
      iterator(compact_set_base* s, const typename Set::iterator& i)
	: iterator_base<typename Set::iterator>(s, i) { }
      operator const_iterator() const {
	return const_iterator(this->set, this->it);
      }
  };
  class const_reverse_iterator : public iterator_base<typename Set::const_reverse_iterator> {
    public:
      const_reverse_iterator() { }
      const_reverse_iterator(const iterator_base<typename Set::const_reverse_iterator>& o)
	: iterator_base<typename Set::const_reverse_iterator>(o) { }
      const_reverse_iterator(const compact_set_base* s) : iterator_base<typename Set::const_reverse_iterator>(s) { }
      const_reverse_iterator(const compact_set_base* s, const typename Set::const_reverse_iterator& i)
	: iterator_base<typename Set::const_reverse_iterator>(s, i) { }
  };
  class reverse_iterator : public iterator_base<typename Set::reverse_iterator> {
    public:
      reverse_iterator() { }
      reverse_iterator(const iterator_base<typename Set::reverse_iterator>& o)
	: iterator_base<typename Set::reverse_iterator>(o) { }
      reverse_iterator(compact_set_base* s) : iterator_base<typename Set::reverse_iterator>(s) { }
      reverse_iterator(compact_set_base* s, const typename Set::reverse_iterator& i)
	: iterator_base<typename Set::reverse_iterator>(s, i) { }
      operator const_iterator() const {
	return const_iterator(this->set, this->it);
      }
  };

  compact_set_base() {}
  compact_set_base(const compact_set_base& o) {
    if (o.set) {
      alloc_internal();
      *set = *o.set;
    }
  }
  ~compact_set_base() {}


  bool empty() const {
    return !set || set->empty();
  }
  size_t size() const {
    return set ? set->size() : 0;
  }
  bool operator==(const compact_set_base& o) const {
    return (empty() && o.empty()) || (set && o.set && *set == *o.set);
  }
  bool operator!=(const compact_set_base& o) const {
    return !(*this == o);
  }
  size_t count(const T& t) const {
    return set ? set->count(t) : 0;
  }
  iterator erase (iterator p) {
    if (set) {
      ceph_assert(this == p.set);
      auto it = set->erase(p.it);
      if (set->empty()) {
        free_internal();
        return iterator(this);
      } else {
        return iterator(this, it);
      }
    } else {
      return iterator(this);
    }
  }
  size_t erase (const T& t) {
    if (!set)
      return 0;
    size_t r = set->erase(t);
    if (set->empty())
      free_internal();
    return r;
  }
  void clear() {
    free_internal();
  }
  void swap(compact_set_base& o) {
    set.swap(o.set);
  }
  compact_set_base& operator=(const compact_set_base& o) {
    if (o.set) {
      alloc_internal();
      *set = *o.set;
    } else
      free_internal();
    return *this;
  }
  std::pair<iterator,bool> insert(const T& t) {
    alloc_internal();
    std::pair<typename Set::iterator,bool> r = set->insert(t);
    return std::make_pair(iterator(this, r.first), r.second);
  }
  template <class... Args>
  std::pair<iterator,bool> emplace ( Args&&... args ) {
    alloc_internal();
    auto em = set->emplace(std::forward<Args>(args)...);
    return std::pair<iterator,bool>(iterator(this, em.first), em.second);
  }

  iterator begin() {
   if (!set)
     return iterator(this);
   return iterator(this, set->begin());
  }
  iterator end() {
   if (!set)
     return iterator(this);
   return iterator(this, set->end());
  }
  reverse_iterator rbegin() {
   if (!set)
     return reverse_iterator(this);
   return reverse_iterator(this, set->rbegin());
  }
  reverse_iterator rend() {
   if (!set)
     return reverse_iterator(this);
   return reverse_iterator(this, set->rend());
  }
  iterator find(const T& t) {
    if (!set)
      return iterator(this);
    return iterator(this, set->find(t));
  }
  iterator lower_bound(const T& t) {
    if (!set)
      return iterator(this);
    return iterator(this, set->lower_bound(t));
  }
  iterator upper_bound(const T& t) {
    if (!set)
      return iterator(this);
    return iterator(this, set->upper_bound(t));
  }
  const_iterator begin() const {
   if (!set)
     return const_iterator(this);
   return const_iterator(this, set->begin());
  }
  const_iterator end() const {
   if (!set)
     return const_iterator(this);
   return const_iterator(this, set->end());
  }
  const_reverse_iterator rbegin() const {
   if (!set)
     return const_reverse_iterator(this);
   return const_reverse_iterator(this, set->rbegin());
  }
  const_reverse_iterator rend() const {
   if (!set)
     return const_reverse_iterator(this);
   return const_reverse_iterator(this, set->rend());
  }
  const_iterator find(const T& t) const {
    if (!set)
      return const_iterator(this);
    return const_iterator(this, set->find(t));
  }
  const_iterator lower_bound(const T& t) const {
    if (!set)
      return const_iterator(this);
    return const_iterator(this, set->lower_bound(t));
  }
  const_iterator upper_bound(const T& t) const {
    if (!set)
      return const_iterator(this);
    return const_iterator(this, set->upper_bound(t));
  }
  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    if (set)
      encode(*set, bl);
    else
      encode((uint32_t)0, bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    uint32_t n;
    decode(n, p);
    if (n > 0) {
      alloc_internal();
      ceph::decode_nohead(n, *set, p);
    } else
      free_internal();
  }
};

template<class T, class Set>
inline void encode(const compact_set_base<T, Set>& m, ceph::buffer::list& bl) {
  m.encode(bl);
}
template<class T, class Set>
inline void decode(compact_set_base<T, Set>& m, ceph::buffer::list::const_iterator& p) {
  m.decode(p);
}

template <class T, class Compare = std::less<T>, class Alloc = std::allocator<T> >
class compact_set : public compact_set_base<T, std::set<T, Compare, Alloc> > {
};

template <class T, class Compare = std::less<T>, class Alloc = std::allocator<T> >
inline std::ostream& operator<<(std::ostream& out, const compact_set<T,Compare,Alloc>& s)
{
  bool first = true;
  for (auto &v : s) {
    if (!first)
      out << ",";
    out << v;
    first = false;
  }
  return out;
}
#endif
