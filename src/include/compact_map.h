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
#ifndef CEPH_COMPACT_MAP_H
#define CEPH_COMPACT_MAP_H

#include "buffer.h"
#include "encoding.h"

#include <map>
#include <memory>

#include "include/encoding.h"

template <class Key, class T, class Map>
class compact_map_base {
protected:
  std::unique_ptr<Map> map;
  void alloc_internal() {
    if (!map)
      map.reset(new Map);
  }
  void free_internal() {
    map.reset();
  }
  template <class It>
  class const_iterator_base {
    const compact_map_base *map;
    It it;
    const_iterator_base() : map(0) { }
    const_iterator_base(const compact_map_base* m) : map(m) { }
    const_iterator_base(const compact_map_base *m, const It& i) : map(m), it(i) { }
    friend class compact_map_base;
    friend class iterator_base;
  public:
    const_iterator_base(const const_iterator_base& o) {
      map = o.map;
      it = o.it;
    }
    bool operator==(const const_iterator_base& o) const {
      return (map == o.map) && (!map->map || it == o.it);
    }
    bool operator!=(const const_iterator_base& o) const {
      return !(*this == o);;
    }
    const_iterator_base& operator=(const const_iterator_base& o) {
      map = o.map;
      it = o.it;
      return *this;
    }
    const_iterator_base& operator++() {
      ++it;
      return *this;
    }
    const_iterator_base& operator--() {
      --it;
      return *this;
    }
    const std::pair<const Key,T>& operator*() {
      return *it;
    }
    const std::pair<const Key,T>* operator->() {
      return it.operator->();
    }
  };
  template <class It>
  class iterator_base {
  private:
    const compact_map_base* map;
    It it;
    iterator_base() : map(0) { }
    iterator_base(compact_map_base* m) : map(m) { }
    iterator_base(compact_map_base* m, const It& i) : map(m), it(i) { }
    friend class compact_map_base;
  public:
    iterator_base(const iterator_base& o) {
      map = o.map;
      it = o.it;
    }
    bool operator==(const iterator_base& o) const {
      return (map == o.map) && (!map->map || it == o.it);
    }
    bool operator!=(const iterator_base& o) const {
      return !(*this == o);;
    }
    iterator_base& operator=(const iterator_base& o) {
      map = o.map;
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
    std::pair<const Key,T>& operator*() {
      return *it;
    }
    std::pair<const Key,T>* operator->() {
      return it.operator->();
    }
    operator const_iterator_base<It>() const {
      return const_iterator_base<It>(map, it);
    }
  };

public:
  class iterator : public iterator_base<typename Map::iterator> {
    public:
      iterator() { }
      iterator(const iterator_base<typename Map::iterator>& o)
	: iterator_base<typename Map::iterator>(o) { }
      iterator(compact_map_base* m) : iterator_base<typename Map::iterator>(m) { }
      iterator(compact_map_base* m, const typename Map::iterator& i)
	: iterator_base<typename Map::iterator>(m, i) { }
  };
  class const_iterator : public const_iterator_base<typename Map::const_iterator> {
    public:
      const_iterator() { }
      const_iterator(const iterator_base<typename Map::const_iterator>& o)
	: const_iterator_base<typename Map::const_iterator>(o) { }
      const_iterator(const compact_map_base* m) : const_iterator_base<typename Map::const_iterator>(m) { }
      const_iterator(const compact_map_base* m, const typename Map::const_iterator& i)
	: const_iterator_base<typename Map::const_iterator>(m, i) { }
  };
  class reverse_iterator : public iterator_base<typename Map::reverse_iterator> {
    public:
      reverse_iterator() { }
      reverse_iterator(const iterator_base<typename Map::reverse_iterator>& o)
	: iterator_base<typename Map::reverse_iterator>(o) { }
      reverse_iterator(compact_map_base* m) : iterator_base<typename Map::reverse_iterator>(m) { }
      reverse_iterator(compact_map_base* m, const typename Map::reverse_iterator& i)
	: iterator_base<typename Map::reverse_iterator>(m, i) { }
  };
  class const_reverse_iterator : public const_iterator_base<typename Map::const_reverse_iterator> {
    public:
      const_reverse_iterator() { }
      const_reverse_iterator(const iterator_base<typename Map::const_reverse_iterator>& o)
	: iterator_base<typename Map::const_reverse_iterator>(o) { }
      const_reverse_iterator(const compact_map_base* m) : const_iterator_base<typename Map::const_reverse_iterator>(m) { }
      const_reverse_iterator(const compact_map_base* m, const typename Map::const_reverse_iterator& i)
	: const_iterator_base<typename Map::const_reverse_iterator>(m, i) { }
  };
  compact_map_base(const compact_map_base& o) {
    if (o.map) {
      alloc_internal();
      *map = *o.map;
    }
  }
  compact_map_base() {}
  ~compact_map_base() {}

  bool empty() const {
    return !map || map->empty();
  }
  size_t size() const {
    return map ? map->size() : 0;
  }
  bool operator==(const compact_map_base& o) const {
    return (empty() && o.empty()) || (map && o.map && *map == *o.map);
  }
  bool operator!=(const compact_map_base& o) const {
    return !(*this == o);
  }
  size_t count (const Key& k) const {
    return map ? map->count(k) : 0;
  }
  iterator erase (iterator p) {
    if (map) {
      ceph_assert(this == p.map);
      auto it = map->erase(p.it);
      if (map->empty()) {
        free_internal();
        return iterator(this);
      } else {
        return iterator(this, it);
      }
    } else {
      return iterator(this);
    }
  }
  size_t erase (const Key& k) {
    if (!map)
      return 0;
    size_t r = map->erase(k);
    if (map->empty())
	free_internal();
    return r;
  }
  void clear() {
    free_internal();
  }
  void swap(compact_map_base& o) {
    map.swap(o.map);
  }
  compact_map_base& operator=(const compact_map_base& o) {
    if (o.map) {
      alloc_internal();
      *map = *o.map;
    } else
      free_internal();
    return *this;
  }
  iterator insert(const std::pair<const Key, T>& val) {
    alloc_internal();
    return iterator(this, map->insert(val));
  }
  template <class... Args>
  std::pair<iterator,bool> emplace ( Args&&... args ) {
    alloc_internal();
    auto em = map->emplace(std::forward<Args>(args)...);
    return std::pair<iterator,bool>(iterator(this, em.first), em.second);
  }
  iterator begin() {
   if (!map)
     return iterator(this);
   return iterator(this, map->begin());
  }
  iterator end() {
   if (!map)
     return iterator(this);
   return iterator(this, map->end());
  }
  reverse_iterator rbegin() {
   if (!map)
     return reverse_iterator(this);
   return reverse_iterator(this, map->rbegin());
  }
  reverse_iterator rend() {
   if (!map)
     return reverse_iterator(this);
   return reverse_iterator(this, map->rend());
  }
  iterator find(const Key& k) {
    if (!map)
      return iterator(this);
    return iterator(this, map->find(k));
  }
  iterator lower_bound(const Key& k) {
    if (!map)
      return iterator(this);
    return iterator(this, map->lower_bound(k));
  }
  iterator upper_bound(const Key& k) {
    if (!map)
      return iterator(this);
    return iterator(this, map->upper_bound(k));
  }
  const_iterator begin() const {
   if (!map)
     return const_iterator(this);
   return const_iterator(this, map->begin());
  }
  const_iterator end() const {
   if (!map)
     return const_iterator(this);
   return const_iterator(this, map->end());
  }
  const_reverse_iterator rbegin() const {
   if (!map)
     return const_reverse_iterator(this);
   return const_reverse_iterator(this, map->rbegin());
  }
  const_reverse_iterator rend() const {
   if (!map)
     return const_reverse_iterator(this);
   return const_reverse_iterator(this, map->rend());
  }
  const_iterator find(const Key& k) const {
    if (!map)
      return const_iterator(this);
    return const_iterator(this, map->find(k));
  }
  const_iterator lower_bound(const Key& k) const {
    if (!map)
      return const_iterator(this);
    return const_iterator(this, map->lower_bound(k));
  }
  const_iterator upper_bound(const Key& k) const {
    if (!map)
      return const_iterator(this);
    return const_iterator(this, map->upper_bound(k));
  }
  void encode(ceph::buffer::list &bl) const {
    using ceph::encode;
    if (map)
      encode(*map, bl);
    else
      encode((uint32_t)0, bl);
  }
  void encode(ceph::buffer::list &bl, uint64_t features) const {
    using ceph::encode;
    if (map)
      encode(*map, bl, features);
    else
      encode((uint32_t)0, bl);
  }
  void decode(ceph::buffer::list::const_iterator& p) {
    using ceph::decode;
    using ceph::decode_nohead;
    uint32_t n;
    decode(n, p);
    if (n > 0) {
      alloc_internal();
      decode_nohead(n, *map, p);
    } else
      free_internal();
  }
};

template<class Key, class T, class Map>
inline void encode(const compact_map_base<Key, T, Map>& m, ceph::buffer::list& bl) {
  m.encode(bl);
}
template<class Key, class T, class Map>
inline void encode(const compact_map_base<Key, T, Map>& m, ceph::buffer::list& bl,
		   uint64_t features) {
  m.encode(bl, features);
}
template<class Key, class T, class Map>
inline void decode(compact_map_base<Key, T, Map>& m, ceph::buffer::list::const_iterator& p) {
  m.decode(p);
}

template <class Key, class T, class Compare = std::less<Key>, class Alloc = std::allocator< std::pair<const Key, T> > >
class compact_map : public compact_map_base<Key, T, std::map<Key,T,Compare,Alloc> > {
public:
  T& operator[](const Key& k) {
    this->alloc_internal();
    return (*(this->map))[k];
  }
};

template <class Key, class T, class Compare = std::less<Key>, class Alloc = std::allocator< std::pair<const Key, T> > >
inline std::ostream& operator<<(std::ostream& out, const compact_map<Key, T, Compare, Alloc>& m)
{
  out << "{";
  bool first = true;
  for (const auto &p : m) {
    if (!first)
      out << ",";
    out << p.first << "=" << p.second;
    first = false;
  }
  out << "}";
  return out;
}

template <class Key, class T, class Compare = std::less<Key>, class Alloc = std::allocator< std::pair<const Key, T> > >
class compact_multimap : public compact_map_base<Key, T, std::multimap<Key,T,Compare,Alloc> > {
};

template <class Key, class T, class Compare = std::less<Key>, class Alloc = std::allocator< std::pair<const Key, T> > >
inline std::ostream& operator<<(std::ostream& out, const compact_multimap<Key, T, Compare, Alloc>& m)
{
  out << "{{";
  bool first = true;
  for (const auto &p : m) {
    if (!first)
      out << ",";
    out << p.first << "=" << p.second;
    first = false;
  }
  out << "}}";
  return out;
}
#endif
