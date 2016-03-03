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

#include <map>

template <class Key, class T, class Map>
class compact_map_base {
protected:
  Map *map;
  void alloc_internal() {
    if (!map)
      map = new Map;
  }
  void free_internal() {
    if (map) {
      delete map;
      map = 0;
    }
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
  compact_map_base() : map(0) {}
  compact_map_base(const compact_map_base& o) : map(0) {
    if (o.map) {
      alloc_internal();
      *map = *o.map;
    }
  }
  ~compact_map_base() { delete map; }

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
  void erase (iterator p) {
    if (map) {
      assert(this == p.map);
      map->erase(p.it);
      if (map->empty())
	free_internal();
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
    Map *tmp = map;
    map = o.map;
    o.map = tmp;
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
  void encode(bufferlist &bl) const {
    if (map)
      ::encode(*map, bl);
    else
      ::encode((uint32_t)0, bl);
  }
  void encode(bufferlist &bl, uint64_t features) const {
    if (map)
      ::encode(*map, bl, features);
    else
      ::encode((uint32_t)0, bl);
  }
  void decode(bufferlist::iterator& p) {
    uint32_t n;
    ::decode(n, p);
    if (n > 0) {
      alloc_internal();
      ::decode_nohead(n, *map, p);
    } else
      free_internal();
  }
};

template<class Key, class T, class Map>
inline void encode(const compact_map_base<Key, T, Map>& m, bufferlist& bl) {
  m.encode(bl);
}
template<class Key, class T, class Map>
inline void encode(const compact_map_base<Key, T, Map>& m, bufferlist& bl,
		   uint64_t features) {
  m.encode(bl, features);
}
template<class Key, class T, class Map>
inline void decode(compact_map_base<Key, T, Map>& m, bufferlist::iterator& p) {
  m.decode(p);
}

template <class Key, class T>
class compact_map : public compact_map_base<Key, T, std::map<Key,T> > {
public:
  T& operator[](const Key& k) {
    this->alloc_internal();
    return (*(this->map))[k];
  }
};

template <class Key, class T>
inline std::ostream& operator<<(std::ostream& out, const compact_map<Key, T>& m)
{
  out << "{";
  for (typename compact_map<Key, T>::const_iterator it = m.begin();
       it != m.end();
       ++it) {
    if (it != m.begin())
      out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}";
  return out;
}

template <class Key, class T>
class compact_multimap : public compact_map_base<Key, T, std::multimap<Key,T> > {
};

template <class Key, class T>
inline std::ostream& operator<<(std::ostream& out, const compact_multimap<Key, T>& m)
{
  out << "{{";
  for (typename compact_map<Key, T>::const_iterator it = m.begin(); !it.end(); ++it) {
    if (it != m.begin())
      out << ",";
    out << it->first << "=" << it->second;
  }
  out << "}}";
  return out;
}
#endif
