// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <functional>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include "include/encoding.h"

/// Combination of an LRU with fast hash-based membership lookup
template<class T, int NUM_BUCKETS=128>
class LRUSet {
  /// internal node
  struct Node
    : boost::intrusive::unordered_set_base_hook<> {
    // actual payload
    T value;

    // for the lru
    boost::intrusive::list_member_hook<> lru_item;

    Node(const T& v) : value(v) {}

    friend std::size_t hash_value(const Node &node) {
      return std::hash<T>{}(node.value);
    }
    friend bool operator<(const Node &a, const Node &b) {
      return a.value < b.value;
    }
    friend bool operator>(const Node &a, const Node &b) {
      return a.value > b.value;
    }
    friend bool operator==(const Node &a, const Node &b) {
      return a.value == b.value;
    }
  };

  struct NodeDeleteDisposer {
    void operator()(Node *n) { delete n; }
  };

  // lru
  boost::intrusive::list<
    Node,
    boost::intrusive::constant_time_size<false>,
    boost::intrusive::member_hook<Node,
				  boost::intrusive::list_member_hook<>,
				  &Node::lru_item>
    > lru;

  // hash-based set
  typename boost::intrusive::unordered_set<Node>::bucket_type base_buckets[NUM_BUCKETS];
  boost::intrusive::unordered_set<Node> set;

 public:
  LRUSet()
    : set(typename boost::intrusive::unordered_set<Node>::bucket_traits(base_buckets,
									NUM_BUCKETS))
    {}
  ~LRUSet() {
    clear();
  }

  LRUSet(const LRUSet& other)
    : set(typename boost::intrusive::unordered_set<Node>::bucket_traits(base_buckets,
									NUM_BUCKETS)) {
    for (auto & i : other.lru) {
      insert(i.value);
    }
  }
  const LRUSet& operator=(const LRUSet& other) {
    clear();
    for (auto& i : other.lru) {
      insert(i.value);
    }
    return *this;
  }

  size_t size() const {
    return set.size();
  }

  bool empty() const {
    return set.empty();
  }

  bool contains(const T& item) const {
    return set.count(item) > 0;
  }

  void clear() {
    prune(0);
  }

  void insert(const T& item) {
    erase(item);
    Node *n = new Node(item);
    lru.push_back(*n);
    set.insert(*n);
  }

  bool erase(const T& item) {
    auto p = set.find(item);
    if (p == set.end()) {
      return false;
    }
    lru.erase(lru.iterator_to(*p));
    set.erase_and_dispose(p, NodeDeleteDisposer());
    return true;
  }

  void prune(size_t max) {
    while (set.size() > max) {
      auto p = lru.begin();
      set.erase(*p);
      lru.erase_and_dispose(p, NodeDeleteDisposer());
    }
  }

  void encode(bufferlist& bl) const {
    using ceph::encode;
    ENCODE_START(1, 1, bl);
    uint32_t n = set.size();
    encode(n, bl);
    auto p = set.begin();
    while (n--) {
      encode(p->value, bl);
      ++p;
    }
    ENCODE_FINISH(bl);
  }
  
  void decode(bufferlist::const_iterator& p) {
    using ceph::decode;
    DECODE_START(1, p);
    uint32_t n;
    decode(n, p);
    while (n--) {
      T v;
      decode(v, p);
      insert(v);
    }
    DECODE_FINISH(p);
  }
};
