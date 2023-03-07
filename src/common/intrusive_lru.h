// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>

namespace ceph::common {

/**
 * intrusive_lru: lru implementation with embedded map and list hook
 *
 * Elements will be stored in an intrusive set. Once an element is no longer
 * referenced it will remain in the set. The unreferenced elements will be
 * evicted from the set once the set size exceeds the `lru_target_size`.
 * Referenced elements will not be evicted as this is a registery with
 * extra caching capabilities.
 *
 * Note, this implementation currently is entirely thread-unsafe.
 */

template <typename K, typename V, typename VToK>
struct intrusive_lru_config {
  using key_type = K;
  using value_type = V;
  using key_of_value = VToK;
};

template <typename Config>
class intrusive_lru;

template <typename Config>
class intrusive_lru_base;

template <typename Config>
void intrusive_ptr_add_ref(intrusive_lru_base<Config> *p);

template <typename Config>
void intrusive_ptr_release(intrusive_lru_base<Config> *p);


template <typename Config>
class intrusive_lru_base {
  unsigned use_count = 0;

  // lru points to the corresponding intrusive_lru
  // which will be set to null if its use_count
  // is zero (aka unreferenced).
  intrusive_lru<Config> *lru = nullptr;

public:
  bool is_referenced() const {
    return static_cast<bool>(lru);
  }
  bool is_unreferenced() const {
    return !is_referenced();
  }
  boost::intrusive::set_member_hook<> set_hook;
  boost::intrusive::list_member_hook<> list_hook;

  using Ref = boost::intrusive_ptr<typename Config::value_type>;
  using lru_t = intrusive_lru<Config>;

  friend intrusive_lru<Config>;
  friend void intrusive_ptr_add_ref<>(intrusive_lru_base<Config> *);
  friend void intrusive_ptr_release<>(intrusive_lru_base<Config> *);

  virtual ~intrusive_lru_base() {}
};

template <typename Config>
class intrusive_lru {
  using base_t = intrusive_lru_base<Config>;
  using K = typename Config::key_type;
  using T = typename Config::value_type;
  using TRef = typename base_t::Ref;

  using lru_set_option_t = boost::intrusive::member_hook<
    base_t,
    boost::intrusive::set_member_hook<>,
    &base_t::set_hook>;

  using VToK = typename Config::key_of_value;
  struct VToKWrapped {
    using type = typename VToK::type;
    const type &operator()(const base_t &obc) {
      return VToK()(static_cast<const T&>(obc));
    }
  };
  using lru_set_t = boost::intrusive::set<
    base_t,
    lru_set_option_t,
    boost::intrusive::key_of_value<VToKWrapped>
    >;
  lru_set_t lru_set;

  using lru_list_t = boost::intrusive::list<
    base_t,
    boost::intrusive::member_hook<
      base_t,
      boost::intrusive::list_member_hook<>,
      &base_t::list_hook>>;
  lru_list_t unreferenced_list;

  size_t lru_target_size = 0;

  // when the lru_set exceeds its target size, evict
  // only unreferenced elements from it (if any).
  void evict() {
    while (!unreferenced_list.empty() &&
	   lru_set.size() > lru_target_size) {
      auto &evict_target = unreferenced_list.front();
      assert(evict_target.is_unreferenced());
      unreferenced_list.pop_front();
      lru_set.erase_and_dispose(
	lru_set.iterator_to(evict_target),
	[](auto *p) { delete p; }
      );
    }
  }

  // access an existing element in the lru_set.
  // mark as referenced if necessary.
  void access(base_t &b) {
    if (b.is_referenced())
      return;
    unreferenced_list.erase(lru_list_t::s_iterator_to(b));
    b.lru = this;
  }

  // insert a new element to the lru_set.
  // attempt to evict if possible.
  void insert(base_t &b) {
    assert(b.is_unreferenced());
    lru_set.insert(b);
    b.lru = this;
    evict();
  }

  // an element in the lru_set has no users,
  // mark it as unreferenced and try to evict.
  void mark_as_unreferenced(base_t &b) {
    assert(b.is_referenced());
    unreferenced_list.push_back(b);
    b.lru = nullptr;
    evict();
  }

public:
  /**
   * Returns the TRef corresponding to k if it exists or
   * creates it otherwise.  Return is:
   * std::pair(reference_to_val, found)
   */
  std::pair<TRef, bool> get_or_create(const K &k) {
    typename lru_set_t::insert_commit_data icd;
    auto [iter, missing] = lru_set.insert_check(
      k,
      icd);
    if (missing) {
      auto ret = new T(k);
      lru_set.insert_commit(*ret, icd);
      insert(*ret);
      return {TRef(ret), false};
    } else {
      access(*iter);
      return {TRef(static_cast<T*>(&*iter)), true};
    }
  }

  /**
   * Returns the TRef corresponding to k if it exists or
   * nullptr otherwise.
   */
  TRef get(const K &k) {
    if (auto iter = lru_set.find(k); iter != std::end(lru_set)) {
      access(*iter);
      return TRef(static_cast<T*>(&*iter));
    } else {
      return nullptr;
    }
  }

  void set_target_size(size_t target_size) {
    lru_target_size = target_size;
    evict();
  }

  ~intrusive_lru() {
    set_target_size(0);
  }

  friend void intrusive_ptr_add_ref<>(intrusive_lru_base<Config> *);
  friend void intrusive_ptr_release<>(intrusive_lru_base<Config> *);
};

template <typename Config>
void intrusive_ptr_add_ref(intrusive_lru_base<Config> *p) {
  assert(p);
  assert(p->lru);
  p->use_count++;
}

template <typename Config>
void intrusive_ptr_release(intrusive_lru_base<Config> *p) {
  assert(p);
  assert(p->use_count > 0);
  --p->use_count;
  if (p->use_count == 0) {
    p->lru->mark_as_unreferenced(*p);
  }
}


}
