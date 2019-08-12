// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <list>
#include <mutex>

/*

Keep a history of item values so that readers can dereference the pointer to
the latest value and continue using it as long as they want.  This container
is only appropriate for values that are updated a handful of times over their
total lifetime.

There is a prune() method to throw out old values, but it should only be used
if the caller has some way of knowing all readers are done.

*/

template<class T>
class mutable_item_history {
private:
  std::mutex lock;
  std::list<T> history;
  T *current = nullptr;

public:
  mutable_item_history() {
    history.emplace_back(T());
    current = &history.back();
  }

  // readers are lock-free
  const T& operator*() const {
    return *current;
  }
  const T *operator->() const {
    return current;
  }

  // non-const variants (be careful!)
  T& operator*() {
    return *current;
  }
  T *operator->() {
    return current;
  }

  // writes are serialized
  const T& operator=(const T& other) {
    std::lock_guard l(lock);
    history.push_back(other);
    current = &history.back();
    return *current;
  }

  void prune() {
    // note: this is not necessarily thread-safe wrt readers
    std::lock_guard l(lock);
    while (history.size() > 1) {
      history.pop_front();
    }
  }
};

template<class T>
class safe_item_history {
private:
  std::mutex lock;
  std::list<T> history;
  T *current = nullptr;

public:
  safe_item_history() {
    history.emplace_back(T());
    current = &history.back();
  }

  // readers are lock-free
  const T& operator*() const {
    return *current;
  }
  const T *operator->() const {
    return current;
  }

  // writes are serialized
  const T& operator=(const T& other) {
    std::lock_guard l(lock);
    history.push_back(other);
    current = &history.back();
    return *current;
  }

  void prune() {
    // note: this is not necessarily thread-safe wrt readers
    std::lock_guard l(lock);
    while (history.size() > 1) {
      history.pop_front();
    }
  }
};
