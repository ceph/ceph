// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef CEPH_CIRCULAR_BUFFER_HH_
#define CEPH_CIRCULAR_BUFFER_HH_

// A growable double-ended queue container that can be efficiently
// extended (and shrunk) from both ends.  Implementation is a single
// storage vector.
//
// Similar to libstdc++'s std::deque, except that it uses a single level
// store, and so is more efficient for simple stored items.
// Similar to boost::circular_buffer_space_optimized, except it uses
// uninitialized storage for unoccupied elements (and thus move/copy
// constructors instead of move/copy assignments, which are less efficient).

#include <memory>
#include <algorithm>

#include "transfer.h"

template <typename T, typename Alloc = std::allocator<T>>
class circular_buffer {
  struct impl : Alloc {
    T* storage = nullptr;
    // begin, end interpreted (mod capacity)
    size_t begin = 0;
    size_t end = 0;
    size_t capacity = 0;
  };
  impl _impl;
 public:
  using value_type = T;
  using size_type = size_t;
  using reference = T&;
  using pointer = T*;
  using const_reference = const T&;
  using const_pointer = const T*;
 public:
  circular_buffer() = default;
  circular_buffer(circular_buffer&& X);
  circular_buffer(const circular_buffer& X) = delete;
  ~circular_buffer();
  circular_buffer& operator=(const circular_buffer&) = delete;
  circular_buffer& operator=(circular_buffer&&) = delete;
  void push_front(const T& data);
  void push_front(T&& data);
  template <typename... A>
  void emplace_front(A&&... args);
  void push_back(const T& data);
  void push_back(T&& data);
  template <typename... A>
  void emplace_back(A&&... args);
  T& front();
  T& back();
  void pop_front();
  void pop_back();
  bool empty() const;
  size_t size() const;
  size_t capacity() const;
  T& operator[](size_t idx);
  template <typename Func>
  void for_each(Func func);
  // access an element, may return wrong or destroyed element
  // only useful if you do not rely on data accuracy (e.g. prefetch)
  T& access_element_unsafe(size_t idx);
 private:
  void expand();
  void maybe_expand(size_t nr = 1);
  size_t mask(size_t idx) const;

  template<typename CB, typename ValueType>
  struct cbiterator : std::iterator<std::random_access_iterator_tag, ValueType> {
    typedef std::iterator<std::random_access_iterator_tag, ValueType> super_t;

    ValueType& operator*() const { return cb->_impl.storage[cb->mask(idx)]; }
    ValueType* operator->() const { return &cb->_impl.storage[cb->mask(idx)]; }
    // prefix
    cbiterator<CB, ValueType>& operator++() {
      idx++;
      return *this;
    }
    // postfix
    cbiterator<CB, ValueType> operator++(int unused) {
      auto v = *this;
      idx++;
      return v;
    }
    // prefix
    cbiterator<CB, ValueType>& operator--() {
      idx--;
      return *this;
    }
    // postfix
    cbiterator<CB, ValueType> operator--(int unused) {
      auto v = *this;
      idx--;
      return v;
    }
    cbiterator<CB, ValueType> operator+(typename super_t::difference_type n) const {
      return cbiterator<CB, ValueType>(cb, idx + n);
    }
    cbiterator<CB, ValueType> operator-(typename super_t::difference_type n) const {
      return cbiterator<CB, ValueType>(cb, idx - n);
    }
    cbiterator<CB, ValueType>& operator+=(typename super_t::difference_type n) {
      idx += n;
      return *this;
    }
    cbiterator<CB, ValueType>& operator-=(typename super_t::difference_type n) {
      idx -= n;
      return *this;
    }
    bool operator==(const cbiterator<CB, ValueType>& rhs) const {
      return idx == rhs.idx;
    }
    bool operator!=(const cbiterator<CB, ValueType>& rhs) const {
      return idx != rhs.idx;
    }
    bool operator<(const cbiterator<CB, ValueType>& rhs) const {
      return idx < rhs.idx;
    }
    bool operator>(const cbiterator<CB, ValueType>& rhs) const {
      return idx > rhs.idx;
    }
    bool operator>=(const cbiterator<CB, ValueType>& rhs) const {
      return idx >= rhs.idx;
    }
    bool operator<=(const cbiterator<CB, ValueType>& rhs) const {
      return idx <= rhs.idx;
    }
    typename super_t::difference_type operator-(const cbiterator<CB, ValueType>& rhs) const {
      return idx - rhs.idx;
    }
   private:
    CB* cb;
    size_t idx;
    cbiterator<CB, ValueType>(CB* b, size_t i) : cb(b), idx(i) {}
    friend class circular_buffer;
  };
  friend class iterator;

 public:
  typedef cbiterator<circular_buffer, T> iterator;
  typedef cbiterator<const circular_buffer, const T> const_iterator;

  iterator begin() {
    return iterator(this, _impl.begin);
  }
  const_iterator begin() const {
    return const_iterator(this, _impl.begin);
  }
  iterator end() {
    return iterator(this, _impl.end);
  }
  const_iterator end() const {
    return const_iterator(this, _impl.end);
  }
  const_iterator cbegin() const {
    return const_iterator(this, _impl.begin);
  }
  const_iterator cend() const {
    return const_iterator(this, _impl.end);
  }
};

template <typename T, typename Alloc>
inline size_t circular_buffer<T, Alloc>::mask(size_t idx) const {
  return idx & (_impl.capacity - 1);
}

template <typename T, typename Alloc>
inline bool circular_buffer<T, Alloc>::empty() const {
  return _impl.begin == _impl.end;
}

template <typename T, typename Alloc>
inline size_t circular_buffer<T, Alloc>::size() const {
  return _impl.end - _impl.begin;
}

template <typename T, typename Alloc>
inline size_t circular_buffer<T, Alloc>::capacity() const {
  return _impl.capacity;
}

template <typename T, typename Alloc>
inline circular_buffer<T, Alloc>::circular_buffer(circular_buffer&& x)
    : _impl(std::move(x._impl)) {
  x._impl = {};
}

template <typename T, typename Alloc>
template <typename Func>
inline void circular_buffer<T, Alloc>::for_each(Func func) {
  auto s = _impl.storage;
  auto m = _impl.capacity - 1;
  for (auto i = _impl.begin; i != _impl.end; ++i) {
    func(s[i & m]);
  }
}

template <typename T, typename Alloc>
inline circular_buffer<T, Alloc>::~circular_buffer() {
  for_each([this] (T& obj) {
    _impl.destroy(&obj);
  });
  _impl.deallocate(_impl.storage, _impl.capacity);
}

template <typename T, typename Alloc>
void circular_buffer<T, Alloc>::expand() {
  auto new_cap = std::max<size_t>(_impl.capacity * 2, 1);
  auto new_storage = _impl.allocate(new_cap);
  auto p = new_storage;
  try {
    for_each([this, &p] (T& obj) {
      transfer_pass1(_impl, &obj, p);
      p++;
    });
  } catch (...) {
    while (p != new_storage) {
      _impl.destroy(--p);
    }
    _impl.deallocate(new_storage, new_cap);
    throw;
  }
  p = new_storage;
  for_each([this, &p] (T& obj) {
    transfer_pass2(_impl, &obj, p++);
  });
  std::swap(_impl.storage, new_storage);
  std::swap(_impl.capacity, new_cap);
  _impl.begin = 0;
  _impl.end = p - _impl.storage;
  _impl.deallocate(new_storage, new_cap);
}

template <typename T, typename Alloc>
inline void circular_buffer<T, Alloc>::maybe_expand(size_t nr) {
  if (_impl.end - _impl.begin + nr > _impl.capacity) {
    expand();
  }
}

template <typename T, typename Alloc>
inline void circular_buffer<T, Alloc>::push_front(const T& data) {
  maybe_expand();
  auto p = &_impl.storage[mask(_impl.begin - 1)];
  _impl.construct(p, data);
  --_impl.begin;
}

template <typename T, typename Alloc>
inline void circular_buffer<T, Alloc>::push_front(T&& data) {
  maybe_expand();
  auto p = &_impl.storage[mask(_impl.begin - 1)];
  _impl.construct(p, std::move(data));
  --_impl.begin;
}

template <typename T, typename Alloc>
template <typename... Args>
inline void circular_buffer<T, Alloc>::emplace_front(Args&&... args) {
  maybe_expand();
  auto p = &_impl.storage[mask(_impl.begin - 1)];
  _impl.construct(p, std::forward<Args>(args)...);
  --_impl.begin;
}

template <typename T, typename Alloc>
inline void circular_buffer<T, Alloc>::push_back(const T& data) {
  maybe_expand();
  auto p = &_impl.storage[mask(_impl.end)];
  _impl.construct(p, data);
  ++_impl.end;
}

template <typename T, typename Alloc>
inline void circular_buffer<T, Alloc>::push_back(T&& data) {
  maybe_expand();
  auto p = &_impl.storage[mask(_impl.end)];
  _impl.construct(p, std::move(data));
  ++_impl.end;
}

template <typename T, typename Alloc>
template <typename... Args>
inline void circular_buffer<T, Alloc>::emplace_back(Args&&... args) {
  maybe_expand();
  auto p = &_impl.storage[mask(_impl.end)];
  _impl.construct(p, std::forward<Args>(args)...);
  ++_impl.end;
}

template <typename T, typename Alloc>
inline T& circular_buffer<T, Alloc>::front() {
  return _impl.storage[mask(_impl.begin)];
}

template <typename T, typename Alloc>
inline T& circular_buffer<T, Alloc>::back() {
  return _impl.storage[mask(_impl.end - 1)];
}

template <typename T, typename Alloc>
inline void circular_buffer<T, Alloc>::pop_front() {
  _impl.destroy(&front());
  ++_impl.begin;
}

template <typename T, typename Alloc>
inline void circular_buffer<T, Alloc>::pop_back() {
  _impl.destroy(&back());
  --_impl.end;
}

template <typename T, typename Alloc>
inline T& circular_buffer<T, Alloc>::operator[](size_t idx) {
  return _impl.storage[mask(_impl.begin + idx)];
}

template <typename T, typename Alloc>
inline T& circular_buffer<T, Alloc>::access_element_unsafe(size_t idx) {
  return _impl.storage[mask(_impl.begin + idx)];
}

#endif /* CEPH_CIRCULAR_BUFFER_HH_ */
