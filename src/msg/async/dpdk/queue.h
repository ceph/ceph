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

#ifndef CEPH_MSG_DPDK_QUEUE_H_
#define CEPH_MSG_DPDK_QUEUE_H_

#include <queue>

#include "circular_buffer.h"

template <typename T>
class queue {
  std::queue<T, circular_buffer<T>> _q;
  size_t _max;

 public:
  explicit queue(size_t size): _max(size) {}

  // Push an item.
  //
  // Returns false if the queue was full and the item was not pushed.
  bool push(T&& a);

  // pops an item.
  T pop();

  // Consumes items from the queue, passing them to @func, until @func
  // returns false or the queue it empty
  //
  // Returns false if func returned false.
  template <typename Func>
  bool consume(Func&& func);

  // Returns true when the queue is empty.
  bool empty() const;

  // Returns true when the queue is full.
  bool full() const;

  size_t size() const { return _q.size(); }

  // Destroy any items in the queue
  void clear() {
    while (!_q.empty()) {
      _q.pop();
    }
  }
};

template <typename T>
inline bool queue<T>::push(T&& data) {
  if (_q.size() < _max) {
    _q.push(std::move(data));
    notify_not_empty();
    return true;
  } else {
    return false;
  }
}

template <typename T>
inline T queue<T>::pop() {
  T data = std::move(_q.front());
  _q.pop();
  return data;
}

template <typename T>
inline bool queue<T>::empty() const {
  return _q.empty();
}

template <typename T>
inline bool queue<T>::full() const {
  return _q.size() == _max;
}

#endif /* CEPH_MSG_DPDK_QUEUE_H_ */
