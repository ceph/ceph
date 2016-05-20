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
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 XSky <haomai@xsky.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MSG_STREAM_H_
#define CEPH_MSG_STREAM_H_

#include <exception>
#include <cassert>

// A stream<> is the producer side.  It may call produce() as long
// as the returned from the previous invocation is ready.
// To signify no more data is available, call close().
//
// A subscription<> is the consumer side.  It is created by a call
// to stream::listen().  Calling subscription::start(),
// which registers the data processing callback, starts processing
// events.  It may register for end-of-stream notifications by
// return the when_done() future, which also delivers error
// events (as exceptions).
//
// The consumer can pause generation of new data by returning
// positive integer; when it becomes ready, the producer
// will resume processing.

template <typename... T>
class stream;

template <typename... T>
class subscription;

template <typename... T>
class stream {
  subscription<T...>* _sub = nullptr;
  int done;
  bool ready;
 public:
  using next_fn = std::function<int (T...)>;
  stream() = default;
  stream(const stream&) = delete;
  stream(stream&&) = delete;
  ~stream() {
    if (_sub) {
      _sub->_stream = nullptr;
    }
  }

  void operator=(const stream&) = delete;
  void operator=(stream&&) = delete;

  // Returns a subscription that reads value from this
  // stream.
  subscription<T...> listen() {
    return subscription<T...>(this);
  }

  // Returns a subscription that reads value from this
  // stream, and also sets up the listen function.
  subscription<T...> listen(next_fn next) {
    auto sub = subscription<T...>(this);
    sub.start(std::move(next));
    return sub;
  }

  // Becomes ready when the listener is ready to accept
  // values.  Call only once, when beginning to produce
  // values.
  bool started() {
    return ready;
  }

  // Produce a value.  Call only after started(), and after
  // a previous produce() is ready.
  int produce(T... data) {
      return _sub->_next(std::move(data)...);
  }

  // End the stream.   Call only after started(), and after
  // a previous produce() is ready.  No functions may be called
  // after this.
  void close() {
    done = 1;
  }

  // Signal an error.   Call only after started(), and after
  // a previous produce() is ready.  No functions may be called
  // after this.
  void set_exception(int error) {
    done = error;
  }
 private:
  void start();
  friend class subscription<T...>;
};

template <typename... T>
class subscription {
 public:
  using next_fn = typename stream<T...>::next_fn;
 private:
  stream<T...>* _stream;
  next_fn _next;
 private:
  explicit subscription(stream<T...>* s): _stream(s) {
    assert(!_stream->_sub);
    _stream->_sub = this;
  }

 public:
  subscription(subscription&& x)
    : _stream(x._stream), _next(std::move(x._next)) {
    x._stream = nullptr;
    if (_stream) {
      _stream->_sub = this;
    }
  }
  ~subscription() {
    if (_stream) {
      _stream->_sub = nullptr;
    }
  }

  /// \brief Start receiving events from the stream.
  ///
  /// \param next Callback to call for each event
  void start(std::function<int (T...)> next) {
    _next = std::move(next);
    _stream->ready = true;
  }

  // Becomes ready when the stream is empty, or when an error
  // happens (in that case, an exception is held).
  int done() {
    return _stream->done;
  }

  friend class stream<T...>;
};

#endif /* CEPH_MSG_STREAM_H_ */
