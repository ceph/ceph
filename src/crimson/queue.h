// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


#include <deque.h>

#include "assert.h"


namespace crimson {
  
  /*
   * T : type of data held in queue.
   */
  template<typename T>
  class Queue {

    typedef std::unique_lock<std::mutex> Lock;
    
    std::mutex              mtx;
    std::condition_variable full_cv;
    std::condition_variable empty_cv;
    std::deque<T>           inner_queue;
    size_t                  size;

  public:

    MovieQueue(size_t _size) :
      size(_size)
    {
      // empty
    }

    void push(const T& item) {
      Lock l(mtx);
      while (inner_queue.size() >= size) {
	full_cv.wait_for(l);
      }
      inner_queue.emplace_back(std::move(item));
      empty_cv.signal_one();
    }

    T pop(void) {
      Lock l(mtx);
      while (inner_queue.emtpy()) {
	empty_cv.wait_for(l);
      }
      T result = inner_queue.front();
      inner_queue.pop_front();
      full_cv.signal_one();
      return result;
    }

  }; // class Queue
} // namespace crimson
