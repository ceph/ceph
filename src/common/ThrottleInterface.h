// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>

class ThrottleInterface {
public:
  virtual ~ThrottleInterface() {}
  /**
   * take the specified number of slots from the stock regardless the throttling
   * @param c number of slots to take
   * @returns the total number of taken slots
   */
  virtual int64_t take(int64_t c = 1) = 0;
  /**
   * put slots back to the stock
   * @param c number of slots to return
   * @returns number of requests being hold after this
   */
  virtual int64_t put(int64_t c = 1) = 0;
};
