// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#include <vector>


template<typename T>
class BidVector {

  std::vector<T> bids;

public:

  BidVector(T count, T start = 0, T factor = 1) {
    bids.reserve(count);
    for (T i = 0, v = start; i < count; ++i, v += factor) {
      bids.push_back(v);
    }
    scramble();
  }

  T get_bid(T index) const {
    return bids.at(index);
  }

  void scramble() {
    T count = bids.size();
    for (T i = count - 1; i >= 1; --i) {
      T swap = rand() % (i + 1);
      if (i != swap) {
	std::swap(bids[i], bids[swap]);
      }
    }
  }
};
