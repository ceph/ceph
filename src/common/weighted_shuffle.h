// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
// https://stackoverflow.com/questions/50221136/c-weighted-stdshuffle/50223540
#ifndef CEPH_WEIGHTED_SHUFFLE_H
#define CEPH_WEIGHTED_SHUFFLE_H

#include <algorithm>
#include <random>
#include <vector>

template <class T, class U, class R>
void weighted_shuffle(std::vector<T> &data, std::vector<U> &weights, R &&gen)
{
  auto itd = data.begin();
  auto itw = weights.begin();

  while (itd != data.end() && itw != weights.end()) {
    std::discrete_distribution d(itw, weights.end());
    auto i = d(gen);
    if (i) {
      std::iter_swap(itd, std::next(itd, i));
      std::iter_swap(itw, std::next(itw, i));
    }
    ++itd;
    ++itw;
  }
}

#endif
