// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <algorithm>
#include <iterator>
#include <random>

template <class RandomIt, class DistIt, class URBG>
void weighted_shuffle(RandomIt first, RandomIt last,
		      DistIt weight_first, DistIt weight_last,
		      URBG &&g)
{
  if (first == last) {
    return;
  } else {
    std::discrete_distribution d{weight_first, weight_last};
    if (auto n = d(g); n > 0) {
      std::iter_swap(first, std::next(first, n));
      std::iter_swap(weight_first, std::next(weight_first, n));
    }
    weighted_shuffle(++first, last, ++weight_first, weight_last, std::move(g));
  }
}
