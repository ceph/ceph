// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
// https://stackoverflow.com/questions/50221136/c-weighted-stdshuffle/50223540

#include "common/weighted_shuffle.h"
#include <map>
#include "gtest/gtest.h"

TEST(WeightedShuffle, Basic) {
  std::random_device rd;
  std::mt19937 gen(rd());

  std::vector<char> data{'a', 'b', 'c', 'd', 'e'};
  // NB: differences between weights should be significant
  // otherwise test might fail
  std::vector<int> weights{100, 50, 25, 10, 1};
  std::map<char, std::vector<int>> frequency {
    {'a', {0, 0, 0, 0, 0}},
    {'b', {0, 0, 0, 0, 0}},
    {'c', {0, 0, 0, 0, 0}},
    {'d', {0, 0, 0, 0, 0}},
    {'e', {0, 0, 0, 0, 0}}
  }; // count each element appearing in each position
  const int samples = 10000;
  for (auto i = 0; i < samples; i++) {
    weighted_shuffle(data, weights, gen);
    for (size_t j = 0; j < data.size(); ++j)
      ++frequency[data[j]][j];
  }

  // verify each element gets a chance to stay at the header of array
  // e.g., because we don't want to starve anybody!
  EXPECT_TRUE(std::all_of(frequency.begin(), frequency.end(), [](auto& it) {
      return it.second.front() > 0;
    }));

  // verify the probability (of staying at the array header)
  // are produced according to their corresponding weight
  EXPECT_TRUE(std::is_sorted(frequency.begin(), frequency.end(),
    [](auto& lhs, auto& rhs) {
      return lhs.second.front() > rhs.second.front();
    }));
}
