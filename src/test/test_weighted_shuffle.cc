// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/weighted_shuffle.h"
#include <array>
#include <map>
#include "gtest/gtest.h"

TEST(WeightedShuffle, Basic) {
  std::array<char, 5> choices{'a', 'b', 'c', 'd', 'e'};
  std::array<int, 5> weights{100, 50, 25, 10, 1};
  std::map<char, std::array<unsigned, 5>> frequency {
    {'a', {0, 0, 0, 0, 0}},
    {'b', {0, 0, 0, 0, 0}},
    {'c', {0, 0, 0, 0, 0}},
    {'d', {0, 0, 0, 0, 0}},
    {'e', {0, 0, 0, 0, 0}}
  }; // count each element appearing in each position
  const int samples = 10000;
  std::random_device rd;
  for (auto i = 0; i < samples; i++) {
    weighted_shuffle(begin(choices), end(choices),
		     begin(weights), end(weights),
		     std::mt19937{rd()});
    for (size_t j = 0; j < choices.size(); ++j)
      ++frequency[choices[j]][j];
  }
  // verify that the probability that the nth choice is selected as the first
  // one is the nth weight divided by the sum of all weights
  const auto total_weight = std::accumulate(weights.begin(), weights.end(), 0);
  constexpr float epsilon = 0.02;
  for (unsigned i = 0; i < choices.size(); i++) {
    const auto& f = frequency[choices[i]];
    const auto& w = weights[i];
    ASSERT_NEAR(float(w) / total_weight,
		float(f.front()) / samples,
		epsilon);
  }
}

TEST(WeightedShuffle, ZeroedWeights) {
  std::array<char, 5> choices{'a', 'b', 'c', 'd', 'e'};
  std::array<int, 5> weights{0, 0, 0, 0, 0};
  std::map<char, std::array<unsigned, 5>> frequency {
    {'a', {0, 0, 0, 0, 0}},
    {'b', {0, 0, 0, 0, 0}},
    {'c', {0, 0, 0, 0, 0}},
    {'d', {0, 0, 0, 0, 0}},
    {'e', {0, 0, 0, 0, 0}}
  }; // count each element appearing in each position
  const int samples = 10000;
  std::random_device rd;
  for (auto i = 0; i < samples; i++) {
    weighted_shuffle(begin(choices), end(choices),
		     begin(weights), end(weights),
		     std::mt19937{rd()});
    for (size_t j = 0; j < choices.size(); ++j)
      ++frequency[choices[j]][j];
  }

  for (char ch : choices) {
    // all samples on the diagonal
    ASSERT_EQ(std::accumulate(begin(frequency[ch]), end(frequency[ch]), 0),
	      samples);
    ASSERT_EQ(frequency[ch][ch-'a'], samples);
  }
}

TEST(WeightedShuffle, SingleNonZeroWeight) {
  std::array<char, 5> choices{'a', 'b', 'c', 'd', 'e'};
  std::array<int, 5> weights{0, 42, 0, 0, 0};
  std::map<char, std::array<unsigned, 5>> frequency {
    {'a', {0, 0, 0, 0, 0}},
    {'b', {0, 0, 0, 0, 0}},
    {'c', {0, 0, 0, 0, 0}},
    {'d', {0, 0, 0, 0, 0}},
    {'e', {0, 0, 0, 0, 0}}
  }; // count each element appearing in each position
  const int samples = 10000;
  std::random_device rd;
  for (auto i = 0; i < samples; i++) {
    weighted_shuffle(begin(choices), end(choices),
		     begin(weights), end(weights),
		     std::mt19937{rd()});
    for (size_t j = 0; j < choices.size(); ++j)
      ++frequency[choices[j]][j];
  }

  // 'b' is always first
  ASSERT_EQ(frequency['b'][0], samples);
}
