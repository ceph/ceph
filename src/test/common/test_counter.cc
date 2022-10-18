#include "common/DecayCounter.h"

#include <gtest/gtest.h>

#include <list>
#include <cmath>

TEST(DecayCounter, steady)
{
  static const double duration = 2.0;
  static const double max = 2048.0;
  static const double rate = 3.5;

  DecayCounter d{DecayRate{rate}};
  d.hit(max);
  const auto start = DecayCounter::clock::now();
  double total = 0.0;
  while (1) {
    const auto now = DecayCounter::clock::now();
    auto el = std::chrono::duration<double>(now-start);
    if (el.count() > duration) {
      break;
    }

    double v = d.get();
    double diff = max-v;
    if (diff > 0.0) {
      d.hit(diff);
      total += diff;
    }
  }

  /* Decay function: dN/dt = -λM where λ = ln(0.5)/rate
   * (where M is the maximum value of the counter, not varying with time.)
   * Integrating over t: N = -λMt (+c)
   */
  double expected = -1*std::log(0.5)/rate*max*duration;
  std::cerr << "t " << total << " e " << expected << std::endl;
  ASSERT_LT(std::abs(total-expected)/expected, 0.05);
}
