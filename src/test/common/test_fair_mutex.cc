// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <array>
#include <mutex>
#include <numeric>
#include <future>
#include <gtest/gtest.h>
#include "common/fair_mutex.h"

TEST(FairMutex, simple)
{
  ceph::fair_mutex mutex{"fair::simple"};
  {
    std::unique_lock lock{mutex};
    ASSERT_TRUE(mutex.is_locked());
    // fair_mutex does not recursive ownership semantics
    ASSERT_FALSE(mutex.try_lock());
  }
  // re-acquire the lock
  {
    std::unique_lock lock{mutex};
    ASSERT_TRUE(mutex.is_locked());
  }
  ASSERT_FALSE(mutex.is_locked());
}

TEST(FairMutex, fair)
{
  // waiters are queued in FIFO order, and they are woken up in the same order
  // we have a marathon participated by multiple teams:
  // - each team is represented by a thread.
  // - each team should have equal chance of being selected and scoring, assuming
  //   the runners in each team are distributed evenly in the waiting queue.
  ceph::fair_mutex mutex{"fair::fair"};
  const int NR_TEAMS = 2;
  std::array<unsigned, NR_TEAMS> scoreboard{0, 0};
  const int NR_ROUNDS = 512;
  auto play = [&](int team) {
    for (int i = 0; i < NR_ROUNDS; i++) {
      std::unique_lock lock{mutex};
      // pretent that i am running.. and it takes time
      std::this_thread::sleep_for(std::chrono::microseconds(20));
      // score!
      scoreboard[team]++;
      // fair?
      unsigned total = std::accumulate(scoreboard.begin(),
                                       scoreboard.end(),
                                       0);
      for (unsigned score : scoreboard) {
        if (std::cmp_less(total, NR_ROUNDS)) {
          // not quite statistically significant. to reduce the false positive,
          // just consider it fair
          continue;
        }
        // check if any team is donimating the game.
        unsigned avg = total / scoreboard.size();
        // leave at least half of the average to other teams
        ASSERT_LE(score, total - avg / 2);
        // don't treat myself too bad
        ASSERT_GT(score, avg / 2);
      };
    }
  };
  std::array<std::future<void>, NR_TEAMS> completed;
  for (int team = 0; team < NR_TEAMS; team++) {
    completed[team] = std::async(std::launch::async, play, team);
  }
}
