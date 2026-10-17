// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#include <array>
#include <mutex>
#include <numeric>
#include <future>
#include <thread>
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
  std::array<bool, NR_TEAMS> started{false, false};
  const unsigned NR_ROUNDS = 512;
  unsigned nr_started = 0;
  unsigned nr_scored = 0;
  bool game_over = false;
  auto play = [&](int team) {
    while (true) {
      std::unique_lock lock{mutex};
      if (game_over) {
        return;
      }
      // pretent that i am running.. and it takes time
      std::this_thread::sleep_for(std::chrono::microseconds(20));
      if (!started[team]) {
        started[team] = true;
        ++nr_started;
      }
      // Rounds before every team has entered the game do not measure fairness.
      if (nr_started < NR_TEAMS) {
        continue;
      }
      // score!
      scoreboard[team]++;
      if (++nr_scored == NR_ROUNDS) {
        game_over = true;
      }
    }
  };
  std::array<std::future<void>, NR_TEAMS> completed;
  for (int team = 0; team < NR_TEAMS; team++) {
    completed[team] = std::async(std::launch::async, play, team);
  }
  for (auto& team : completed) {
    team.get();
  }

  unsigned total = std::accumulate(scoreboard.begin(), scoreboard.end(), 0u);
  ASSERT_EQ(NR_ROUNDS, total);
  unsigned avg = total / scoreboard.size();
  for (unsigned score : scoreboard) {
    // leave at least half of the average to other teams
    ASSERT_LE(score, total - avg / 2);
    // don't treat myself too bad
    ASSERT_GT(score, avg / 2);
  }
}
