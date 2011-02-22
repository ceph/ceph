#include "common/config.h"
#include "common/signal.h"

#include "gtest/gtest.h"

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

static volatile sig_atomic_t got_sigusr1 = 0;

static void handle_sigusr1(int signo)
{
  got_sigusr1 = 1;
}

TEST(SignalApi, SimpleInstall)
{
  install_sighandler(SIGUSR1, handle_sigusr1, 0);
}

TEST(SignalApi, SimpleInstallAndTest)
{
  install_sighandler(SIGUSR1, handle_sigusr1, 0);

  // blocked signal should not be delievered until we call sigsuspend()
  sigset_t old_sigset;
  block_all_signals(&old_sigset);

  int ret = kill(getpid(), SIGUSR1);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(got_sigusr1, 0);

  sigset_t mask;
  sigemptyset(&mask);
  sigdelset(&mask, SIGUSR1);
  ret = sigsuspend(&mask);
  if (ret == -1)
    ret = errno;
  ASSERT_EQ(ret, EINTR);
  ASSERT_EQ(got_sigusr1, 1);
  unblock_all_signals(&old_sigset);
}

TEST(SignalEffects, ErrnoTest1)
{
}
