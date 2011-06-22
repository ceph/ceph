#include "common/config.h"
#include "common/signal.h"
#include "global/signal_handler.h"

#include "test/unit.h"

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
  install_sighandler(SIGPIPE, handle_sigusr1, 0);
}

TEST(SignalApi, SimpleInstallAndTest)
{
  install_sighandler(SIGPIPE, handle_sigusr1, 0);

  // SIGPIPE starts out blocked
  int ret = kill(getpid(), SIGPIPE);
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(got_sigusr1, 0);

  // handle SIGPIPE
  sigset_t mask;
  sigemptyset(&mask);
  ret = sigsuspend(&mask);
  if (ret == -1)
    ret = errno;

  // we should have gotten it
  ASSERT_EQ(ret, EINTR);
  ASSERT_EQ(got_sigusr1, 1);
}

TEST(SignalEffects, ErrnoTest1)
{
}
