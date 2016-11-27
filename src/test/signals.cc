#include "common/config.h"
#include "common/signal.h"
#include "global/signal_handler.h"
#include "common/debug.h"

#include "test/unit.h"

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#include "include/assert.h"

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

bool usr1 = false;
bool usr2 = false;

void reset()
{
  usr1 = false;
  usr2 = false;
}

void testhandler(int signal)
{
  switch (signal) {
  case SIGUSR1:
    usr1 = true;
    break;
  case SIGUSR2:
    usr2 = true;
    break;
  default:
    assert(0 == "unexpected signal");
  }
}

TEST(SignalHandler, Single)
{
  reset();
  init_async_signal_handler();
  register_async_signal_handler(SIGUSR1, testhandler);
  ASSERT_TRUE(usr1 == false);

  int ret = kill(getpid(), SIGUSR1);
  ASSERT_EQ(ret, 0);

  sleep(1);
  ASSERT_TRUE(usr1 == true);

  unregister_async_signal_handler(SIGUSR1, testhandler);
  shutdown_async_signal_handler();
}

TEST(SignalHandler, Multiple)
{
  int ret;

  reset();
  init_async_signal_handler();
  register_async_signal_handler(SIGUSR1, testhandler);
  register_async_signal_handler(SIGUSR2, testhandler);
  ASSERT_TRUE(usr1 == false);
  ASSERT_TRUE(usr2 == false);

  ret = kill(getpid(), SIGUSR1);
  ASSERT_EQ(ret, 0);
  ret = kill(getpid(), SIGUSR2);
  ASSERT_EQ(ret, 0);

  sleep(1);
  ASSERT_TRUE(usr1 == true);
  ASSERT_TRUE(usr2 == true);

  unregister_async_signal_handler(SIGUSR1, testhandler);
  unregister_async_signal_handler(SIGUSR2, testhandler);
  shutdown_async_signal_handler();
}

TEST(SignalHandler, LogInternal)
{
  g_ceph_context->_log->inject_segv();
  ASSERT_DEATH(derr << "foo" << dendl, ".*");
  g_ceph_context->_log->reset_segv();
}


/*
TEST(SignalHandler, MultipleBigFd)
{
  int ret;

  for (int i = 0; i < 1500; i++)
    ::open(".", O_RDONLY);

  reset();
  init_async_signal_handler();
  register_async_signal_handler(SIGUSR1, testhandler);
  register_async_signal_handler(SIGUSR2, testhandler);
  ASSERT_TRUE(usr1 == false);
  ASSERT_TRUE(usr2 == false);

  ret = kill(getpid(), SIGUSR1);
  ASSERT_EQ(ret, 0);
  ret = kill(getpid(), SIGUSR2);
  ASSERT_EQ(ret, 0);

  sleep(1);
  ASSERT_TRUE(usr1 == true);
  ASSERT_TRUE(usr2 == true);

  unregister_async_signal_handler(SIGUSR1, testhandler);
  unregister_async_signal_handler(SIGUSR2, testhandler);
  shutdown_async_signal_handler();
}
*/
