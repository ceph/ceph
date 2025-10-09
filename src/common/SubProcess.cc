#include "SubProcess.h"

#if defined(__FreeBSD__) || defined(__APPLE__)
#include <sys/types.h>
#include <signal.h>
#endif
#ifdef __linux__
#include <sys/syscall.h>
#endif
#include <stdarg.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>

#include "common/errno.h"
#include "include/ceph_assert.h"
#include "include/compat.h"

SubProcess::SubProcess(const char *cmd_, std_fd_op stdin_op_, std_fd_op stdout_op_, std_fd_op stderr_op_) :
  cmd(cmd_),
  cmd_args(),
  stdin_op(stdin_op_),
  stdout_op(stdout_op_),
  stderr_op(stderr_op_),
  stdin_pipe_out_fd(-1),
  stdout_pipe_in_fd(-1),
  stderr_pipe_in_fd(-1),
  pid(-1),
  errstr() {
}

SubProcess::~SubProcess() {
  ceph_assert(!is_spawned());
  ceph_assert(stdin_pipe_out_fd == -1);
  ceph_assert(stdout_pipe_in_fd == -1);
  ceph_assert(stderr_pipe_in_fd == -1);
}

void SubProcess::add_cmd_args(const char *arg, ...) {
  ceph_assert(!is_spawned());

  va_list ap;
  va_start(ap, arg);
  const char *p = arg;
  do {
    add_cmd_arg(p);
    p = va_arg(ap, const char*);
  } while (p != NULL);
  va_end(ap);
}

void SubProcess::add_cmd_arg(const char *arg) {
  ceph_assert(!is_spawned());

  cmd_args.push_back(arg);
}

int SubProcess::get_stdin() const {
  ceph_assert(is_spawned());
  ceph_assert(stdin_op == PIPE);

  return stdin_pipe_out_fd;
}

int SubProcess::get_stdout() const {
  ceph_assert(is_spawned());
  ceph_assert(stdout_op == PIPE);

  return stdout_pipe_in_fd;
}

int SubProcess::get_stderr() const {
  ceph_assert(is_spawned());
  ceph_assert(stderr_op == PIPE);

  return stderr_pipe_in_fd;
}

void SubProcess::close(int &fd) {
  if (fd == -1)
    return;

  ::close(fd);
  fd = -1;
}

void SubProcess::close_stdin() {
  ceph_assert(is_spawned());
  ceph_assert(stdin_op == PIPE);

  close(stdin_pipe_out_fd);
}

void SubProcess::close_stdout() {
  ceph_assert(is_spawned());
  ceph_assert(stdout_op == PIPE);

  close(stdout_pipe_in_fd);
}

void SubProcess::close_stderr() {
  ceph_assert(is_spawned());
  ceph_assert(stderr_op == PIPE);

  close(stderr_pipe_in_fd);
}

void SubProcess::kill(int signo) const {
  ceph_assert(is_spawned());

  int ret = ::kill(pid, signo);
  ceph_assert(ret == 0);
}

const std::string SubProcess::err() const {
  return errstr.str();
}

class fd_buf : public std::streambuf {
  int fd;
public:
  fd_buf (int fd) : fd(fd)
  {}
protected:
  int_type overflow (int_type c) override {
    if (c == EOF) return EOF;
    char buf = c;
    if (write (fd, &buf, 1) != 1) {
      return EOF;
    }
    return c;
  }
  std::streamsize xsputn (const char* s, std::streamsize count) override {
    return write(fd, s, count);
  }
};

int SubProcess::spawn() {
  ceph_assert(!is_spawned());
  ceph_assert(stdin_pipe_out_fd == -1);
  ceph_assert(stdout_pipe_in_fd == -1);
  ceph_assert(stderr_pipe_in_fd == -1);

  enum { IN = 0, OUT = 1 };

  int ipipe[2], opipe[2], epipe[2];

  ipipe[0] = ipipe[1] = opipe[0] = opipe[1] = epipe[0] = epipe[1] = -1;

  int ret = 0;

  if ((stdin_op == PIPE  && pipe_cloexec(ipipe, 0) == -1) ||
      (stdout_op == PIPE && pipe_cloexec(opipe, 0) == -1) ||
      (stderr_op == PIPE && pipe_cloexec(epipe, 0) == -1)) {
    ret = -errno;
    errstr << "pipe failed: " << cpp_strerror(errno);
    goto fail;
  }

  pid = fork();

  if (pid > 0) { // Parent
    stdin_pipe_out_fd = ipipe[OUT]; close(ipipe[IN ]);
    stdout_pipe_in_fd = opipe[IN ]; close(opipe[OUT]);
    stderr_pipe_in_fd = epipe[IN ]; close(epipe[OUT]);
    return 0;
  }

  if (pid == 0) { // Child
    close(ipipe[OUT]);
    close(opipe[IN ]);
    close(epipe[IN ]);

    if (ipipe[IN] >= 0) {
      if (ipipe[IN] == STDIN_FILENO) {
        ::fcntl(STDIN_FILENO, F_SETFD, 0); /* clear FD_CLOEXEC */
      } else {
        ::dup2(ipipe[IN], STDIN_FILENO);
        ::close(ipipe[IN]);
      }
    }
    if (opipe[OUT] >= 0) {
      if (opipe[OUT] == STDOUT_FILENO) {
        ::fcntl(STDOUT_FILENO, F_SETFD, 0); /* clear FD_CLOEXEC */
      } else {
        ::dup2(opipe[OUT], STDOUT_FILENO);
        ::close(opipe[OUT]);
        static fd_buf buf(STDOUT_FILENO);
        std::cout.rdbuf(&buf);
      }
    }
    if (epipe[OUT] >= 0) {
      if (epipe[OUT] == STDERR_FILENO) {
        ::fcntl(STDERR_FILENO, F_SETFD, 0); /* clear FD_CLOEXEC */
      } else {
        ::dup2(epipe[OUT], STDERR_FILENO);
        ::close(epipe[OUT]);
        static fd_buf buf(STDERR_FILENO);
        std::cerr.rdbuf(&buf);
      }
    }

    int maxfd = sysconf(_SC_OPEN_MAX);
    if (maxfd == -1)
      maxfd = 16384;

#if defined(__linux__) && defined(SYS_close_range)
    if (::syscall(SYS_close_range, STDERR_FILENO + 1, ~0U, 0) == 0)
      maxfd = STDERR_FILENO;
#endif

    for (int fd = 0; fd <= maxfd; fd++) {
      if (fd == STDIN_FILENO && stdin_op != CLOSE)
	continue;
      if (fd == STDOUT_FILENO && stdout_op != CLOSE)
	continue;
      if (fd == STDERR_FILENO && stderr_op != CLOSE)
	continue;
      ::close(fd);
    }

    exec();
    ceph_abort(); // Never reached
  }

  ret = -errno;
  errstr << "fork failed: " << cpp_strerror(errno);

fail:
  close(ipipe[0]);
  close(ipipe[1]);
  close(opipe[0]);
  close(opipe[1]);
  close(epipe[0]);
  close(epipe[1]);

  return ret;
}

void SubProcess::exec() {
  ceph_assert(is_child());

  std::vector<const char *> args;
  args.push_back(cmd.c_str());
  for (std::vector<std::string>::iterator i = cmd_args.begin();
       i != cmd_args.end();
       i++) {
    args.push_back(i->c_str());
  }
  args.push_back(NULL);

  int ret = execvp(cmd.c_str(), (char * const *)&args[0]);
  ceph_assert(ret == -1);

  std::cerr << cmd << ": exec failed: " << cpp_strerror(errno) << "\n";
  _exit(EXIT_FAILURE);
}

int SubProcess::join() {
  ceph_assert(is_spawned());

  close(stdin_pipe_out_fd);
  close(stdout_pipe_in_fd);
  close(stderr_pipe_in_fd);

  int status;

  while (waitpid(pid, &status, 0) == -1)
    ceph_assert(errno == EINTR);

  pid = -1;

  if (WIFEXITED(status)) {
    if (WEXITSTATUS(status) != EXIT_SUCCESS)
      errstr << cmd << ": exit status: " << WEXITSTATUS(status);
    return WEXITSTATUS(status);
  }
  if (WIFSIGNALED(status)) {
    errstr << cmd << ": got signal: " << WTERMSIG(status);
    return 128 + WTERMSIG(status);
  }
  errstr << cmd << ": waitpid: unknown status returned\n";
  return EXIT_FAILURE;
}

SubProcessTimed::SubProcessTimed(const char *cmd, std_fd_op stdin_op,
				 std_fd_op stdout_op, std_fd_op stderr_op,
				 int timeout_, int sigkill_) :
  SubProcess(cmd, stdin_op, stdout_op, stderr_op),
  timeout(timeout_),
  sigkill(sigkill_) {
}

static bool timedout = false; // only used after fork
void timeout_sighandler(int sig) {
  timedout = true;
}
static void dummy_sighandler(int sig) {}

void SubProcessTimed::exec() {
  ceph_assert(is_child());

  if (timeout <= 0) {
    SubProcess::exec();
    ceph_abort(); // Never reached
  }

  sigset_t mask, oldmask;
  int pid;

  // Restore default action for SIGTERM in case the parent process decided
  // to ignore it.
  if (signal(SIGTERM, SIG_DFL) == SIG_ERR) {
    std::cerr << cmd << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Because SIGCHLD is ignored by default, setup dummy handler for it,
  // so we can mask it.
  if (signal(SIGCHLD, dummy_sighandler) == SIG_ERR) {
    std::cerr << cmd << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Setup timeout handler.
  if (signal(SIGALRM, timeout_sighandler) == SIG_ERR) {
    std::cerr << cmd << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Block interesting signals.
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  sigaddset(&mask, SIGCHLD);
  sigaddset(&mask, SIGALRM);
  if (sigprocmask(SIG_SETMASK, &mask, &oldmask) == -1) {
    std::cerr << cmd << ": sigprocmask failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }

  pid = fork();

  if (pid == -1) {
    std::cerr << cmd << ": fork failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }

  if (pid == 0) { // Child
    // Restore old sigmask.
    if (sigprocmask(SIG_SETMASK, &oldmask, NULL) == -1) {
      std::cerr << cmd << ": sigprocmask failed: " << cpp_strerror(errno) << "\n";
      goto fail_exit;
    }
    (void)setpgid(0, 0); // Become process group leader.
    SubProcess::exec();
    ceph_abort(); // Never reached
  }

  // Parent
  (void)alarm(timeout);

  for (;;) {
    int signo;
    if (sigwait(&mask, &signo) == -1) {
      std::cerr << cmd << ": sigwait failed: " << cpp_strerror(errno) << "\n";
      goto fail_exit;
    }
    switch (signo) {
    case SIGCHLD:
      int status;
      if (waitpid(pid, &status, WNOHANG) == -1) {
	std::cerr << cmd << ": waitpid failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      if (WIFEXITED(status))
	_exit(WEXITSTATUS(status));
      if (WIFSIGNALED(status))
	_exit(128 + WTERMSIG(status));
      std::cerr << cmd << ": unknown status returned\n";
      goto fail_exit;
    case SIGINT:
    case SIGTERM:
      // Pass SIGINT and SIGTERM, which are usually used to terminate
      // a process, to the child.
      if (::kill(pid, signo) == -1) {
	std::cerr << cmd << ": kill failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      continue;
    case SIGALRM:
      std::cerr << cmd << ": timed out (" << timeout << " sec)\n";
      if (::killpg(pid, sigkill) == -1) {
	std::cerr << cmd << ": kill failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      continue;
    default:
      std::cerr << cmd << ": sigwait: invalid signal: " << signo << "\n";
      goto fail_exit;
    }
  }

fail_exit:
  _exit(EXIT_FAILURE);
}
