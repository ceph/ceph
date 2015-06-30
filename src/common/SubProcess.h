// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2015 Mirantis Inc
 *
 * Author: Mykola Golub <mgolub@mirantis.com>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#ifndef SUB_PROCESS_H
#define SUB_PROCESS_H

#include <sys/types.h>
#include <sys/wait.h>

#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sstream>
#include <vector>

#include <include/assert.h>
#include <common/errno.h>

/**
 * SubProcess:
 * A helper class to spawn a subprocess.
 *
 * Example:
 *
 *   SubProcess cat("cat", true, true);
 *   if (cat.spawn() != 0) {
 *     std::cerr << "cat failed: " << cat.err() << std::endl;
 *     return false;
 *   }
 *   write_to_fd(cat.stdout(), "hello world!\n");
 *   cat.close_stdout();
 *   read_from_fd(cat.stdin(), buf);
 *   if (cat.join() != 0) {
 *     std::cerr << cat.err() << std::endl;
 *     return false;
 *   }
 */

class SubProcess {
public:
  SubProcess(const char *cmd, bool pipe_stdin = false, bool pipe_stdout = false,
	     bool pipe_stderr = false);
  virtual ~SubProcess();

  void add_cmd_args(const char *arg, ...);
  void add_cmd_arg(const char *arg);

  virtual int spawn(); // Returns 0 on success or -errno on failure.
  virtual int join();  // Returns exit code (0 on success).

  bool is_spawned() const { return pid > 0; }

  int stdin() const;
  int stdout() const;
  int stderr() const;

  void close_stdin();
  void close_stdout();
  void close_stderr();

  void kill(int signo = SIGTERM) const;

  const char* err() const;

protected:
  bool is_child() const { return pid == 0; }
  virtual void exec();

private:
  void close(int &fd);

protected:
  std::string cmd;
  std::vector<std::string> cmd_args;
  bool pipe_stdin;
  bool pipe_stdout;
  bool pipe_stderr;
  int stdin_pipe_out_fd;
  int stdout_pipe_in_fd;
  int stderr_pipe_in_fd;
  int pid;
  std::ostringstream errstr;
};

class SubProcessTimed : public SubProcess {
public:
  SubProcessTimed(const char *cmd, bool pipe_stdin = false,
		  bool pipe_stdout = false, bool pipe_stderr = false,
		  int timeout = 0, int sigkill = SIGKILL);

protected:
  virtual void exec();

private:
  int timeout;
  int sigkill;
};

SubProcess::SubProcess(const char *cmd_, bool stdin, bool stdout, bool stderr) :
  cmd(cmd_),
  cmd_args(),
  pipe_stdin(stdin),
  pipe_stdout(stdout),
  pipe_stderr(stderr),
  stdin_pipe_out_fd(-1),
  stdout_pipe_in_fd(-1),
  stderr_pipe_in_fd(-1),
  pid(-1),
  errstr() {
}

SubProcess::~SubProcess() {
  assert(!is_spawned());
  assert(stdin_pipe_out_fd == -1);
  assert(stdout_pipe_in_fd == -1);
  assert(stderr_pipe_in_fd == -1);
}

void SubProcess::add_cmd_args(const char *arg, ...) {
  assert(!is_spawned());

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
  assert(!is_spawned());

  cmd_args.push_back(arg);
}

int SubProcess::stdin() const {
  assert(is_spawned());
  assert(pipe_stdin);

  return stdin_pipe_out_fd;
}

int SubProcess::stdout() const {
  assert(is_spawned());
  assert(pipe_stdout);

  return stdout_pipe_in_fd;
}

int SubProcess::stderr() const {
  assert(is_spawned());
  assert(pipe_stderr);

  return stderr_pipe_in_fd;
}

void SubProcess::close(int &fd) {
  if (fd == -1)
    return;

  ::close(fd);
  fd = -1;
}

void SubProcess::close_stdin() {
  assert(is_spawned());
  assert(pipe_stdin);

  close(stdin_pipe_out_fd);
}

void SubProcess::close_stdout() {
  assert(is_spawned());
  assert(pipe_stdout);

  close(stdout_pipe_in_fd);
}

void SubProcess::close_stderr() {
  assert(is_spawned());
  assert(pipe_stderr);

  close(stderr_pipe_in_fd);
}

void SubProcess::kill(int signo) const {
  assert(is_spawned());

  int ret = ::kill(pid, signo);
  assert(ret == 0);
}

const char* SubProcess::err() const {
  return errstr.str().c_str();
}

int SubProcess::spawn() {
  assert(!is_spawned());
  assert(stdin_pipe_out_fd == -1);
  assert(stdout_pipe_in_fd == -1);
  assert(stderr_pipe_in_fd == -1);

  enum { IN = 0, OUT = 1 };

  int ipipe[2], opipe[2], epipe[2];

  ipipe[0] = ipipe[1] = opipe[0] = opipe[1] = epipe[0] = epipe[1] = -1;

  int ret = 0;

  if ((pipe_stdin  && ::pipe(ipipe) == -1) ||
      (pipe_stdout && ::pipe(opipe) == -1) ||
      (pipe_stderr && ::pipe(epipe) == -1)) {
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

    if (ipipe[IN] != -1 && ipipe[IN] != STDIN_FILENO) {
      ::dup2(ipipe[IN], STDIN_FILENO);
      close(ipipe[IN]);
    }
    if (opipe[OUT] != -1 && opipe[OUT] != STDOUT_FILENO) {
      ::dup2(opipe[OUT], STDOUT_FILENO);
      close(opipe[OUT]);
    }
    if (epipe[OUT] != -1 && epipe[OUT] != STDERR_FILENO) {
      ::dup2(epipe[OUT], STDERR_FILENO);
      close(epipe[OUT]);
    }

    int maxfd = sysconf(_SC_OPEN_MAX);
    if (maxfd == -1)
      maxfd = 16384;
    for (int fd = 0; fd <= maxfd; fd++) {
      if (fd == STDIN_FILENO && pipe_stdin)
	continue;
      if (fd == STDOUT_FILENO && pipe_stdout)
	continue;
      if (fd == STDERR_FILENO && pipe_stderr)
	continue;
      ::close(fd);
    }

    exec();
    assert(0); // Never reached
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
  assert(is_child());

  std::vector<const char *> args;
  args.push_back(cmd.c_str());
  for (std::vector<std::string>::iterator i = cmd_args.begin();
       i != cmd_args.end();
       i++) {
    args.push_back(i->c_str());
  }
  args.push_back(NULL);

  int ret = execvp(cmd.c_str(), (char * const *)&args[0]);
  assert(ret == -1);

  std::ostringstream err;
  err << cmd << ": exec failed: " << cpp_strerror(errno) << "\n";
  write(STDERR_FILENO, err.str().c_str(), err.str().size());
  _exit(EXIT_FAILURE);
}

int SubProcess::join() {
  assert(is_spawned());

  close(stdin_pipe_out_fd);
  close(stdout_pipe_in_fd);
  close(stderr_pipe_in_fd);

  int status;

  while (waitpid(pid, &status, 0) == -1)
    assert(errno == EINTR);

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

SubProcessTimed::SubProcessTimed(const char *cmd, bool pipe_stdin,
				 bool pipe_stdout, bool pipe_stderr,
				 int timeout_, int sigkill_) :
  SubProcess(cmd, pipe_stdin, pipe_stdout, pipe_stderr),
  timeout(timeout_),
  sigkill(sigkill_) {
}

static bool timedout = false; // only used after fork
static void timeout_sighandler(int sig) {
  timedout = true;
}
static void dummy_sighandler(int sig) {}

void SubProcessTimed::exec() {
  assert(is_child());

  if (timeout <= 0) {
    SubProcess::exec();
    assert(0); // Never reached
  }

  sigset_t mask, oldmask;
  std::ostringstream err;
  int pid;

  // Restore default action for SIGTERM in case the parent process decided
  // to ignore it.
  if (signal(SIGTERM, SIG_DFL) == SIG_ERR) {
    err << cmd << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Because SIGCHLD is ignored by default, setup dummy handler for it,
  // so we can mask it.
  if (signal(SIGCHLD, dummy_sighandler) == SIG_ERR) {
    err << cmd << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Setup timeout handler.
  if (signal(SIGALRM, timeout_sighandler) == SIG_ERR) {
    err << cmd << ": signal failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }
  // Block interesting signals.
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  sigaddset(&mask, SIGCHLD);
  sigaddset(&mask, SIGALRM);
  if (sigprocmask(SIG_SETMASK, &mask, &oldmask) == -1) {
    err << cmd << ": sigprocmask failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }

  pid = fork();

  if (pid == -1) {
    err << cmd << ": fork failed: " << cpp_strerror(errno) << "\n";
    goto fail_exit;
  }

  if (pid == 0) { // Child
    // Restore old sigmask.
    if (sigprocmask(SIG_SETMASK, &oldmask, NULL) == -1) {
      err << cmd << ": sigprocmask failed: " << cpp_strerror(errno) << "\n";
      goto fail_exit;
    }
    (void)setpgid(0, 0); // Become process group leader.
    SubProcess::exec();
    assert(0); // Never reached
  }

  // Parent
  (void)alarm(timeout);

  for (;;) {
    int signo;
    if (sigwait(&mask, &signo) == -1) {
      err << cmd << ": sigwait failed: " << cpp_strerror(errno) << "\n";
      goto fail_exit;
    }
    switch (signo) {
    case SIGCHLD:
      int status;
      if (waitpid(pid, &status, WNOHANG) == -1) {
	err << cmd << ": waitpid failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      write(STDERR_FILENO, err.str().c_str(), err.str().size());
      if (WIFEXITED(status))
	_exit(WEXITSTATUS(status));
      if (WIFSIGNALED(status))
	_exit(128 + WTERMSIG(status));
      err << cmd << ": unknown status returned\n";
      goto fail_exit;
    case SIGINT:
    case SIGTERM:
      // Pass SIGINT and SIGTERM, which are usually used to terminate
      // a process, to the child.
      if (::kill(pid, signo) == -1) {
	err << cmd << ": kill failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      continue;
    case SIGALRM:
      err << cmd << ": timed out (" << timeout << " sec)\n";
      write(STDERR_FILENO, err.str().c_str(), err.str().size());
      if (::killpg(pid, sigkill) == -1) {
	err << cmd << ": kill failed: " << cpp_strerror(errno) << "\n";
	goto fail_exit;
      }
      continue;
    default:
      err << cmd << ": sigwait: invalid signal: " << signo << "\n";
      goto fail_exit;
    }
  }

fail_exit:
  write(STDERR_FILENO, err.str().c_str(), err.str().size());
  _exit(EXIT_FAILURE);
}

#endif
