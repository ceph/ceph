// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <sys/utsname.h>

#include "include/compat.h"
#include "pthread.h"

#include "common/ceph_mutex.h"
#include "common/BackTrace.h"
#include "common/debug.h"
#include "common/safe_io.h"
#include "common/version.h"

#include "include/uuid.h"
#include "global/pidfile.h"
#include "global/signal_handler.h"

#include <poll.h>
#include <signal.h>
#include <sstream>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "common/errno.h"
#if defined(_AIX)
extern char *sys_siglist[]; 
#endif 

#define dout_context g_ceph_context

using std::ostringstream;
using std::string;

using ceph::BackTrace;
using ceph::JSONFormatter;

void install_sighandler(int signum, signal_handler_t handler, int flags)
{
  int ret;
  struct sigaction oldact;
  struct sigaction act;
  memset(&act, 0, sizeof(act));

  act.sa_handler = handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = flags;

  ret = sigaction(signum, &act, &oldact);
  if (ret != 0) {
    char buf[1024];
#if defined(__sun)
    char message[SIG2STR_MAX];
    sig2str(signum,message);
    snprintf(buf, sizeof(buf), "install_sighandler: sigaction returned "
	    "%d when trying to install a signal handler for %s\n",
	     ret, message);
#else
    snprintf(buf, sizeof(buf), "install_sighandler: sigaction returned "
	    "%d when trying to install a signal handler for %s\n",
	     ret, sig_str(signum));
#endif
    dout_emergency(buf);
    exit(1);
  }
}

void sighup_handler(int signum)
{
  g_ceph_context->reopen_logs();
}

static void reraise_fatal(int signum)
{
  // Use default handler to dump core
  signal(signum, SIG_DFL);
  int ret = raise(signum);

  // Normally, we won't get here. If we do, something is very weird.
  char buf[1024];
  if (ret) {
    snprintf(buf, sizeof(buf), "reraise_fatal: failed to re-raise "
	    "signal %d\n", signum);
    dout_emergency(buf);
  }
  else {
    snprintf(buf, sizeof(buf), "reraise_fatal: default handler for "
	    "signal %d didn't terminate the process?\n", signum);
    dout_emergency(buf);
  }
  exit(1);
}


// /etc/os-release looks like
//
//   NAME=Fedora
//   VERSION="28 (Server Edition)"
//   ID=fedora
//   VERSION_ID=28
//
// or
//
//   NAME="Ubuntu"
//   VERSION="16.04.3 LTS (Xenial Xerus)"
//   ID=ubuntu
//   ID_LIKE=debian
//
// get_from_os_release("FOO=bar\nTHIS=\"that\"\n", "FOO=", ...) will
// write "bar\0" to out buffer, which is assumed to be as large as the input
// file.
static int parse_from_os_release(
  const char *file, const char *key,
  char *out)
{
  const char *p = strstr(file, key);
  if (!p) {
    return -1;
  }
  const char *start = p + strlen(key);
  const char *end = strchr(start, '\n');
  if (!end) {
    return -1;
  }
  if (*start == '"' && *(end - 1) == '"') {
    ++start;
    --end;
  }
  if (start >= end) {
    return -1;
  }
  memcpy(out, start, end - start);
  out[end - start] = 0;
  return 0;
}


void generate_crash_dump(char *base,
			 const BackTrace& bt,
			 std::map<std::string,std::string> *extra)
{
  if (g_ceph_context &&
      g_ceph_context->_conf->crash_dir.size()) {

    // -- crash dump --
    // id
    ostringstream idss;
    utime_t now = ceph_clock_now();
    now.gmtime(idss);
    uuid_d uuid;
    uuid.generate_random();
    idss << "_" << uuid;
    string id = idss.str();
    std::replace(id.begin(), id.end(), ' ', '_');

    snprintf(base, PATH_MAX, "%s/%s",
	     g_ceph_context->_conf->crash_dir.c_str(),
	     id.c_str());
    int r = ::mkdir(base, 0700);
    if (r >= 0) {
      char fn[PATH_MAX*2];
      snprintf(fn, sizeof(fn)-1, "%s/meta", base);
      int fd = ::open(fn, O_CREAT|O_WRONLY|O_CLOEXEC, 0600);
      if (fd >= 0) {
	JSONFormatter jf(true);
	jf.open_object_section("crash");
	jf.dump_string("crash_id", id);
	now.gmtime(jf.dump_stream("timestamp"));
	jf.dump_string("process_name", g_process_name);
	jf.dump_string("entity_name", g_ceph_context->_conf->name.to_str());
	jf.dump_string("ceph_version", ceph_version_to_str());

	struct utsname u;
	r = uname(&u);
	if (r >= 0) {
	  jf.dump_string("utsname_hostname", u.nodename);
	  jf.dump_string("utsname_sysname", u.sysname);
	  jf.dump_string("utsname_release", u.release);
	  jf.dump_string("utsname_version", u.version);
	  jf.dump_string("utsname_machine", u.machine);
	}
#if defined(__linux__)
	// os-release
	int in = ::open("/etc/os-release", O_RDONLY|O_CLOEXEC);
	if (in >= 0) {
	  char buf[4096];
	  r = safe_read(in, buf, sizeof(buf)-1);
	  if (r >= 0) {
	    buf[r] = 0;
	    char v[4096];
	    if (parse_from_os_release(buf, "NAME=", v) >= 0) {
	      jf.dump_string("os_name", v);
	    }
	    if (parse_from_os_release(buf, "ID=", v) >= 0) {
	      jf.dump_string("os_id", v);
	    }
	    if (parse_from_os_release(buf, "VERSION_ID=", v) >= 0) {
	      jf.dump_string("os_version_id", v);
	    }
	    if (parse_from_os_release(buf, "VERSION=", v) >= 0) {
	      jf.dump_string("os_version", v);
	    }
	  }
	  ::close(in);
	}
#endif

	// assert?
	if (g_assert_condition) {
	  jf.dump_string("assert_condition", g_assert_condition);
	}
	if (g_assert_func) {
	  jf.dump_string("assert_func", g_assert_func);
	}
	if (g_assert_file) {
	  jf.dump_string("assert_file", g_assert_file);
	}
	if (g_assert_line) {
	  jf.dump_unsigned("assert_line", g_assert_line);
	}
	if (g_assert_thread_name[0]) {
	  jf.dump_string("assert_thread_name", g_assert_thread_name);
	}
	if (g_assert_msg[0]) {
	  jf.dump_string("assert_msg", g_assert_msg);
	}

	// eio?
	if (g_eio) {
	  jf.dump_bool("io_error", true);
	  if (g_eio_devname[0]) {
	    jf.dump_string("io_error_devname", g_eio_devname);
	  }
	  if (g_eio_path[0]) {
	    jf.dump_string("io_error_path", g_eio_path);
	  }
	  if (g_eio_error) {
	    jf.dump_int("io_error_code", g_eio_error);
	  }
	  if (g_eio_iotype) {
	    jf.dump_int("io_error_optype", g_eio_iotype);
	  }
	  if (g_eio_offset) {
	    jf.dump_unsigned("io_error_offset", g_eio_offset);
	  }
	  if (g_eio_length) {
	    jf.dump_unsigned("io_error_length", g_eio_length);
	  }
	}

	bt.dump(&jf);

	if (extra) {
	  for (auto& i : *extra) {
	    jf.dump_string(i.first, i.second);
	  }
	}

	jf.close_section();
	ostringstream oss;
	jf.flush(oss);
	string s = oss.str();
	r = safe_write(fd, s.c_str(), s.size());
	(void)r;
	::close(fd);
      }
      snprintf(fn, sizeof(fn)-1, "%s/done", base);
      #ifdef _WIN32
      ::creat(fn, _S_IREAD);
      #else
      ::creat(fn, 0444);
      #endif
    }
  }
}

static void handle_oneshot_fatal_signal(int signum)
{
  constexpr static pid_t NULL_TID{0};
  static std::atomic<pid_t> handler_tid{NULL_TID};
  if (auto expected{NULL_TID};
      !handler_tid.compare_exchange_strong(expected, ceph_gettid())) {
    if (expected == ceph_gettid()) {
      // The handler code may itself trigger a SIGSEGV if the heap is corrupt.
      // In that case, SIG_DFL followed by return specifies that the default
      // signal handler -- presumably dump core -- will handle it.
      signal(signum, SIG_DFL);
    } else {
      // Huh, another thread got into troubles while we are handling the fault.
      // If this is i.e. SIGSEGV handler, returning means retrying the faulty
      // instruction one more time, and thus all those extra threads will run
      // into a busy-wait basically.
    }
    return;
  }

  char buf[1024];
  char pthread_name[16] = {0}; //limited by 16B include terminating null byte.
  int r = ceph_pthread_getname(pthread_self(), pthread_name, sizeof(pthread_name));
  (void)r;
#if defined(__sun)
  char message[SIG2STR_MAX];
  sig2str(signum,message);
  snprintf(buf, sizeof(buf), "*** Caught signal (%s) **\n "
	    "in thread %llx thread_name:%s\n", message, (unsigned long long)pthread_self(),
	    pthread_name);
#else
  snprintf(buf, sizeof(buf), "*** Caught signal (%s) **\n "
	    "in thread %llx thread_name:%s\n", sig_str(signum), (unsigned long long)pthread_self(),
	    pthread_name);
#endif
  dout_emergency(buf);
  pidfile_remove();

  // TODO: don't use an ostringstream here. It could call malloc(), which we
  // don't want inside a signal handler.
  // Also fix the backtrace code not to allocate memory.
  ClibBackTrace bt(1);
  ostringstream oss;
  bt.print(oss);
  dout_emergency(oss.str());

  char crash_base[PATH_MAX] = { 0 };
  
  generate_crash_dump(crash_base, bt);

  // avoid recursion back into logging code if that is where
  // we got the SEGV.
  if (g_ceph_context &&
      g_ceph_context->_log &&
      !g_ceph_context->_log->is_inside_log_lock()) {
    // dump to log.  this uses the heap extensively, but we're better
    // off trying than not.
    derr << buf << std::endl;
    bt.print(*_dout);
    *_dout << " NOTE: a copy of the executable, or `objdump -rdS <executable>` "
	   << "is needed to interpret this.\n"
	   << dendl;

    g_ceph_context->_log->dump_recent();

    if (crash_base[0]) {
      char fn[PATH_MAX*2];
      snprintf(fn, sizeof(fn)-1, "%s/log", crash_base);
      g_ceph_context->_log->set_log_file(fn);
      g_ceph_context->_log->reopen_log_file();
      g_ceph_context->_log->dump_recent();
    }
  }

  if (g_eio) {
    // if this was an EIO crash, we don't need to trigger a core dump,
    // since the problem is hardware, or some layer beneath us.
    _exit(EIO);
  } else {
    reraise_fatal(signum);
  }
}

void install_standard_sighandlers(void)
{
  install_sighandler(SIGSEGV, handle_oneshot_fatal_signal, SA_NODEFER);
  install_sighandler(SIGABRT, handle_oneshot_fatal_signal, SA_NODEFER);
  install_sighandler(SIGBUS, handle_oneshot_fatal_signal, SA_NODEFER);
  install_sighandler(SIGILL, handle_oneshot_fatal_signal, SA_NODEFER);
  install_sighandler(SIGFPE, handle_oneshot_fatal_signal, SA_NODEFER);
  install_sighandler(SIGXCPU, handle_oneshot_fatal_signal, SA_NODEFER);
  install_sighandler(SIGXFSZ, handle_oneshot_fatal_signal, SA_NODEFER);
  install_sighandler(SIGSYS, handle_oneshot_fatal_signal, SA_NODEFER);
}



/// --- safe handler ---

#include "common/Thread.h"
#include <errno.h>

#ifdef __APPLE__
#include <libproc.h>

string get_name_by_pid(pid_t pid)
{
  char buf[PROC_PIDPATHINFO_MAXSIZE];
  int ret = proc_pidpath(pid, buf, sizeof(buf));
  if (ret == 0) {
    derr << "Fail to proc_pidpath(" << pid << ")"
	 << " error = " << cpp_strerror(ret)
	 << dendl;
    return "<unknown>";
  }
  return string(buf, ret);
}
#else
string get_name_by_pid(pid_t pid)
{
  // If the PID is 0, its means the sender is the Kernel itself
  if (pid == 0) {
    return "Kernel";
  }
  char proc_pid_path[PATH_MAX] = {0};
  snprintf(proc_pid_path, PATH_MAX, PROCPREFIX "/proc/%d/cmdline", pid);
  int fd = open(proc_pid_path, O_RDONLY);

  if (fd < 0) {
    fd = -errno;
    derr << "Fail to open '" << proc_pid_path 
         << "' error = " << cpp_strerror(fd) 
         << dendl;
    return "<unknown>";
  }
  // assuming the cmdline length does not exceed PATH_MAX. if it
  // really does, it's fine to return a truncated version.
  char buf[PATH_MAX] = {0};
  int ret = read(fd, buf, sizeof(buf));
  close(fd);
  if (ret < 0) {
    ret = -errno;
    derr << "Fail to read '" << proc_pid_path
         << "' error = " << cpp_strerror(ret)
         << dendl;
    return "<unknown>";
  }
  std::replace(buf, buf + ret, '\0', ' ');
  return string(buf, ret);
}
#endif
 
/**
 * safe async signal handler / dispatcher
 *
 * This is an async unix signal handler based on the design from
 *
 *  http://evbergen.home.xs4all.nl/unix-signals.html
 *
 * Features:
 *   - no unsafe work is done in the signal handler itself
 *   - callbacks are called from a regular thread
 *   - signals are not lost, unless multiple instances of the same signal
 *     are sent twice in quick succession.
 */
struct SignalHandler : public Thread {
  /// to kick the thread, for shutdown, new handlers, etc.
  int pipefd[2];  // write to [1], read from [0]

  /// to signal shutdown
  bool stop = false;

  /// for an individual signal
  struct safe_handler {

    safe_handler() {
      memset(pipefd, 0, sizeof(pipefd));
      memset(&handler, 0, sizeof(handler));
      memset(&info_t, 0, sizeof(info_t));    
    }

    siginfo_t info_t;
    int pipefd[2];  // write to [1], read from [0]
    signal_handler_t handler;
  };

  /// all handlers
  safe_handler *handlers[32] = {nullptr};

  /// to protect the handlers array
  ceph::mutex lock = ceph::make_mutex("SignalHandler::lock");

  SignalHandler() {
    // create signal pipe
    int r = pipe_cloexec(pipefd, 0);
    ceph_assert(r == 0);
    r = fcntl(pipefd[0], F_SETFL, O_NONBLOCK);
    ceph_assert(r == 0);

    // create thread
    create("signal_handler");
  }

  ~SignalHandler() override {
    shutdown();
  }

  void signal_thread() {
    int r = write(pipefd[1], "\0", 1);
    ceph_assert(r == 1);
  }

  void shutdown() {
    stop = true;
    signal_thread();
    join();
  }

  // thread entry point
  void *entry() override {
    while (!stop) {
      // build fd list
      struct pollfd fds[33];

      lock.lock();
      int num_fds = 0;
      fds[num_fds].fd = pipefd[0];
      fds[num_fds].events = POLLIN | POLLERR;
      fds[num_fds].revents = 0;
      ++num_fds;
      for (unsigned i=0; i<32; i++) {
	if (handlers[i]) {
	  fds[num_fds].fd = handlers[i]->pipefd[0];
	  fds[num_fds].events = POLLIN | POLLERR;
	  fds[num_fds].revents = 0;
	  ++num_fds;
	}
      }
      lock.unlock();

      // wait for data on any of those pipes
      int r = poll(fds, num_fds, -1);
      if (stop)
	break;
      if (r > 0) {
	char v;

	// consume byte from signal socket, if any.
	TEMP_FAILURE_RETRY(read(pipefd[0], &v, 1));

	lock.lock();
	for (unsigned signum=0; signum<32; signum++) {
	  if (handlers[signum]) {
	    r = read(handlers[signum]->pipefd[0], &v, 1);
	    if (r == 1) {
	      siginfo_t * siginfo = &handlers[signum]->info_t;
              ostringstream message;
              message << "received  signal: " << sig_str(signum);
              switch (siginfo->si_code) {
                case SI_USER:
                  message << " from " << get_name_by_pid(siginfo->si_pid);
                  // If PID is undefined, it doesn't have a meaning to be displayed
                  if (siginfo->si_pid) {
                    message << " (PID: " << siginfo->si_pid << ")";
                  } else {
                    message << " ( Could be generated by pthread_kill(), raise(), abort(), alarm() )";
                  }
                  message << " UID: " << siginfo->si_uid;
                  break;
                default:
                  /* As we have a not expected signal, let's report the structure to help debugging */
                  message << ", si_code : " << siginfo->si_code;
                  message << ", si_value (int): " << siginfo->si_value.sival_int;
                  message << ", si_value (ptr): " << siginfo->si_value.sival_ptr;
                  message << ", si_errno: " << siginfo->si_errno;
                  message << ", si_pid : " << siginfo->si_pid;
                  message << ", si_uid : " << siginfo->si_uid;
                  message << ", si_addr" << siginfo->si_addr;
                  message << ", si_status" << siginfo->si_status;
                  break;
              }
              derr << message.str() << dendl;
              handlers[signum]->handler(signum);
	    }
	  }
	}
	lock.unlock();
      } 
    }
    return NULL;
  }

  void queue_signal(int signum) {
    // If this signal handler is registered, the callback must be
    // defined.  We can do this without the lock because we will never
    // have the signal handler defined without the handlers entry also
    // being filled in.
    ceph_assert(handlers[signum]);
    int r = write(handlers[signum]->pipefd[1], " ", 1);
    ceph_assert(r == 1);
  }

  void queue_signal_info(int signum, siginfo_t *siginfo, void * content) {
    // If this signal handler is registered, the callback must be
    // defined.  We can do this without the lock because we will never
    // have the signal handler defined without the handlers entry also
    // being filled in.
    ceph_assert(handlers[signum]);
    memcpy(&handlers[signum]->info_t, siginfo, sizeof(siginfo_t));
    int r = write(handlers[signum]->pipefd[1], " ", 1);
    ceph_assert(r == 1);
  }

  void register_handler(int signum, signal_handler_t handler, bool oneshot);
  void unregister_handler(int signum, signal_handler_t handler);
};

static SignalHandler *g_signal_handler = NULL;

static void handler_signal_hook(int signum, siginfo_t * siginfo, void * content) {
  g_signal_handler->queue_signal_info(signum, siginfo, content);
}

void SignalHandler::register_handler(int signum, signal_handler_t handler, bool oneshot)
{
  int r;

  ceph_assert(signum >= 0 && signum < 32);

  safe_handler *h = new safe_handler;

  r = pipe_cloexec(h->pipefd, 0);
  ceph_assert(r == 0);
  r = fcntl(h->pipefd[0], F_SETFL, O_NONBLOCK);
  ceph_assert(r == 0);

  h->handler = handler;
  lock.lock();
  handlers[signum] = h;
  lock.unlock();

  // signal thread so that it sees our new handler
  signal_thread();
  
  // install our handler
  struct sigaction oldact;
  struct sigaction act;
  memset(&act, 0, sizeof(act));

  act.sa_handler = (signal_handler_t)handler_signal_hook;
  sigfillset(&act.sa_mask);  // mask all signals in the handler
  act.sa_flags = SA_SIGINFO | (oneshot ? SA_RESETHAND : 0);
  int ret = sigaction(signum, &act, &oldact);
  ceph_assert(ret == 0);
}

void SignalHandler::unregister_handler(int signum, signal_handler_t handler)
{
  ceph_assert(signum >= 0 && signum < 32);
  safe_handler *h = handlers[signum];
  ceph_assert(h);
  ceph_assert(h->handler == handler);

  // restore to default
  signal(signum, SIG_DFL);

  // _then_ remove our handlers entry
  lock.lock();
  handlers[signum] = NULL;
  lock.unlock();

  // this will wake up select() so that worker thread sees our handler is gone
  close(h->pipefd[0]);
  close(h->pipefd[1]);
  delete h;
}


// -------

void init_async_signal_handler()
{
  ceph_assert(!g_signal_handler);
  g_signal_handler = new SignalHandler;
}

void shutdown_async_signal_handler()
{
  ceph_assert(g_signal_handler);
  delete g_signal_handler;
  g_signal_handler = NULL;
}

void queue_async_signal(int signum)
{
  ceph_assert(g_signal_handler);
  g_signal_handler->queue_signal(signum);
}

void register_async_signal_handler(int signum, signal_handler_t handler)
{
  ceph_assert(g_signal_handler);
  g_signal_handler->register_handler(signum, handler, false);
}

void register_async_signal_handler_oneshot(int signum, signal_handler_t handler)
{
  ceph_assert(g_signal_handler);
  g_signal_handler->register_handler(signum, handler, true);
}

void unregister_async_signal_handler(int signum, signal_handler_t handler)
{
  ceph_assert(g_signal_handler);
  g_signal_handler->unregister_handler(signum, handler);
}



