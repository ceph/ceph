#include "common/DoutStreambuf.h"
#include "common/code_environment.h"
#include "common/config.h"
#include "common/version.h"
#include "debug.h"

#include <iostream>
#include <sstream>

// debug output
std::ostream *_dout = NULL;
DoutStreambuf <char> *_doss = NULL;
bool _dout_need_open = true;

/*
 * The dout lock protects calls to dout()
 */
pthread_mutex_t _dout_lock = PTHREAD_MUTEX_INITIALIZER;

void _dout_open_log()
{
  // should hold _dout_lock here

  if (!_doss) {
    _doss = new DoutStreambuf <char>();
  }
  _doss->read_global_config();
  if (!_dout) {
    _dout = new std::ostream(_doss);
  }

  char buf[1024];
  snprintf(buf, sizeof(buf), "ceph version %s.commit: %s. process: %s. "
      "pid: %d\n",
      ceph_version_to_str(), git_version_to_str(), get_process_name_cpp().c_str(),
      getpid());
  _doss->dout_emergency_to_file_and_syslog(buf);

  _dout_need_open = false;
}

int dout_handle_daemonize()
{
  DoutLocker _dout_locker;

  if (_dout_need_open)
       _dout_open_log();

  assert(_doss);
  _doss->handle_stderr_closed();
  return _doss->handle_pid_change();
}

int dout_create_rank_symlink(int n)
{
  DoutLocker _dout_locker;

  if (_dout_need_open)
    _dout_open_log();

  assert(_doss);
  return _doss->create_rank_symlink(n);
}
