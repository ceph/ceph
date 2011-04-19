#include "common/DoutStreambuf.h"
#include "common/code_environment.h"
#include "common/config.h"
#include "common/version.h"
#include "debug.h"

#include <iostream>
#include <sstream>

// Originally, dout was global. Now, there is one for each md_config_t structure.
// These variables are here temporarily to make the transition easier.
std::ostream *_dout = &g_conf._dout;
DoutStreambuf <char> *_doss = g_conf._doss;

/*
 * The dout lock protects calls to dout()
 */
pthread_mutex_t _dout_lock = PTHREAD_MUTEX_INITIALIZER;

int dout_handle_daemonize(md_config_t *conf)
{
  DoutLocker _dout_locker;

  conf->_doss->handle_stderr_closed();
  return conf->_doss->handle_pid_change(&g_conf);
}

void output_ceph_version()
{
  char buf[1024];
  snprintf(buf, sizeof(buf), "ceph version %s.commit: %s. process: %s. "
	    "pid: %d", ceph_version_to_str(), git_version_to_str(),
	    get_process_name_cpp().c_str(), getpid());
  generic_dout(0) << buf << dendl;
}
