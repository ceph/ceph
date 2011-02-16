#include "Mutex.h"
#include "ceph_ver.h"
#include "common/DoutStreambuf.h"
#include "config.h"
#include "debug.h"

#include <errno.h>
#include <fstream>
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

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

std::string ceph_version_to_string(void)
{
  std::ostringstream oss;
  oss << "ceph version " << VERSION << " (commit:"
      << STRINGIFY(CEPH_GIT_VER) << ")";
  return oss.str();
}

void dout_output_ceph_version(void)
{
  generic_dout(-1) << ceph_version_to_string() << dendl;
}

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

void hex2str(const char *s, int len, char *buf, int dest_len)
{
  int pos = 0;
  for (int i=0; i<len && pos<dest_len; i++) {
    if (i && !(i%8))
      pos += snprintf(&buf[pos], dest_len-pos, " ");
    if (i && !(i%16))
      pos += snprintf(&buf[pos], dest_len-pos, "\n");
    pos += snprintf(&buf[pos], dest_len-pos, "%.2x ", (int)(unsigned char)s[i]);
  }
}

void hexdump(string msg, const char *s, int len)
{
  int buf_len = len*4;
  char buf[buf_len];
  hex2str(s, len, buf, buf_len);
  generic_dout(0) << msg << ":\n" << buf << dendl;
}
