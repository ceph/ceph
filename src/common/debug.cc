
#include "include/types.h"
#include "config.h"
#include "debug.h"
#include "Mutex.h"
#include "Clock.h"

#include "ceph_ver.h"

#include <fstream>
#include <iostream>
using namespace std;

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

// debug output
Mutex _dout_lock("_dout_lock", false, false /* no lockdep */);  
ostream *_dout = &std::cout;
ostream *_derr = &std::cerr;
char _dout_dir[1000] = {0};
char _dout_symlink_dir[1000] = {0};
char _dout_file[1000] = {0};
char _dout_path[1000] = {0};
char _dout_symlink_path[1000] = {0};
char *_dout_symlink_target = 0;   // _dout_path or _dout_file
bool _dout_is_open = false;
bool _dout_need_open = true;
std::ofstream _dout_out;

void _dout_open_log()
{
  // logging enabled?
  if (!g_conf.log_dir || g_conf.log_to_stdout) {
    _dout_need_open = false;
    return;
  }

  // calculate log dir, filename, etc.
  if (!_dout_path[0]) {
    if (g_conf.log_dir[0] == '/') 
      strcpy(_dout_dir, g_conf.log_dir);
    else {
      getcwd(_dout_dir, sizeof(_dout_dir));
      strncat(_dout_dir, "/", sizeof(_dout_dir));
      strncat(_dout_dir, g_conf.log_dir, sizeof(_dout_dir));
    }

    if (!g_conf.log_sym_dir)
      g_conf.log_sym_dir = strdup(g_conf.log_dir);

    if (g_conf.log_sym_dir[0] == '/') 
      strcpy(_dout_symlink_dir, g_conf.log_sym_dir);
    else {
      getcwd(_dout_symlink_dir, sizeof(_dout_symlink_dir));
      strncat(_dout_symlink_dir, "/", sizeof(_dout_symlink_dir));
      strncat(_dout_symlink_dir, g_conf.log_sym_dir, sizeof(_dout_symlink_dir));
    }

    // make symlink target absolute or relative?
    if (strcmp(_dout_symlink_dir, _dout_dir) == 0)
      _dout_symlink_target = _dout_file;
    else
      _dout_symlink_target = _dout_path;

    char hostname[80];
    gethostname(hostname, 79);
    snprintf(_dout_path, sizeof(_dout_path), "%s/%s.%d", _dout_dir, hostname, getpid());
    snprintf(_dout_file, sizeof(_dout_file), "%s.%d", hostname, getpid());
  }

  _dout_out.close();
  _dout_out.open(_dout_path, ios::trunc|ios::out);
  if (!_dout_out.is_open()) {
    std::cerr << "error opening output file " << _dout_path << std::endl;
    _dout = &std::cout;
  } else {
    _dout_need_open = false;
    _dout_is_open = true;
    _dout = &_dout_out;
    *_dout << g_clock.now() << " --- opened log " << _dout_path << " ---" << std::endl;
  }
  *_dout << "ceph version " << VERSION << " (" << STRINGIFY(CEPH_GIT_VER) << ")" << std::endl;
}

int _dout_rename_output_file()  // after calling daemon()
{
  if (g_conf.log_dir && !g_conf.log_to_stdout) {
    char oldpath[1000];
    char hostname[80];
    gethostname(hostname, 79);

    strcpy(oldpath, _dout_path);
    snprintf(_dout_path, sizeof(_dout_path), "%s/%s.%d", _dout_dir, hostname, getpid());
    snprintf(_dout_file, sizeof(_dout_file), "%s.%d", hostname, getpid());
    dout(0) << "---- renamed log " << oldpath << " -> " << _dout_path << " ----" << dendl;
    ::rename(oldpath, _dout_path);

    if (_dout_symlink_path[0]) {
      // fix symlink
      ::unlink(_dout_symlink_path);
      ::symlink(_dout_symlink_target, _dout_symlink_path);
    }
  }
  return 0;
}

int _dout_create_courtesy_output_symlink(const char *type, __s64 n)
{
  if (g_conf.log_dir && !g_conf.log_to_stdout) {
    if (_dout_need_open)
      _dout_open_log();

    snprintf(_dout_symlink_path, sizeof(_dout_symlink_path), "%s/%s%lld", _dout_symlink_dir, type, (long long)n);

    // rotate out old symlink
    int n = 0;
    while (1) {
      char fn[200];
      struct stat st;
      snprintf(fn, sizeof(fn), "%s.%lld", _dout_symlink_path, (long long)n);
      if (::stat(fn, &st) != 0)
	break;
      n++;
    }
    while (n >= 0) {
      char a[200], b[200];
      if (n)
	snprintf(a, sizeof(a), "%s.%lld", _dout_symlink_path, (long long)n-1);
      else
	snprintf(a, sizeof(a), "%s", _dout_symlink_path);
      snprintf(b, sizeof(b), "%s.%lld", _dout_symlink_path, (long long)n);
      ::rename(a, b);
      dout(0) << "---- renamed symlink " << a << " -> " << b << " ----" << dendl;
      n--;
    }

    ::symlink(_dout_symlink_target, _dout_symlink_path);
    dout(0) << "---- created symlink " << _dout_symlink_path
	    << " -> " << _dout_symlink_target << " ----" << dendl;
  }
  return 0;
}


