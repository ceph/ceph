
#include "include/types.h"
#include "config.h"
#include "debug.h"
#include "Mutex.h"
#include "Clock.h"

#include "ceph_ver.h"

#include <errno.h>
#include <fstream>
#include <iostream>
using namespace std;

#define _STR(x) #x
#define STRINGIFY(x) _STR(x)

// debug output
Mutex _dout_lock("_dout_lock", false, false /* no lockdep */);  
ostream *_dout = &std::cout;
ostream *_derr = &std::cerr;
char _dout_dir[PATH_MAX] = {0};
char _dout_symlink_dir[PATH_MAX] = {0};
char _dout_file[PATH_MAX] = {0};
char _dout_path[PATH_MAX] = {0};
char _dout_rank_symlink_path[PATH_MAX] = {0};
char _dout_name_symlink_path[PATH_MAX] = {0};
char *_dout_symlink_target = 0;   // _dout_path or _dout_file
bool _dout_is_open = false;
bool _dout_need_open = true;
std::ofstream _dout_out;

static void normalize_relative(const char *from, char *to, int tolen)
{
  if (from[0] == '/') 
    strncpy(to, from, tolen);
  else {
    char *c = getcwd(to, tolen);
    assert(c);
    strncat(to, "/", tolen);
    strncat(to, from, tolen);
  }
}

static void build_log_paths()
{
  if (g_conf.log_file && g_conf.log_file[0]) {
    normalize_relative(g_conf.log_file, _dout_path, sizeof(_dout_path));
  } else {
    if (g_conf.log_per_instance) {
      char hostname[80];
      gethostname(hostname, 79);
      snprintf(_dout_file, sizeof(_dout_file), "%s.%d", hostname, getpid());
    } else {
      snprintf(_dout_file, sizeof(_dout_file), "%s.%s.log", g_conf.type, g_conf.id);
    }
    snprintf(_dout_path, sizeof(_dout_path), "%s/%s", _dout_dir, _dout_file);
  }
}

static bool log_to_file()
{
  return (g_conf.log_dir || g_conf.log_file) && !g_conf.log_to_stdout;
}

static int create_symlink(const char *from)
{
  ::unlink(from);
  int r = ::symlink(_dout_symlink_target, from);
  if (r) {
    char buf[80];
    *_dout << "---- " << getpid() << " failed to symlink " << _dout_symlink_target
	   << " from " << from
	   << ": " << strerror_r(errno, buf, sizeof(buf)) << std::endl;
  }
  return r;
}

static void rotate_file(const char *fn, int max)
{
  char a[200], b[200];
  // rotate out old
  int n = 0;
  while (1) {
    struct stat st;
    snprintf(a, sizeof(a), "%s.%lld", fn, (long long)n);
    if (::lstat(a, &st) != 0)
      break;
    n++;
  }
  while (n >= 0) {
    if (n)
      snprintf(a, sizeof(a), "%s.%lld", fn, (long long)n-1);
    else
      snprintf(a, sizeof(a), "%s", fn);
    if (n >= max) {
      ::unlink(a);
      *_dout << "---- " << getpid() << " removed " << a << " ----" << std::endl;
    } else {
      snprintf(b, sizeof(b), "%s.%lld", fn, (long long)n);
      ::rename(a, b);
      *_dout << "---- " << getpid() << " renamed " << a << " -> " << b << " ----" << std::endl;
    }
    n--;
  }
}

static int create_name_symlink()
{
  int r = 0;
  if (log_to_file() && g_conf.log_per_instance) {
    snprintf(_dout_name_symlink_path, sizeof(_dout_name_symlink_path),
	     "%s/%s.%s", _dout_symlink_dir, g_conf.type, g_conf.id);

    rotate_file(_dout_name_symlink_path, g_conf.log_sym_history);
    r = create_symlink(_dout_name_symlink_path);
  }
  return r;
}


void _dout_open_log()
{
  bool need_symlink = false;

  // logging enabled?
  if (!log_to_file()) {
    _dout_need_open = false;
    return;
  }

  // calculate log dir, filename, etc.
  // do this _once_.
  if (!_dout_path[0]) {

    // normalize paths
    normalize_relative(g_conf.log_dir, _dout_dir, sizeof(_dout_dir));
    if (!g_conf.log_sym_dir)
      g_conf.log_sym_dir = strdup(g_conf.log_dir);
    normalize_relative(g_conf.log_sym_dir, _dout_symlink_dir, sizeof(_dout_symlink_dir));

    // make symlink targets absolute or relative?
    if ((g_conf.log_file && g_conf.log_file[0]) ||
	strcmp(_dout_symlink_dir, _dout_dir) == 0)
      _dout_symlink_target = _dout_file;
    else
      _dout_symlink_target = _dout_path;

    build_log_paths();

    need_symlink = true;
  }

  _dout_out.close();
  // only truncate if log_per_instance is set.
  _dout_out.open(_dout_path, g_conf.log_per_instance ? (ios::trunc | ios::out) : (ios::out | ios::app));
  if (!_dout_out.is_open()) {
    std::cerr << "error opening output file " << _dout_path << std::endl;
    _dout = &std::cout;
  } else {
    _dout_need_open = false;
    _dout_is_open = true;
    _dout = &_dout_out;
    *_dout << g_clock.now() << " --- " << getpid()
	   << (g_conf.log_per_instance ? " created new log " : " appending to log ")
	   << _dout_path << " ---" << std::endl;
  }
  *_dout << "ceph version " << VERSION << " (" << STRINGIFY(CEPH_GIT_VER) << ")" << std::endl;

  if (need_symlink)
    create_name_symlink();
}

int dout_rename_output_file()  // after calling daemon()
{
  Mutex::Locker l(_dout_lock);
  if (log_to_file() && g_conf.log_per_instance) {
    char oldpath[PATH_MAX];
    char hostname[80];
    gethostname(hostname, 79);

    strcpy(oldpath, _dout_path);
    
    build_log_paths();

    *_dout << "---- " << getpid() << " renamed log " << oldpath << " -> " << _dout_path << " ----" << std::endl;
    ::rename(oldpath, _dout_path);

    // $type.$id symlink
    if (g_conf.log_per_instance && _dout_name_symlink_path[0])
      create_symlink(_dout_name_symlink_path);
    if (_dout_rank_symlink_path[0])
      create_symlink(_dout_rank_symlink_path);
  }
  return 0;
}

int dout_create_rank_symlink(int64_t n)
{
  Mutex::Locker l(_dout_lock);
  int r = 0;
  if (log_to_file() && !(g_conf.log_file && g_conf.log_file[0])) {
    if (_dout_need_open)
      _dout_open_log();

    snprintf(_dout_rank_symlink_path, sizeof(_dout_rank_symlink_path),
	     "%s/%s%lld", _dout_symlink_dir, g_conf.type, (long long)n);
    r = create_symlink(_dout_rank_symlink_path);
  }
  return r;
}



