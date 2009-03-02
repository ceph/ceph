
#include "include/types.h"
#include "config.h"
#include "debug.h"
#include "Mutex.h"
#include "Clock.h"

#include <fstream>
#include <iostream>
using namespace std;

// debug output
Mutex _dout_lock("_dout_lock", false, false /* no lockdep */);  
ostream *_dout = &std::cout;
ostream *_derr = &std::cerr;
char _dout_dir[1000] = {0};
char _dout_symlink_dir[1000] = {0};
char _dout_path[1000] = {0};
char _dout_symlink_path[1000] = {0};
bool _dout_is_open = false;
bool _dout_need_open = true;

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
      getcwd(_dout_dir, 100);
      strcat(_dout_dir, "/");
      strcat(_dout_dir, g_conf.log_dir);
    }

    if (g_conf.log_sym_dir[0] == '/') 
      strcpy(_dout_symlink_dir, g_conf.log_sym_dir);
    else {
      getcwd(_dout_symlink_dir, 100);
      strcat(_dout_symlink_dir, "/");
      strcat(_dout_symlink_dir, g_conf.log_sym_dir);
    }

    char hostname[80];
    gethostname(hostname, 79);
    sprintf(_dout_path, "%s/%s.%d", _dout_dir, hostname, getpid());
  }

  if (_dout && _dout_is_open)
    delete _dout;

  std::ofstream *out = new std::ofstream(_dout_path, ios::trunc|ios::out);
  if (!out->is_open()) {
    std::cerr << "error opening output file " << _dout_path << std::endl;
    delete out;
    _dout = &std::cout;
  } else {
    _dout_need_open = false;
    _dout_is_open = true;
    _dout = out;
    *_dout << g_clock.now() << " --- opened log " << _dout_path << " ---" << std::endl;
  }
}

int _dout_rename_output_file()  // after calling daemon()
{
  if (g_conf.log_dir && !g_conf.log_to_stdout) {
    char oldpath[1000];
    char hostname[80];
    gethostname(hostname, 79);

    strcpy(oldpath, _dout_path);
    sprintf(_dout_path, "%s/%s.%d", _dout_dir, hostname, getpid());
    dout(0) << "---- renamed log " << oldpath << " -> " << _dout_path << " ----" << dendl;
    ::rename(oldpath, _dout_path);

    if (_dout_symlink_path[0]) {
      // fix symlink
      ::unlink(_dout_symlink_path);
      ::symlink(_dout_path, _dout_symlink_path);
    }
  }
  return 0;
}

int _dout_create_courtesy_output_symlink(const char *type, int n)
{
  if (g_conf.log_dir && !g_conf.log_to_stdout) {
    if (_dout_need_open)
      _dout_open_log();

    sprintf(_dout_symlink_path, "%s/%s%d", _dout_symlink_dir, type, n);

    // rotate out old symlink
    int n = 0;
    while (1) {
      char fn[200];
      struct stat st;
      sprintf(fn, "%s.%d", _dout_symlink_path, n);
      if (::stat(fn, &st) != 0)
	break;
      n++;
    }
    while (n >= 0) {
      char a[200], b[200];
      if (n)
	sprintf(a, "%s.%d", _dout_symlink_path, n-1);
      else
	sprintf(a, "%s", _dout_symlink_path);
      sprintf(b, "%s.%d", _dout_symlink_path, n);
      ::rename(a, b);
      dout(0) << "---- renamed symlink " << a << " -> " << b << " ----" << dendl;
      n--;
    }

    ::symlink(_dout_path, _dout_symlink_path);
    dout(0) << "---- created symlink " << _dout_symlink_path
	    << " -> " << _dout_path << " ----" << dendl;
  }
  return 0;
}


