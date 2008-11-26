
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
char _dout_file[100] = {0};
char _dout_dir[1000] = {0};
char _dout_symlink_path[1000] = {0};
bool _dout_is_open = false;
bool _dout_need_open = true;

void _dout_open_log()
{
  char fn[80];

  // logging enabled?
  if (!(g_conf.dout_dir && g_conf.file_logs)) {
    _dout_need_open = false;
    return;
  }

  // calculate log dir, filename, etc.
  if (!_dout_file[0]) {
    char hostname[80];
    gethostname(hostname, 79);
    
    if (g_conf.dout_dir[0] == '/') 
      strcpy(_dout_dir, g_conf.dout_dir);
    else {
      getcwd(_dout_dir, 100);
      strcat(_dout_dir, "/");
      strcat(_dout_dir, g_conf.dout_dir);
    }
    sprintf(_dout_file, "%s.%d", hostname, getpid());
  }

  if (_dout && _dout_is_open)
    delete _dout;

  sprintf(fn, "%s/%s", _dout_dir, _dout_file);
  std::ofstream *out = new std::ofstream(fn, ios::trunc|ios::out);
  if (!out->is_open()) {
    std::cerr << "error opening output file " << fn << std::endl;
    delete out;
    _dout = &std::cout;
  } else {
    _dout_need_open = false;
    _dout_is_open = true;
    _dout = out;
    *_dout << g_clock.now() << " --- opened log " << fn << " ---" << std::endl;
  }
}

int _dout_rename_output_file()  // after calling daemon()
{
  if (g_conf.dout_dir && g_conf.file_logs) {
    char oldfn[100];
    char newfn[100];
    char hostname[80];
    gethostname(hostname, 79);
    
    sprintf(oldfn, "%s/%s", _dout_dir, _dout_file);
    sprintf(newfn, "%s/%s.%d", _dout_dir, hostname, getpid());
    dout(0) << "---- renamed log " << oldfn << " -> " << newfn << " ----" << dendl;
    ::rename(oldfn, newfn);
    sprintf(_dout_file, "%s.%d", hostname, getpid());

    if (_dout_symlink_path[0]) {
      // new symlink
      ::unlink(_dout_symlink_path);
      ::symlink(_dout_file, _dout_symlink_path);
    }
  }
  return 0;
}

int _dout_create_courtesy_output_symlink(const char *type, int n)
{
  if (g_conf.dout_dir && g_conf.file_logs) {
    if (_dout_need_open)
      _dout_open_log();

    sprintf(_dout_symlink_path, "%s/%s%d", _dout_dir, type, n);

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

    ::symlink(_dout_file, _dout_symlink_path);
    dout(0) << "---- created symlink " << _dout_symlink_path
	    << " -> " << _dout_file << " ----" << dendl;
  }
  return 0;
}


