// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef __CEPH_DEBUG_H
#define __CEPH_DEBUG_H

#include "include/assert.h"
#include "Mutex.h"
#include "Clock.h"

#include <ostream>
using std::ostream;

// the streams
extern ostream *_dout;
extern ostream *_derr;

extern Mutex _dout_lock;

extern bool _dout_need_open;
extern bool _dout_is_open;

extern void _dout_open_log();
extern int _dout_rename_output_file();  // after calling daemon()
extern int _dout_create_courtesy_output_symlink(const char *type, int64_t n);
extern int _dout_create_courtesy_output_symlink(const char *name);

static inline void _dout_check_log() {
  _dout_lock.Lock();
  if (_dout_need_open)
    _dout_open_log();
  _dout_lock.Unlock();
}

static inline void _dout_begin_line() {
  _dout_lock.Lock();
  if (_dout_need_open)
    _dout_open_log();
  *_dout << g_clock.now() << " " << std::hex << pthread_self() << std::dec << " ";
}
static void _dout_begin_line_static() {
  _dout_begin_line();
}

static inline void _dout_end_line() {
  _dout_lock.Unlock();
}

struct _dbeginl_t { _dbeginl_t(int) {} };
static const _dbeginl_t dbeginl = 0;
inline ostream& operator<<(ostream& out, _dbeginl_t) {
  _dout_begin_line();
  return out;
}

struct _dbeginlstatic_t { _dbeginlstatic_t(int) {} };
static const _dbeginlstatic_t dbeginlstatic = 0;
inline ostream& operator<<(ostream& out, _dbeginlstatic_t) {
  _dout_begin_line_static();
  return out;
}

// intentionally conflict with endl
class _bad_endl_use_dendl_t { public: _bad_endl_use_dendl_t(int) {} };
static const _bad_endl_use_dendl_t endl = 0;
inline ostream& operator<<(ostream& out, _bad_endl_use_dendl_t) {
  assert(0 && "you are using the wrong endl.. use std::endl or dendl");
  return out;
}


// generic macros
#define generic_dout(x) do { if ((x) <= g_conf.debug) { *_dout << dbeginl << std::hex << pthread_self() << std::dec << " "
#define generic_derr(x) do { if ((x) <= g_conf.debug) { *_derr << dbeginl << std::hex << pthread_self() << std::dec << " "

#define pdout(x,p) do { if ((x) <= (p)) { *_dout << dbeginl

#define debug_DOUT_SUBSYS debug
#define dout_prefix *_dout << dbeginlstatic
#define DOUT_CONDVAR(x) g_conf.debug_ ## x
#define XDOUT_CONDVAR(x) DOUT_CONDVAR(x)
#define DOUT_COND(l) l <= XDOUT_CONDVAR(DOUT_SUBSYS)

#define dout(l) do { if (DOUT_COND(l)) { dout_prefix
#define derr(l) do { if (DOUT_COND(l)) { dout_prefix

#define dendl std::endl; _dout_end_line(); } } while (0)


inline static void hex2str(const char *s, int len, char *buf, int dest_len)
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

inline static void hexdump(string msg, const char *s, int len)
{
  int buf_len = len*4;
  char buf[buf_len];
  hex2str(s, len, buf, buf_len);
  generic_dout(0) << msg << ":\n" << buf << dendl;
}



#endif
