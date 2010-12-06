// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_DEBUG_H
#define CEPH_DEBUG_H

#include "common/likely.h"
#include "include/assert.h"
#include "Mutex.h"
#include "Clock.h"

#include <iosfwd>
using std::ostream;

// the streams
extern ostream *_dout;

extern Mutex _dout_lock;

extern bool _dout_need_open;
extern bool _dout_is_open;

extern void _dout_open_log();

extern int dout_rename_output_file();  // after calling daemon()
extern int dout_create_rank_symlink(int64_t n);

static inline void _dout_check_log() {
  _dout_lock.Lock();
  if (unlikely(_dout_need_open))
    _dout_open_log();
  _dout_lock.Unlock();
}

static inline void _dout_begin_line() {
  _dout_lock.Lock();
  if (unlikely(_dout_need_open))
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
#define generic_dout(x) do { if ((x) <= g_conf.debug) { *_dout << dbeginl

#define pdout(x,p) do { if ((x) <= (p)) { *_dout << dbeginl

#define debug_DOUT_SUBSYS debug
#define dout_prefix *_dout << dbeginlstatic
#define DOUT_CONDVAR(x) g_conf.debug_ ## x
#define XDOUT_CONDVAR(x) DOUT_CONDVAR(x)
#define DOUT_COND(l) l <= XDOUT_CONDVAR(DOUT_SUBSYS)

#define dout(l) do { if (DOUT_COND(l)) { dout_prefix

#define dendl std::endl; _dout_end_line(); } } while (0)


extern void hex2str(const char *s, int len, char *buf, int dest_len);

extern void hexdump(string msg, const char *s, int len);

#endif
