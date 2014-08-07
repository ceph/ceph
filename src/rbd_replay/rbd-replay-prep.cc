// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

// This code assumes that IO IDs and timestamps are related monotonically.
// In other words, (a.id < b.id) == (a.timestamp < b.timestamp) for all IOs a and b.

#include <babeltrace/babeltrace.h>
#include <babeltrace/ctf/events.h>
#include <babeltrace/ctf/iterator.h>
#include <cstdlib>
#include <string>
#include <assert.h>
#include <iostream>
#include <fstream>
#include <boost/thread/thread.hpp>
#include "actions.hpp"
#include "Ser.hpp"

using namespace std;
using namespace rbd_replay;

// Allows us to easily expose all the functions to make debugging easier.
#define STATIC static

struct extent {
  extent() : offset(0), length(0) {
  }
  extent(uint64_t offset, uint64_t length) : offset(offset), length(length) {
  }
  uint64_t offset;
  uint64_t length;
};

class IO;

typedef set<boost::shared_ptr<IO> > io_set_t;

typedef map<action_id_t, boost::shared_ptr<IO> > io_map_t;

STATIC void batch_unreachable_from(const io_set_t& deps, const io_set_t& base, io_set_t* unreachable);

class IO : public boost::enable_shared_from_this<IO> {
public:
  typedef boost::shared_ptr<IO> ptr;

  typedef boost::weak_ptr<IO> weak_ptr;

  IO(action_id_t ionum,
     uint64_t start_time,
     thread_id_t thread_id,
     ptr prev)
    : m_ionum(ionum),
      m_start_time(start_time),
      m_dependencies(io_set_t()),
      m_completion(weak_ptr()),
      m_num_successors(0),
      m_thread_id(thread_id),
      m_prev(prev) {
  }

  virtual ~IO() {
  }

  uint64_t start_time() const {
    return m_start_time;
  }

  io_set_t& dependencies() {
    return m_dependencies;
  }

  const io_set_t& dependencies() const {
    return m_dependencies;
  }

  void add_dependencies(const io_set_t& deps) {
    io_set_t base(m_dependencies);
    for (io_set_t::const_iterator itr = deps.begin(); itr != deps.end(); ++itr) {
      ptr dep(*itr);
      for (io_set_t::const_iterator itr2 = dep->m_dependencies.begin(); itr2 != dep->m_dependencies.end(); ++itr2) {
	base.insert(*itr2);
      }
    }
    batch_unreachable_from(deps, base, &m_dependencies);
  }

  uint64_t num_completion_successors() const {
    ptr c(m_completion.lock());
    return c ? c->m_num_successors : 0;
  }

  void write_to(Ser& out, io_type iotype) const;

  virtual void write_to(Ser& out) const = 0;

  virtual bool is_completion() const {
    return false;
  }

  void set_ionum(action_id_t ionum) {
    m_ionum = ionum;
  }

  action_id_t ionum() const {
    return m_ionum;
  }

  ptr prev() const {
    return m_prev;
  }

  void set_num_successors(uint32_t n) {
    m_num_successors = n;
  }

  uint32_t num_successors() const {
    return m_num_successors;
  }

  void write_debug_base(ostream& out, string iotype);

  virtual void write_debug(ostream& out) = 0;

  // The result must be stored somewhere, or else m_completion will expire
  ptr create_completion(uint64_t start_time, thread_id_t thread_id);

private:
  action_id_t m_ionum;
  uint64_t m_start_time;
  io_set_t m_dependencies;
  boost::weak_ptr<IO> m_completion;
  uint32_t m_num_successors;
  thread_id_t m_thread_id;
  ptr m_prev;
};

ostream& operator<<(ostream& out, IO::ptr io) {
  io->write_debug(out);
  return out;
}

class Thread {
public:
  typedef boost::shared_ptr<Thread> ptr;

  Thread(thread_id_t id,
	 uint64_t window)
    : m_id(id),
      m_window(window),
      m_pending_io(IO::ptr()),
      m_latest_io(IO::ptr()),
      m_max_ts(0) {
  }

  void insert_ts(uint64_t ts) {
    if (m_max_ts == 0 || ts > m_max_ts) {
      m_max_ts = ts;
    }
  }

  uint64_t max_ts() const {
    return m_max_ts;
  }

  void issued_io(IO::ptr io, const map<thread_id_t, ptr>& threads) {
    assert(io);
    io_set_t latest_ios;
    for (map<thread_id_t, ptr>::const_iterator itr = threads.begin(), end = threads.end(); itr != end; ++itr) {
      assertf(itr->second, "id = %ld", itr->first);
      ptr thread(itr->second);
      if (thread->m_latest_io) {
	if (thread->m_latest_io->start_time() + m_window > io->start_time()) {
	  latest_ios.insert(thread->m_latest_io);
	}
      }
    }
    io->add_dependencies(latest_ios);
    m_latest_io = io;
    m_pending_io = io;
  }

  thread_id_t id() const {
    return m_id;
  }

  IO::ptr pending_io() {
    return m_pending_io;
  }

private:
  thread_id_t m_id;
  uint64_t m_window;
  IO::ptr m_pending_io;
  IO::ptr m_latest_io;
  uint64_t m_max_ts;
};

void IO::write_debug_base(ostream& out, string type) {
  out << m_ionum << ": " << m_start_time / 1000000.0 << ": " << type << ", thread = " << m_thread_id << ", deps = {";
  bool first = true;
  for (io_set_t::iterator itr = m_dependencies.begin(), end = m_dependencies.end(); itr != end; ++itr) {
    if (first) {
      first = false;
    } else {
      out << ", ";
    }
    out << (*itr)->m_ionum << ": " << m_start_time - (*itr)->m_start_time;
  }
  out << "}, num_successors = " << m_num_successors << ", numCompletionSuccessors = " << num_completion_successors();
}

class StartThreadIO : public IO {
public:
  StartThreadIO(action_id_t ionum,
		uint64_t start_time,
		thread_id_t thread_id)
    : IO(ionum, start_time, thread_id, IO::ptr()) {
  }

  void write_to(Ser& out) const {
    IO::write_to(out, IO_START_THREAD);
  }

  void write_debug(ostream& out) {
    write_debug_base(out, "start thread");
  }
};

class StopThreadIO : public IO {
public:
  StopThreadIO(action_id_t ionum,
	       uint64_t start_time,
	       thread_id_t thread_id)
    : IO(ionum, start_time, thread_id, IO::ptr()) {
  }

  void write_to(Ser& out) const {
    IO::write_to(out, IO_STOP_THREAD);
  }

  void write_debug(ostream& out) {
    write_debug_base(out, "stop thread");
  }
};

class ReadIO : public IO {
public:
  ReadIO(action_id_t ionum,
	 uint64_t start_time,
	 thread_id_t thread_id,
	 IO::ptr prev,
	 imagectx_id_t imagectx)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_extents(vector<extent>()) {
  }

  void write_to(Ser& out) const {
    IO::write_to(out, IO_READ);
    // TODO: figure out how to handle empty IO, i.e. reads/writes with no extents.
    // These happen if the trace cuts off mid-IO.  We should just toss it, but it
    // might mess up the dependency graph.
    assertf(m_extents.size() == 1, "m_extents.size() = %d", m_extents.size());
    out.write_uint64_t(m_imagectx);
    out.write_uint64_t(m_extents[0].offset);
    out.write_uint64_t(m_extents[0].length);
  }

  void add_extent(const extent& e) {
    m_extents.push_back(e);
  }

  void write_debug(ostream& out) {
    write_debug_base(out, "read");
    out << ", imagectx=" << m_imagectx << ", extents=[";
    for (int i = 0, n = m_extents.size(); i < n; i++) {
      if (i > 0) {
	out << ", ";
      }
      out << m_extents[i].offset << "+" << m_extents[i].length;
    }
    out << "]";
  }

private:
  imagectx_id_t m_imagectx;
  vector<extent> m_extents;
};

class WriteIO : public IO {
public:
  WriteIO(action_id_t ionum,
	  uint64_t start_time,
	  thread_id_t thread_id,
	  IO::ptr prev,
	  imagectx_id_t imagectx,
	  const vector<extent>& extents)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_extents(extents) {
  }

  void write_to(Ser& out) const {
    IO::write_to(out, IO_WRITE);
    assertf(m_extents.size() == 1, "m_extents.size() = %d", m_extents.size());
    out.write_uint64_t(m_imagectx);
    out.write_uint64_t(m_extents[0].offset);
    out.write_uint64_t(m_extents[0].length);
  }

  void write_debug(ostream& out) {
    write_debug_base(out, "write");
    out << ", imagectx=" << m_imagectx << ", extents=[";
    for (int i = 0, n = m_extents.size(); i < n; i++) {
      if (i > 0) {
	out << ", ";
      }
      out << m_extents[i].offset << "+" << m_extents[i].length;
    }
    out << "]";
  }

private:
  imagectx_id_t m_imagectx;
  vector<extent> m_extents;
};

class AioReadIO : public IO {
public:
  AioReadIO(action_id_t ionum,
	    uint64_t start_time,
	    thread_id_t thread_id,
	    IO::ptr prev,
	    imagectx_id_t imagectx)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_extents(vector<extent>()) {
  }

  void write_to(Ser& out) const {
    IO::write_to(out, IO_ASYNC_READ);
    assertf(m_extents.size() == 1, "m_extents.size() = %d", m_extents.size());
    out.write_uint64_t(m_imagectx);
    out.write_uint64_t(m_extents[0].offset);
    out.write_uint64_t(m_extents[0].length);
  }

  void add_extent(const extent& e) {
    m_extents.push_back(e);
  }


  void write_debug(ostream& out) {
    write_debug_base(out, "aio read");
    out << ", imagectx=" << m_imagectx << ", extents=[";
    for (int i = 0, n = m_extents.size(); i < n; i++) {
      if (i > 0) {
	out << ", ";
      }
      out << m_extents[i].offset << "+" << m_extents[i].length;
    }
    out << "]";
  }
private:
  imagectx_id_t m_imagectx;
  vector<extent> m_extents;
};

class AioWriteIO : public IO {
public:
  AioWriteIO(action_id_t ionum,
	     uint64_t start_time,
	     thread_id_t thread_id,
	     IO::ptr prev,
	     imagectx_id_t imagectx,
	     const vector<extent>& extents)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_extents(extents) {
  }

  void write_to(Ser& out) const {
    IO::write_to(out, IO_ASYNC_WRITE);
    assertf(m_extents.size() == 1, "m_extents.size() = %d", m_extents.size());
    out.write_uint64_t(m_imagectx);
    out.write_uint64_t(m_extents[0].offset);
    out.write_uint64_t(m_extents[0].length);
  }

  void write_debug(ostream& out) {
    write_debug_base(out, "aio write");
    out << ", imagectx=" << m_imagectx << ", extents=[";
    for (int i = 0, n = m_extents.size(); i < n; i++) {
      if (i > 0) {
	out << ", ";
      }
      out << m_extents[i].offset << "+" << m_extents[i].length;
    }
    out << "]";
  }

private:
  imagectx_id_t m_imagectx;
  vector<extent> m_extents;
};

class OpenImageIO : public IO {
public:
  OpenImageIO(action_id_t ionum,
	      uint64_t start_time,
	      thread_id_t thread_id,
	      IO::ptr prev,
	      imagectx_id_t imagectx,
	      const string& name,
	      const string& snap_name,
	      bool readonly)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx),
      m_name(name),
      m_snap_name(snap_name),
      m_readonly(readonly) {
  }

  void write_to(Ser& out) const {
    IO::write_to(out, IO_OPEN_IMAGE);
    out.write_uint64_t(m_imagectx);
    out.write_string(m_name);
    out.write_string(m_snap_name);
    out.write_bool(m_readonly);
  }

  imagectx_id_t imagectx() const {
    return m_imagectx;
  }

  void write_debug(ostream& out) {
    write_debug_base(out, "open image");
    out << ", imagectx=" << m_imagectx << ", name='" << m_name << "', snap_name='" << m_snap_name << "', readonly=" << m_readonly;
  }

private:
  imagectx_id_t m_imagectx;
  string m_name;
  string m_snap_name;
  bool m_readonly;
};

class CloseImageIO : public IO {
public:
  CloseImageIO(action_id_t ionum,
	       uint64_t start_time,
	       thread_id_t thread_id,
	       IO::ptr prev,
	       imagectx_id_t imagectx)
    : IO(ionum, start_time, thread_id, prev),
      m_imagectx(imagectx) {
  }

  void write_to(Ser& out) const {
    IO::write_to(out, IO_CLOSE_IMAGE);
    out.write_uint64_t(m_imagectx);
  }

  imagectx_id_t imagectx() const {
    return m_imagectx;
  }

  void write_debug(ostream& out) {
    write_debug_base(out, "close image");
    out << ", imagectx=" << m_imagectx;
  }

private:
  imagectx_id_t m_imagectx;
};

class CompletionIO : public IO {
public:
  CompletionIO(action_id_t ionum,
	       uint64_t start_time,
	       thread_id_t thread_id)
    : IO(ionum, start_time, thread_id, IO::ptr()) {
  }

  void write_to(Ser& out) const {
  }

  bool is_completion() const {
    return true;
  }

  void write_debug(ostream& out) {
    write_debug_base(out, "completion");
  }
};

STATIC bool compare_io_ptrs_by_start_time(IO::ptr p1, IO::ptr p2) {
  return p1->start_time() < p2->start_time();
}

void IO::write_to(Ser& out, io_type iotype) const {
  out.write_uint8_t(iotype);
  out.write_uint32_t(m_ionum);
  out.write_uint64_t(m_thread_id);
  out.write_uint32_t(m_num_successors);
  out.write_uint32_t(num_completion_successors());
  out.write_uint32_t(m_dependencies.size());
  vector<IO::ptr> deps;
  for (io_set_t::const_iterator itr = m_dependencies.begin(), end = m_dependencies.end(); itr != end; ++itr) {
    deps.push_back(*itr);
  }
  sort(deps.begin(), deps.end(), compare_io_ptrs_by_start_time);
  for (vector<IO::ptr>::const_iterator itr = deps.begin(), end = deps.end(); itr != end; ++itr) {
    out.write_uint32_t((*itr)->m_ionum);
    out.write_uint64_t(m_start_time - (*itr)->m_start_time);
  }
}

IO::ptr IO::create_completion(uint64_t start_time, thread_id_t thread_id) {
  assert(!m_completion.lock());
  IO::ptr completion(new CompletionIO(m_ionum + 1, start_time, thread_id));
  m_completion = completion;
  completion->m_dependencies.insert(shared_from_this());
  return completion;
}

STATIC uint64_t min_time(const map<action_id_t, IO::ptr>& s) {
  if (s.empty()) {
    return 0;
  }
  return s.begin()->second->start_time();
}

STATIC uint64_t max_time(const map<action_id_t, IO::ptr>& s) {
  if (s.empty()) {
    return 0;
  }
  map<action_id_t, IO::ptr>::const_iterator itr(s.end());
  --itr;
  return itr->second->start_time();
}

// TODO: Add unit tests
// Anything in 'deps' which is not reachable from 'base' is added to 'unreachable'
STATIC void batch_unreachable_from(const io_set_t& deps, const io_set_t& base, io_set_t* unreachable) {
  if (deps.empty()) {
    return;
  }

  map<action_id_t, IO::ptr> searching_for;
  for (io_set_t::const_iterator itr = deps.begin(); itr != deps.end(); ++itr) {
    searching_for[(*itr)->ionum()] = *itr;
  }

  map<action_id_t, IO::ptr> boundary;
  for (io_set_t::const_iterator itr = base.begin(); itr != base.end(); ++itr) {
    boundary[(*itr)->ionum()] = *itr;
  }

  // The boundary horizon is the maximum timestamp of IOs in the boundary.
  // This monotonically decreases, because dependencies (which are added to the set)
  // have earlier timestamp than the dependent IOs (which were just removed from the set).
  uint64_t boundary_horizon = max_time(boundary);

  for (io_map_t::iterator itr = searching_for.begin(); itr != searching_for.end(); ) {
    if (boundary_horizon >= itr->second->start_time()) {
      break;
    }
    unreachable->insert(itr->second);
    searching_for.erase(itr++);
  }
  if (searching_for.empty()) {
    return;
  }

  // The searching horizon is the minimum timestamp of IOs in the searching set.
  // This monotonically increases, because elements are only removed from the set.
  uint64_t searching_horizon = min_time(searching_for);

  while (!boundary.empty()) {
    // Take an IO from the end, which has the highest timestamp.
    // This reduces the boundary horizon as early as possible,
    // which means we can short cut as soon as possible.
    map<action_id_t, boost::shared_ptr<IO> >::iterator b_itr(boundary.end());
    --b_itr;
    boost::shared_ptr<IO> io(b_itr->second);
    boundary.erase(b_itr);

    for (io_set_t::const_iterator itr = io->dependencies().begin(), end = io->dependencies().end(); itr != end; ++itr) {
      IO::ptr dep(*itr);
      assertf(dep->ionum() < io->ionum(), "IO: %d, dependency: %d", io->ionum(), dep->ionum());
      io_map_t::iterator p = searching_for.find(dep->ionum());
      if (p != searching_for.end()) {
	searching_for.erase(p);
	if (dep->start_time() == searching_horizon) {
	  searching_horizon = min_time(searching_for);
	  if (searching_horizon == 0) {
	    return;
	  }
	}
      }
      boundary[dep->ionum()] = dep;
    }

    boundary_horizon = max_time(boundary);
    if (boundary_horizon != 0) {
      // Anything we're searching for that has a timestamp greater than the
      // boundary horizon will never be found, since the boundary horizon
      // falls monotonically.
      for (io_map_t::iterator itr = searching_for.begin(); itr != searching_for.end(); ) {
	if (boundary_horizon >= itr->second->start_time()) {
	  break;
	}
	unreachable->insert(itr->second);
	searching_for.erase(itr++);
      }
      searching_horizon = min_time(searching_for);
      if (searching_horizon == 0) {
	return;
      }
    }
  }

  // Anything we're still searching for has not been found.
  for (io_map_t::iterator itr = searching_for.begin(), end = searching_for.end(); itr != end; ++itr) {
    unreachable->insert(itr->second);
  }
}

STATIC void usage(string prog) {
  cout << "Usage: " << prog << " [ --window <seconds> ] <trace-input> <replay-output>" << endl;
}

__attribute__((noreturn)) STATIC void usage_exit(string prog, string msg) {
  cerr << msg << endl;
  usage(prog);
  exit(1);
}

class Processor {
public:
  Processor()
    : m_window(1000000000ULL), // 1 billion nanoseconds, i.e., one second
      m_threads(),
      m_io_count(0),
      m_recent_completions(io_set_t()),
      m_open_images(set<imagectx_id_t>()),
      m_ios(vector<IO::ptr>()),
      m_pending_ios(map<uint64_t, IO::ptr>()) {
  }

  void run(vector<string> args) {
    string input_file_name;
    string output_file_name;
    bool got_input = false;
    bool got_output = false;
    for (int i = 1, nargs = args.size(); i < nargs; i++) {
      const string& arg(args[i]);
      if (arg == "--window") {
	if (i == nargs - 1) {
	  usage_exit(args[0], "--window requires an argument");
	}
	m_window = (uint64_t)(1e9 * atof(args[++i].c_str()));
      } else if (arg.find("--window=") == 0) {
	// TODO: test
	printf("Arg: '%s'\n", arg.c_str() + sizeof("--window="));
	m_window = (uint64_t)(1e9 * atof(arg.c_str() + sizeof("--window=")));
      } else if (arg == "-h" || arg == "--help") {
	usage(args[0]);
	exit(0);
      } else if (arg.find("-") == 0) {
	usage_exit(args[0], "Unrecognized argument: " + arg);
      } else if (!got_input) {
	input_file_name = arg;
	got_input = true;
      } else if (!got_output) {
	output_file_name = arg;
	got_output = true;
      } else {
	usage_exit(args[0], "Too many arguments");
      }
    }
    if (!got_output) {
      usage_exit(args[0], "Not enough arguments");
    }

    struct bt_context *ctx = bt_context_create();
    int trace_handle = bt_context_add_trace(ctx,
					    input_file_name.c_str(), // path
					    "ctf", // format
					    NULL, // packet_seek
					    NULL, // stream_list
					    NULL); // metadata
    assertf(trace_handle >= 0, "trace_handle = %d", trace_handle);

    uint64_t start_time_ns = bt_trace_handle_get_timestamp_begin(ctx, trace_handle, BT_CLOCK_REAL);
    assert(start_time_ns != -1ULL);

    struct bt_ctf_iter *itr = bt_ctf_iter_create(ctx,
						 NULL, // begin_pos
						 NULL); // end_pos
    assert(itr);

    struct bt_iter *bt_itr = bt_ctf_get_iter(itr);

    uint64_t trace_start = 0;
    struct bt_ctf_event *evt;
    bool first = true;
    while(true) {
      evt = bt_ctf_iter_read_event(itr);
      if(!evt) {
	break;
      }
      uint64_t ts = bt_ctf_get_timestamp(evt);
      assert(ts != -1ULL);

      if (first) {
	trace_start = ts;
	first = false;
      }
      ts -= trace_start;
      ts += 4; // This is so we have room to insert two events (thread start and open image) at unique timestamps before whatever the first event is.

      process_event(ts, evt);

      int r = bt_iter_next(bt_itr);
      assert(!r);
    }

    bt_ctf_iter_destroy(itr);

    insert_thread_stops();

    for (vector<IO::ptr>::const_iterator itr = m_ios.begin(); itr != m_ios.end(); ++itr) {
      IO::ptr io(*itr);
      IO::ptr prev(io->prev());
      if (prev) {
	// TODO: explain when prev is and isn't a dep
	io_set_t::iterator depitr = io->dependencies().find(prev);
	if (depitr != io->dependencies().end()) {
	  io->dependencies().erase(depitr);
	}
      }
      if (io->is_completion()) {
	io->dependencies().clear();
      }
      for (io_set_t::const_iterator depitr = io->dependencies().begin(); depitr != io->dependencies().end(); ++depitr) {
	IO::ptr dep(*depitr);
	dep->set_num_successors(dep->num_successors() + 1);
      }
    }

    ofstream myfile;
    myfile.open(output_file_name.c_str(), ios::out | ios::binary);
    Ser ser(myfile);
    for (vector<IO::ptr>::iterator itr = m_ios.begin(); itr != m_ios.end(); ++itr) {
      (*itr)->write_to(ser);
    }
    myfile.close();
  }

private:
  void insert_thread_stops() {
    sort(m_ios.begin(), m_ios.end(), compare_io_ptrs_by_start_time);
    for (map<thread_id_t, Thread::ptr>::const_iterator itr = m_threads.begin(), end = m_threads.end(); itr != end; ++itr) {
      Thread::ptr thread(itr->second);
      const action_id_t none = -1;
      action_id_t ionum = none;
      action_id_t maxIONum = 0; // only valid if ionum is none
      for (vector<IO::ptr>::const_iterator itr2 = m_ios.begin(); itr2 != m_ios.end(); ++itr2) {
	IO::ptr io(*itr2);
	if (io->ionum() > maxIONum) {
	  maxIONum = io->ionum();
	}
	if (io->start_time() > thread->max_ts()) {
	  ionum = io->ionum();
	  if (ionum & 1) {
	    ionum++;
	  }
	  break;
	}
      }
      if (ionum == none) {
	if (maxIONum & 1) {
	  maxIONum--;
	}
	ionum = maxIONum + 2;
      }
      for (vector<IO::ptr>::const_iterator itr2 = m_ios.begin(); itr2 != m_ios.end(); ++itr2) {
	IO::ptr io(*itr2);
	if (io->ionum() >= ionum) {
	  io->set_ionum(io->ionum() + 2);
	}
      }
      IO::ptr stop_thread_io(new StopThreadIO(ionum, thread->max_ts(), thread->id()));
      vector<IO::ptr>::iterator insertion_point = lower_bound(m_ios.begin(), m_ios.end(), stop_thread_io, compare_io_ptrs_by_start_time);
      m_ios.insert(insertion_point, stop_thread_io);
    }
  }

  void process_event(uint64_t ts, struct bt_ctf_event *evt) {
    const char *event_name = bt_ctf_event_name(evt);
    const struct bt_definition *scope_context = bt_ctf_get_top_level_scope(evt,
									   BT_STREAM_EVENT_CONTEXT);
    assert(scope_context);
    const struct bt_definition *scope_fields = bt_ctf_get_top_level_scope(evt,
									  BT_EVENT_FIELDS);
    assert(scope_fields);

    const struct bt_definition *pthread_id_field = bt_ctf_get_field(evt, scope_context, "pthread_id");
    assert(pthread_id_field);
    thread_id_t threadID = bt_ctf_get_uint64(pthread_id_field);
    Thread::ptr &thread(m_threads[threadID]);
    if (!thread) {
      thread.reset(new Thread(threadID, m_window));
      IO::ptr io(new StartThreadIO(next_id(), ts - 4, threadID));
      m_ios.push_back(io);
    }
    thread->insert_ts(ts);

    class FieldLookup {
    public:
      FieldLookup(struct bt_ctf_event *evt,
		  const struct bt_definition *scope)
	: m_evt(evt),
	  m_scope(scope) {
      }

      const char* string(const char* name) {
	const struct bt_definition *field = bt_ctf_get_field(m_evt, m_scope, name);
	assertf(field, "field name = '%s'", name);
	const char* c = bt_ctf_get_string(field);
	int err = bt_ctf_field_get_error();
	assertf(c && err == 0, "field name = '%s', err = %d", name, err);
	return c;
      }

      int64_t int64(const char* name) {
	const struct bt_definition *field = bt_ctf_get_field(m_evt, m_scope, name);
	assertf(field, "field name = '%s'", name);
	int64_t val = bt_ctf_get_int64(field);
	int err = bt_ctf_field_get_error();
	assertf(err == 0, "field name = '%s', err = %d", name, err);
	return val;
      }

      uint64_t uint64(const char* name) {
	const struct bt_definition *field = bt_ctf_get_field(m_evt, m_scope, name);
	assertf(field, "field name = '%s'", name);
	uint64_t val = bt_ctf_get_uint64(field);
	int err = bt_ctf_field_get_error();
	assertf(err == 0, "field name = '%s', err = %d", name, err);
	return val;
      }

    private:
      struct bt_ctf_event *m_evt;
      const struct bt_definition *m_scope;
    } fields(evt, scope_fields);

    if (strcmp(event_name, "librbd:read_enter") == 0) {
      string name(fields.string("name"));
      string snap_name(fields.string("snap_name"));
      bool readonly = fields.int64("read_only");
      imagectx_id_t imagectx = fields.uint64("imagectx");
      require_image(ts, thread, imagectx, name, snap_name, readonly);
      action_id_t ionum = next_id();
      IO::ptr io(new ReadIO(ionum, ts, threadID, thread->pending_io(), imagectx));
      io->add_dependencies(m_recent_completions);
      thread->issued_io(io, m_threads);
      m_ios.push_back(io);
    } else if (strcmp(event_name, "librbd:open_image_enter") == 0) {
      string name(fields.string("name"));
      string snap_name(fields.string("snap_name"));
      bool readonly = fields.int64("read_only");
      imagectx_id_t imagectx = fields.uint64("imagectx");
      action_id_t ionum = next_id();
      IO::ptr io(new OpenImageIO(ionum, ts, threadID, thread->pending_io(), imagectx, name, snap_name, readonly));
      io->add_dependencies(m_recent_completions);
      thread->issued_io(io, m_threads);
      m_ios.push_back(io);
    } else if (strcmp(event_name, "librbd:open_image_exit") == 0) {
      IO::ptr completionIO(thread->pending_io()->create_completion(ts, threadID));
      m_ios.push_back(completionIO);
      boost::shared_ptr<OpenImageIO> io(boost::dynamic_pointer_cast<OpenImageIO>(thread->pending_io()));
      assert(io);
      m_open_images.insert(io->imagectx());
    } else if (strcmp(event_name, "librbd:close_image_enter") == 0) {
      imagectx_id_t imagectx = fields.uint64("imagectx");
      action_id_t ionum = next_id();
      IO::ptr io(new CloseImageIO(ionum, ts, threadID, thread->pending_io(), imagectx));
      io->add_dependencies(m_recent_completions);
      thread->issued_io(thread->pending_io(), m_threads);
      m_ios.push_back(thread->pending_io());
    } else if (strcmp(event_name, "librbd:close_image_exit") == 0) {
      IO::ptr completionIO(thread->pending_io()->create_completion(ts, threadID));
      m_ios.push_back(completionIO);
      completed(completionIO);
      boost::shared_ptr<CloseImageIO> io(boost::dynamic_pointer_cast<CloseImageIO>(thread->pending_io()));
      assert(io);
      m_open_images.erase(io->imagectx());
    } else if (strcmp(event_name, "librbd:read_extent") == 0) {
      boost::shared_ptr<ReadIO> io(boost::dynamic_pointer_cast<ReadIO>(thread->pending_io()));
      assert(io);
      uint64_t offset = fields.uint64("offset");
      uint64_t length = fields.uint64("length");
      io->add_extent(extent(offset, length));
    } else if (strcmp(event_name, "librbd:read_exit") == 0) {
      IO::ptr completionIO(thread->pending_io()->create_completion(ts, threadID));
      m_ios.push_back(completionIO);
      completed(completionIO);
    } else if (strcmp(event_name, "librbd:write_enter") == 0) {
      string name(fields.string("name"));
      string snap_name(fields.string("snap_name"));
      bool readonly = fields.int64("read_only");
      uint64_t offset = fields.uint64("off");
      uint64_t length = fields.uint64("buf_len");
      imagectx_id_t imagectx = fields.uint64("imagectx");
      require_image(ts, thread, imagectx, name, snap_name, readonly);
      action_id_t ionum = next_id();
      vector<extent> extents;
      extents.push_back(extent(offset, length));
      IO::ptr io(new WriteIO(ionum, ts, threadID, thread->pending_io(), imagectx, extents));
      io->add_dependencies(m_recent_completions);
      thread->issued_io(io, m_threads);
      m_ios.push_back(io);
    } else if (strcmp(event_name, "librbd:write_exit") == 0) {
      IO::ptr completionIO(thread->pending_io()->create_completion(ts, threadID));
      m_ios.push_back(completionIO);
      completed(completionIO);
    } else if (strcmp(event_name, "librbd:aio_read_enter") == 0) {
      string name(fields.string("name"));
      string snap_name(fields.string("snap_name"));
      bool readonly = fields.int64("read_only");
      uint64_t completion = fields.uint64("completion");
      imagectx_id_t imagectx = fields.uint64("imagectx");
      require_image(ts, thread, imagectx, name, snap_name, readonly);
      action_id_t ionum = next_id();
      IO::ptr io(new AioReadIO(ionum, ts, threadID, thread->pending_io(), imagectx));
      io->add_dependencies(m_recent_completions);
      m_ios.push_back(io);
      thread->issued_io(io, m_threads);
      m_pending_ios[completion] = io;
    } else if (strcmp(event_name, "librbd:aio_read_extent") == 0) {
      boost::shared_ptr<AioReadIO> io(boost::dynamic_pointer_cast<AioReadIO>(thread->pending_io()));
      assert(io);
      uint64_t offset = fields.uint64("offset");
      uint64_t length = fields.uint64("length");
      io->add_extent(extent(offset, length));
    } else if (strcmp(event_name, "librbd:aio_write_enter") == 0) {
      string name(fields.string("name"));
      string snap_name(fields.string("snap_name"));
      bool readonly = fields.int64("read_only");
      uint64_t offset = fields.uint64("off");
      uint64_t length = fields.uint64("len");
      uint64_t completion = fields.uint64("completion");
      imagectx_id_t imagectx = fields.uint64("imagectx");
      require_image(ts, thread, imagectx, name, snap_name, readonly);
      action_id_t ionum = next_id();
      vector<extent> extents;
      extents.push_back(extent(offset, length));
      IO::ptr io(new AioWriteIO(ionum, ts, threadID, thread->pending_io(), imagectx, extents));
      io->add_dependencies(m_recent_completions);
      thread->issued_io(io, m_threads);
      m_ios.push_back(io);
      m_pending_ios[completion] = io;
    } else if (strcmp(event_name, "librbd:aio_complete_enter") == 0) {
      uint64_t completion = fields.uint64("completion");
      map<uint64_t, IO::ptr>::iterator itr = m_pending_ios.find(completion);
      if (itr != m_pending_ios.end()) {
	IO::ptr completedIO(itr->second);
	m_pending_ios.erase(itr);
	IO::ptr completionIO(completedIO->create_completion(ts, threadID));
	m_ios.push_back(completionIO);
	completed(completionIO);
      }
    }

    //        cout << ts << "\t" << event_name << "\tthreadID = " << threadID << endl;
  }

  action_id_t next_id() {
    action_id_t id = m_io_count;
    m_io_count += 2;
    return id;
  }

  void completed(IO::ptr io) {
    uint64_t limit = io->start_time() < m_window ? 0 : io->start_time() - m_window;
    for (io_set_t::iterator itr = m_recent_completions.begin(); itr != m_recent_completions.end(); ) {
      if ((*itr)->start_time() < limit) {
	m_recent_completions.erase(itr++);
      } else {
	++itr;
      }
    }
    m_recent_completions.insert(io);
  }

  void require_image(uint64_t ts,
		     Thread::ptr thread,
		     imagectx_id_t imagectx,
		     const string& name,
		     const string& snap_name,
		     bool readonly) {
    assert(thread);
    if (m_open_images.count(imagectx) > 0) {
      return;
    }
    action_id_t ionum = next_id();
    IO::ptr io(new OpenImageIO(ionum, ts - 2, thread->id(), thread->pending_io(), imagectx, name, snap_name, readonly));
    io->add_dependencies(m_recent_completions);
    thread->issued_io(io, m_threads);
    m_ios.push_back(io);
    IO::ptr completionIO(io->create_completion(ts - 1, thread->id()));
    m_ios.push_back(completionIO);
    completed(completionIO);
    m_open_images.insert(imagectx);
  }

  uint64_t m_window;
  map<thread_id_t, Thread::ptr> m_threads;
  uint32_t m_io_count;
  io_set_t m_recent_completions;
  set<imagectx_id_t> m_open_images;
  vector<IO::ptr> m_ios;

  // keyed by completion
  map<uint64_t, IO::ptr> m_pending_ios;
};

int main(int argc, char** argv) {
  vector<string> args;
  for (int i = 0; i < argc; i++) {
    args.push_back(string(argv[i]));
  }

  Processor p;
  p.run(args);
}
