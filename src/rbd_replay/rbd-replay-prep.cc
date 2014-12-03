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
#include <fstream>
#include <boost/thread/thread.hpp>
#include "ios.hpp"

using namespace std;
using namespace rbd_replay;


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

class AnonymizedImage {
public:
  void init(string image_name, int index) {
    assert(m_image_name == "");
    m_image_name = image_name;
    ostringstream oss;
    oss << "image" << index;
    m_anonymized_image_name = oss.str();
  }

  string image_name() const {
    return m_image_name;
  }

  pair<string, string> anonymize(string snap_name) {
    if (snap_name == "") {
      return pair<string, string>(m_anonymized_image_name, "");
    }
    string& anonymized_snap_name(m_snaps[snap_name]);
    if (anonymized_snap_name == "") {
      ostringstream oss;
      oss << "snap" << m_snaps.size();
      anonymized_snap_name = oss.str();
    }
    return pair<string, string>(m_anonymized_image_name, anonymized_snap_name);
  }

private:
  string m_image_name;
  string m_anonymized_image_name;
  map<string, string> m_snaps;
};

static void usage(string prog) {
  cout << "Usage: " << prog << " [ --window <seconds> ] [ --anonymize ] <trace-input> <replay-output>" << endl;
}

__attribute__((noreturn)) static void usage_exit(string prog, string msg) {
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
      m_pending_ios(map<uint64_t, IO::ptr>()),
      m_anonymize(false),
      m_anonymized_images(map<string, AnonymizedImage>()) {
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
      } else if (arg == "--anonymize") {
	m_anonymize = true;
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
    bool first = true;
    while(true) {
      struct bt_ctf_event *evt = bt_ctf_iter_read_event(itr);
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
      uint64_t offset = fields.uint64("offset");
      uint64_t length = fields.uint64("length");
      require_image(ts, thread, imagectx, name, snap_name, readonly);
      action_id_t ionum = next_id();
      IO::ptr io(new ReadIO(ionum, ts, threadID, thread->pending_io(), imagectx, offset, length));
      io->add_dependencies(m_recent_completions);
      thread->issued_io(io, m_threads);
      m_ios.push_back(io);
    } else if (strcmp(event_name, "librbd:open_image_enter") == 0) {
      string name(fields.string("name"));
      string snap_name(fields.string("snap_name"));
      bool readonly = fields.int64("read_only");
      imagectx_id_t imagectx = fields.uint64("imagectx");
      action_id_t ionum = next_id();
      pair<string, string> aname(map_image_snap(name, snap_name));
      IO::ptr io(new OpenImageIO(ionum, ts, threadID, thread->pending_io(), imagectx, aname.first, aname.second, readonly));
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
      thread->issued_io(io, m_threads);
      m_ios.push_back(thread->pending_io());
    } else if (strcmp(event_name, "librbd:close_image_exit") == 0) {
      IO::ptr completionIO(thread->pending_io()->create_completion(ts, threadID));
      m_ios.push_back(completionIO);
      completed(completionIO);
      boost::shared_ptr<CloseImageIO> io(boost::dynamic_pointer_cast<CloseImageIO>(thread->pending_io()));
      assert(io);
      m_open_images.erase(io->imagectx());
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
      IO::ptr io(new WriteIO(ionum, ts, threadID, thread->pending_io(), imagectx, offset, length));
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
      uint64_t offset = fields.uint64("offset");
      uint64_t length = fields.uint64("length");
      require_image(ts, thread, imagectx, name, snap_name, readonly);
      action_id_t ionum = next_id();
      IO::ptr io(new AioReadIO(ionum, ts, threadID, thread->pending_io(), imagectx, offset, length));
      io->add_dependencies(m_recent_completions);
      m_ios.push_back(io);
      thread->issued_io(io, m_threads);
      m_pending_ios[completion] = io;
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
      IO::ptr io(new AioWriteIO(ionum, ts, threadID, thread->pending_io(), imagectx, offset, length));
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

  pair<string, string> map_image_snap(string image_name, string snap_name) {
    if (!m_anonymize) {
      return pair<string, string>(image_name, snap_name);
    }
    AnonymizedImage& m(m_anonymized_images[image_name]);
    if (m.image_name() == "") {
      m.init(image_name, m_anonymized_images.size());
    }
    return m.anonymize(snap_name);
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
    pair<string, string> aname(map_image_snap(name, snap_name));
    IO::ptr io(new OpenImageIO(ionum, ts - 2, thread->id(), thread->pending_io(), imagectx, aname.first, aname.second, readonly));
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

  bool m_anonymize;
  map<string, AnonymizedImage> m_anonymized_images;
};

int main(int argc, char** argv) {
  vector<string> args;
  for (int i = 0; i < argc; i++) {
    args.push_back(string(argv[i]));
  }

  Processor p;
  p.run(args);
}
